# stps_7zip File Extraction Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable `stps_7zip` to extract and parse files from 7z archives, matching the functionality of `stps_zip`.

**Architecture:** Implement the stubbed `Sz7z_Extract` function in the LZMA library to decompress files using LZMA. Then modify `SevenZipBind`, `SevenZipInit`, and `SevenZipScan` in `sevenzip_functions.cpp` to use the extraction API and parse CSV content like `stps_zip` does.

**Tech Stack:** C/C++, LZMA SDK, DuckDB extension API

---

### Task 1: Add Folder/Stream Tracking Structures to 7z.h

**Files:**
- Modify: `src/lzma/7z.h:23-54`

**Step 1: Read the current 7z.h structures**

Review the current `CSz7zFileInfo` and `CSz7zArchive` structures.

**Step 2: Add folder and coder structures for extraction**

Add the following structures after line 35 (after `CSz7zFileInfo`):

```c
/* Coder info for decompression */
typedef struct
{
    UInt32 MethodID;        /* Compression method ID */
    Byte Props[16];         /* Coder properties (max 16 bytes) */
    UInt32 PropsSize;       /* Size of properties */
    UInt32 NumInStreams;
    UInt32 NumOutStreams;
} CSz7zCoder;

/* Folder represents a compression unit */
typedef struct
{
    CSz7zCoder *Coders;
    UInt32 NumCoders;
    UInt64 *UnpackSizes;    /* Unpack size per output stream */
    UInt32 NumUnpackStreams;
    UInt32 UnpackCRCDefined;
    UInt32 UnpackCRC;
} CSz7zFolder;
```

**Step 3: Update CSz7zArchive structure**

Add folder tracking fields to `CSz7zArchive` (after line 53):

```c
/* Folders info */
CSz7zFolder *folders;
UInt32 numFolders;

/* File to folder mapping */
UInt32 *fileToFolder;      /* Which folder contains each file */
UInt32 *fileIndexInFolder; /* Index within the folder's output */
```

**Step 4: Commit**

```bash
git add src/lzma/7z.h
git commit -m "feat(7z): add folder/coder structures for extraction support"
```

---

### Task 2: Add Folder Parsing to 7z.c

**Files:**
- Modify: `src/lzma/7z.c:187-206` (Sz7z_Free function)
- Modify: `src/lzma/7z.c:544-586` (ParseHeaderFromBuffer function)

**Step 1: Update Sz7z_Free to clean up new structures**

Modify `Sz7z_Free` to also free folders:

```c
void Sz7z_Free(CSz7zArchive *archive)
{
    if (archive->files)
    {
        for (UInt32 i = 0; i < archive->numFiles; i++)
        {
            if (archive->files[i].Name)
                archive->alloc->Free(archive->alloc, archive->files[i].Name);
        }
        archive->alloc->Free(archive->alloc, archive->files);
        archive->files = NULL;
    }
    if (archive->packSizes)
    {
        archive->alloc->Free(archive->alloc, archive->packSizes);
        archive->packSizes = NULL;
    }
    if (archive->folders)
    {
        for (UInt32 i = 0; i < archive->numFolders; i++)
        {
            if (archive->folders[i].Coders)
                archive->alloc->Free(archive->alloc, archive->folders[i].Coders);
            if (archive->folders[i].UnpackSizes)
                archive->alloc->Free(archive->alloc, archive->folders[i].UnpackSizes);
        }
        archive->alloc->Free(archive->alloc, archive->folders);
        archive->folders = NULL;
    }
    if (archive->fileToFolder)
    {
        archive->alloc->Free(archive->alloc, archive->fileToFolder);
        archive->fileToFolder = NULL;
    }
    if (archive->fileIndexInFolder)
    {
        archive->alloc->Free(archive->alloc, archive->fileIndexInFolder);
        archive->fileIndexInFolder = NULL;
    }
    archive->numFiles = 0;
    archive->numPackStreams = 0;
    archive->numFolders = 0;
}
```

**Step 2: Run build to verify compilation**

Run: `make`
Expected: Build succeeds (no functional changes yet)

**Step 3: Commit**

```bash
git add src/lzma/7z.c
git commit -m "feat(7z): update Sz7z_Free to handle folder structures"
```

---

### Task 3: Implement Sz7z_Extract Function

**Files:**
- Modify: `src/lzma/7z.c:1049-1060`

**Step 1: Replace the stubbed Sz7z_Extract with full implementation**

```c
SRes Sz7z_Extract(CSz7zArchive *archive, UInt32 fileIndex,
                   Byte **outBuf, size_t *outSize)
{
    if (!archive || !outBuf || !outSize)
        return SZ_ERROR_PARAM;

    if (fileIndex >= archive->numFiles)
        return SZ_ERROR_PARAM;

    const CSz7zFileInfo *fileInfo = &archive->files[fileIndex];

    /* Directories have no content */
    if (fileInfo->IsDir)
    {
        *outBuf = NULL;
        *outSize = 0;
        return SZ_OK;
    }

    /* For files with zero size, return empty buffer */
    if (fileInfo->UnpackSize == 0)
    {
        *outBuf = (Byte *)archive->alloc->Alloc(archive->alloc, 1);
        if (!*outBuf)
            return SZ_ERROR_MEM;
        (*outBuf)[0] = 0;
        *outSize = 0;
        return SZ_OK;
    }

    /* Read packed data from archive */
    if (!archive->file)
        return SZ_ERROR_READ;

    /* Calculate offset to packed data for this file */
    /* In a simple 7z archive, packed data starts at dataOffset */
    UInt64 packOffset = archive->dataOffset;
    UInt64 packSize = 0;

    /* Sum pack sizes to get total compressed size */
    for (UInt32 i = 0; i < archive->numPackStreams; i++)
    {
        packSize += archive->packSizes[i];
    }

    /* If no pack info, try to calculate from file positions */
    if (packSize == 0)
    {
        /* Fallback: estimate from archive size minus header */
        packSize = archive->archiveSize - packOffset;
    }

    /* Seek to packed data */
    if (fseek(archive->file, (long)packOffset, SEEK_SET) != 0)
        return SZ_ERROR_READ;

    /* Allocate buffer for packed data */
    Byte *packedData = (Byte *)archive->alloc->Alloc(archive->alloc, (size_t)packSize);
    if (!packedData)
        return SZ_ERROR_MEM;

    /* Read packed data */
    if (fread(packedData, 1, (size_t)packSize, archive->file) != (size_t)packSize)
    {
        archive->alloc->Free(archive->alloc, packedData);
        return SZ_ERROR_READ;
    }

    /* Calculate total unpack size for solid block */
    UInt64 totalUnpackSize = 0;
    for (UInt32 i = 0; i < archive->numFiles; i++)
    {
        if (!archive->files[i].IsDir)
            totalUnpackSize += archive->files[i].UnpackSize;
    }

    /* Allocate output buffer for full decompression */
    Byte *unpackedData = (Byte *)archive->alloc->Alloc(archive->alloc, (size_t)totalUnpackSize);
    if (!unpackedData)
    {
        archive->alloc->Free(archive->alloc, packedData);
        return SZ_ERROR_MEM;
    }

    /* Try LZMA decompression */
    /* LZMA stream format: 5-byte props + compressed data */
    if (packSize < 5)
    {
        archive->alloc->Free(archive->alloc, packedData);
        archive->alloc->Free(archive->alloc, unpackedData);
        return SZ_ERROR_DATA;
    }

    size_t destLen = (size_t)totalUnpackSize;
    size_t srcLen = (size_t)packSize - 5;  /* Exclude 5-byte props header */
    ELzmaStatus status;

    SRes res = LzmaDecode(unpackedData, &destLen,
                          packedData + 5, &srcLen,
                          packedData, 5,  /* Props are first 5 bytes */
                          LZMA_FINISH_END, &status, archive->alloc);

    archive->alloc->Free(archive->alloc, packedData);

    if (res != SZ_OK)
    {
        archive->alloc->Free(archive->alloc, unpackedData);
        return res;
    }

    /* Find offset for requested file in unpacked stream */
    UInt64 fileOffset = 0;
    for (UInt32 i = 0; i < fileIndex; i++)
    {
        if (!archive->files[i].IsDir)
            fileOffset += archive->files[i].UnpackSize;
    }

    /* Allocate and copy file data */
    *outSize = (size_t)fileInfo->UnpackSize;
    *outBuf = (Byte *)archive->alloc->Alloc(archive->alloc, *outSize + 1);
    if (!*outBuf)
    {
        archive->alloc->Free(archive->alloc, unpackedData);
        return SZ_ERROR_MEM;
    }

    memcpy(*outBuf, unpackedData + fileOffset, *outSize);
    (*outBuf)[*outSize] = 0;  /* Null terminate for text files */

    archive->alloc->Free(archive->alloc, unpackedData);

    return SZ_OK;
}
```

**Step 2: Run build to verify compilation**

Run: `make`
Expected: Build succeeds

**Step 3: Commit**

```bash
git add src/lzma/7z.c
git commit -m "feat(7z): implement Sz7z_Extract with LZMA decompression"
```

---

### Task 4: Add Pack Stream Info Parsing

**Files:**
- Modify: `src/lzma/7z.c:551-586` (ParseHeaderFromBuffer)

**Step 1: Update ParseHeaderFromBuffer to parse streams info**

Before parsing FilesInfo, we need to parse MainStreamsInfo to get pack sizes and unpack sizes. Update the function to also scan for and parse streams info:

```c
/* Parse header from buffer (for encoded headers) */
static SRes ParseHeaderFromBuffer(CSz7zArchive *archive, const Byte *buf, size_t bufSize)
{
    const Byte *p = buf;
    const Byte *bufEnd = buf + bufSize;
    Byte type;
    SRes res;

    /* Read header type */
    RINOK(ReadByteFromBuf(&p, bufEnd, &type));

    if (type != k7zIdHeader)
        return SZ_ERROR_ARCHIVE;

    /* Read next type */
    RINOK(ReadByteFromBuf(&p, bufEnd, &type));

    /* Parse MainStreamsInfo if present */
    if (type == k7zIdMainStreamsInfo)
    {
        res = ParseStreamsInfoFromBuf(archive, &p, bufEnd);
        if (res != SZ_OK)
            return res;
        RINOK(ReadByteFromBuf(&p, bufEnd, &type));
    }

    /* Now parse FilesInfo */
    if (type == k7zIdFilesInfo)
    {
        return ReadFilesInfoFromBuf(archive, &p, bufEnd);
    }

    /* Fallback: scan for FilesInfo section */
    const Byte *files_pos = NULL;
    const Byte *scan;

    for (scan = buf; scan < bufEnd; scan++)
    {
        if (*scan == k7zIdFilesInfo)
        {
            files_pos = scan;
            break;
        }
    }

    if (!files_pos)
        return SZ_ERROR_ARCHIVE;

    const Byte *files_ptr = files_pos + 1;
    return ReadFilesInfoFromBuf(archive, &files_ptr, bufEnd);
}
```

**Step 2: Add ParseStreamsInfoFromBuf helper function**

Add this function before `ParseHeaderFromBuffer`:

```c
/* Parse streams info from buffer to get pack sizes and unpack sizes */
static SRes ParseStreamsInfoFromBuf(CSz7zArchive *archive, const Byte **bufPtr, const Byte *bufEnd)
{
    const Byte *p = *bufPtr;
    Byte type;

    RINOK(ReadByteFromBuf(&p, bufEnd, &type));

    while (type != k7zIdEnd && p < bufEnd)
    {
        switch (type)
        {
        case k7zIdPackInfo:
            {
                UInt64 packPos, numPackStreams;
                RINOK(ReadNumberFromBuf(&p, bufEnd, &packPos));
                RINOK(ReadNumberFromBuf(&p, bufEnd, &numPackStreams));

                archive->numPackStreams = (UInt32)numPackStreams;
                archive->dataOffset = 32 + packPos;

                RINOK(ReadByteFromBuf(&p, bufEnd, &type));
                if (type == k7zIdSize)
                {
                    archive->packSizes = (UInt64 *)archive->alloc->Alloc(
                        archive->alloc, sizeof(UInt64) * archive->numPackStreams);
                    if (!archive->packSizes)
                        return SZ_ERROR_MEM;

                    for (UInt32 i = 0; i < archive->numPackStreams; i++)
                    {
                        RINOK(ReadNumberFromBuf(&p, bufEnd, &archive->packSizes[i]));
                    }
                    RINOK(ReadByteFromBuf(&p, bufEnd, &type));
                }
                /* Skip to end of PackInfo */
                while (type != k7zIdEnd && p < bufEnd)
                {
                    RINOK(ReadByteFromBuf(&p, bufEnd, &type));
                }
            }
            break;

        case k7zIdUnpackInfo:
        case k7zIdSubStreamsInfo:
            /* Skip these sections for now - we'll use file UnpackSize */
            {
                int depth = 1;
                while (depth > 0 && p < bufEnd)
                {
                    RINOK(ReadByteFromBuf(&p, bufEnd, &type));
                    if (type == k7zIdEnd)
                        depth--;
                }
            }
            break;

        default:
            /* Unknown section, try to skip */
            break;
        }

        if (p < bufEnd)
            RINOK(ReadByteFromBuf(&p, bufEnd, &type));
    }

    *bufPtr = p;
    return SZ_OK;
}
```

**Step 3: Add forward declaration**

Add before `ParseHeaderFromBuffer`:

```c
static SRes ParseStreamsInfoFromBuf(CSz7zArchive *archive, const Byte **bufPtr, const Byte *bufEnd);
```

**Step 4: Run build**

Run: `make`
Expected: Build succeeds

**Step 5: Commit**

```bash
git add src/lzma/7z.c
git commit -m "feat(7z): add streams info parsing for extraction support"
```

---

### Task 5: Update SevenZipBind to Extract and Parse Files

**Files:**
- Modify: `src/sevenzip_functions.cpp:146-190`

**Step 1: Add CSV parsing helpers (reuse from zip_functions.cpp)**

At the top of the file after includes, add or import the CSV parsing functions:

```cpp
// Helper function to detect delimiter
static char DetectDelimiter(const string &content) {
    size_t semicolon_count = std::count(content.begin(), content.end(), ';');
    size_t comma_count = std::count(content.begin(), content.end(), ',');
    size_t tab_count = std::count(content.begin(), content.end(), '\t');
    size_t pipe_count = std::count(content.begin(), content.end(), '|');

    if (semicolon_count >= comma_count && semicolon_count >= tab_count && semicolon_count >= pipe_count) {
        return ';';
    } else if (tab_count >= comma_count && tab_count >= pipe_count) {
        return '\t';
    } else if (pipe_count >= comma_count) {
        return '|';
    }
    return ',';
}

// Helper function to split a line by delimiter
static vector<string> SplitLine(const string &line, char delimiter) {
    vector<string> result;
    string current;
    bool in_quotes = false;

    for (size_t i = 0; i < line.size(); i++) {
        char c = line[i];
        if (c == '"') {
            in_quotes = !in_quotes;
        } else if (c == delimiter && !in_quotes) {
            result.push_back(current);
            current.clear();
        } else {
            current += c;
        }
    }
    result.push_back(current);

    return result;
}

// Parse CSV content
static void ParseCSVContent(const string &content, vector<string> &column_names,
                            vector<LogicalType> &column_types, vector<vector<Value>> &rows) {
    if (content.empty()) {
        return;
    }

    char delimiter = DetectDelimiter(content);

    // Split into lines
    vector<string> lines;
    size_t start = 0;
    for (size_t i = 0; i < content.size(); i++) {
        if (content[i] == '\n') {
            string line = content.substr(start, i - start);
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            if (!line.empty()) {
                lines.push_back(line);
            }
            start = i + 1;
        }
    }
    if (start < content.size()) {
        string line = content.substr(start);
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (!line.empty()) {
            lines.push_back(line);
        }
    }

    if (lines.empty()) {
        return;
    }

    // Parse header
    column_names = SplitLine(lines[0], delimiter);

    // Initialize all columns as VARCHAR
    for (size_t i = 0; i < column_names.size(); i++) {
        column_types.push_back(LogicalType::VARCHAR);
    }

    // Parse data rows
    for (size_t i = 1; i < lines.size(); i++) {
        vector<string> values = SplitLine(lines[i], delimiter);
        vector<Value> row;

        for (size_t j = 0; j < column_names.size(); j++) {
            if (j < values.size()) {
                row.push_back(Value(values[j]));
            } else {
                row.push_back(Value());
            }
        }
        rows.push_back(row);
    }
}
```

**Step 2: Rewrite SevenZipBind to extract and parse files**

Replace the `SevenZipBind` function (lines 146-190):

```cpp
// Bind function for stps_7zip
static unique_ptr<FunctionData> SevenZipBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<SevenZipBindData>();

    if (input.inputs.empty()) {
        throw BinderException("stps_7zip requires at least one argument: archive_path");
    }
    result->archive_path = input.inputs[0].GetValue<string>();

    // Optional second argument: inner filename
    if (input.inputs.size() >= 2) {
        result->inner_filename = input.inputs[1].GetValue<string>();
        result->auto_detect_file = false;
    }

    // Open the archive
    CSz7zArchive archive;
    Sz7z_Init(&archive, nullptr);

    SRes res = Sz7z_Open(&archive, result->archive_path.c_str());
    if (res != SZ_OK) {
        switch (res) {
            case SZ_ERROR_NO_ARCHIVE:
                throw IOException("Not a valid 7z archive: " + result->archive_path);
            case SZ_ERROR_UNSUPPORTED:
                throw IOException("Unsupported 7z archive format: " + result->archive_path);
            case SZ_ERROR_READ:
                throw IOException("Failed to read 7z file: " + result->archive_path);
            default:
                throw IOException("Failed to open 7z file: " + result->archive_path);
        }
    }

    UInt32 numFiles = Sz7z_GetNumFiles(&archive);
    if (numFiles == 0) {
        Sz7z_Close(&archive);
        throw IOException("7z archive is empty: " + result->archive_path);
    }

    // Find the file to read
    int file_index = -1;

    if (result->auto_detect_file) {
        // Find first non-directory file
        for (UInt32 i = 0; i < numFiles; i++) {
            const CSz7zFileInfo *info = Sz7z_GetFileInfo(&archive, i);
            if (info && !info->IsDir) {
                file_index = i;
                result->inner_filename = info->Name ? info->Name : "";
                break;
            }
        }
    } else {
        // Look for specific file
        for (UInt32 i = 0; i < numFiles; i++) {
            const CSz7zFileInfo *info = Sz7z_GetFileInfo(&archive, i);
            if (info && info->Name && result->inner_filename == info->Name) {
                file_index = i;
                break;
            }
        }
    }

    if (file_index < 0) {
        Sz7z_Close(&archive);
        if (result->auto_detect_file) {
            throw IOException("No files found in 7z archive: " + result->archive_path);
        } else {
            throw IOException("File not found in 7z archive: " + result->inner_filename);
        }
    }

    // Extract file content
    Byte *outBuf = nullptr;
    size_t outSize = 0;

    res = Sz7z_Extract(&archive, file_index, &outBuf, &outSize);
    if (res != SZ_OK) {
        Sz7z_Close(&archive);
        throw IOException("Failed to extract file from 7z archive: " + result->inner_filename);
    }

    string content(reinterpret_cast<char*>(outBuf), outSize);
    free(outBuf);
    Sz7z_Close(&archive);

    // Parse to determine schema
    vector<string> column_names;
    vector<LogicalType> column_types;
    vector<vector<Value>> temp_rows;

    ParseCSVContent(content, column_names, column_types, temp_rows);

    if (column_names.empty()) {
        // If parsing failed, return raw content
        names = {"content"};
        return_types = {LogicalType::VARCHAR};
    } else {
        names = column_names;
        return_types = column_types;
    }

    return std::move(result);
}
```

**Step 3: Run build**

Run: `make`
Expected: Build succeeds

**Step 4: Commit**

```bash
git add src/sevenzip_functions.cpp
git commit -m "feat(7z): update SevenZipBind to extract and parse CSV files"
```

---

### Task 6: Update SevenZipInit to Extract File Content

**Files:**
- Modify: `src/sevenzip_functions.cpp:193-223`

**Step 1: Rewrite SevenZipInit to extract and parse content**

Replace the `SevenZipInit` function:

```cpp
// Init function for stps_7zip
static unique_ptr<GlobalTableFunctionState> SevenZipInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<SevenZipBindData>();
    auto result = make_uniq<SevenZipGlobalState>();

    // Open the archive
    CSz7zArchive archive;
    Sz7z_Init(&archive, nullptr);

    SRes res = Sz7z_Open(&archive, bind_data.archive_path.c_str());
    if (res != SZ_OK) {
        result->error_message = "Failed to open 7z archive: " + bind_data.archive_path;
        return std::move(result);
    }

    // Find the file
    int file_index = -1;
    UInt32 numFiles = Sz7z_GetNumFiles(&archive);

    for (UInt32 i = 0; i < numFiles; i++) {
        const CSz7zFileInfo *info = Sz7z_GetFileInfo(&archive, i);
        if (info && info->Name && bind_data.inner_filename == info->Name) {
            file_index = i;
            break;
        }
    }

    if (file_index < 0) {
        Sz7z_Close(&archive);
        result->error_message = "File not found in 7z archive: " + bind_data.inner_filename;
        return std::move(result);
    }

    // Extract file content
    Byte *outBuf = nullptr;
    size_t outSize = 0;

    res = Sz7z_Extract(&archive, file_index, &outBuf, &outSize);
    if (res != SZ_OK) {
        Sz7z_Close(&archive);
        result->error_message = "Failed to extract file from 7z archive";
        return std::move(result);
    }

    string content(reinterpret_cast<char*>(outBuf), outSize);
    free(outBuf);
    Sz7z_Close(&archive);

    // Parse CSV content
    ParseCSVContent(content, result->column_names, result->column_types, result->rows);
    result->parsed = true;

    return std::move(result);
}
```

**Step 2: Run build**

Run: `make`
Expected: Build succeeds

**Step 3: Commit**

```bash
git add src/sevenzip_functions.cpp
git commit -m "feat(7z): update SevenZipInit to extract and parse content"
```

---

### Task 7: Create Test Data File

**Files:**
- Create: `test/data/test.7z` (7z archive containing a CSV file)

**Step 1: Create a test CSV file**

Create `test/data/test_data.csv`:

```csv
id,name,value
1,Alice,100
2,Bob,200
3,Charlie,300
```

**Step 2: Compress it to 7z format**

Run: `7z a test/data/test.7z test/data/test_data.csv`

Note: If 7z command is not available, you can use any 7z tool or download a pre-made test file. The archive should use LZMA compression (the default).

**Step 3: Verify archive**

Run: `7z l test/data/test.7z`
Expected: Shows test_data.csv file

**Step 4: Remove temporary CSV**

Run: `rm test/data/test_data.csv` (or keep it for reference)

**Step 5: Commit**

```bash
git add test/data/test.7z
git commit -m "test: add 7z test archive with CSV data"
```

---

### Task 8: Write Failing Tests for stps_7zip Extraction

**Files:**
- Modify: `test/sql/sevenzip.test`

**Step 1: Add extraction tests**

Append to `test/sql/sevenzip.test`:

```sql
# Test stps_7zip extraction with auto-detect
query III
SELECT * FROM stps_7zip('test/data/test.7z') ORDER BY id;
----
1	Alice	100
2	Bob	200
3	Charlie	300

# Test stps_7zip extraction with specific filename
query III
SELECT * FROM stps_7zip('test/data/test.7z', 'test_data.csv') ORDER BY id;
----
1	Alice	100
2	Bob	200
3	Charlie	300

# Test column names
query T
SELECT name FROM stps_7zip('test/data/test.7z') WHERE id = '2';
----
Bob
```

**Step 2: Run tests to verify they fail (before implementation is complete)**

Run: `make test`
Expected: New tests fail (extraction not yet working)

**Step 3: Commit**

```bash
git add test/sql/sevenzip.test
git commit -m "test: add stps_7zip extraction tests"
```

---

### Task 9: Run Full Test Suite and Fix Issues

**Files:**
- Potentially modify: `src/lzma/7z.c`, `src/sevenzip_functions.cpp`

**Step 1: Build the extension**

Run: `make`
Expected: Build succeeds

**Step 2: Run the test suite**

Run: `make test`
Expected: All tests pass

**Step 3: If tests fail, debug and fix**

Common issues to check:
- LZMA props parsing (5-byte header)
- File offset calculation in solid blocks
- Memory allocation/deallocation
- Archive data offset calculation

**Step 4: Iterate until all tests pass**

**Step 5: Commit any fixes**

```bash
git add -A
git commit -m "fix: address test failures in 7z extraction"
```

---

### Task 10: Manual Testing

**Step 1: Build extension**

Run: `make`

**Step 2: Test with DuckDB CLI**

Run:
```bash
duckdb -cmd "LOAD 'build/release/extension/stps/stps.duckdb_extension';"
```

Then execute:
```sql
SELECT * FROM stps_7zip('test/data/test.7z');
```

Expected: Returns CSV data as table rows

**Step 3: Test with user's actual file**

```sql
SELECT * FROM stps_7zip('path/to/your.7z', 'some_table.csv');
```

Expected: Returns CSV data

**Step 4: Document any edge cases found**

---

### Task 11: Final Cleanup and Documentation

**Files:**
- Modify: `src/sevenzip_functions.cpp:121-125` (update comment)

**Step 1: Update the comment block**

Change the comment at lines 121-125 from:
```cpp
//===--------------------------------------------------------------------===//
// stps_7zip - Read a file from inside a 7-Zip archive
// Note: Full extraction requires complex stream handling
// This function lists the files for now with a note about extraction support
//===--------------------------------------------------------------------===//
```

To:
```cpp
//===--------------------------------------------------------------------===//
// stps_7zip - Read a file from inside a 7-Zip archive
// Usage: SELECT * FROM stps_7zip('data.7z') -- reads first/only file
// Usage: SELECT * FROM stps_7zip('data.7z', 'data.csv') -- reads specific file
//===--------------------------------------------------------------------===//
```

**Step 2: Remove extraction_not_supported flag from SevenZipGlobalState**

Remove line 142 (`bool extraction_not_supported = false;`) as it's no longer needed.

**Step 3: Run final build and tests**

Run: `make && make test`
Expected: All tests pass

**Step 4: Commit**

```bash
git add -A
git commit -m "docs: update stps_7zip comments and remove unused flag"
```
