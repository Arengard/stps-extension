#include "sevenzip_functions.hpp"
#include "shared/archive_utils.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/file_system.hpp"
#include <cstring>
#include <algorithm>
#include <fstream>

#ifdef HAVE_LIBARCHIVE
#include <archive.h>
#include <archive_entry.h>
#endif

namespace duckdb {
namespace stps {

#ifdef HAVE_LIBARCHIVE

//===--------------------------------------------------------------------===//
// libarchive-based implementation (reliable, supports many formats)
//===--------------------------------------------------------------------===//

// Helper to extract a specific file from archive to memory
static bool ExtractFileFromArchive(const std::string &archive_path,
                                   const std::string &target_filename,
                                   std::string &out_content,
                                   std::string &out_actual_filename,
                                   std::string &error_msg) {
    struct archive *a = archive_read_new();
    struct archive_entry *entry;

    archive_read_support_format_all(a);
    archive_read_support_filter_all(a);

    if (archive_read_open_filename(a, archive_path.c_str(), 10240) != ARCHIVE_OK) {
        error_msg = "Failed to open archive: " + std::string(archive_error_string(a));
        archive_read_free(a);
        return false;
    }

    bool found = false;
    bool auto_detect = target_filename.empty();
    std::string target_lower = target_filename;
    std::transform(target_lower.begin(), target_lower.end(), target_lower.begin(), ::tolower);

    while (archive_read_next_header(a, &entry) == ARCHIVE_OK) {
        const char *name = archive_entry_pathname(entry);
        if (!name) continue;

        // Skip directories
        if (archive_entry_filetype(entry) == AE_IFDIR) {
            archive_read_data_skip(a);
            continue;
        }

        std::string entry_name = name;
        std::string entry_lower = entry_name;
        std::transform(entry_lower.begin(), entry_lower.end(), entry_lower.begin(), ::tolower);

        bool match = auto_detect || (entry_lower == target_lower) ||
                     (entry_lower.find(target_lower) != std::string::npos);

        if (match) {
            // Read file content
            la_int64_t size = archive_entry_size(entry);
            if (size > 0) {
                out_content.resize(static_cast<size_t>(size));
                la_ssize_t read = archive_read_data(a, &out_content[0], static_cast<size_t>(size));
                if (read < 0) {
                    error_msg = "Failed to read file data: " + std::string(archive_error_string(a));
                    archive_read_free(a);
                    return false;
                }
                out_content.resize(static_cast<size_t>(read));
            } else {
                // Size unknown, read in chunks
                out_content.clear();
                char buffer[8192];
                la_ssize_t read;
                while ((read = archive_read_data(a, buffer, sizeof(buffer))) > 0) {
                    out_content.append(buffer, static_cast<size_t>(read));
                }
                if (read < 0) {
                    error_msg = "Failed to read file data: " + std::string(archive_error_string(a));
                    archive_read_free(a);
                    return false;
                }
            }
            out_actual_filename = entry_name;
            found = true;
            break;
        }

        archive_read_data_skip(a);
    }

    archive_read_free(a);

    if (!found) {
        error_msg = target_filename.empty()
            ? "Archive is empty or contains no files"
            : "File '" + target_filename + "' not found in archive";
        return false;
    }

    return true;
}

// Helper to list all files in archive
struct ArchiveFileInfo {
    std::string filename;
    int64_t size;
    bool is_directory;
    int index;
};

static bool ListArchiveFiles(const std::string &archive_path,
                            std::vector<ArchiveFileInfo> &files,
                            std::string &error_msg) {
    struct archive *a = archive_read_new();
    struct archive_entry *entry;

    archive_read_support_format_all(a);
    archive_read_support_filter_all(a);

    if (archive_read_open_filename(a, archive_path.c_str(), 10240) != ARCHIVE_OK) {
        error_msg = "Failed to open archive: " + std::string(archive_error_string(a));
        archive_read_free(a);
        return false;
    }

    int index = 0;
    while (archive_read_next_header(a, &entry) == ARCHIVE_OK) {
        ArchiveFileInfo info;
        const char *name = archive_entry_pathname(entry);
        info.filename = name ? name : "";
        info.size = archive_entry_size(entry);
        info.is_directory = (archive_entry_filetype(entry) == AE_IFDIR);
        info.index = index++;
        files.push_back(info);
        archive_read_data_skip(a);
    }

    archive_read_free(a);
    return true;
}

//===--------------------------------------------------------------------===//
// stps_view_7zip - List files inside a 7-Zip archive
//===--------------------------------------------------------------------===//

struct View7zipBindData : public TableFunctionData {
    string archive_path;
};

struct View7zipGlobalState : public GlobalTableFunctionState {
    std::vector<ArchiveFileInfo> files;
    idx_t current_index = 0;
    std::string error_message;
};

// Bind function for stps_view_7zip
static unique_ptr<FunctionData> View7zipBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<View7zipBindData>();
    
    if (input.inputs.empty()) {
        throw BinderException("stps_view_7zip requires at least one argument: archive_path");
    }
    result->archive_path = input.inputs[0].GetValue<string>();
    
    // Define output columns
    names = {"filename", "uncompressed_size", "is_directory", "index"};
    return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, 
                    LogicalType::BOOLEAN, LogicalType::INTEGER};
    
    return result;
}

// Init function for stps_view_7zip
static unique_ptr<GlobalTableFunctionState> View7zipInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<View7zipBindData>();
    auto result = make_uniq<View7zipGlobalState>();
    
    if (!ListArchiveFiles(bind_data.archive_path, result->files, result->error_message)) {
        // Error will be thrown in scan
    }
    
    return result;
}

// Scan function for stps_view_7zip
static void View7zipScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<View7zipGlobalState>();
    
    if (!state.error_message.empty()) {
        throw IOException(state.error_message);
    }
    
    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;
    
    while (state.current_index < state.files.size() && count < max_count) {
        auto &file = state.files[state.current_index];
        output.SetValue(0, count, Value(file.filename));
        output.SetValue(1, count, Value::BIGINT(file.size));
        output.SetValue(2, count, Value::BOOLEAN(file.is_directory));
        output.SetValue(3, count, Value::INTEGER(file.index));
        state.current_index++;
        count++;
    }
    
    output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// stps_7zip - Read a file from inside a 7-Zip archive
//===--------------------------------------------------------------------===//

struct SevenZipBindData : public TableFunctionData {
    string archive_path;
    string inner_filename;
    bool is_binary_mode = false;

    // Schema from bind phase (use std::vector for ParseCSVContent compatibility)
    std::vector<std::string> column_names;
    std::vector<LogicalType> column_types;

    // Content extracted in bind phase
    std::string extracted_content;
    std::string actual_filename;
};

struct SevenZipGlobalState : public GlobalTableFunctionState {
    std::vector<std::vector<Value>> rows;
    idx_t current_row = 0;
    string error_message;
};

// Bind function for stps_7zip - determines file index and schema only
static unique_ptr<FunctionData> SevenZipBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<SevenZipBindData>();

    if (input.inputs.empty()) {
        throw BinderException("stps_7zip requires at least one argument: archive_path");
    }
    result->archive_path = input.inputs[0].GetValue<string>();

    if (input.inputs.size() >= 2) {
        result->inner_filename = input.inputs[1].GetValue<string>();
    }

    // Extract the file
    std::string error_msg;
    if (!ExtractFileFromArchive(result->archive_path, result->inner_filename,
                                result->extracted_content, result->actual_filename, error_msg)) {
        // Try to list available files for better error message
        std::vector<ArchiveFileInfo> files;
        std::string list_error;
        if (ListArchiveFiles(result->archive_path, files, list_error)) {
            std::string available;
            int count = 0;
            for (auto &f : files) {
                if (!f.is_directory && count < 10) {
                    if (count > 0) available += ", ";
                    available += "'" + f.filename + "'";
                    count++;
                }
            }
            if (count > 0) {
                error_msg += ". Available files: " + available;
                if (files.size() > 10) error_msg += ", ...";
            }
        }
        throw IOException(error_msg);
    }

    // Check if binary
    if (IsBinaryFormat(result->actual_filename) || LooksLikeBinary(result->extracted_content)) {
        result->is_binary_mode = true;
        names = {"extracted_path", "original_filename", "file_size", "file_type", "usage_hint"};
        return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
                       LogicalType::VARCHAR, LogicalType::VARCHAR};
        return result;
    }

    // Parse CSV to determine schema
    std::vector<std::string> col_names;
    std::vector<LogicalType> col_types;
    std::vector<std::vector<Value>> temp_rows;
    ParseCSVContent(result->extracted_content, col_names, col_types, temp_rows);

    if (col_names.empty()) {
        result->column_names = {"content"};
        result->column_types = {LogicalType::VARCHAR};
        names = {"content"};
        return_types = {LogicalType::VARCHAR};
    } else {
        result->column_names = col_names;
        result->column_types = col_types;
        for (const auto &n : col_names) {
            names.push_back(n);
        }
        for (const auto &t : col_types) {
            return_types.push_back(t);
        }
    }

    return result;
}

// Init function for stps_7zip - performs full extraction and parsing
static unique_ptr<GlobalTableFunctionState> SevenZipInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<SevenZipBindData>();
    auto state = make_uniq<SevenZipGlobalState>();

    // Handle binary mode
    if (bind_data.is_binary_mode) {
        string temp_path = ExtractToTemp(bind_data.extracted_content, bind_data.actual_filename, "stps_7zip_");
        string ext = GetFileExtension(bind_data.actual_filename);

        string usage_hint;
        if (ext == "parquet") {
            usage_hint = "SELECT * FROM read_parquet('" + temp_path + "')";
        } else if (ext == "xlsx" || ext == "xls") {
            usage_hint = "INSTALL spatial; LOAD spatial; SELECT * FROM st_read('" + temp_path + "')";
        } else if (ext == "arrow" || ext == "feather") {
            usage_hint = "SELECT * FROM read_parquet('" + temp_path + "')";
        } else {
            usage_hint = "Binary file extracted to: " + temp_path;
        }

        vector<Value> row;
        row.push_back(Value(temp_path));
        row.push_back(Value(bind_data.actual_filename));
        row.push_back(Value::BIGINT(static_cast<int64_t>(bind_data.extracted_content.size())));
        row.push_back(Value(ext.empty() ? "binary" : ext));
        row.push_back(Value(usage_hint));
        state->rows.push_back(std::move(row));

        return state;
    }

    // Parse CSV content
    std::vector<std::string> col_names;
    std::vector<LogicalType> col_types;
    ParseCSVContent(bind_data.extracted_content, col_names, col_types, state->rows);

    // Fallback to raw content if parsing failed
    if (state->rows.empty() && !bind_data.extracted_content.empty()) {
        vector<Value> row;
        row.push_back(Value(bind_data.extracted_content));
        state->rows.push_back(std::move(row));
    }

    return state;
}

// Scan function for stps_7zip - just iterates rows
static void SevenZipScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<SevenZipGlobalState>();
    
    if (!state.error_message.empty()) {
        throw IOException(state.error_message);
    }
    
    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;
    
    while (state.current_row < state.rows.size() && count < max_count) {
        auto &row = state.rows[state.current_row];
        for (idx_t col = 0; col < row.size(); col++) {
            output.SetValue(col, count, row[col]);
        }
        state.current_row++;
        count++;
    }
    
    output.SetCardinality(count);
}

#else // HAVE_LIBARCHIVE not defined - use fallback LZMA implementation

#include "lzma/7z.h"

struct View7zipBindData : public TableFunctionData {
    string archive_path;
};

struct View7zipGlobalState : public GlobalTableFunctionState {
    CSz7zArchive archive;
    idx_t current_file_index = 0;
    idx_t total_files = 0;
    bool initialized = false;
    string error_message;

    ~View7zipGlobalState() {
        if (initialized) {
            Sz7z_Close(&archive);
            initialized = false;
        }
    }
};

// Bind function for stps_view_7zip
static unique_ptr<FunctionData> View7zipBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<View7zipBindData>();

    if (input.inputs.empty()) {
        throw BinderException("stps_view_7zip requires at least one argument: archive_path");
    }
    result->archive_path = input.inputs[0].GetValue<string>();

    // Define output columns
    names = {"filename", "uncompressed_size", "is_directory", "index"};
    return_types = {LogicalType::VARCHAR, LogicalType::BIGINT,
                    LogicalType::BOOLEAN, LogicalType::INTEGER};

    return result;
}

// Init function for stps_view_7zip
static unique_ptr<GlobalTableFunctionState> View7zipInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<View7zipBindData>();
    auto result = make_uniq<View7zipGlobalState>();

    Sz7z_Init(&result->archive, nullptr);

    // Open the 7z file
    SRes res = Sz7z_Open(&result->archive, bind_data.archive_path.c_str());
    if (res != SZ_OK) {
        result->error_message = "Failed to open 7z file (LZMA fallback): " + bind_data.archive_path;
        result->initialized = false;
    } else {
        result->total_files = Sz7z_GetNumFiles(&result->archive);
        result->initialized = true;
    }

    return result;
}

// Scan function for stps_view_7zip
static void View7zipScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<View7zipGlobalState>();

    if (!state.initialized) {
        if (!state.error_message.empty()) {
            throw IOException(state.error_message);
        }
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.current_file_index < state.total_files && count < max_count) {
        const CSz7zFileInfo *file_info = Sz7z_GetFileInfo(&state.archive, state.current_file_index);
        if (!file_info) {
            state.current_file_index++;
            continue;
        }

        UInt64 logical_size = file_info->UnpackSize ? file_info->UnpackSize : file_info->PackedSize;
        string filename = file_info->Name ? file_info->Name : "";
        output.SetValue(0, count, Value(filename));
        output.SetValue(1, count, Value::BIGINT(logical_size));
        output.SetValue(2, count, Value::BOOLEAN(file_info->IsDir));
        output.SetValue(3, count, Value::INTEGER(static_cast<int32_t>(state.current_file_index)));

        state.current_file_index++;
        count++;
    }

    output.SetCardinality(count);
}

struct SevenZipBindData : public TableFunctionData {
    string archive_path;
    string inner_filename;
    int file_index = -1;
    bool is_binary_mode = false;
};

struct SevenZipGlobalState : public GlobalTableFunctionState {
    std::vector<std::string> column_names;
    std::vector<LogicalType> column_types;
    std::vector<std::vector<Value>> rows;
    idx_t current_row = 0;
    string error_message;
};

// Bind function for stps_7zip - determines file index and schema only
static unique_ptr<FunctionData> SevenZipBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
    throw IOException("stps_7zip requires libarchive for reliable 7z support. Please rebuild with libarchive, or use stps_zip() for .zip files.");
}

// Init function for stps_7zip - performs full extraction and parsing
static unique_ptr<GlobalTableFunctionState> SevenZipInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<SevenZipGlobalState>();
}

// Scan function for stps_7zip - just iterates rows
static void SevenZipScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    output.SetCardinality(0);
}

#endif // HAVE_LIBARCHIVE

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//

void Register7zipFunctions(ExtensionLoader &loader) {
    // Register stps_view_7zip table function
    TableFunction view_7zip_func("stps_view_7zip", {LogicalType::VARCHAR}, View7zipScan, View7zipBind, View7zipInit);
    loader.RegisterFunction(view_7zip_func);
    
    // Register stps_7zip table function (with optional second argument)
    TableFunction sevenzip_func1("stps_7zip", {LogicalType::VARCHAR}, SevenZipScan, SevenZipBind, SevenZipInit);
    loader.RegisterFunction(sevenzip_func1);
    
    TableFunction sevenzip_func2("stps_7zip", {LogicalType::VARCHAR, LogicalType::VARCHAR}, SevenZipScan, SevenZipBind, SevenZipInit);
    loader.RegisterFunction(sevenzip_func2);
}

} // namespace stps
} // namespace duckdb
