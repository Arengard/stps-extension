#include "sevenzip_functions.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/file_system.hpp"
#include "../lzma/7z.h"
#include <cstring>
#include <algorithm>
#include <sstream>

namespace duckdb {
namespace stps {

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

//===--------------------------------------------------------------------===//
// stps_view_7zip - List files inside a 7-Zip archive
//===--------------------------------------------------------------------===//

struct View7zipBindData : public TableFunctionData {
    string archive_path;
};

struct View7zipGlobalState : public GlobalTableFunctionState {
    CSz7zArchive archive;
    idx_t current_file_index = 0;
    idx_t total_files = 0;
    bool initialized = false;
    string error_message;
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
    
    return std::move(result);
}

// Init function for stps_view_7zip
static unique_ptr<GlobalTableFunctionState> View7zipInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<View7zipBindData>();
    auto result = make_uniq<View7zipGlobalState>();
    
    Sz7z_Init(&result->archive, nullptr);
    
    // Open the 7z file
    SRes res = Sz7z_Open(&result->archive, bind_data.archive_path.c_str());
    if (res != SZ_OK) {
        switch (res) {
            case SZ_ERROR_NO_ARCHIVE:
                result->error_message = "Not a valid 7z archive: " + bind_data.archive_path;
                break;
            case SZ_ERROR_UNSUPPORTED:
                result->error_message = "Unsupported 7z archive format (may have encoded header): " + bind_data.archive_path;
                break;
            case SZ_ERROR_READ:
                result->error_message = "Failed to read 7z file: " + bind_data.archive_path;
                break;
            default:
                result->error_message = "Failed to open 7z file: " + bind_data.archive_path;
                break;
        }
        result->initialized = false;
    } else {
        result->total_files = Sz7z_GetNumFiles(&result->archive);
        result->initialized = true;
    }
    
    return std::move(result);
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
        
        string filename = file_info->Name ? file_info->Name : "";
        output.SetValue(0, count, Value(filename));
        output.SetValue(1, count, Value::BIGINT(file_info->UnpackSize));
        output.SetValue(2, count, Value::BOOLEAN(file_info->IsDir));
        output.SetValue(3, count, Value::INTEGER(static_cast<int32_t>(state.current_file_index)));
        
        state.current_file_index++;
        count++;
    }
    
    output.SetCardinality(count);
    
    // Clean up when done
    if (state.current_file_index >= state.total_files && state.initialized) {
        Sz7z_Close(&state.archive);
        state.initialized = false;
    }
}

//===--------------------------------------------------------------------===//
// stps_7zip - Read a file from inside a 7-Zip archive
// Note: Full extraction requires complex stream handling
// This function lists the files for now with a note about extraction support
//===--------------------------------------------------------------------===//

struct SevenZipBindData : public TableFunctionData {
    string archive_path;
    string inner_filename;
    bool auto_detect_file = true;
};

struct SevenZipGlobalState : public GlobalTableFunctionState {
    unique_ptr<char[]> file_content;
    size_t file_size = 0;
    vector<string> column_names;
    vector<LogicalType> column_types;
    vector<vector<Value>> rows;
    idx_t current_row = 0;
    bool parsed = false;
    string error_message;
    bool extraction_not_supported = false;
};

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

    // Find the file (case-insensitive search)
    int file_index = -1;
    UInt32 numFiles = Sz7z_GetNumFiles(&archive);
    string target_lower = bind_data.inner_filename;
    std::transform(target_lower.begin(), target_lower.end(), target_lower.begin(), ::tolower);

    for (UInt32 i = 0; i < numFiles; i++) {
        const CSz7zFileInfo *info = Sz7z_GetFileInfo(&archive, i);
        if (info && info->Name) {
            string name_lower = info->Name;
            std::transform(name_lower.begin(), name_lower.end(), name_lower.begin(), ::tolower);

            if (target_lower == name_lower) {
                file_index = i;
                break;
            }
        }
    }

    if (file_index < 0) {
        Sz7z_Close(&archive);
        result->error_message = "File not found in 7z archive: '" + bind_data.inner_filename + "'. Available files: ";
        // List available files to help user
        for (UInt32 i = 0; i < std::min(numFiles, (UInt32)5); i++) {
            const CSz7zFileInfo *info = Sz7z_GetFileInfo(&archive, i);
            if (info && info->Name && !info->IsDir) {
                if (i > 0) result->error_message += ", ";
                result->error_message += "'" + string(info->Name) + "'";
            }
        }
        if (numFiles > 5) result->error_message += ", ...";
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

    // If parsing returned no columns, treat as raw text
    if (result->column_names.empty()) {
        result->error_message = "Failed to parse file as CSV/TXT. File size: " + std::to_string(outSize) + " bytes. " +
                               "Content preview: " + content.substr(0, std::min((size_t)100, content.size()));
        Sz7z_Close(&archive);
        return std::move(result);
    }

    return std::move(result);
}

// Scan function for stps_7zip
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

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//

void Register7zipFunctions(ExtensionLoader &loader) {
    // Register stps_view_7zip table function
    TableFunction view_7zip_func("stps_view_7zip", {LogicalType::VARCHAR}, View7zipScan, View7zipBind, View7zipInit);
    view_7zip_func.description = "Lists contents of a 7zip archive without extracting.\n"
                                 "Usage: SELECT * FROM stps_view_7zip('/path/to/archive.7z');\n"
                                 "Returns: TABLE(name VARCHAR, size BIGINT, compressed_size BIGINT, modified TIMESTAMP)";
    loader.RegisterFunction(view_7zip_func);
    
    // Register stps_7zip table function (with optional second argument)
    TableFunction sevenzip_func1("stps_7zip", {LogicalType::VARCHAR}, SevenZipScan, SevenZipBind, SevenZipInit);
    sevenzip_func1.description = "Extracts and reads files from a 7zip archive.\n"
                                "Usage: SELECT * FROM stps_7zip('/path/to/archive.7z');\n"
                                "Returns: TABLE with archive contents";
    loader.RegisterFunction(sevenzip_func1);
    
    TableFunction sevenzip_func2("stps_7zip", {LogicalType::VARCHAR, LogicalType::VARCHAR}, SevenZipScan, SevenZipBind, SevenZipInit);
    sevenzip_func2.description = "Extracts and reads a specific file from a 7zip archive.\n"
                                "Usage: SELECT * FROM stps_7zip('/path/to/archive.7z', 'file_pattern');\n"
                                "Returns: TABLE with archive contents matching pattern";
    loader.RegisterFunction(sevenzip_func2);
}

} // namespace stps
} // namespace duckdb
