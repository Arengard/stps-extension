#include "sevenzip_functions.hpp"
#include "shared/archive_utils.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/file_system.hpp"
#include "../lzma/7z.h"
#include <cstring>
#include <algorithm>
#include <fstream>

namespace duckdb {
namespace stps {

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

//===--------------------------------------------------------------------===//
// stps_7zip - Read a file from inside a 7-Zip archive
//===--------------------------------------------------------------------===//

struct SevenZipBindData : public TableFunctionData {
    string archive_path;
    string inner_filename;
    int file_index = -1;
    bool is_binary_mode = false;
    string temp_file_path;  // For binary files extracted to temp
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
    auto result = make_uniq<SevenZipBindData>();

    if (input.inputs.empty()) {
        throw BinderException("stps_7zip requires at least one argument: archive_path");
    }
    result->archive_path = input.inputs[0].GetValue<string>();

    string target_filename;
    bool auto_detect = true;
    if (input.inputs.size() >= 2) {
        target_filename = input.inputs[1].GetValue<string>();
        auto_detect = false;
    }

    // Open archive to determine file index and schema
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

    // Find the target file
    if (auto_detect) {
        // Find first non-directory file
        for (UInt32 i = 0; i < numFiles; i++) {
            const CSz7zFileInfo *info = Sz7z_GetFileInfo(&archive, i);
            if (info && !info->IsDir) {
                result->file_index = static_cast<int>(i);
                result->inner_filename = info->Name ? info->Name : "";
                break;
            }
        }
    } else {
        // Case-insensitive search
        string target_lower = target_filename;
        std::transform(target_lower.begin(), target_lower.end(), target_lower.begin(), ::tolower);

        for (UInt32 i = 0; i < numFiles; i++) {
            const CSz7zFileInfo *info = Sz7z_GetFileInfo(&archive, i);
            if (info && info->Name) {
                string name_lower = info->Name;
                std::transform(name_lower.begin(), name_lower.end(), name_lower.begin(), ::tolower);
                if (target_lower == name_lower) {
                    result->file_index = static_cast<int>(i);
                    result->inner_filename = info->Name;
                    break;
                }
            }
        }
    }

    if (result->file_index < 0) {
        // Build available files list for error message
        string available;
        int count = 0;
        for (UInt32 i = 0; i < numFiles && count < 10; i++) {
            const CSz7zFileInfo *info = Sz7z_GetFileInfo(&archive, i);
            if (info && info->Name && !info->IsDir) {
                if (count > 0) available += ", ";
                available += "'" + string(info->Name) + "'";
                count++;
            }
        }
        if (count == 0) available = "(no files)";
        else if (numFiles > 10) available += ", ...";

        Sz7z_Close(&archive);
        if (auto_detect) {
            throw IOException("No files found in 7z archive: " + result->archive_path);
        } else {
            throw IOException("File '" + target_filename + "' not found. Available: " + available);
        }
    }

    // Determine if binary by filename
    if (IsBinaryFormat(result->inner_filename)) {
        result->is_binary_mode = true;
        names = {"extracted_path", "original_filename", "file_size", "file_type", "usage_hint"};
        return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
                       LogicalType::VARCHAR, LogicalType::VARCHAR};
        Sz7z_Close(&archive);
        return result;
    }

    // Extract a small preview to determine schema (first 8KB)
    Byte *outBuf = nullptr;
    size_t outSize = 0;

    res = Sz7z_Extract(&archive, result->file_index, &outBuf, &outSize);
    Sz7z_Close(&archive);

    if (res != SZ_OK || outBuf == nullptr || outSize == 0) {
        if (outBuf) free(outBuf);
        throw IOException("Failed to extract file for schema detection: " + result->inner_filename);
    }

    string content(reinterpret_cast<char*>(outBuf), outSize);
    free(outBuf);

    // Check if content is binary
    if (LooksLikeBinary(content)) {
        result->is_binary_mode = true;
        names = {"extracted_path", "original_filename", "file_size", "file_type", "usage_hint"};
        return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
                       LogicalType::VARCHAR, LogicalType::VARCHAR};
        return result;
    }

    // Parse CSV to determine schema
    std::vector<string> col_names;
    std::vector<LogicalType> col_types;
    std::vector<std::vector<Value>> temp_rows;
    ParseCSVContent(content, col_names, col_types, temp_rows);

    if (col_names.empty()) {
        names = {"content"};
        return_types = {LogicalType::VARCHAR};
    } else {
        names = col_names;
        return_types = col_types;
    }

    return result;
}

// Init function for stps_7zip - performs full extraction and parsing
static unique_ptr<GlobalTableFunctionState> SevenZipInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<SevenZipBindData>();
    auto state = make_uniq<SevenZipGlobalState>();

    // Open archive and extract
    CSz7zArchive archive;
    Sz7z_Init(&archive, nullptr);

    SRes res = Sz7z_Open(&archive, bind_data.archive_path.c_str());
    if (res != SZ_OK) {
        state->error_message = "Failed to open 7z archive: " + bind_data.archive_path;
        return state;
    }

    Byte *outBuf = nullptr;
    size_t outSize = 0;

    res = Sz7z_Extract(&archive, bind_data.file_index, &outBuf, &outSize);
    Sz7z_Close(&archive);

    if (res != SZ_OK || outBuf == nullptr) {
        if (outBuf) free(outBuf);
        state->error_message = "Failed to extract file from 7z archive";
        return state;
    }

    string content(reinterpret_cast<char*>(outBuf), outSize);
    free(outBuf);

    // Handle binary mode
    if (bind_data.is_binary_mode) {
        string temp_path = ExtractToTemp(content, bind_data.inner_filename, "stps_7zip_");
        string ext = GetFileExtension(bind_data.inner_filename);

        string usage_hint;
        if (ext == "parquet") {
            usage_hint = "SELECT * FROM read_parquet('" + temp_path + "')";
        } else if (ext == "xlsx" || ext == "xls") {
            usage_hint = "Install and use st_read() for Excel files";
        } else if (ext == "arrow" || ext == "feather") {
            usage_hint = "SELECT * FROM read_parquet('" + temp_path + "')";
        } else {
            usage_hint = "Binary file extracted to: " + temp_path;
        }

        state->column_names = {"extracted_path", "original_filename", "file_size", "file_type", "usage_hint"};
        state->column_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
                              LogicalType::VARCHAR, LogicalType::VARCHAR};

        vector<Value> row;
        row.push_back(Value(temp_path));
        row.push_back(Value(bind_data.inner_filename));
        row.push_back(Value::BIGINT(static_cast<int64_t>(outSize)));
        row.push_back(Value(ext.empty() ? "binary" : ext));
        row.push_back(Value(usage_hint));
        state->rows.push_back(std::move(row));

        return state;
    }

    // Parse CSV content
    ParseCSVContent(content, state->column_names, state->column_types, state->rows);

    // Fallback to raw content if parsing failed
    if (state->column_names.empty()) {
        state->column_names = {"content"};
        state->column_types = {LogicalType::VARCHAR};
        state->rows.clear();
        vector<Value> row;
        row.push_back(Value(content));
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
