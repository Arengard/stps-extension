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
    std::vector<string> column_names;
    std::vector<LogicalType> column_types;
    std::vector<std::vector<Value>> rows;
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
        // Look for specific file (case-insensitive)
        string target_lower = result->inner_filename;
        std::transform(target_lower.begin(), target_lower.end(), target_lower.begin(), ::tolower);

        for (UInt32 i = 0; i < numFiles; i++) {
            const CSz7zFileInfo *info = Sz7z_GetFileInfo(&archive, i);
            if (info && info->Name) {
                string name_lower = info->Name;
                std::transform(name_lower.begin(), name_lower.end(), name_lower.begin(), ::tolower);
                if (target_lower == name_lower) {
                    file_index = i;
                    result->inner_filename = info->Name;  // Use actual case from archive
                    break;
                }
            }
        }
    }

    if (file_index < 0) {
        // Build list of available files for error message
        string available_files;
        int file_count = 0;
        for (UInt32 i = 0; i < numFiles && file_count < 10; i++) {
            const CSz7zFileInfo *info = Sz7z_GetFileInfo(&archive, i);
            if (info && info->Name && !info->IsDir) {
                if (file_count > 0) available_files += ", ";
                available_files += "'" + string(info->Name) + "'";
                file_count++;
            }
        }
        if (file_count == 0) available_files = "(no files)";
        else if (numFiles > 10) available_files += ", ...";

        Sz7z_Close(&archive);
        if (result->auto_detect_file) {
            throw IOException("No files found in 7z archive: " + result->archive_path);
        } else {
            throw IOException("File '" + result->inner_filename + "' not found in 7z archive. Available files: " + available_files);
        }
    }

    // Extract file content
    Byte *outBuf = nullptr;
    size_t outSize = 0;

    res = Sz7z_Extract(&archive, file_index, &outBuf, &outSize);
    if (res != SZ_OK) {
        Sz7z_Close(&archive);
        if (res == SZ_ERROR_DATA) {
            throw IOException("7z extraction failed: unsupported/complex archive or exceeds safety limits (256MB compressed / 512MB uncompressed).");
        }
        throw IOException("Failed to extract file from 7z archive: " + result->inner_filename);
    }

    string content(reinterpret_cast<char*>(outBuf), outSize);
    free(outBuf);
    Sz7z_Close(&archive);

    // Check if this is a binary format that can't be parsed as CSV
    if (IsBinaryFormat(result->inner_filename) || LooksLikeBinary(content)) {
        // Extract to temp file and return path for user to use with read_parquet etc.
        string temp_path = ExtractToTemp(content, result->inner_filename, "stps_7zip_");
        string ext = GetFileExtension(result->inner_filename);

        // Return info about extracted file
        names = {"extracted_path", "original_filename", "file_size", "file_type", "usage_hint"};
        return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
                       LogicalType::VARCHAR, LogicalType::VARCHAR};

        // Store temp path in inner_filename for Init to use
        result->inner_filename = temp_path;
        result->auto_detect_file = false;  // Signal that this is binary mode

        return result;
    }

    // Parse to determine schema for CSV/text files
    std::vector<string> column_names;
    std::vector<LogicalType> column_types;
    std::vector<std::vector<Value>> temp_rows;

    ParseCSVContent(content, column_names, column_types, temp_rows);

    if (column_names.empty()) {
        // If parsing failed, return raw content
        names = {"content"};
        return_types = {LogicalType::VARCHAR};
    } else {
        names.clear();
        return_types.clear();
        for (const auto &name : column_names) {
            names.push_back(name);
        }
        for (const auto &type : column_types) {
            return_types.push_back(type);
        }
    }

    return result;
}

// Init function for stps_7zip
static unique_ptr<GlobalTableFunctionState> SevenZipInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<SevenZipBindData>();
    auto result = make_uniq<SevenZipGlobalState>();

    // Check if this is binary mode (inner_filename contains temp path)
    if (!bind_data.auto_detect_file && bind_data.inner_filename.find("stps_7zip_") != string::npos) {
        // Binary file mode - return extraction info
        string temp_path = bind_data.inner_filename;
        string ext = GetFileExtension(temp_path);

        // Get file size safely (avoid ambiguous ternary with pos_type)
        size_t file_size = 0;
        std::ifstream file(temp_path, std::ios::binary | std::ios::ate);
        if (file.is_open()) {
            std::streampos pos = file.tellg();
            if (pos != std::streampos(-1)) {
                file_size = static_cast<size_t>(pos);
            }
        }

        // Determine usage hint based on extension
        string usage_hint;
        if (ext == "parquet") {
            usage_hint = "SELECT * FROM read_parquet('" + temp_path + "')";
        } else if (ext == "xlsx" || ext == "xls") {
            usage_hint = "Install and use st_read() for Excel files";
        } else if (ext == "arrow" || ext == "feather") {
            usage_hint = "SELECT * FROM read_parquet('" + temp_path + "') -- Arrow/Feather compatible";
        } else {
            usage_hint = "Binary file extracted to: " + temp_path;
        }

        // Extract original filename from temp path
        string original_name = temp_path;
        size_t prefix_pos = original_name.find("stps_7zip_");
        if (prefix_pos != string::npos) {
            original_name = original_name.substr(prefix_pos + 10);  // Skip "stps_7zip_"
        }

        result->column_names = {"extracted_path", "original_filename", "file_size", "file_type", "usage_hint"};
        result->column_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
                               LogicalType::VARCHAR, LogicalType::VARCHAR};

        vector<Value> row;
        row.push_back(Value(temp_path));
        row.push_back(Value(original_name));
        row.push_back(Value::BIGINT(file_size));
        row.push_back(Value(ext.empty() ? "binary" : ext));
        row.push_back(Value(usage_hint));
        result->rows.push_back(row);
        result->parsed = true;

        return result;
    }

    // Open the archive for CSV/text files
    CSz7zArchive archive;
    Sz7z_Init(&archive, nullptr);

    SRes res = Sz7z_Open(&archive, bind_data.archive_path.c_str());
    if (res != SZ_OK) {
        result->error_message = "Failed to open 7z archive: " + bind_data.archive_path;
        return result;
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
        int file_count = 0;
        for (UInt32 i = 0; i < numFiles && file_count < 5; i++) {
            const CSz7zFileInfo *info = Sz7z_GetFileInfo(&archive, i);
            if (info && info->Name && !info->IsDir) {
                if (file_count > 0) result->error_message += ", ";
                result->error_message += "'" + string(info->Name) + "'";
                file_count++;
            }
        }
        if (numFiles > 5) result->error_message += ", ...";
        return result;
    }

    // Extract file content
    Byte *outBuf = nullptr;
    size_t outSize = 0;

    res = Sz7z_Extract(&archive, file_index, &outBuf, &outSize);
    if (res != SZ_OK) {
        Sz7z_Close(&archive);
        if (res == SZ_ERROR_DATA) {
            result->error_message = "7z extraction failed: unsupported/complex archive or exceeds safety limits (256MB compressed / 512MB uncompressed).";
        } else {
            result->error_message = "Failed to extract file from 7z archive";
        }
        return result;
    }

    string content(reinterpret_cast<char*>(outBuf), outSize);
    free(outBuf);
    Sz7z_Close(&archive);

    // Parse CSV content
    ParseCSVContent(content, result->column_names, result->column_types, result->rows);
    result->parsed = true;

    // If parsing returned no columns, provide helpful error
    if (result->column_names.empty()) {
        result->error_message = "Failed to parse file as CSV/text. File may be binary. Size: " +
                               std::to_string(outSize) + " bytes.";
        return result;
    }

    return result;
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
    loader.RegisterFunction(view_7zip_func);
    
    // Register stps_7zip table function (with optional second argument)
    TableFunction sevenzip_func1("stps_7zip", {LogicalType::VARCHAR}, SevenZipScan, SevenZipBind, SevenZipInit);
    loader.RegisterFunction(sevenzip_func1);
    
    TableFunction sevenzip_func2("stps_7zip", {LogicalType::VARCHAR, LogicalType::VARCHAR}, SevenZipScan, SevenZipBind, SevenZipInit);
    loader.RegisterFunction(sevenzip_func2);
}

} // namespace stps
} // namespace duckdb
