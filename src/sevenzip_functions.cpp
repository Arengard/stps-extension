#include "sevenzip_functions.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/file_system.hpp"
#include "../lzma/7z.h"
#include <cstring>
#include <algorithm>

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
    
    // Check if we can open the archive
    CSz7zArchive archive;
    Sz7z_Init(&archive, nullptr);
    
    SRes res = Sz7z_Open(&archive, result->archive_path.c_str());
    if (res != SZ_OK) {
        switch (res) {
            case SZ_ERROR_NO_ARCHIVE:
                throw IOException("Not a valid 7z archive: " + result->archive_path);
            case SZ_ERROR_UNSUPPORTED:
                throw IOException("Unsupported 7z archive format: " + result->archive_path + 
                                " (7z archives with encoded/compressed headers are not yet supported. "
                                "Try using standard ZIP format with stps_zip or use stps_view_7zip to list contents.)");
            case SZ_ERROR_READ:
                throw IOException("Failed to read 7z file: " + result->archive_path);
            default:
                throw IOException("Failed to open 7z file: " + result->archive_path);
        }
    }
    
    // For now, since full extraction is complex, we return an info message
    // explaining that 7z extraction requires additional implementation
    Sz7z_Close(&archive);
    
    // Return a simple schema with info about the archive
    names = {"message"};
    return_types = {LogicalType::VARCHAR};
    
    return std::move(result);
}

// Init function for stps_7zip
static unique_ptr<GlobalTableFunctionState> SevenZipInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<SevenZipBindData>();
    auto result = make_uniq<SevenZipGlobalState>();
    
    // Since full extraction is complex, we provide a helpful message
    CSz7zArchive archive;
    Sz7z_Init(&archive, nullptr);
    
    SRes res = Sz7z_Open(&archive, bind_data.archive_path.c_str());
    if (res == SZ_OK) {
        UInt32 numFiles = Sz7z_GetNumFiles(&archive);
        
        string message = "7z archive '" + bind_data.archive_path + "' contains " + 
                        to_string(numFiles) + " file(s). ";
        message += "Full content extraction is not yet implemented. ";
        message += "Use stps_view_7zip('" + bind_data.archive_path + "') to list files. ";
        message += "For extractable archives, consider using ZIP format with stps_zip().";
        
        result->column_names = {"message"};
        result->column_types = {LogicalType::VARCHAR};
        result->rows.push_back({Value(message)});
        result->parsed = true;
        
        Sz7z_Close(&archive);
    } else {
        result->error_message = "Failed to open 7z archive";
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
    loader.RegisterFunction(view_7zip_func);
    
    // Register stps_7zip table function (with optional second argument)
    TableFunction sevenzip_func1("stps_7zip", {LogicalType::VARCHAR}, SevenZipScan, SevenZipBind, SevenZipInit);
    loader.RegisterFunction(sevenzip_func1);
    
    TableFunction sevenzip_func2("stps_7zip", {LogicalType::VARCHAR, LogicalType::VARCHAR}, SevenZipScan, SevenZipBind, SevenZipInit);
    loader.RegisterFunction(sevenzip_func2);
}

} // namespace stps
} // namespace duckdb
