#include "zip_functions.hpp"
#include "shared/archive_utils.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/file_system.hpp"
#include "../miniz/miniz.h"
#include <cstring>
#include <fstream>

namespace duckdb {
namespace stps {

//===--------------------------------------------------------------------===//
// stps_view_zip - List files inside a ZIP archive
//===--------------------------------------------------------------------===//

struct ViewZipBindData : public TableFunctionData {
    string zip_path;
};

struct ViewZipGlobalState : public GlobalTableFunctionState {
    mz_zip_archive zip_archive;
    idx_t current_file_index = 0;
    idx_t total_files = 0;
    bool initialized = false;
    string error_message;
};

// Bind function for stps_view_zip
static unique_ptr<FunctionData> ViewZipBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<ViewZipBindData>();
    
    if (input.inputs.empty()) {
        throw BinderException("stps_view_zip requires at least one argument: zip_path");
    }
    result->zip_path = input.inputs[0].GetValue<string>();
    
    // Define output columns
    names = {"filename", "uncompressed_size", "compressed_size", "is_directory", "index"};
    return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT, 
                    LogicalType::BOOLEAN, LogicalType::INTEGER};
    
    return result;
}

// Init function for stps_view_zip
static unique_ptr<GlobalTableFunctionState> ViewZipInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<ViewZipBindData>();
    auto result = make_uniq<ViewZipGlobalState>();
    
    memset(&result->zip_archive, 0, sizeof(result->zip_archive));
    
    // Open the ZIP file
    if (!mz_zip_reader_init_file(&result->zip_archive, bind_data.zip_path.c_str(), 0)) {
        result->error_message = "Failed to open ZIP file: " + bind_data.zip_path;
        result->initialized = false;
    } else {
        result->total_files = mz_zip_reader_get_num_files(&result->zip_archive);
        result->initialized = true;
    }
    
    return result;
}

// Scan function for stps_view_zip
static void ViewZipScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<ViewZipGlobalState>();
    
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
        mz_zip_archive_file_stat file_stat;
        if (!mz_zip_reader_file_stat(&state.zip_archive, state.current_file_index, &file_stat)) {
            state.current_file_index++;
            continue;
        }
        
        output.SetValue(0, count, Value(string(file_stat.m_filename)));
        output.SetValue(1, count, Value::BIGINT(file_stat.m_uncomp_size));
        output.SetValue(2, count, Value::BIGINT(file_stat.m_comp_size));
        output.SetValue(3, count, Value::BOOLEAN(mz_zip_reader_is_file_a_directory(&state.zip_archive, state.current_file_index)));
        output.SetValue(4, count, Value::INTEGER(static_cast<int32_t>(state.current_file_index)));
        
        state.current_file_index++;
        count++;
    }
    
    output.SetCardinality(count);
    
    // Clean up when done
    if (state.current_file_index >= state.total_files && state.initialized) {
        mz_zip_reader_end(&state.zip_archive);
        state.initialized = false;
    }
}

//===--------------------------------------------------------------------===//
// stps_zip - Read a file from inside a ZIP archive
// Usage: SELECT * FROM stps_zip('data.zip') -- reads first/only file
// Usage: SELECT * FROM stps_zip('data.zip', 'data.csv') -- reads specific file
//===--------------------------------------------------------------------===//

struct ZipBindData : public TableFunctionData {
    string zip_path;
    string inner_filename;
    bool auto_detect_file = true;
};

struct ZipGlobalState : public GlobalTableFunctionState {
    unique_ptr<char[]> file_content;
    size_t file_size = 0;
    std::vector<string> column_names;
    std::vector<LogicalType> column_types;
    std::vector<std::vector<Value>> rows;
    idx_t current_row = 0;
    bool parsed = false;
    string error_message;
};

// Bind function for stps_zip
static unique_ptr<FunctionData> ZipBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<ZipBindData>();
    
    if (input.inputs.empty()) {
        throw BinderException("stps_zip requires at least one argument: zip_path");
    }
    result->zip_path = input.inputs[0].GetValue<string>();
    
    // Optional second argument: inner filename
    if (input.inputs.size() >= 2) {
        result->inner_filename = input.inputs[1].GetValue<string>();
        result->auto_detect_file = false;
    }
    
    // We need to actually read the file at bind time to determine the schema
    mz_zip_archive zip_archive;
    memset(&zip_archive, 0, sizeof(zip_archive));
    
    if (!mz_zip_reader_init_file(&zip_archive, result->zip_path.c_str(), 0)) {
        throw IOException("Failed to open ZIP file: " + result->zip_path);
    }
    
    int num_files = mz_zip_reader_get_num_files(&zip_archive);
    if (num_files == 0) {
        mz_zip_reader_end(&zip_archive);
        throw IOException("ZIP file is empty: " + result->zip_path);
    }
    
    // Find the file to read
    int file_index = -1;
    
    if (result->auto_detect_file) {
        // Find first non-directory file
        for (int i = 0; i < num_files; i++) {
            if (!mz_zip_reader_is_file_a_directory(&zip_archive, i)) {
                file_index = i;
                break;
            }
        }
    } else {
        // Look for specific file
        file_index = mz_zip_reader_locate_file(&zip_archive, result->inner_filename.c_str(), nullptr, 0);
    }
    
    if (file_index < 0) {
        mz_zip_reader_end(&zip_archive);
        if (result->auto_detect_file) {
            throw IOException("No files found in ZIP archive: " + result->zip_path);
        } else {
            throw IOException("File not found in ZIP archive: " + result->inner_filename);
        }
    }
    
    // Get file info
    mz_zip_archive_file_stat file_stat;
    if (!mz_zip_reader_file_stat(&zip_archive, file_index, &file_stat)) {
        mz_zip_reader_end(&zip_archive);
        throw IOException("Failed to get file info from ZIP archive");
    }
    
    // Store the actual filename for later use
    result->inner_filename = file_stat.m_filename;
    
    // Extract file content
    size_t file_size;
    void *file_data = mz_zip_reader_extract_to_heap(&zip_archive, file_index, &file_size, 0);
    
    if (!file_data) {
        mz_zip_reader_end(&zip_archive);
        throw IOException("Failed to extract file from ZIP archive: " + result->inner_filename);
    }
    
    string content(static_cast<char*>(file_data), file_size);
    mz_free(file_data);
    mz_zip_reader_end(&zip_archive);
    
    // Check if this is a binary format that can't be parsed as CSV
    if (IsBinaryFormat(result->inner_filename) || LooksLikeBinary(content)) {
        // Extract to temp file and return path for user to use with read_parquet etc.
        string temp_path = ExtractToTemp(content, result->inner_filename, "stps_zip_");
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

// Init function for stps_zip
static unique_ptr<GlobalTableFunctionState> ZipInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<ZipBindData>();
    auto result = make_uniq<ZipGlobalState>();
    
    // Check if this is binary mode (inner_filename contains temp path)
    if (!bind_data.auto_detect_file && bind_data.inner_filename.find("stps_zip_") != string::npos) {
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
            usage_hint = "Install and use read_sheet() for Excel files";
        } else if (ext == "arrow" || ext == "feather") {
            usage_hint = "SELECT * FROM read_parquet('" + temp_path + "') -- Arrow/Feather compatible";
        } else {
            usage_hint = "Binary file extracted to: " + temp_path;
        }

        // Extract original filename from temp path
        string original_name = temp_path;
        size_t prefix_pos = original_name.find("stps_zip_");
        if (prefix_pos != string::npos) {
            original_name = original_name.substr(prefix_pos + 9);  // Skip "stps_zip_"
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

    mz_zip_archive zip_archive;
    memset(&zip_archive, 0, sizeof(zip_archive));
    
    if (!mz_zip_reader_init_file(&zip_archive, bind_data.zip_path.c_str(), 0)) {
        result->error_message = "Failed to open ZIP file: " + bind_data.zip_path;
        return result;
    }
    
    int file_index = mz_zip_reader_locate_file(&zip_archive, bind_data.inner_filename.c_str(), nullptr, 0);
    if (file_index < 0) {
        mz_zip_reader_end(&zip_archive);
        result->error_message = "File not found in ZIP archive: " + bind_data.inner_filename;
        return result;
    }
    
    // Extract file content
    size_t file_size;
    void *file_data = mz_zip_reader_extract_to_heap(&zip_archive, file_index, &file_size, 0);
    
    if (!file_data) {
        mz_zip_reader_end(&zip_archive);
        result->error_message = "Failed to extract file from ZIP archive";
        return result;
    }
    
    string content(static_cast<char*>(file_data), file_size);
    mz_free(file_data);
    mz_zip_reader_end(&zip_archive);
    
    // Parse CSV content
    ParseCSVContent(content, result->column_names, result->column_types, result->rows);
    result->parsed = true;
    
    return result;
}

// Scan function for stps_zip
static void ZipScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<ZipGlobalState>();
    
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

void RegisterZipFunctions(ExtensionLoader &loader) {
    // Register stps_view_zip table function
    TableFunction view_zip_func("stps_view_zip", {LogicalType::VARCHAR}, ViewZipScan, ViewZipBind, ViewZipInit);
    loader.RegisterFunction(view_zip_func);
    
    // Register stps_zip table function (with optional second argument)
    TableFunction zip_func1("stps_zip", {LogicalType::VARCHAR}, ZipScan, ZipBind, ZipInit);
    loader.RegisterFunction(zip_func1);
    
    TableFunction zip_func2("stps_zip", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ZipScan, ZipBind, ZipInit);
    loader.RegisterFunction(zip_func2);
}

} // namespace stps
} // namespace duckdb
