#include "zip_functions.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/file_system.hpp"
#include "../miniz/miniz.h"
#include <cstring>
#include <algorithm>

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
    
    return std::move(result);
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
    
    return std::move(result);
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
    vector<string> column_names;
    vector<LogicalType> column_types;
    vector<vector<Value>> rows;
    idx_t current_row = 0;
    bool parsed = false;
    string error_message;
};

// Helper function to detect delimiter
static char DetectDelimiter(const string &content) {
    // Check for common delimiters
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
            // Remove carriage return if present
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            if (!line.empty()) {
                lines.push_back(line);
            }
            start = i + 1;
        }
    }
    // Add last line if no trailing newline
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
    
    // Initialize all columns as VARCHAR for simplicity
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

// Init function for stps_zip
static unique_ptr<GlobalTableFunctionState> ZipInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<ZipBindData>();
    auto result = make_uniq<ZipGlobalState>();
    
    mz_zip_archive zip_archive;
    memset(&zip_archive, 0, sizeof(zip_archive));
    
    if (!mz_zip_reader_init_file(&zip_archive, bind_data.zip_path.c_str(), 0)) {
        result->error_message = "Failed to open ZIP file: " + bind_data.zip_path;
        return std::move(result);
    }
    
    int file_index = mz_zip_reader_locate_file(&zip_archive, bind_data.inner_filename.c_str(), nullptr, 0);
    if (file_index < 0) {
        mz_zip_reader_end(&zip_archive);
        result->error_message = "File not found in ZIP archive: " + bind_data.inner_filename;
        return std::move(result);
    }
    
    // Extract file content
    size_t file_size;
    void *file_data = mz_zip_reader_extract_to_heap(&zip_archive, file_index, &file_size, 0);
    
    if (!file_data) {
        mz_zip_reader_end(&zip_archive);
        result->error_message = "Failed to extract file from ZIP archive";
        return std::move(result);
    }
    
    string content(static_cast<char*>(file_data), file_size);
    mz_free(file_data);
    mz_zip_reader_end(&zip_archive);
    
    // Parse CSV content
    ParseCSVContent(content, result->column_names, result->column_types, result->rows);
    result->parsed = true;
    
    return std::move(result);
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
    view_zip_func.description = "Lists contents of a ZIP archive without extracting.\n"
                                "Usage: SELECT * FROM stps_view_zip('/path/to/archive.zip');\n"
                                "Returns: TABLE(filename VARCHAR, uncompressed_size BIGINT, compressed_size BIGINT, is_directory BOOLEAN, index INTEGER)";
    loader.RegisterFunction(view_zip_func);
    
    // Register stps_zip table function (with optional second argument)
    TableFunction zip_func1("stps_zip", {LogicalType::VARCHAR}, ZipScan, ZipBind, ZipInit);
    zip_func1.description = "Extracts and reads CSV/text files from a ZIP archive.\n"
                           "Usage: SELECT * FROM stps_zip('/path/to/archive.zip');\n"
                           "Returns: TABLE with contents of the first CSV file in the archive";
    loader.RegisterFunction(zip_func1);
    
    TableFunction zip_func2("stps_zip", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ZipScan, ZipBind, ZipInit);
    zip_func2.description = "Extracts and reads a specific file from a ZIP archive by pattern.\n"
                           "Usage: SELECT * FROM stps_zip('/path/to/archive.zip', 'filename_pattern');\n"
                           "Returns: TABLE with contents of the matched file";
    loader.RegisterFunction(zip_func2);
}

} // namespace stps
} // namespace duckdb
