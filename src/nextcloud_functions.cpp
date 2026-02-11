#include "nextcloud_functions.hpp"
#include "webdav_utils.hpp"
#include "shared/archive_utils.hpp"
#include "curl_utils.hpp"
#include "case_transform.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_result.hpp"
#include <fstream>
#include <algorithm>
#include <sstream>
#include <random>
#include <chrono>

namespace duckdb {
namespace stps {

struct NextcloudBindData : public TableFunctionData {
    std::string url;
    std::string username;
    std::string password;
    std::vector<std::string> extra_headers;
    bool all_varchar = false;
    bool ignore_errors = false;
    std::string reader_options;
    std::string sheet;
    std::string range;

    // Fetched data stored in bind for schema detection
    std::string fetched_body;
    std::string file_extension;
    std::string temp_file_path;
    bool is_binary = false;
    bool needs_temp_file = false;  // Only for formats that absolutely require file access

    // Schema detected in bind
    vector<std::string> column_names;
    vector<LogicalType> column_types;

    // For binary formats: store the materialized result
    vector<vector<Value>> materialized_rows;
};

struct NextcloudGlobalState : public GlobalTableFunctionState {
    std::vector<std::vector<Value>> rows;
    idx_t current_row = 0;
};


// Generate unique temp filename
static std::string GenerateTempFilename(const std::string &ext) {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1000, 9999);

    std::string temp_dir = GetTempDirectory();
    return temp_dir + "/nextcloud_" + std::to_string(ms) + "_" + std::to_string(dis(gen)) + "." + ext;
}

// Forward declaration
static bool ReadFileViaDuckDB(ClientContext &context, const std::string &body, const std::string &file_type,
                               bool all_varchar, bool ignore_errors, const std::string &reader_options,
                               const std::string &sheet, const std::string &range,
                               std::vector<std::string> &col_names, std::vector<LogicalType> &col_types,
                               std::vector<std::vector<Value>> &rows,
                               std::string *error_out = nullptr);

static unique_ptr<FunctionData> NextcloudBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<NextcloudBindData>();

    if (input.inputs.empty()) {
        throw BinderException("next_cloud requires a URL argument");
    }
    result->url = input.inputs[0].GetValue<std::string>();

    // Parse named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "username") {
            result->username = kv.second.ToString();
        } else if (kv.first == "password") {
            result->password = kv.second.ToString();
        } else if (kv.first == "headers") {
            std::string hdrs = kv.second.ToString();
            std::stringstream ss(hdrs);
            std::string line;
            while (std::getline(ss, line)) {
                // Trim whitespace
                size_t start = line.find_first_not_of(" \t\r\n");
                size_t end = line.find_last_not_of(" \t\r\n");
                if (start != std::string::npos && end != std::string::npos) {
                    result->extra_headers.push_back(line.substr(start, end - start + 1));
                }
            }
        } else if (kv.first == "all_varchar") {
            result->all_varchar = kv.second.GetValue<bool>();
        } else if (kv.first == "ignore_errors") {
            result->ignore_errors = kv.second.GetValue<bool>();
        } else if (kv.first == "reader_options") {
            result->reader_options = kv.second.ToString();
        } else if (kv.first == "sheet") {
            result->sheet = kv.second.ToString();
        } else if (kv.first == "range") {
            result->range = kv.second.ToString();
        }
    }

    // Build HTTP headers
    CurlHeaders headers;
    if (!result->username.empty() || !result->password.empty()) {
        std::string credentials = result->username + ":" + result->password;
        std::string basic = "Authorization: Basic " + Base64Encode(credentials);
        headers.append(basic);
    }
    for (auto &h : result->extra_headers) {
        headers.append(h);
    }

    // Fetch data immediately in bind phase for schema detection
    long http_code = 0;
    std::string request_url = NormalizeRequestUrl(result->url);
    std::string body = curl_get(request_url, headers, &http_code);

    // Handle curl errors immediately
    if (body.find("ERROR:") == 0) {
        throw IOException(GetCurlErrorDetails(body));
    }

    // Handle HTTP errors immediately
    if (http_code >= 400) {
        throw IOException(GetHttpErrorMessage(http_code, result->url));
    }

    if (body.empty()) {
        throw IOException("Empty response from server: " + result->url);
    }

    result->fetched_body = std::move(body);

    // Determine file extension
    result->file_extension = GetFileExtension(result->url);
    std::transform(result->file_extension.begin(), result->file_extension.end(),
                   result->file_extension.begin(), ::tolower);

    // Read file content using DuckDB's built-in readers (supports all_varchar, ignore_errors, reader_options)
    std::vector<std::string> col_names;
    std::vector<LogicalType> col_types;
    std::vector<std::vector<Value>> mat_rows;

    bool read_ok = ReadFileViaDuckDB(context, result->fetched_body, result->file_extension,
                                      result->all_varchar, result->ignore_errors, result->reader_options,
                                      result->sheet, result->range,
                                      col_names, col_types, mat_rows);

    if (!read_ok) {
        // DuckDB reader failed - try custom CSV parser as fallback for text files
        bool is_binary = (result->file_extension == "parquet" || result->file_extension == "arrow" ||
                          result->file_extension == "feather" || result->file_extension == "xlsx" ||
                          result->file_extension == "xls");
        if (!is_binary) {
            ParseCSVContent(result->fetched_body, col_names, col_types, mat_rows);
        }
    }

    // Transfer rows to bind data (std::vector -> duckdb::vector)
    for (auto &row : mat_rows) {
        result->materialized_rows.push_back(std::move(row));
    }

    if (!col_names.empty()) {
        for (const auto &name : col_names) {
            result->column_names.push_back(name);
            names.push_back(name);
        }
        for (const auto &type : col_types) {
            result->column_types.push_back(type);
            return_types.push_back(type);
        }
    } else {
        // Fallback: return raw content
        result->column_names.push_back("content");
        result->column_types.push_back(LogicalType::VARCHAR);
        names = {"content"};
        return_types = {LogicalType::VARCHAR};
        if (result->materialized_rows.empty()) {
            result->materialized_rows.push_back({Value(result->fetched_body)});
        }
    }

    // Free fetched body to save memory
    result->fetched_body.clear();
    result->fetched_body.shrink_to_fit();

    return result;
}

static unique_ptr<GlobalTableFunctionState> NextcloudInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind = input.bind_data->Cast<NextcloudBindData>();
    auto state = make_uniq<NextcloudGlobalState>();

    // All data is materialized in bind phase
    for (const auto &row : bind.materialized_rows) {
        state->rows.push_back(row);
    }

    return state;
}

static void NextcloudScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NextcloudGlobalState>();

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.current_row < state.rows.size() && count < max_count) {
        auto &row = state.rows[state.current_row];
        for (idx_t col = 0; col < row.size() && col < output.ColumnCount(); col++) {
            output.SetValue(col, count, row[col]);
        }
        state.current_row++;
        count++;
    }
    output.SetCardinality(count);
}

// ============================================================================
// next_cloud_folder - Scan Nextcloud parent folder, enter each company's
// child_folder, read all matching files, return unified table with metadata.
// ============================================================================



// Normalize a column name to snake_case
static std::string NormalizeColumnName(const std::string &name) {
    return to_snake_case(name);
}

struct NextcloudFolderFileInfo {
    std::string parent_folder;   // company folder name
    std::string child_folder;    // e.g. "bank"
    std::string file_name;       // e.g. "data.csv"
    std::string download_url;    // full URL to download
};

struct NextcloudFolderBindData : public TableFunctionData {
    std::string parent_url;
    std::string child_folder;
    std::string file_type;
    std::string username;
    std::string password;
    bool all_varchar = false;
    bool ignore_errors = false;
    std::string reader_options;
    std::string sheet;
    std::string range;

    // Discovered files
    std::vector<NextcloudFolderFileInfo> files;

    // Schema from first file
    std::vector<std::string> column_names;
    std::vector<LogicalType> column_types;

    // Materialized rows from first file (transferred to init)
    std::vector<std::vector<Value>> first_file_rows;
    idx_t first_file_col_count = 0;
};

struct NextcloudFolderGlobalState : public GlobalTableFunctionState {
    std::vector<std::vector<Value>> rows;
    idx_t current_row = 0;
};


// Check if a file type requires binary reading (not CSV parsing)
static bool IsBinaryFileType(const std::string &file_type) {
    return file_type == "xlsx" || file_type == "xls" ||
           file_type == "parquet" || file_type == "arrow" || file_type == "feather";
}

// Unified file reader: writes content to temp file, reads via DuckDB's built-in readers with options.
// Handles CSV, TSV, Parquet, Arrow, Feather, XLSX, XLS.
// Returns true on success, false on failure.
static bool ReadFileViaDuckDB(ClientContext &context, const std::string &body, const std::string &file_type,
                               bool all_varchar, bool ignore_errors, const std::string &reader_options,
                               const std::string &sheet, const std::string &range,
                               std::vector<std::string> &col_names, std::vector<LogicalType> &col_types,
                               std::vector<std::vector<Value>> &rows,
                               std::string *error_out) {
    std::string ext = file_type.empty() ? "csv" : file_type;
    std::string temp_path = GenerateTempFilename(ext);
    std::ofstream out(temp_path, std::ios::binary);
    if (!out) return false;
    out.write(body.data(), body.size());
    out.close();

    try {
        auto &db = DatabaseInstance::GetDatabase(context);
        Connection conn(db);

        // Build the reader expression based on file type
        std::string read_expr;

        if (ext == "xlsx" || ext == "xls") {
            conn.Query("INSTALL spatial");
            conn.Query("LOAD spatial");
            read_expr = "st_read('" + temp_path + "'";
            // Build layer parameter from sheet and/or range
            if (!sheet.empty() || !range.empty()) {
                std::string layer = sheet.empty() ? "Sheet1" : sheet;
                if (!range.empty()) {
                    layer += "$" + range;
                }
                read_expr += ", layer='" + layer + "'";
            }
            if (all_varchar) {
                read_expr += ", open_options=['FIELD_TYPES=STRING']";
            }
            if (!reader_options.empty()) {
                read_expr += ", " + reader_options;
            }
            read_expr += ")";
        } else if (ext == "parquet" || ext == "arrow" || ext == "feather") {
            read_expr = "read_parquet('" + temp_path + "'";
            if (!reader_options.empty()) {
                read_expr += ", " + reader_options;
            }
            read_expr += ")";
        } else {
            // CSV/TSV - use DuckDB's read_csv_auto for robust parsing
            read_expr = "read_csv_auto('" + temp_path + "'";
            if (all_varchar) {
                read_expr += ", all_varchar=true";
            }
            if (ignore_errors) {
                read_expr += ", ignore_errors=true";
            }
            if (!reader_options.empty()) {
                read_expr += ", " + reader_options;
            }
            read_expr += ")";
        }

        std::string base_query = "SELECT * FROM " + read_expr;

        // Get schema
        auto schema_result = conn.Query(base_query + " LIMIT 0");
        if (schema_result->HasError()) {
            if (error_out) *error_out = schema_result->GetError();
            std::remove(temp_path.c_str());
            return false;
        }

        for (idx_t i = 0; i < schema_result->ColumnCount(); i++) {
            col_names.push_back(schema_result->ColumnName(i));
            col_types.push_back(schema_result->types[i]);
        }

        // Read all data
        auto data_result = conn.Query(base_query);
        if (data_result->HasError()) {
            if (error_out) *error_out = data_result->GetError();
            std::remove(temp_path.c_str());
            return false;
        }

        while (true) {
            auto chunk = data_result->Fetch();
            if (!chunk || chunk->size() == 0) break;
            for (idx_t row = 0; row < chunk->size(); row++) {
                vector<Value> row_values;
                for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
                    row_values.push_back(chunk->GetValue(col, row));
                }
                rows.push_back(std::move(row_values));
            }
        }

        std::remove(temp_path.c_str());
        return true;
    } catch (std::exception &e) {
        if (error_out) *error_out = e.what();
        std::remove(temp_path.c_str());
        return false;
    } catch (...) {
        if (error_out) *error_out = "Unknown error reading file";
        std::remove(temp_path.c_str());
        return false;
    }
}

static unique_ptr<FunctionData> NextcloudFolderBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<NextcloudFolderBindData>();

    if (input.inputs.empty()) {
        throw BinderException("next_cloud_folder requires a parent URL argument");
    }
    result->parent_url = input.inputs[0].GetValue<std::string>();

    // Defaults
    result->child_folder = "";
    result->file_type = "csv";

    // Parse named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "child_folder") {
            result->child_folder = kv.second.ToString();
        } else if (kv.first == "file_type") {
            result->file_type = kv.second.ToString();
            std::transform(result->file_type.begin(), result->file_type.end(),
                           result->file_type.begin(), ::tolower);
        } else if (kv.first == "username") {
            result->username = kv.second.ToString();
        } else if (kv.first == "password") {
            result->password = kv.second.ToString();
        } else if (kv.first == "all_varchar") {
            result->all_varchar = kv.second.GetValue<bool>();
        } else if (kv.first == "ignore_errors") {
            result->ignore_errors = kv.second.GetValue<bool>();
        } else if (kv.first == "reader_options") {
            result->reader_options = kv.second.ToString();
        } else if (kv.first == "sheet") {
            result->sheet = kv.second.ToString();
        } else if (kv.first == "range") {
            result->range = kv.second.ToString();
        }
    }

    // Ensure parent URL ends with /
    if (!result->parent_url.empty() && result->parent_url.back() != '/') {
        result->parent_url += '/';
    }

    std::string base_url = GetBaseUrl(result->parent_url);

    // Step 1: PROPFIND the parent folder to list company subfolders
    {
        CurlHeaders headers;
        BuildAuthHeaders(headers, result->username, result->password);
        headers.append("Depth: 1");
        headers.append("Content-Type: application/xml");

        long http_code = 0;
        std::string request_url = NormalizeRequestUrl(result->parent_url);
        std::string response = curl_propfind(request_url, PROPFIND_BODY, headers, &http_code);

        if (response.find("ERROR:") == 0) {
            throw IOException(GetCurlErrorDetails(response));
        }
        if (http_code >= 400) {
            throw IOException(GetHttpErrorMessage(http_code, result->parent_url));
        }

        auto entries = ParsePropfindResponse(response);

        // For each collection entry (skip the parent itself), PROPFIND child_folder
        for (auto &entry : entries) {
            if (!entry.is_collection) continue;

            std::string decoded_href = PercentDecodePath(entry.href);
            std::string company_name = GetLastPathSegment(decoded_href);

            // Skip the parent folder itself (its href matches the parent path)
            // The parent URL path decoded should end with the same thing
            std::string parent_path = PercentDecodePath(result->parent_url);
            // Remove scheme+host from parent_url to compare paths
            size_t scheme_end = parent_path.find("://");
            if (scheme_end != std::string::npos) {
                size_t path_start = parent_path.find('/', scheme_end + 3);
                if (path_start != std::string::npos) {
                    parent_path = parent_path.substr(path_start);
                }
            }
            // Normalize trailing slashes for comparison
            std::string norm_href = decoded_href;
            while (!norm_href.empty() && norm_href.back() == '/') norm_href.pop_back();
            while (!parent_path.empty() && parent_path.back() == '/') parent_path.pop_back();
            if (norm_href == parent_path) continue;

            if (company_name.empty()) continue;

            // Step 2: Build the child folder URL and PROPFIND it
            std::string child_url;
            if (!result->child_folder.empty()) {
                // entry.href is already the full path from server root
                child_url = base_url + entry.href;
                if (child_url.back() != '/') child_url += '/';
                child_url += result->child_folder + "/";
            } else {
                // No child folder: list files directly in the company folder
                child_url = base_url + entry.href;
                if (child_url.back() != '/') child_url += '/';
            }

            CurlHeaders child_headers;
            BuildAuthHeaders(child_headers, result->username, result->password);
            child_headers.append("Depth: 1");
            child_headers.append("Content-Type: application/xml");

            long child_http_code = 0;
            std::string child_request_url = NormalizeRequestUrl(child_url);
            std::string child_response = curl_propfind(child_request_url, PROPFIND_BODY, child_headers, &child_http_code);

            // Skip if child_folder doesn't exist (404) or other error
            if (child_response.find("ERROR:") == 0 || child_http_code >= 400) {
                continue;
            }

            auto child_entries = ParsePropfindResponse(child_response);

            // Filter files by extension
            std::string ext_suffix = "." + result->file_type;
            for (auto &file_entry : child_entries) {
                if (file_entry.is_collection) continue;

                std::string file_href_decoded = PercentDecodePath(file_entry.href);
                std::string file_name = GetLastPathSegment(file_href_decoded);

                // Check extension match
                if (file_name.size() <= ext_suffix.size()) continue;
                std::string file_ext = file_name.substr(file_name.size() - ext_suffix.size());
                std::transform(file_ext.begin(), file_ext.end(), file_ext.begin(), ::tolower);
                if (file_ext != ext_suffix) continue;

                NextcloudFolderFileInfo info;
                info.parent_folder = company_name;
                info.child_folder = result->child_folder;
                info.file_name = file_name;
                info.download_url = base_url + file_entry.href;
                result->files.push_back(info);
            }
        }
    }

    if (result->files.empty()) {
        // No files found - return a minimal schema with metadata columns only
        names = {"parent_folder", "child_folder", "file_name"};
        return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        return result;
    }

    // Step 3: Download the first file to detect schema
    {
        CurlHeaders headers;
        BuildAuthHeaders(headers, result->username, result->password);

        long http_code = 0;
        std::string request_url = NormalizeRequestUrl(result->files[0].download_url);
        std::string body = curl_get(request_url, headers, &http_code);

        if (body.find("ERROR:") == 0 || http_code >= 400 || body.empty()) {
            throw IOException("Failed to download first file for schema detection: " + result->files[0].download_url);
        }

        std::vector<std::string> col_names;
        std::vector<LogicalType> col_types;
        std::vector<std::vector<Value>> rows;

        std::string read_error;
        if (!ReadFileViaDuckDB(context, body, result->file_type, result->all_varchar,
                               result->ignore_errors, result->reader_options,
                               result->sheet, result->range,
                               col_names, col_types, rows, &read_error)) {
            // DuckDB reader failed - try custom CSV parser as fallback for text files
            if (!IsBinaryFileType(result->file_type)) {
                ParseCSVContent(body, col_names, col_types, rows);
            } else {
                throw IOException("Could not read file: " + result->files[0].file_name +
                                  (read_error.empty() ? "" : " - " + read_error));
            }
        }

        if (col_names.empty()) {
            throw IOException("Could not detect schema from first file: " + result->files[0].file_name);
        }

        // Normalize column names to snake_case
        for (auto &name : col_names) {
            name = NormalizeColumnName(name);
        }

        result->column_names = col_names;
        result->column_types = col_types;
        result->first_file_col_count = col_names.size();

        // Store first file rows with metadata prepended
        for (auto &row : rows) {
            std::vector<Value> full_row;
            full_row.push_back(Value(result->files[0].parent_folder));
            full_row.push_back(Value(result->files[0].child_folder));
            full_row.push_back(Value(result->files[0].file_name));
            for (auto &val : row) {
                full_row.push_back(std::move(val));
            }
            result->first_file_rows.push_back(std::move(full_row));
        }
    }

    // Build return schema: metadata columns + data columns
    names.push_back("parent_folder");
    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("child_folder");
    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("file_name");
    return_types.push_back(LogicalType::VARCHAR);

    for (size_t i = 0; i < result->column_names.size(); i++) {
        names.push_back(result->column_names[i]);
        return_types.push_back(result->column_types[i]);
    }

    return result;
}

static unique_ptr<GlobalTableFunctionState> NextcloudFolderInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind = input.bind_data->Cast<NextcloudFolderBindData>();
    auto state = make_uniq<NextcloudFolderGlobalState>();

    // Copy first file rows
    for (auto &row : bind.first_file_rows) {
        state->rows.push_back(row);
    }

    // Download and parse remaining files (index 1+)
    for (size_t file_idx = 1; file_idx < bind.files.size(); file_idx++) {
        auto &file_info = bind.files[file_idx];

        CurlHeaders headers;
        BuildAuthHeaders(headers, bind.username, bind.password);

        long http_code = 0;
        std::string request_url = NormalizeRequestUrl(file_info.download_url);
        std::string body = curl_get(request_url, headers, &http_code);

        // Skip files that fail to download
        if (body.find("ERROR:") == 0 || http_code >= 400 || body.empty()) {
            continue;
        }

        std::vector<std::string> col_names;
        std::vector<LogicalType> col_types;
        std::vector<std::vector<Value>> rows;

        if (!ReadFileViaDuckDB(context, body, bind.file_type, bind.all_varchar,
                               bind.ignore_errors, bind.reader_options,
                               bind.sheet, bind.range,
                               col_names, col_types, rows)) {
            // Fallback for text files
            if (!IsBinaryFileType(bind.file_type)) {
                ParseCSVContent(body, col_names, col_types, rows);
            } else {
                continue;  // Skip binary files that fail to parse
            }
        }

        // Skip files with different column count (schema mismatch)
        if (col_names.size() != bind.first_file_col_count) {
            continue;
        }

        // Prepend metadata columns to each row
        for (auto &row : rows) {
            std::vector<Value> full_row;
            full_row.push_back(Value(file_info.parent_folder));
            full_row.push_back(Value(file_info.child_folder));
            full_row.push_back(Value(file_info.file_name));
            for (auto &val : row) {
                full_row.push_back(std::move(val));
            }
            state->rows.push_back(std::move(full_row));
        }
    }

    return state;
}

static void NextcloudFolderScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NextcloudFolderGlobalState>();

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.current_row < state.rows.size() && count < max_count) {
        auto &row = state.rows[state.current_row];
        for (idx_t col = 0; col < row.size() && col < output.ColumnCount(); col++) {
            output.SetValue(col, count, row[col]);
        }
        state.current_row++;
        count++;
    }
    output.SetCardinality(count);
}

// ============================================================================
// Registration
// ============================================================================

void RegisterNextcloudFunctions(ExtensionLoader &loader) {
    // next_cloud - single file download
    TableFunction func("next_cloud", {LogicalType::VARCHAR}, NextcloudScan, NextcloudBind, NextcloudInit);
    func.named_parameters["username"] = LogicalType::VARCHAR;
    func.named_parameters["password"] = LogicalType::VARCHAR;
    func.named_parameters["headers"] = LogicalType::VARCHAR;
    func.named_parameters["all_varchar"] = LogicalType::BOOLEAN;
    func.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
    func.named_parameters["reader_options"] = LogicalType::VARCHAR;
    func.named_parameters["sheet"] = LogicalType::VARCHAR;
    func.named_parameters["range"] = LogicalType::VARCHAR;
    loader.RegisterFunction(func);

    // next_cloud_folder - scan parent folder's company subfolders
    TableFunction folder_func("next_cloud_folder", {LogicalType::VARCHAR},
                              NextcloudFolderScan, NextcloudFolderBind, NextcloudFolderInit);
    folder_func.named_parameters["child_folder"] = LogicalType::VARCHAR;
    folder_func.named_parameters["file_type"] = LogicalType::VARCHAR;
    folder_func.named_parameters["username"] = LogicalType::VARCHAR;
    folder_func.named_parameters["password"] = LogicalType::VARCHAR;
    folder_func.named_parameters["all_varchar"] = LogicalType::BOOLEAN;
    folder_func.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
    folder_func.named_parameters["reader_options"] = LogicalType::VARCHAR;
    folder_func.named_parameters["sheet"] = LogicalType::VARCHAR;
    folder_func.named_parameters["range"] = LogicalType::VARCHAR;
    loader.RegisterFunction(folder_func);
}

} // namespace stps
} // namespace duckdb
