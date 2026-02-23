#include "nextcloud_functions.hpp"
#include "webdav_utils.hpp"
#include "shared/archive_utils.hpp"
#include "curl_utils.hpp"
#include "case_transform.hpp"
#include "gobd_reader.hpp"
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
#include <unordered_map>

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
    std::string encoding;

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
                               std::string *error_out = nullptr,
                               const std::string &encoding = "");

static unique_ptr<FunctionData> NextcloudBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<NextcloudBindData>();

    if (input.inputs.empty()) {
        throw BinderException("stps_nextcloud requires a URL argument");
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
        } else if (kv.first == "encoding") {
            result->encoding = kv.second.ToString();
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
                                      col_names, col_types, mat_rows, nullptr, result->encoding);

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
    std::string encoding;

    // Discovered files
    std::vector<NextcloudFolderFileInfo> files;

    // Unified schema from all files (UNION ALL BY NAME)
    std::vector<std::string> column_names;
    std::vector<LogicalType> column_types;

    // All materialized rows (metadata + data columns)
    std::vector<std::vector<Value>> all_rows;

    // Track skipped subfolders for error reporting
    struct SkippedFolder {
        std::string folder_name;
        std::string error;
    };
    std::vector<SkippedFolder> skipped_folders;
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

// Escape a string for use inside a single-quoted SQL literal (double any single quotes)
static std::string EscapeSqlLiteral(const std::string &s) {
    std::string result;
    result.reserve(s.size());
    for (char c : s) {
        if (c == '\'') {
            result += "''";
        } else {
            result += c;
        }
    }
    return result;
}

// Validate reader_options to prevent SQL injection.
// Only allows safe characters: alphanumeric, spaces, commas, equals, dots, underscores,
// single-quoted strings (with no unbalanced quotes), square brackets, and parentheses.
// Rejects semicolons, double-dashes, and block comments.
static bool ValidateReaderOptions(const std::string &opts) {
    if (opts.empty()) return true;
    // Reject statement terminators and comment markers
    if (opts.find(';') != std::string::npos) return false;
    if (opts.find("--") != std::string::npos) return false;
    if (opts.find("/*") != std::string::npos) return false;
    if (opts.find("*/") != std::string::npos) return false;
    // Check for balanced single quotes
    int quote_count = 0;
    for (char c : opts) {
        if (c == '\'') quote_count++;
    }
    if (quote_count % 2 != 0) return false;
    return true;
}

// Deduplicate column names by appending _1, _2, etc. for repeats
static void DeduplicateColumnNames(std::vector<std::string> &col_names) {
    std::unordered_map<std::string, int> name_counts;
    // First pass: count occurrences
    for (const auto &name : col_names) {
        name_counts[name]++;
    }
    // Second pass: rename duplicates (first occurrence keeps original name)
    std::unordered_map<std::string, int> name_seen;
    for (auto &name : col_names) {
        if (name_counts[name] > 1) {
            int idx = name_seen[name]++;
            if (idx > 0) {
                name = name + "_" + std::to_string(idx);
            }
        }
    }
}

// Unified file reader: writes content to temp file, reads via DuckDB's built-in readers with options.
// Handles CSV, TSV, Parquet, Arrow, Feather, XLSX, XLS.
// Returns true on success, false on failure.
static bool ReadFileViaDuckDB(ClientContext &context, const std::string &body, const std::string &file_type,
                               bool all_varchar, bool ignore_errors, const std::string &reader_options,
                               const std::string &sheet, const std::string &range,
                               std::vector<std::string> &col_names, std::vector<LogicalType> &col_types,
                               std::vector<std::vector<Value>> &rows,
                               std::string *error_out,
                               const std::string &encoding) {
    // Validate reader_options to prevent SQL injection
    if (!reader_options.empty() && !ValidateReaderOptions(reader_options)) {
        if (error_out) *error_out = "Invalid reader_options: contains disallowed characters (;, --, /*)";
        return false;
    }

    std::string ext = file_type.empty() ? "csv" : file_type;
    std::string temp_path = GenerateTempFilename(ext);

    // For text-based formats, convert encoding to UTF-8 if specified
    bool is_text = (ext == "csv" || ext == "tsv" || ext == "txt");
    const std::string *write_ptr = &body;
    std::string converted;
    if (is_text) {
        if (!encoding.empty()) {
            converted = ConvertToUtf8(body, encoding);
            write_ptr = &converted;
        } else {
            converted = EnsureUtf8(body);
            write_ptr = &converted;
        }
    }

    std::ofstream out(temp_path, std::ios::binary);
    if (!out) return false;
    out.write(write_ptr->data(), write_ptr->size());
    out.close();

    try {
        auto &db = DatabaseInstance::GetDatabase(context);
        Connection conn(db);

        // Build the reader expression based on file type
        std::string read_expr;

        if (ext == "xlsx" || ext == "xls") {
            conn.Query("INSTALL rusty_sheet FROM community");
            conn.Query("LOAD rusty_sheet");
            read_expr = "read_sheet('" + temp_path + "'";
            if (!sheet.empty()) {
                read_expr += ", sheet='" + EscapeSqlLiteral(sheet) + "'";
            }
            if (!range.empty()) {
                read_expr += ", range='" + EscapeSqlLiteral(range) + "'";
            }
            if (all_varchar) {
                read_expr += ", columns={'*': 'VARCHAR'}";
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
            std::string err = schema_result->GetError();

            // Handle duplicate column names in Excel files by retrying with header=false
            if ((ext == "xlsx" || ext == "xls") &&
                err.find("duplicate column name") != std::string::npos) {

                // Step 1: Read with header=false + all VARCHAR to get correct column names
                // (text headers stay as strings instead of becoming NULL in numeric columns)
                std::string names_expr = "read_sheet('" + temp_path + "', header=false, columns={'*': 'VARCHAR'}";
                if (!sheet.empty()) {
                    names_expr += ", sheet='" + EscapeSqlLiteral(sheet) + "'";
                }
                if (!range.empty()) {
                    names_expr += ", range='" + EscapeSqlLiteral(range) + "'";
                }
                names_expr += ")";

                auto names_result = conn.Query("SELECT * FROM " + names_expr + " LIMIT 1");
                if (names_result->HasError()) {
                    if (error_out) *error_out = names_result->GetError();
                    std::remove(temp_path.c_str());
                    return false;
                }

                // Extract column names from first row, deduplicate
                auto names_chunk = names_result->Fetch();
                if (!names_chunk || names_chunk->size() == 0) {
                    if (error_out) *error_out = "No rows found (including header)";
                    std::remove(temp_path.c_str());
                    return false;
                }
                for (idx_t c = 0; c < names_chunk->ColumnCount(); c++) {
                    auto val = names_chunk->GetValue(c, 0);
                    col_names.push_back(val.IsNull() ? "column" : val.ToString());
                }
                DeduplicateColumnNames(col_names);

                // Step 2: Read data with native types (skip header row via OFFSET 1)
                std::string data_expr = "read_sheet('" + temp_path + "', header=false";
                if (!sheet.empty()) {
                    data_expr += ", sheet='" + EscapeSqlLiteral(sheet) + "'";
                }
                if (!range.empty()) {
                    data_expr += ", range='" + EscapeSqlLiteral(range) + "'";
                }
                if (all_varchar) {
                    data_expr += ", columns={'*': 'VARCHAR'}";
                }
                if (!reader_options.empty()) {
                    data_expr += ", " + reader_options;
                }
                data_expr += ")";

                std::string data_query = "SELECT * FROM " + data_expr + " OFFSET 1";
                auto data_result = conn.Query(data_query);
                if (data_result->HasError()) {
                    if (error_out) *error_out = data_result->GetError();
                    std::remove(temp_path.c_str());
                    return false;
                }

                // Use actual column types from the data query
                for (idx_t i = 0; i < data_result->ColumnCount(); i++) {
                    col_types.push_back(data_result->types[i]);
                }

                // Collect data rows with native values (preserving DOUBLE precision)
                while (true) {
                    auto chunk = data_result->Fetch();
                    if (!chunk || chunk->size() == 0) break;
                    for (idx_t r = 0; r < chunk->size(); r++) {
                        std::vector<Value> rv;
                        for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
                            rv.push_back(chunk->GetValue(c, r));
                        }
                        rows.push_back(std::move(rv));
                    }
                }

                std::remove(temp_path.c_str());
                return true;
            }

            if (error_out) *error_out = err;
            std::remove(temp_path.c_str());
            return false;
        }

        for (idx_t i = 0; i < schema_result->ColumnCount(); i++) {
            col_names.push_back(schema_result->ColumnName(i));
            if (all_varchar && (ext == "xlsx" || ext == "xls")) {
                col_types.push_back(LogicalType::VARCHAR);
            } else {
                col_types.push_back(schema_result->types[i]);
            }
        }

        // Build data query — for xlsx with all_varchar, cast all columns to VARCHAR
        std::string data_query;
        if (all_varchar && (ext == "xlsx" || ext == "xls")) {
            data_query = "SELECT ";
            for (idx_t i = 0; i < col_names.size(); i++) {
                if (i > 0) data_query += ", ";
                data_query += "CAST(\"" + col_names[i] + "\" AS VARCHAR) AS \"" + col_names[i] + "\"";
            }
            data_query += " FROM " + read_expr;
        } else {
            data_query = base_query;
        }

        // Read data
        auto data_result = conn.Query(data_query);
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
        throw BinderException("stps_nextcloud_folder requires a parent URL argument");
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
        } else if (kv.first == "encoding") {
            result->encoding = kv.second.ToString();
        }
    }

    // Ensure parent URL ends with /
    if (!result->parent_url.empty() && result->parent_url.back() != '/') {
        result->parent_url += '/';
    }

    std::string base_url = GetBaseUrl(result->parent_url);

    // Step 1: PROPFIND the parent folder to list company subfolders
    std::vector<PropfindEntry> parent_entries;
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

        parent_entries = ParsePropfindResponse(response);

        // For each collection entry (skip the parent itself), PROPFIND child_folder
        for (auto &entry : parent_entries) {
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

            // Track if child_folder doesn't exist (404) or other error
            if (child_response.find("ERROR:") == 0 || child_http_code >= 400) {
                result->skipped_folders.push_back({company_name,
                    child_http_code >= 400
                        ? "PROPFIND failed: HTTP " + std::to_string(child_http_code)
                        : "PROPFIND failed: " + child_response.substr(0, 200)});
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

    // Also check for files directly in the parent folder (not just in subfolders)
    if (!parent_entries.empty()) {
        std::string ext_suffix = "." + result->file_type;
        std::string parent_folder_name = GetLastPathSegment(
            PercentDecodePath(result->parent_url));

        for (auto &entry : parent_entries) {
            if (entry.is_collection) continue;

            std::string file_href_decoded = PercentDecodePath(entry.href);
            std::string file_name = GetLastPathSegment(file_href_decoded);

            if (file_name.size() <= ext_suffix.size()) continue;
            std::string file_ext = file_name.substr(file_name.size() - ext_suffix.size());
            std::transform(file_ext.begin(), file_ext.end(), file_ext.begin(), ::tolower);
            if (file_ext != ext_suffix) continue;

            NextcloudFolderFileInfo info;
            info.parent_folder = parent_folder_name;
            info.child_folder = "";
            info.file_name = file_name;
            info.download_url = base_url + entry.href;
            result->files.push_back(info);
        }
    }

    if (result->files.empty()) {
        // No files found - return a minimal schema with metadata columns + _read_status
        names = {"parent_folder", "child_folder", "file_name", "_read_status"};
        return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        // Include error rows for any skipped subfolders
        for (auto &sf : result->skipped_folders) {
            result->all_rows.push_back({
                Value(sf.folder_name), Value(""), Value(""), Value(sf.error)
            });
        }
        return result;
    }

    // Step 3: Download and parse ALL files (UNION ALL BY NAME)
    struct ParsedFileData {
        std::vector<std::string> col_names;
        std::vector<LogicalType> col_types;
        std::vector<std::vector<Value>> rows;
        size_t file_idx;
        std::string error;  // empty = success
    };
    std::vector<ParsedFileData> parsed_files;

    for (size_t file_idx = 0; file_idx < result->files.size(); file_idx++) {
        CurlHeaders headers;
        BuildAuthHeaders(headers, result->username, result->password);

        long http_code = 0;
        std::string request_url = NormalizeRequestUrl(result->files[file_idx].download_url);
        std::string body = curl_get(request_url, headers, &http_code);

        if (body.find("ERROR:") == 0 || http_code >= 400 || body.empty()) {
            ParsedFileData pf;
            pf.file_idx = file_idx;
            pf.error = body.empty() ? "Empty response from server"
                     : (http_code >= 400 ? "HTTP " + std::to_string(http_code) : body.substr(0, 200));
            parsed_files.push_back(std::move(pf));
            continue;
        }

        std::vector<std::string> col_names;
        std::vector<LogicalType> col_types;
        std::vector<std::vector<Value>> rows;

        std::string read_error;
        if (!ReadFileViaDuckDB(context, body, result->file_type, result->all_varchar,
                               result->ignore_errors, result->reader_options,
                               result->sheet, result->range,
                               col_names, col_types, rows, &read_error, result->encoding)) {
            if (!IsBinaryFileType(result->file_type)) {
                ParseCSVContent(body, col_names, col_types, rows);
            }
            if (col_names.empty() || rows.empty()) {
                ParsedFileData pf;
                pf.file_idx = file_idx;
                pf.error = read_error.empty() ? "Failed to parse file" : read_error;
                parsed_files.push_back(std::move(pf));
                continue;
            }
        }

        if (col_names.empty() || rows.empty()) {
            ParsedFileData pf;
            pf.file_idx = file_idx;
            pf.error = "File has no data rows";
            parsed_files.push_back(std::move(pf));
            continue;
        }

        // Normalize column names to snake_case
        for (auto &name : col_names) {
            name = NormalizeColumnName(name);
        }
        // Deduplicate after normalization (e.g. "Zeitraum A" and "ZeitraumA" both -> "zeitraum_a")
        DeduplicateColumnNames(col_names);

        ParsedFileData pf;
        pf.col_names = std::move(col_names);
        pf.col_types = std::move(col_types);
        pf.rows = std::move(rows);
        pf.file_idx = file_idx;
        parsed_files.push_back(std::move(pf));
    }

    // Check if ANY files were parsed successfully
    bool has_successful_files = false;
    for (auto &pf : parsed_files) {
        if (pf.error.empty()) { has_successful_files = true; break; }
    }

    if (!has_successful_files) {
        // No files with data — return metadata + _read_status with error rows
        names = {"parent_folder", "child_folder", "file_name", "_read_status"};
        return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
        for (auto &sf : result->skipped_folders) {
            result->all_rows.push_back({
                Value(sf.folder_name), Value(""), Value(""), Value(sf.error)
            });
        }
        for (auto &pf : parsed_files) {
            result->all_rows.push_back({
                Value(result->files[pf.file_idx].parent_folder),
                Value(result->files[pf.file_idx].child_folder),
                Value(result->files[pf.file_idx].file_name),
                Value(pf.error)
            });
        }
        return result;
    }

    // Build unified schema: superset of all column names from successful files (UNION ALL BY NAME)
    // When the same column appears with different types across files, promote to the wider type
    // (e.g., BIGINT + DOUBLE → DOUBLE) to avoid truncating decimal values.
    std::unordered_map<std::string, size_t> col_name_to_idx;
    for (auto &pf : parsed_files) {
        if (!pf.error.empty()) continue;
        for (size_t i = 0; i < pf.col_names.size(); i++) {
            auto it = col_name_to_idx.find(pf.col_names[i]);
            if (it == col_name_to_idx.end()) {
                col_name_to_idx[pf.col_names[i]] = result->column_names.size();
                result->column_names.push_back(pf.col_names[i]);
                result->column_types.push_back(pf.col_types[i]);
            } else {
                // Promote to wider type if types differ across files
                auto &existing_type = result->column_types[it->second];
                if (existing_type != pf.col_types[i]) {
                    existing_type = LogicalType::ForceMaxLogicalType(existing_type, pf.col_types[i]);
                }
            }
        }
    }

    // Add error rows for skipped subfolders
    for (auto &sf : result->skipped_folders) {
        std::vector<Value> row;
        row.reserve(4 + result->column_names.size());
        row.push_back(Value(sf.folder_name));
        row.push_back(Value(""));
        row.push_back(Value(""));
        row.push_back(Value(sf.error));
        for (size_t i = 0; i < result->column_names.size(); i++) {
            row.push_back(Value());
        }
        result->all_rows.push_back(std::move(row));
    }

    // Map each file's rows to the unified schema, NULLs for missing columns
    for (auto &pf : parsed_files) {
        if (!pf.error.empty()) {
            // Error row: metadata + error status + NULL data columns
            std::vector<Value> row;
            row.reserve(4 + result->column_names.size());
            row.push_back(Value(result->files[pf.file_idx].parent_folder));
            row.push_back(Value(result->files[pf.file_idx].child_folder));
            row.push_back(Value(result->files[pf.file_idx].file_name));
            row.push_back(Value(pf.error));
            for (size_t i = 0; i < result->column_names.size(); i++) {
                row.push_back(Value());
            }
            result->all_rows.push_back(std::move(row));
            continue;
        }

        // Build column mapping: unified_col_idx -> file_col_idx (-1 if missing)
        std::vector<int> col_map(result->column_names.size(), -1);
        for (size_t i = 0; i < pf.col_names.size(); i++) {
            auto it = col_name_to_idx.find(pf.col_names[i]);
            if (it != col_name_to_idx.end()) {
                col_map[it->second] = static_cast<int>(i);
            }
        }

        for (auto &row : pf.rows) {
            std::vector<Value> full_row;
            full_row.reserve(4 + result->column_names.size());
            // Metadata columns
            full_row.push_back(Value(result->files[pf.file_idx].parent_folder));
            full_row.push_back(Value(result->files[pf.file_idx].child_folder));
            full_row.push_back(Value(result->files[pf.file_idx].file_name));
            full_row.push_back(Value());  // _read_status = NULL (success)
            // Data columns mapped to unified schema
            for (size_t col = 0; col < result->column_names.size(); col++) {
                if (col_map[col] >= 0 && static_cast<size_t>(col_map[col]) < row.size()) {
                    full_row.push_back(row[col_map[col]]);
                } else {
                    full_row.push_back(Value());
                }
            }
            result->all_rows.push_back(std::move(full_row));
        }
    }

    // Build return schema: metadata columns + _read_status + unified data columns
    names.push_back("parent_folder");
    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("child_folder");
    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("file_name");
    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("_read_status");
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

    // All rows are already materialized in bind phase (UNION ALL BY NAME)
    for (const auto &row : bind.all_rows) {
        state->rows.push_back(row);
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
    TableFunction func("stps_nextcloud", {LogicalType::VARCHAR}, NextcloudScan, NextcloudBind, NextcloudInit);
    func.named_parameters["username"] = LogicalType::VARCHAR;
    func.named_parameters["password"] = LogicalType::VARCHAR;
    func.named_parameters["headers"] = LogicalType::VARCHAR;
    func.named_parameters["all_varchar"] = LogicalType::BOOLEAN;
    func.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
    func.named_parameters["reader_options"] = LogicalType::VARCHAR;
    func.named_parameters["sheet"] = LogicalType::VARCHAR;
    func.named_parameters["range"] = LogicalType::VARCHAR;
    func.named_parameters["encoding"] = LogicalType::VARCHAR;
    loader.RegisterFunction(func);

    // next_cloud_folder - scan parent folder's company subfolders
    TableFunction folder_func("stps_nextcloud_folder", {LogicalType::VARCHAR},
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
    folder_func.named_parameters["encoding"] = LogicalType::VARCHAR;
    loader.RegisterFunction(folder_func);
}

} // namespace stps
} // namespace duckdb
