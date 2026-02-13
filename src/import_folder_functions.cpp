#include "import_folder_functions.hpp"
#include "gobd_reader.hpp"
#include "shared/archive_utils.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <set>
#include <random>
#include <chrono>

#ifdef _WIN32
#define NOMINMAX
#include <windows.h>
#else
#include <dirent.h>
#include <sys/stat.h>
#endif

#ifdef HAVE_CURL
#include "webdav_utils.hpp"
#include "curl_utils.hpp"
#endif

namespace duckdb {
namespace stps {

// ============================================================================
// Shared helpers (same patterns as gobd_reader.cpp)
// ============================================================================

static string ToSnakeCase(const string &input) {
    string result;
    result.reserve(input.size());
    bool last_was_underscore = false;

    for (size_t i = 0; i < input.size(); i++) {
        char c = input[i];
        if (std::isalnum(static_cast<unsigned char>(c))) {
            if (std::isupper(static_cast<unsigned char>(c)) && !result.empty() && !last_was_underscore) {
                char prev = result.back();
                if (std::islower(static_cast<unsigned char>(prev)) || std::isdigit(static_cast<unsigned char>(prev))) {
                    result += '_';
                }
            }
            result += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            last_was_underscore = false;
        } else {
            if (!result.empty() && !last_was_underscore) {
                result += '_';
                last_was_underscore = true;
            }
        }
    }

    while (!result.empty() && result.back() == '_') {
        result.pop_back();
    }

    return result.empty() ? "column" : result;
}

static string EscapeIdentifier(const string &name) {
    string escaped;
    for (char c : name) {
        if (c == '"') escaped += "\"\"";
        else escaped += c;
    }
    return "\"" + escaped + "\"";
}

static string EscapeStringLiteral(const string &value) {
    string escaped;
    for (char c : value) {
        if (c == '\'') escaped += "''";
        else escaped += c;
    }
    return "'" + escaped + "'";
}

static string NormalizeSqlPath(const string &path) {
    string result = path;
    for (auto &c : result) {
        if (c == '\\') c = '/';
    }
    return result;
}

// ============================================================================
// Supported file types
// ============================================================================

static bool IsSupportedImportFile(const string &filename) {
    string ext = GetFileExtension(filename);
    return ext == "csv" || ext == "tsv" || ext == "parquet" ||
           ext == "json" || ext == "xlsx" || ext == "xls";
}

// ============================================================================
// Reader options (forwarded to read_csv_auto / read_sheet / read_json_auto / read_parquet)
// ============================================================================

struct ReaderOptions {
    // Common
    bool all_varchar = false;
    bool header_set = false;
    bool header = true;
    bool ignore_errors = false;

    // CSV-specific
    string delimiter;
    string quote;
    string escape;
    int64_t skip = 0;
    string null_str;

    // Excel-specific
    string sheet;
    string range;

    // Generic passthrough (appended as-is to reader call)
    string reader_options;
};

// ============================================================================
// Import result
// ============================================================================

struct ImportFileResult {
    string table_name;
    string file_name;
    int64_t rows_imported = 0;
    int32_t columns_created = 0;
    string error;
};

// ============================================================================
// Generate unique temp file path preserving extension
// ============================================================================

static std::string GenerateImportTempPath(const string &original_filename) {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1000, 9999);

    string ext = GetFileExtension(original_filename);
    std::string temp_dir = GetTempDirectory();
    return temp_dir + "stps_import_" + std::to_string(ms) + "_" + std::to_string(dis(gen)) +
           (ext.empty() ? "" : "." + ext);
}

// ============================================================================
// Parse ReaderOptions from named parameters
// ============================================================================

static ReaderOptions ParseReaderOptions(const named_parameter_map_t &params) {
    ReaderOptions opts;
    for (auto &kv : params) {
        if (kv.first == "all_varchar") {
            opts.all_varchar = BooleanValue::Get(kv.second);
        } else if (kv.first == "header") {
            opts.header_set = true;
            opts.header = BooleanValue::Get(kv.second);
        } else if (kv.first == "ignore_errors") {
            opts.ignore_errors = BooleanValue::Get(kv.second);
        } else if (kv.first == "delimiter" || kv.first == "sep") {
            opts.delimiter = kv.second.ToString();
        } else if (kv.first == "quote") {
            opts.quote = kv.second.ToString();
        } else if (kv.first == "escape") {
            opts.escape = kv.second.ToString();
        } else if (kv.first == "skip") {
            opts.skip = kv.second.GetValue<int64_t>();
        } else if (kv.first == "null_str" || kv.first == "nullstr") {
            opts.null_str = kv.second.ToString();
        } else if (kv.first == "sheet") {
            opts.sheet = kv.second.ToString();
        } else if (kv.first == "range") {
            opts.range = kv.second.ToString();
        } else if (kv.first == "reader_options") {
            opts.reader_options = kv.second.ToString();
        }
    }
    return opts;
}

// ============================================================================
// Core import logic for a single file
// ============================================================================

static ImportFileResult ImportSingleFile(ClientContext &context, const string &file_path,
                                          const string &file_name, bool overwrite,
                                          std::set<string> &used_table_names,
                                          const ReaderOptions &opts = ReaderOptions()) {
    ImportFileResult result;
    result.file_name = file_name;

    try {
        Connection conn(context.db->GetDatabase(context));

        // 1. Determine file type
        string ext = GetFileExtension(file_name);

        // 2. Derive table name from filename
        string base_name = file_name;
        size_t dot_pos = base_name.rfind('.');
        if (dot_pos != string::npos) {
            base_name = base_name.substr(0, dot_pos);
        }
        string table_name = ToSnakeCase(base_name);

        // Ensure valid SQL identifier (can't start with digit)
        if (!table_name.empty() && std::isdigit(static_cast<unsigned char>(table_name[0]))) {
            table_name = "t_" + table_name;
        }
        if (table_name.empty()) {
            table_name = "imported_table";
        }

        // Handle duplicate table names
        string unique_name = table_name;
        int suffix = 2;
        while (used_table_names.count(unique_name)) {
            unique_name = table_name + "_" + std::to_string(suffix++);
        }
        table_name = unique_name;
        used_table_names.insert(table_name);
        result.table_name = table_name;

        string escaped_table = EscapeIdentifier(table_name);

        // 3. Handle overwrite
        {
            auto check = conn.Query("SELECT 1 FROM information_schema.tables WHERE table_name = " +
                                    EscapeStringLiteral(table_name) + " LIMIT 1");
            if (check && check->RowCount() > 0) {
                if (overwrite) {
                    conn.Query("DROP TABLE IF EXISTS " + escaped_table);
                } else {
                    result.error = "Table already exists: " + table_name + " (use overwrite := true to replace)";
                    return result;
                }
            }
        }

        // 4. Normalize path for SQL
        string sql_path = EscapeStringLiteral(NormalizeSqlPath(file_path));

        // 5. Install rusty_sheet if xlsx/xls
        if (ext == "xlsx" || ext == "xls") {
            conn.Query("INSTALL rusty_sheet FROM community");
            conn.Query("LOAD rusty_sheet");
        }

        // 6. CTAS with appropriate reader
        string create_sql;
        if (ext == "csv" || ext == "tsv") {
            string reader_expr = "read_csv_auto(" + sql_path;
            if (opts.all_varchar) reader_expr += ", all_varchar=true";
            if (opts.header_set) reader_expr += opts.header ? ", header=true" : ", header=false";
            if (opts.ignore_errors) reader_expr += ", ignore_errors=true";
            if (!opts.delimiter.empty()) reader_expr += ", delimiter=" + EscapeStringLiteral(opts.delimiter);
            if (!opts.quote.empty()) reader_expr += ", quote=" + EscapeStringLiteral(opts.quote);
            if (!opts.escape.empty()) reader_expr += ", escape=" + EscapeStringLiteral(opts.escape);
            if (opts.skip > 0) reader_expr += ", skip=" + std::to_string(opts.skip);
            if (!opts.null_str.empty()) reader_expr += ", null_padding=true, nullstr=" + EscapeStringLiteral(opts.null_str);
            if (!opts.reader_options.empty()) reader_expr += ", " + opts.reader_options;
            reader_expr += ")";
            create_sql = "CREATE TABLE " + escaped_table + " AS SELECT * FROM " + reader_expr;
        } else if (ext == "json") {
            string reader_expr = "read_json_auto(" + sql_path;
            if (opts.ignore_errors) reader_expr += ", ignore_errors=true";
            if (!opts.reader_options.empty()) reader_expr += ", " + opts.reader_options;
            reader_expr += ")";
            create_sql = "CREATE TABLE " + escaped_table + " AS SELECT * FROM " + reader_expr;
        } else if (ext == "parquet") {
            string reader_expr = "read_parquet(" + sql_path;
            if (!opts.reader_options.empty()) reader_expr += ", " + opts.reader_options;
            reader_expr += ")";
            create_sql = "CREATE TABLE " + escaped_table + " AS SELECT * FROM " + reader_expr;
        } else if (ext == "xlsx" || ext == "xls") {
            string sheet_expr = "read_sheet(" + sql_path;
            if (!opts.sheet.empty()) sheet_expr += ", sheet=" + EscapeStringLiteral(opts.sheet);
            if (!opts.range.empty()) sheet_expr += ", range=" + EscapeStringLiteral(opts.range);
            if (opts.header_set) sheet_expr += opts.header ? ", header=true" : ", header=false";
            if (opts.all_varchar) sheet_expr += ", columns={'*': 'VARCHAR'}";
            if (!opts.reader_options.empty()) sheet_expr += ", " + opts.reader_options;
            sheet_expr += ")";
            create_sql = "CREATE TABLE " + escaped_table + " AS SELECT * FROM " + sheet_expr;
        } else {
            result.error = "Unsupported file type: " + ext;
            return result;
        }

        auto create_result = conn.Query(create_sql);
        if (!create_result || create_result->HasError()) {
            result.error = "Failed to import: " + (create_result ? create_result->GetError() : "unknown error");
            return result;
        }

        // 7. Rename columns to snake_case
        {
            vector<string> original_cols;
            auto cols_result = conn.Query(
                "SELECT column_name FROM information_schema.columns WHERE table_name = " +
                EscapeStringLiteral(table_name) + " ORDER BY ordinal_position");
            if (cols_result && !cols_result->HasError()) {
                for (idx_t i = 0; i < cols_result->RowCount(); i++) {
                    original_cols.push_back(cols_result->GetValue(0, i).ToString());
                }
            }

            std::set<string> used_col_names;
            for (auto &col : original_cols) {
                string new_name = ToSnakeCase(col);
                string unique_col = new_name;
                int col_suffix = 2;
                while (used_col_names.count(unique_col)) {
                    unique_col = new_name + "_" + std::to_string(col_suffix++);
                }
                used_col_names.insert(unique_col);

                if (col != unique_col) {
                    conn.Query("ALTER TABLE " + escaped_table + " RENAME COLUMN " +
                               EscapeIdentifier(col) + " TO " + EscapeIdentifier(unique_col));
                }
            }
        }

        // 8. Drop empty columns
        {
            vector<string> current_cols;
            auto cols_result = conn.Query(
                "SELECT column_name FROM information_schema.columns WHERE table_name = " +
                EscapeStringLiteral(table_name) + " ORDER BY ordinal_position");
            if (cols_result && !cols_result->HasError()) {
                for (idx_t i = 0; i < cols_result->RowCount(); i++) {
                    current_cols.push_back(cols_result->GetValue(0, i).ToString());
                }
            }

            if (!current_cols.empty()) {
                vector<string> cols_to_drop;

                string check_sql = "SELECT ";
                for (size_t i = 0; i < current_cols.size(); i++) {
                    if (i > 0) check_sql += ", ";
                    check_sql += "COUNT(CASE WHEN " + EscapeIdentifier(current_cols[i]) +
                                 " IS NOT NULL AND CAST(" + EscapeIdentifier(current_cols[i]) +
                                 " AS VARCHAR) <> '' THEN 1 END)";
                }
                check_sql += " FROM " + escaped_table;

                auto check_result = conn.Query(check_sql);
                if (check_result && !check_result->HasError() && check_result->RowCount() > 0) {
                    for (size_t i = 0; i < current_cols.size(); i++) {
                        auto count_val = check_result->GetValue(i, 0).GetValue<int64_t>();
                        if (count_val == 0) {
                            cols_to_drop.push_back(current_cols[i]);
                        }
                    }
                }

                for (auto &col_name : cols_to_drop) {
                    conn.Query("ALTER TABLE " + escaped_table + " DROP COLUMN " + EscapeIdentifier(col_name));
                }
            }
        }

        // Count rows and columns
        {
            auto count_result = conn.Query("SELECT COUNT(*) FROM " + escaped_table);
            if (count_result && count_result->RowCount() > 0) {
                result.rows_imported = count_result->GetValue(0, 0).GetValue<int64_t>();
            }
        }
        {
            auto cols_result = conn.Query(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = " +
                EscapeStringLiteral(table_name));
            if (cols_result && cols_result->RowCount() > 0) {
                result.columns_created = cols_result->GetValue(0, 0).GetValue<int32_t>();
            }
        }

        // 9. Smart cast
        if (result.rows_imported > 0) {
            string cast_sql = "CREATE OR REPLACE TABLE " + escaped_table +
                              " AS SELECT * FROM stps_smart_cast(" + EscapeStringLiteral(table_name) + ")";
            conn.Query(cast_sql);  // Non-fatal if fails
        }

    } catch (std::exception &e) {
        if (result.table_name.empty()) {
            string base_name = file_name;
            size_t dot_pos = base_name.rfind('.');
            if (dot_pos != string::npos) base_name = base_name.substr(0, dot_pos);
            result.table_name = ToSnakeCase(base_name);
        }
        result.error = string(e.what());
    }

    return result;
}

// ============================================================================
// stps_import_folder (local filesystem)
// ============================================================================

struct ImportFolderBindData : public TableFunctionData {
    vector<ImportFileResult> results;
};

struct ImportFolderGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> ImportFolderBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<ImportFolderBindData>();

    string folder_path = input.inputs[0].ToString();
    bool overwrite = false;
    ReaderOptions opts = ParseReaderOptions(input.named_parameters);

    for (auto &kv : input.named_parameters) {
        if (kv.first == "overwrite") {
            overwrite = BooleanValue::Get(kv.second);
        }
    }

    // Ensure trailing separator
    if (!folder_path.empty() && folder_path.back() != '/' && folder_path.back() != '\\') {
#ifdef _WIN32
        folder_path += '\\';
#else
        folder_path += '/';
#endif
    }

    // List and import files
    std::set<string> used_table_names;

#ifdef _WIN32
    WIN32_FIND_DATAA find_data;
    string search_path = folder_path + "*";
    HANDLE hFind = FindFirstFileA(search_path.c_str(), &find_data);
    if (hFind == INVALID_HANDLE_VALUE) {
        throw IOException("Folder does not exist or cannot be listed: " + folder_path);
    }
    do {
        string filename = find_data.cFileName;
        if (filename == "." || filename == "..") continue;
        if (find_data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) continue;
        if (!IsSupportedImportFile(filename)) continue;

        string file_path_str = folder_path + filename;
        auto import_result = ImportSingleFile(context, file_path_str, filename, overwrite, used_table_names, opts);
        result->results.push_back(import_result);
    } while (FindNextFileA(hFind, &find_data));
    FindClose(hFind);
#else
    DIR *dir = opendir(folder_path.c_str());
    if (!dir) {
        throw IOException("Folder does not exist or cannot be listed: " + folder_path);
    }
    struct dirent *entry;
    while ((entry = readdir(dir)) != nullptr) {
        string filename = entry->d_name;
        if (filename == "." || filename == "..") continue;

        // Check if regular file
        string file_path_str = folder_path + filename;
        struct stat st;
        if (stat(file_path_str.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) continue;
        if (!IsSupportedImportFile(filename)) continue;

        auto import_result = ImportSingleFile(context, file_path_str, filename, overwrite, used_table_names, opts);
        result->results.push_back(import_result);
    }
    closedir(dir);
#endif

    // Run clean_database after all imports
    if (!result->results.empty()) {
        Connection conn(context.db->GetDatabase(context));
        conn.Query("SELECT * FROM stps_clean_database()");
    }

    // Output schema
    names.emplace_back("table_name");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("file_name");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("rows_imported");
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("columns_created");
    return_types.emplace_back(LogicalType::INTEGER);
    names.emplace_back("error");
    return_types.emplace_back(LogicalType::VARCHAR);

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ImportFolderInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<ImportFolderGlobalState>();
}

static void ImportFolderScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<ImportFolderBindData>();
    auto &state = data_p.global_state->Cast<ImportFolderGlobalState>();

    idx_t count = 0;
    while (state.offset < bind_data.results.size() && count < STANDARD_VECTOR_SIZE) {
        auto &r = bind_data.results[state.offset];
        output.SetValue(0, count, Value(r.table_name));
        output.SetValue(1, count, Value(r.file_name));
        output.SetValue(2, count, Value::BIGINT(r.rows_imported));
        output.SetValue(3, count, Value::INTEGER(r.columns_created));
        output.SetValue(4, count, r.error.empty() ? Value() : Value(r.error));
        state.offset++;
        count++;
    }
    output.SetCardinality(count);
}

#ifdef HAVE_CURL

// ============================================================================
// Cloud helpers (same patterns as gobd_cloud_reader.cpp)
// ============================================================================

static std::string EnsureTrailingSlash(const std::string &url) {
    if (!url.empty() && url.back() != '/') {
        return url + "/";
    }
    return url;
}

static std::string ExtractDecodedPath(const std::string &url) {
    std::string decoded = PercentDecodePath(url);
    size_t scheme_end = decoded.find("://");
    if (scheme_end == std::string::npos) return decoded;
    size_t path_start = decoded.find('/', scheme_end + 3);
    if (path_start == std::string::npos) return "/";
    return decoded.substr(path_start);
}

static std::string NormalizeUrlPath(const std::string &path) {
    std::string decoded = PercentDecodePath(path);
    while (!decoded.empty() && decoded.back() == '/') decoded.pop_back();
    return decoded;
}

static bool IsParentEntry(const std::string &entry_href, const std::string &parent_url) {
    return NormalizeUrlPath(entry_href) == NormalizeUrlPath(ExtractDecodedPath(parent_url));
}

static std::string DownloadFile(const std::string &url, const std::string &username, const std::string &password,
                                long *http_code_out = nullptr) {
    CurlHeaders headers;
    BuildAuthHeaders(headers, username, password);

    long http_code = 0;
    std::string request_url = NormalizeRequestUrl(url);
    std::string body = curl_get(request_url, headers, &http_code);

    if (http_code_out) *http_code_out = http_code;

    if (body.find("ERROR:") == 0 || http_code >= 400 || body.empty()) {
        return "";
    }
    return body;
}

static std::vector<PropfindEntry> PropfindFolder(const std::string &url, const std::string &username,
                                                  const std::string &password) {
    CurlHeaders headers;
    BuildAuthHeaders(headers, username, password);
    headers.append("Depth: 1");
    headers.append("Content-Type: application/xml");

    long http_code = 0;
    std::string request_url = NormalizeRequestUrl(url);
    std::string response = curl_propfind(request_url, PROPFIND_BODY, headers, &http_code);

    if (response.find("ERROR:") == 0 || http_code >= 400) {
        return {};
    }

    return ParsePropfindResponse(response);
}

// ============================================================================
// stps_import_nextcloud_folder (cloud)
// ============================================================================

struct ImportNextcloudFolderBindData : public TableFunctionData {
    vector<ImportFileResult> results;
};

struct ImportNextcloudFolderGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> ImportNextcloudFolderBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<ImportNextcloudFolderBindData>();

    string folder_url = input.inputs[0].ToString();
    string username, password;
    bool overwrite = false;
    ReaderOptions opts = ParseReaderOptions(input.named_parameters);

    for (auto &kv : input.named_parameters) {
        if (kv.first == "username") {
            username = kv.second.ToString();
        } else if (kv.first == "password") {
            password = kv.second.ToString();
        } else if (kv.first == "overwrite") {
            overwrite = BooleanValue::Get(kv.second);
        }
    }

    folder_url = EnsureTrailingSlash(folder_url);
    string server_base = GetBaseUrl(folder_url);

    // PROPFIND the folder
    auto entries = PropfindFolder(folder_url, username, password);
    if (entries.empty()) {
        throw IOException("Could not list folder (PROPFIND failed or empty): " + folder_url);
    }

    std::set<string> used_table_names;

    for (auto &entry : entries) {
        // Skip collections and parent entry
        if (entry.is_collection) continue;
        if (IsParentEntry(entry.href, folder_url)) continue;

        string decoded_href = PercentDecodePath(entry.href);
        string filename = GetLastPathSegment(decoded_href);
        if (filename.empty() || !IsSupportedImportFile(filename)) continue;

        // Download the file
        string file_url = server_base + entry.href;
        string content = DownloadFile(file_url, username, password);
        if (content.empty()) {
            ImportFileResult r;
            r.file_name = filename;
            string bn = filename;
            size_t dp = bn.rfind('.');
            if (dp != string::npos) bn = bn.substr(0, dp);
            r.table_name = ToSnakeCase(bn);
            r.error = "Failed to download file: " + filename;
            result->results.push_back(r);
            continue;
        }

        // Write to temp file (preserving extension for reader detection)
        string temp_path = GenerateImportTempPath(filename);
        {
            std::ofstream out(temp_path, std::ios::binary);
            if (!out) {
                ImportFileResult r;
                r.file_name = filename;
                string bn = filename;
                size_t dp = bn.rfind('.');
                if (dp != string::npos) bn = bn.substr(0, dp);
                r.table_name = ToSnakeCase(bn);
                r.error = "Failed to create temp file for: " + filename;
                result->results.push_back(r);
                continue;
            }
            out.write(content.data(), content.size());
            out.close();
        }

        // Import the file
        auto import_result = ImportSingleFile(context, temp_path, filename, overwrite, used_table_names, opts);
        result->results.push_back(import_result);

        // Cleanup temp file
        std::remove(temp_path.c_str());
    }

    // Run clean_database after all imports
    if (!result->results.empty()) {
        Connection conn(context.db->GetDatabase(context));
        conn.Query("SELECT * FROM stps_clean_database()");
    }

    // Output schema
    names.emplace_back("table_name");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("file_name");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("rows_imported");
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("columns_created");
    return_types.emplace_back(LogicalType::INTEGER);
    names.emplace_back("error");
    return_types.emplace_back(LogicalType::VARCHAR);

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ImportNextcloudFolderInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<ImportNextcloudFolderGlobalState>();
}

static void ImportNextcloudFolderScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<ImportNextcloudFolderBindData>();
    auto &state = data_p.global_state->Cast<ImportNextcloudFolderGlobalState>();

    idx_t count = 0;
    while (state.offset < bind_data.results.size() && count < STANDARD_VECTOR_SIZE) {
        auto &r = bind_data.results[state.offset];
        output.SetValue(0, count, Value(r.table_name));
        output.SetValue(1, count, Value(r.file_name));
        output.SetValue(2, count, Value::BIGINT(r.rows_imported));
        output.SetValue(3, count, Value::INTEGER(r.columns_created));
        output.SetValue(4, count, r.error.empty() ? Value() : Value(r.error));
        state.offset++;
        count++;
    }
    output.SetCardinality(count);
}

#endif // HAVE_CURL

// ============================================================================
// Registration
// ============================================================================

static void RegisterReaderNamedParameters(TableFunction &func) {
    // Common
    func.named_parameters["all_varchar"] = LogicalType::BOOLEAN;
    func.named_parameters["header"] = LogicalType::BOOLEAN;
    func.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
    // CSV-specific
    func.named_parameters["delimiter"] = LogicalType::VARCHAR;
    func.named_parameters["sep"] = LogicalType::VARCHAR;
    func.named_parameters["quote"] = LogicalType::VARCHAR;
    func.named_parameters["escape"] = LogicalType::VARCHAR;
    func.named_parameters["skip"] = LogicalType::BIGINT;
    func.named_parameters["null_str"] = LogicalType::VARCHAR;
    func.named_parameters["nullstr"] = LogicalType::VARCHAR;
    // Excel-specific
    func.named_parameters["sheet"] = LogicalType::VARCHAR;
    func.named_parameters["range"] = LogicalType::VARCHAR;
    // Generic passthrough
    func.named_parameters["reader_options"] = LogicalType::VARCHAR;
}

void RegisterImportFolderFunctions(ExtensionLoader &loader) {
    // stps_import_folder(path, overwrite, ...)
    {
        TableFunction func("stps_import_folder",
                          {LogicalType::VARCHAR},
                          ImportFolderScan, ImportFolderBind, ImportFolderInit);
        func.named_parameters["overwrite"] = LogicalType::BOOLEAN;
        RegisterReaderNamedParameters(func);

        CreateTableFunctionInfo info(func);
        loader.RegisterFunction(info);
    }

#ifdef HAVE_CURL
    // stps_import_nextcloud_folder(url, username, password, overwrite, ...)
    {
        TableFunction func("stps_import_nextcloud_folder",
                          {LogicalType::VARCHAR},
                          ImportNextcloudFolderScan, ImportNextcloudFolderBind, ImportNextcloudFolderInit);
        func.named_parameters["username"] = LogicalType::VARCHAR;
        func.named_parameters["password"] = LogicalType::VARCHAR;
        func.named_parameters["overwrite"] = LogicalType::BOOLEAN;
        RegisterReaderNamedParameters(func);

        CreateTableFunctionInfo info(func);
        loader.RegisterFunction(info);
    }
#endif
}

} // namespace stps
} // namespace duckdb
