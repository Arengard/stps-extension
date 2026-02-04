#include "nextcloud_functions.hpp"
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

// Simple base64 for basic auth (handles UTF-8 input correctly)
static std::string Base64Encode(const std::string &input) {
    static const char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((input.size() + 2) / 3) * 4);

    int val = 0, valb = -6;
    for (unsigned char c : input) {
        val = (val << 8) + c;
        valb += 8;
        while (valb >= 0) {
            out.push_back(table[(val >> valb) & 0x3F]);
            valb -= 6;
        }
    }
    if (valb > -6) {
        out.push_back(table[((val << 8) >> (valb + 8)) & 0x3F]);
    }
    while (out.size() % 4) {
        out.push_back('=');
    }
    return out;
}

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

// Get detailed HTTP error message
static std::string GetHttpErrorMessage(long http_code, const std::string &url) {
    std::string msg = "HTTP error " + std::to_string(http_code);
    switch (http_code) {
        case 400: msg += " (Bad Request)"; break;
        case 401: msg += " (Unauthorized - check username/password)"; break;
        case 403: msg += " (Forbidden - access denied)"; break;
        case 404: msg += " (Not Found - file does not exist)"; break;
        case 405: msg += " (Method Not Allowed)"; break;
        case 408: msg += " (Request Timeout)"; break;
        case 429: msg += " (Too Many Requests)"; break;
        case 500: msg += " (Internal Server Error)"; break;
        case 502: msg += " (Bad Gateway)"; break;
        case 503: msg += " (Service Unavailable)"; break;
        case 504: msg += " (Gateway Timeout)"; break;
        default: break;
    }
    msg += " fetching: " + url;
    return msg;
}

// Get detailed curl error message
static std::string GetCurlErrorDetails(const std::string &error) {
    if (error.find("SSL") != std::string::npos || error.find("certificate") != std::string::npos) {
        return error + " (SSL/TLS certificate issue - check server certificate or use http://)";
    }
    if (error.find("timeout") != std::string::npos || error.find("Timeout") != std::string::npos) {
        return error + " (Connection timed out - server may be slow or unreachable)";
    }
    if (error.find("resolve") != std::string::npos || error.find("host") != std::string::npos) {
        return error + " (DNS resolution failed - check URL hostname)";
    }
    if (error.find("connect") != std::string::npos || error.find("refused") != std::string::npos) {
        return error + " (Connection refused - server may be down or port blocked)";
    }
    return error;
}

static bool IsHexChar(char c) {
    return (c >= '0' && c <= '9') ||
           (c >= 'a' && c <= 'f') ||
           (c >= 'A' && c <= 'F');
}

static std::string PercentEncodePath(const std::string &path) {
    static const char hex[] = "0123456789ABCDEF";
    std::string encoded;
    encoded.reserve(path.size());

    for (size_t i = 0; i < path.size(); i++) {
        unsigned char c = static_cast<unsigned char>(path[i]);
        if (c == '%' && i + 2 < path.size() && IsHexChar(path[i + 1]) && IsHexChar(path[i + 2])) {
            encoded.append(path, i, 3);
            i += 2;
            continue;
        }
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
            (c >= '0' && c <= '9') || c == '-' || c == '_' ||
            c == '.' || c == '~' || c == '/') {
            encoded.push_back(static_cast<char>(c));
        } else {
            encoded.push_back('%');
            encoded.push_back(hex[(c >> 4) & 0x0F]);
            encoded.push_back(hex[c & 0x0F]);
        }
    }
    return encoded;
}

static std::string NormalizeRequestUrl(const std::string &url) {
    size_t scheme_pos = url.find("://");
    if (scheme_pos == std::string::npos) {
        return PercentEncodePath(url);
    }
    size_t host_start = scheme_pos + 3;
    size_t path_start = url.find('/', host_start);
    if (path_start == std::string::npos) {
        return url;
    }
    std::string prefix = url.substr(0, path_start);
    std::string path = url.substr(path_start);
    return prefix + PercentEncodePath(path);
}

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

    // Handle binary formats (parquet, arrow, feather)
    if (result->file_extension == "parquet" || result->file_extension == "arrow" ||
        result->file_extension == "feather") {
        result->is_binary = true;
        result->needs_temp_file = true;

        // Write to temp file for DuckDB's parquet reader
        result->temp_file_path = GenerateTempFilename(result->file_extension);
        std::ofstream out(result->temp_file_path, std::ios::binary);
        if (!out) {
            throw IOException("Failed to create temp file: " + result->temp_file_path);
        }
        out.write(result->fetched_body.data(), result->fetched_body.size());
        out.close();

        // Clear body to free memory
        result->fetched_body.clear();
        result->fetched_body.shrink_to_fit();

        // Use DuckDB to read the parquet and get schema
        try {
            auto &db = DatabaseInstance::GetDatabase(context);
            Connection conn(db);

            // Get schema from parquet file
            std::string query = "SELECT * FROM read_parquet('" + result->temp_file_path + "') LIMIT 0";
            auto schema_result = conn.Query(query);

            if (schema_result->HasError()) {
                throw IOException("Failed to read parquet schema: " + schema_result->GetError());
            }

            // Extract column names and types
            for (idx_t i = 0; i < schema_result->ColumnCount(); i++) {
            result->column_names.push_back(schema_result->ColumnName(i));
            result->column_types.push_back(schema_result->types[i]); // ✅ updated
    }

            // Now read all data and materialize
            query = "SELECT * FROM read_parquet('" + result->temp_file_path + "')";
            auto data_result = conn.Query(query);

            if (data_result->HasError()) {
                throw IOException("Failed to read parquet data: " + data_result->GetError());
            }

            // Materialize all rows
            while (true) {
                auto chunk = data_result->Fetch();
                if (!chunk || chunk->size() == 0) break;

                for (idx_t row = 0; row < chunk->size(); row++) {
                    vector<Value> row_values;
                    for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
                        row_values.push_back(chunk->GetValue(col, row));
                    }
                    result->materialized_rows.push_back(std::move(row_values));
                }
            }

            // Clean up temp file
            std::remove(result->temp_file_path.c_str());
            result->temp_file_path.clear();

        } catch (std::exception &e) {
            // Clean up on error
            if (!result->temp_file_path.empty()) {
                std::remove(result->temp_file_path.c_str());
            }
            throw IOException("Failed to process parquet file: " + std::string(e.what()));
        }

        // Set return schema
        for (const auto &name : result->column_names) {
            names.push_back(name);
        }
        for (const auto &type : result->column_types) {
            return_types.push_back(type);
        }

        return result;
    }

    // Handle Excel files
    if (result->file_extension == "xlsx" || result->file_extension == "xls") {
        result->is_binary = true;
        result->needs_temp_file = true;

        result->temp_file_path = GenerateTempFilename(result->file_extension);
        std::ofstream out(result->temp_file_path, std::ios::binary);
        if (!out) {
            throw IOException("Failed to create temp file: " + result->temp_file_path);
        }
        out.write(result->fetched_body.data(), result->fetched_body.size());
        out.close();

        result->fetched_body.clear();
        result->fetched_body.shrink_to_fit();

        // Try to read Excel using spatial extension
        try {
            auto &db = DatabaseInstance::GetDatabase(context);
            Connection conn(db);

            // Install and load spatial extension if needed
            conn.Query("INSTALL spatial");
            conn.Query("LOAD spatial");

            // Get schema from Excel file
            std::string query = "SELECT * FROM st_read('" + result->temp_file_path + "') LIMIT 0";
            auto schema_result = conn.Query(query);

            if (schema_result->HasError()) {
                // Spatial extension not available - fall back to metadata return
                names = {"temp_path", "file_type", "query"};
                return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
                return result;
            }

            // Extract column names and types
            for (idx_t i = 0; i < schema_result->ColumnCount(); i++) {
                result->column_names.push_back(schema_result->ColumnName(i));
                result->column_types.push_back(schema_result->types[i]);
            }

            // Now read all data and materialize
            query = "SELECT * FROM st_read('" + result->temp_file_path + "')";
            auto data_result = conn.Query(query);

            if (data_result->HasError()) {
                throw IOException("Failed to read Excel data: " + data_result->GetError());
            }

            // Materialize all rows
            while (true) {
                auto chunk = data_result->Fetch();
                if (!chunk || chunk->size() == 0) break;

                for (idx_t row = 0; row < chunk->size(); row++) {
                    vector<Value> row_values;
                    for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
                        row_values.push_back(chunk->GetValue(col, row));
                    }
                    result->materialized_rows.push_back(std::move(row_values));
                }
            }

            // Clean up temp file
            std::remove(result->temp_file_path.c_str());
            result->temp_file_path.clear();

        } catch (std::exception &e) {
            // Spatial extension failed - fall back to metadata return
            names = {"temp_path", "file_type", "query"};
            return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
            return result;
        }

        // Set return schema from Excel
        for (const auto &name : result->column_names) {
            names.push_back(name);
        }
        for (const auto &type : result->column_types) {
            return_types.push_back(type);
        }

        return result;
    }

    // Parse CSV/TSV content for schema detection
    std::vector<std::vector<Value>> temp_rows;
    ParseCSVContent(result->fetched_body, result->column_names, result->column_types, temp_rows);

    if (!result->column_names.empty()) {
        // Use detected schema
        for (const auto &name : result->column_names) {
            names.push_back(name);
        }
        for (const auto &type : result->column_types) {
            return_types.push_back(type);
        }
    } else {
        // Fallback: return raw content
        result->column_names = {"content"};
        result->column_types = {LogicalType::VARCHAR};
        names = {"content"};
        return_types = {LogicalType::VARCHAR};
    }

    return result;
}

static unique_ptr<GlobalTableFunctionState> NextcloudInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind = input.bind_data->Cast<NextcloudBindData>();
    auto state = make_uniq<NextcloudGlobalState>();

    // Handle parquet/arrow/feather - use materialized rows from bind
    if (bind.is_binary && (bind.file_extension == "parquet" || bind.file_extension == "arrow" ||
                           bind.file_extension == "feather")) {
        // Data was already materialized in Bind phase
        for (auto &row : bind.materialized_rows) {
            state->rows.push_back(row);
        }
        return state;
    }

    // Handle Excel files
    if (bind.is_binary && (bind.file_extension == "xlsx" || bind.file_extension == "xls")) {
        // Check if data was materialized (spatial extension worked)
        if (!bind.materialized_rows.empty()) {
            for (auto &row : bind.materialized_rows) {
                state->rows.push_back(row);
            }
        } else {
            // Fallback: return metadata for manual query
            std::string query = "INSTALL spatial; LOAD spatial; SELECT * FROM st_read('" +
                               bind.temp_file_path + "')";
            state->rows.push_back({
                Value(bind.temp_file_path),
                Value(bind.file_extension),
                Value(query)
            });
        }
        return state;
    }

    // Parse CSV/TSV content
    std::vector<std::string> col_names;
    std::vector<LogicalType> col_types;
    ParseCSVContent(bind.fetched_body, col_names, col_types, state->rows);

    if (state->rows.empty() && !bind.fetched_body.empty()) {
        // Fallback: return raw content as single row
        state->rows.push_back({Value(bind.fetched_body)});
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

struct PropfindEntry {
    std::string href;
    bool is_collection;
};

// Parse WebDAV PROPFIND XML response into entries
// Handles both d: and D: namespace prefixes
static std::vector<PropfindEntry> ParsePropfindResponse(const std::string &xml) {
    std::vector<PropfindEntry> entries;

    // Find all <d:response> or <D:response> blocks
    size_t pos = 0;
    while (pos < xml.size()) {
        // Find response opening tag (case-insensitive prefix)
        size_t resp_start = std::string::npos;
        for (const char *tag : {"<d:response>", "<D:response>", "<d:response ", "<D:response "}) {
            size_t found = xml.find(tag, pos);
            if (found != std::string::npos && (resp_start == std::string::npos || found < resp_start)) {
                resp_start = found;
            }
        }
        if (resp_start == std::string::npos) break;

        // Find response closing tag
        size_t resp_end = std::string::npos;
        for (const char *tag : {"</d:response>", "</D:response>"}) {
            size_t found = xml.find(tag, resp_start);
            if (found != std::string::npos && (resp_end == std::string::npos || found < resp_end)) {
                resp_end = found;
            }
        }
        if (resp_end == std::string::npos) break;

        std::string block = xml.substr(resp_start, resp_end - resp_start);

        PropfindEntry entry;
        entry.is_collection = false;

        // Extract href
        for (const char *open : {"<d:href>", "<D:href>"}) {
            size_t href_start = block.find(open);
            if (href_start != std::string::npos) {
                href_start += strlen(open);
                size_t href_end = block.find("</", href_start);
                if (href_end != std::string::npos) {
                    entry.href = block.substr(href_start, href_end - href_start);
                }
                break;
            }
        }

        // Check if it's a collection (directory)
        if (block.find("<d:collection") != std::string::npos ||
            block.find("<D:collection") != std::string::npos) {
            entry.is_collection = true;
        }

        if (!entry.href.empty()) {
            entries.push_back(entry);
        }

        pos = resp_end + 1;
    }

    return entries;
}

// Decode percent-encoded URL path segments
static std::string PercentDecodePath(const std::string &encoded) {
    std::string decoded;
    decoded.reserve(encoded.size());

    for (size_t i = 0; i < encoded.size(); i++) {
        if (encoded[i] == '%' && i + 2 < encoded.size() &&
            IsHexChar(encoded[i + 1]) && IsHexChar(encoded[i + 2])) {
            char hi = encoded[i + 1];
            char lo = encoded[i + 2];
            auto hex_val = [](char c) -> int {
                if (c >= '0' && c <= '9') return c - '0';
                if (c >= 'a' && c <= 'f') return 10 + c - 'a';
                if (c >= 'A' && c <= 'F') return 10 + c - 'A';
                return 0;
            };
            decoded.push_back(static_cast<char>((hex_val(hi) << 4) | hex_val(lo)));
            i += 2;
        } else {
            decoded.push_back(encoded[i]);
        }
    }
    return decoded;
}

// Extract the last path segment from a URL path (filename or folder name)
static std::string GetLastPathSegment(const std::string &path) {
    std::string clean = path;
    // Remove trailing slash
    while (!clean.empty() && clean.back() == '/') {
        clean.pop_back();
    }
    size_t last_slash = clean.rfind('/');
    if (last_slash != std::string::npos) {
        return clean.substr(last_slash + 1);
    }
    return clean;
}

// Extract base URL (scheme + host) from full URL
static std::string GetBaseUrl(const std::string &url) {
    size_t scheme_end = url.find("://");
    if (scheme_end == std::string::npos) return "";
    size_t host_end = url.find('/', scheme_end + 3);
    if (host_end == std::string::npos) return url;
    return url.substr(0, host_end);
}

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

// Build auth headers for PROPFIND/GET requests
static void BuildAuthHeaders(CurlHeaders &headers, const std::string &username, const std::string &password) {
    if (!username.empty() || !password.empty()) {
        std::string credentials = username + ":" + password;
        headers.append("Authorization: Basic " + Base64Encode(credentials));
    }
}

// PROPFIND request body for depth-1 directory listing
static const char* PROPFIND_BODY =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    "<d:propfind xmlns:d=\"DAV:\">"
    "<d:prop><d:resourcetype/></d:prop>"
    "</d:propfind>";

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
        ParseCSVContent(body, col_names, col_types, rows);

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
        ParseCSVContent(body, col_names, col_types, rows);

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
    loader.RegisterFunction(func);

    // next_cloud_folder - scan parent folder's company subfolders
    TableFunction folder_func("next_cloud_folder", {LogicalType::VARCHAR},
                              NextcloudFolderScan, NextcloudFolderBind, NextcloudFolderInit);
    folder_func.named_parameters["child_folder"] = LogicalType::VARCHAR;
    folder_func.named_parameters["file_type"] = LogicalType::VARCHAR;
    folder_func.named_parameters["username"] = LogicalType::VARCHAR;
    folder_func.named_parameters["password"] = LogicalType::VARCHAR;
    loader.RegisterFunction(folder_func);
}

} // namespace stps
} // namespace duckdb
