#include "nextcloud_functions.hpp"
#include "shared/archive_utils.hpp"
#include "curl_utils.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/client_context.hpp"
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

    // Schema detected in bind
    std::vector<std::string> column_names;
    std::vector<LogicalType> column_types;
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
    std::string body = curl_get(result->url, headers, &http_code);

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

    // Handle binary formats
    if (result->file_extension == "parquet" || result->file_extension == "arrow" ||
        result->file_extension == "feather") {
        result->is_binary = true;

        // Write to temp file
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

        // Return metadata columns
        names = {"temp_path", "file_type", "row_count", "query"};
        return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR,
                       LogicalType::BIGINT, LogicalType::VARCHAR};
        return result;
    }

    // Handle Excel files
    if (result->file_extension == "xlsx" || result->file_extension == "xls") {
        result->is_binary = true;

        result->temp_file_path = GenerateTempFilename(result->file_extension);
        std::ofstream out(result->temp_file_path, std::ios::binary);
        if (!out) {
            throw IOException("Failed to create temp file: " + result->temp_file_path);
        }
        out.write(result->fetched_body.data(), result->fetched_body.size());
        out.close();

        result->fetched_body.clear();
        result->fetched_body.shrink_to_fit();

        names = {"temp_path", "file_type", "query"};
        return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
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

    // Handle binary formats - just return metadata row
    if (bind.is_binary) {
        if (bind.file_extension == "parquet" || bind.file_extension == "arrow" ||
            bind.file_extension == "feather") {
            std::string query = "SELECT * FROM read_parquet('" + bind.temp_file_path + "')";
            // Try to get row count
            int64_t row_count = -1; // Unknown
            state->rows.push_back({
                Value(bind.temp_file_path),
                Value(bind.file_extension),
                Value::BIGINT(row_count),
                Value(query)
            });
        } else if (bind.file_extension == "xlsx" || bind.file_extension == "xls") {
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

void RegisterNextcloudFunctions(ExtensionLoader &loader) {
    TableFunction func("next_cloud", {LogicalType::VARCHAR}, NextcloudScan, NextcloudBind, NextcloudInit);

    // Register named parameters
    func.named_parameters["username"] = LogicalType::VARCHAR;
    func.named_parameters["password"] = LogicalType::VARCHAR;
    func.named_parameters["headers"] = LogicalType::VARCHAR;

    loader.RegisterFunction(func);
}

} // namespace stps
} // namespace duckdb
