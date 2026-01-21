#include "nextcloud_functions.hpp"
#include "shared/archive_utils.hpp"
#include "curl_utils.hpp"
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
                result->column_types.push_back(schema_result->GetTypes()[i]);
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
                result->column_types.push_back(schema_result->GetTypes()[i]);
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
