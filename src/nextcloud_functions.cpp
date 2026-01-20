#include "nextcloud_functions.hpp"
#include "shared/archive_utils.hpp"
#include "curl_utils.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include <fstream>
#include <algorithm>
#include <sstream>

namespace duckdb {
namespace stps {

struct NextcloudBindData : public TableFunctionData {
    std::string url;
    std::string username;
    std::string password;
    vector<std::string> extra_headers; // use duckdb::vector for TableFunction compatibility
};

struct NextcloudGlobalState : public GlobalTableFunctionState {
    vector<vector<Value>> rows;
    vector<std::string> column_names;
    vector<LogicalType> column_types;
    idx_t current_row = 0;
    std::string error_message;
};

// Simple base64 for basic auth
static std::string Base64Encode(const std::string &input) {
    static const char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    int val = 0, valb = -6;
    for (unsigned char c : input) {
        val = (val << 8) + c;
        valb += 8;
        while (valb >= 0) {
            out.push_back(table[(val >> valb) & 0x3F]);
            valb -= 6;
        }
    }
    if (valb > -6) out.push_back(table[((val << 8) >> (valb + 8)) & 0x3F]);
    while (out.size() % 4) out.push_back('=');
    return out;
}

static unique_ptr<FunctionData> NextcloudBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<NextcloudBindData>();
    if (input.inputs.empty()) {
        throw BinderException("next_cloud requires url");
    }
    result->url = input.inputs[0].GetValue<std::string>();

    // Named parameters: username, password, headers (\n separated)
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
                if (!line.empty()) result->extra_headers.push_back(line);
            }
        }
    }

    // Default return until we parse; will be set in Init
    names = {"content"};
    return_types = {LogicalType::VARCHAR};
    return result;
}

static unique_ptr<GlobalTableFunctionState> NextcloudInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind = input.bind_data->Cast<NextcloudBindData>();
    auto state = make_uniq<NextcloudGlobalState>();

    CurlHeaders headers;
    if (!bind.username.empty() || !bind.password.empty()) {
        std::string basic = "Authorization: Basic " + Base64Encode(bind.username + ":" + bind.password);
        headers.append(basic);
    }
    for (auto &h : bind.extra_headers) headers.append(h);

    // Fetch data via HTTP GET
    long http_code = 0;
    std::string body = curl_get(bind.url, headers, &http_code);
    if (body.find("ERROR:") == 0) {
        state->error_message = body;
        return state;
    }
    if (http_code >= 400) {
        state->error_message = "HTTP error " + std::to_string(http_code) + " fetching URL";
        return state;
    }

    std::string ext = GetFileExtension(bind.url);
    std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);

    // Binary formats -> write temp and return hint
    if (ext == "parquet" || ext == "arrow" || ext == "feather" || ext == "xlsx" || ext == "xls") {
        std::string temp_path = ExtractToTemp(body, "nextcloud_download." + ext, "nextcloud_");

        state->column_names = {"extracted_path", "file_type", "usage_hint"};
        state->column_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};

        std::string hint;
        if (ext == "parquet" || ext == "arrow" || ext == "feather") {
            hint = "SELECT * FROM read_parquet('" + temp_path + "')";
        } else if (ext == "xlsx" || ext == "xls") {
            hint = "Use an Excel reader extension on '" + temp_path + "'";
        }

        state->rows.push_back({Value(temp_path), Value(ext), Value(hint)});
        return state;
    }

    // Attempt CSV/TSV parsing using std::vector to match ParseCSVContent signature
    std::vector<std::string> col_names_std;
    std::vector<LogicalType> col_types_std;
    std::vector<std::vector<Value>> rows_std;
    ParseCSVContent(body, col_names_std, col_types_std, rows_std);

    if (!col_names_std.empty()) {
        // Copy element-by-element to avoid std::vector -> duckdb::vector assignment issues
        state->column_names.clear();
        for (auto &n : col_names_std) {
            state->column_names.push_back(n);
        }
        state->column_types.clear();
        for (auto &t : col_types_std) {
            state->column_types.push_back(t);
        }
        state->rows.clear();
        for (auto &r : rows_std) {
            vector<Value> row;
            for (auto &v : r) {
                row.push_back(v);
            }
            state->rows.push_back(std::move(row));
        }
        return state;
    }

    // Fallback: return raw content
    state->column_names = {"content"};
    state->column_types = {LogicalType::VARCHAR};
    state->rows.push_back({Value(body)});
    return state;
}

static void NextcloudScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NextcloudGlobalState>();

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

void RegisterNextcloudFunctions(ExtensionLoader &loader) {
    TableFunction func("next_cloud", {LogicalType::VARCHAR}, NextcloudScan, NextcloudBind, NextcloudInit);
    loader.RegisterFunction(func);
}

} // namespace stps
} // namespace duckdb
