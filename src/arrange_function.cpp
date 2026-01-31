#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include <unordered_set>
#include <unordered_map>

namespace duckdb {
namespace stps {

// Simple JSON parser for {"key": ["val1", "val2"], ...} format
// Returns groups in insertion order as vector<pair<string, vector<string>>>
static vector<pair<string, vector<string>>> ParseGroupsJson(const string &json) {
    vector<pair<string, vector<string>>> groups;
    idx_t pos = 0;

    auto skip_ws = [&]() {
        while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t' || json[pos] == '\n' || json[pos] == '\r')) {
            pos++;
        }
    };

    auto expect_char = [&](char c) {
        skip_ws();
        if (pos >= json.size() || json[pos] != c) {
            throw InvalidInputException("stps_arrange: expected '%c' at position %d in JSON", c, (int)pos);
        }
        pos++;
    };

    auto parse_string = [&]() -> string {
        skip_ws();
        if (pos >= json.size() || json[pos] != '"') {
            throw InvalidInputException("stps_arrange: expected '\"' at position %d in JSON", (int)pos);
        }
        pos++; // skip opening quote
        string result;
        while (pos < json.size() && json[pos] != '"') {
            if (json[pos] == '\\' && pos + 1 < json.size()) {
                pos++;
                result += json[pos];
            } else {
                result += json[pos];
            }
            pos++;
        }
        if (pos >= json.size()) {
            throw InvalidInputException("stps_arrange: unterminated string in JSON");
        }
        pos++; // skip closing quote
        return result;
    };

    auto parse_string_array = [&]() -> vector<string> {
        vector<string> arr;
        expect_char('[');
        skip_ws();
        if (pos < json.size() && json[pos] == ']') {
            pos++;
            return arr;
        }
        while (true) {
            arr.push_back(parse_string());
            skip_ws();
            if (pos >= json.size()) {
                throw InvalidInputException("stps_arrange: unterminated array in JSON");
            }
            if (json[pos] == ']') {
                pos++;
                break;
            }
            expect_char(',');
        }
        return arr;
    };

    expect_char('{');
    skip_ws();
    if (pos < json.size() && json[pos] == '}') {
        pos++;
        return groups;
    }

    while (true) {
        string key = parse_string();
        expect_char(':');
        vector<string> values = parse_string_array();
        groups.push_back({key, values});
        skip_ws();
        if (pos >= json.size()) {
            throw InvalidInputException("stps_arrange: unterminated object in JSON");
        }
        if (json[pos] == '}') {
            pos++;
            break;
        }
        expect_char(',');
    }

    return groups;
}

// Bind data for stps_arrange function
struct ArrangeBindData : public TableFunctionData {
    string table_name;
    vector<string> ordered_columns;
    vector<LogicalType> ordered_types;
};

// Global state for stps_arrange function
struct ArrangeGlobalState : public GlobalTableFunctionState {
    unique_ptr<QueryResult> result;
};

// Bind function for stps_arrange
static unique_ptr<FunctionData> ArrangeBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<ArrangeBindData>();

    if (input.inputs.size() < 2) {
        throw BinderException("stps_arrange requires two arguments: table_name and groups JSON");
    }
    result->table_name = input.inputs[0].GetValue<string>();
    string json_str = input.inputs[1].GetValue<string>();

    // Parse groups JSON
    auto groups = ParseGroupsJson(json_str);

    // Query table schema
    Connection conn(context.db->GetDatabase(context));
    auto schema_query = "SELECT * FROM \"" + result->table_name + "\" LIMIT 0";
    auto schema_result = conn.Query(schema_query);
    if (schema_result->HasError()) {
        throw BinderException("stps_arrange: table '%s' does not exist or cannot be queried: %s",
                              result->table_name, schema_result->GetError());
    }

    // Build column name -> index map
    std::unordered_map<string, idx_t> col_index;
    for (idx_t i = 0; i < schema_result->names.size(); i++) {
        col_index[schema_result->names[i]] = i;
    }

    // Validate and collect grouped columns
    std::unordered_set<string> seen_columns;
    vector<string> ordered_cols;
    vector<LogicalType> ordered_types_vec;

    for (auto &group : groups) {
        for (auto &col_name : group.second) {
            // Check column exists
            auto it = col_index.find(col_name);
            if (it == col_index.end()) {
                throw InvalidInputException("stps_arrange: column '%s' (in group '%s') not found in table '%s'",
                                            col_name, group.first, result->table_name);
            }
            // Check for duplicates across groups
            if (seen_columns.count(col_name)) {
                throw InvalidInputException("stps_arrange: column '%s' appears in multiple groups", col_name);
            }
            seen_columns.insert(col_name);
            ordered_cols.push_back(col_name);
            ordered_types_vec.push_back(schema_result->types[it->second]);
        }
    }

    // Append remaining columns in their original order
    for (idx_t i = 0; i < schema_result->names.size(); i++) {
        if (!seen_columns.count(schema_result->names[i])) {
            ordered_cols.push_back(schema_result->names[i]);
            ordered_types_vec.push_back(schema_result->types[i]);
        }
    }

    result->ordered_columns = ordered_cols;
    result->ordered_types = ordered_types_vec;

    names = ordered_cols;
    return_types = ordered_types_vec;

    return std::move(result);
}

// Init function for stps_arrange
static unique_ptr<GlobalTableFunctionState> ArrangeInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<ArrangeBindData>();
    auto result = make_uniq<ArrangeGlobalState>();

    // Build SELECT with reordered columns
    string column_list;
    for (idx_t i = 0; i < bind_data.ordered_columns.size(); i++) {
        if (i > 0) column_list += ", ";
        column_list += "\"" + bind_data.ordered_columns[i] + "\"";
    }

    auto query = "SELECT " + column_list + " FROM \"" + bind_data.table_name + "\"";
    Connection conn(context.db->GetDatabase(context));
    result->result = conn.Query(query);

    if (result->result->HasError()) {
        throw InternalException("stps_arrange: failed to query table: %s", result->result->GetError());
    }

    return std::move(result);
}

// Scan function for stps_arrange
static void ArrangeScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<ArrangeGlobalState>();

    if (!state.result) {
        output.SetCardinality(0);
        return;
    }

    auto chunk = state.result->Fetch();
    if (!chunk || chunk->size() == 0) {
        output.SetCardinality(0);
        return;
    }

    output.Reference(*chunk);
}

// Register stps_arrange function
void RegisterArrangeFunctions(ExtensionLoader &loader) {
    TableFunction func("stps_arrange", {LogicalType::VARCHAR, LogicalType::VARCHAR},
                       ArrangeScan, ArrangeBind, ArrangeInit);
    loader.RegisterFunction(func);
}

} // namespace stps
} // namespace duckdb
