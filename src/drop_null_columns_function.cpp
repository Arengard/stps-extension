#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include <unordered_map>

namespace duckdb {
namespace stps {

// Bind data for stps_drop_null_columns function
struct DropNullColumnsBindData : public TableFunctionData {
    string table_name;
    vector<string> non_null_columns;
    vector<LogicalType> column_types;
};

// Global state for stps_drop_null_columns function
struct DropNullColumnsGlobalState : public GlobalTableFunctionState {
    unique_ptr<QueryResult> result;
    unique_ptr<DataChunk> current_chunk;
    idx_t chunk_offset = 0;
    bool has_returned_data = false;
};

// Bind function for stps_drop_null_columns
static unique_ptr<FunctionData> DropNullColumnsBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<DropNullColumnsBindData>();

    // Required parameter: table_name
    if (input.inputs.empty()) {
        throw BinderException("stps_drop_null_columns requires one argument: table_name");
    }
    result->table_name = input.inputs[0].GetValue<string>();

    // Create a connection to run queries
    Connection conn(context.db->GetDatabase(context));

    // Step 1: Get column types via SELECT * (reliable for types)
    auto type_query = "SELECT * FROM " + result->table_name + " LIMIT 0";
    auto type_result = conn.Query(type_query);
    if (type_result->HasError()) {
        throw BinderException("Table '%s' does not exist or cannot be queried: %s",
                            result->table_name, type_result->GetError());
    }

    // Build a map: column_name -> LogicalType
    std::unordered_map<string, LogicalType> type_map;
    for (idx_t i = 0; i < type_result->names.size(); i++) {
        type_map[type_result->names[i]] = type_result->types[i];
    }

    // Step 2: Get column order via PRAGMA table_info (guaranteed definition order)
    auto pragma_query = "PRAGMA table_info('" + result->table_name + "')";
    auto pragma_result = conn.Query(pragma_query);
    if (pragma_result->HasError()) {
        throw BinderException("Failed to get table info: %s", pragma_result->GetError());
    }

    vector<string> ordered_names;
    vector<LogicalType> ordered_types;

    while (auto pragma_chunk = pragma_result->Fetch()) {
        if (!pragma_chunk || pragma_chunk->size() == 0) break;
        for (idx_t row = 0; row < pragma_chunk->size(); row++) {
            // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            string col_name = pragma_chunk->GetValue(1, row).ToString();
            ordered_names.push_back(col_name);
            ordered_types.push_back(type_map[col_name]);
        }
    }

    // Step 3: Build COUNT query using ordered columns
    string count_query = "SELECT ";
    for (idx_t i = 0; i < ordered_names.size(); i++) {
        if (i > 0) count_query += ", ";
        count_query += "COUNT(\"" + ordered_names[i] + "\")";
    }
    count_query += " FROM " + result->table_name;

    auto count_result = conn.Query(count_query);
    if (count_result->HasError()) {
        throw BinderException("Failed to analyze table columns: %s", count_result->GetError());
    }

    // Step 4: Filter non-null columns (preserving definition order)
    auto chunk = count_result->Fetch();
    if (chunk && chunk->size() > 0) {
        for (idx_t i = 0; i < ordered_names.size(); i++) {
            auto count_value = chunk->GetValue(i, 0);
            if (!count_value.IsNull() && count_value.GetValue<int64_t>() > 0) {
                // This column has at least one non-NULL value
                result->non_null_columns.push_back(ordered_names[i]);
                result->column_types.push_back(ordered_types[i]);
            }
        }
    }

    // Step 5: Set output schema
    if (result->non_null_columns.empty()) {
        names = {"message"};
        return_types = {LogicalType::VARCHAR};
    } else {
        names = result->non_null_columns;
        return_types = result->column_types;
    }

    return std::move(result);
}

// Init function for stps_drop_null_columns
static unique_ptr<GlobalTableFunctionState> DropNullColumnsInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<DropNullColumnsBindData>();
    auto result = make_uniq<DropNullColumnsGlobalState>();

    // If we have non-null columns, build a query to select only those columns
    if (!bind_data.non_null_columns.empty()) {
        string column_list = "";
        for (idx_t i = 0; i < bind_data.non_null_columns.size(); i++) {
            if (i > 0) column_list += ", ";
            column_list += "\"" + bind_data.non_null_columns[i] + "\"";
        }

        auto query = "SELECT " + column_list + " FROM " + bind_data.table_name;
        Connection conn(context.db->GetDatabase(context));
        result->result = conn.Query(query);

        if (result->result->HasError()) {
            throw InternalException("Failed to query table: %s", result->result->GetError());
        }
    }

    return std::move(result);
}

// Scan function for stps_drop_null_columns
static void DropNullColumnsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<DropNullColumnsGlobalState>();
    auto &bind_data = data_p.bind_data->Cast<DropNullColumnsBindData>();

    // If no non-null columns, return message once
    if (bind_data.non_null_columns.empty()) {
        if (state.has_returned_data) {
            output.SetCardinality(0);
            return;
        }
        output.SetValue(0, 0, Value("All columns are NULL"));
        output.SetCardinality(1);
        state.has_returned_data = true;
        return;
    }

    // If no result, we're done
    if (!state.result) {
        output.SetCardinality(0);
        return;
    }

    // Fetch next chunk from the query result
    auto chunk = state.result->Fetch();
    if (!chunk || chunk->size() == 0) {
        output.SetCardinality(0);
        return;
    }

    // Copy the chunk data to output
    output.Reference(*chunk);
}

// Register stps_drop_null_columns function
void RegisterDropNullColumnsFunction(ExtensionLoader &loader) {
    TableFunction func("stps_drop_null_columns", {LogicalType::VARCHAR}, DropNullColumnsScan,
                       DropNullColumnsBind, DropNullColumnsInit);
    loader.RegisterFunction(func);
}

} // namespace stps
} // namespace duckdb
