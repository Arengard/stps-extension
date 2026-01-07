#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"

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

    // Create a connection to execute queries
    auto conn = make_uniq<Connection>(*context.db);

    // Step 1: Check if table exists and get its schema
    string schema_query = "SELECT column_name, column_type FROM (DESCRIBE " + result->table_name + ")";
    unique_ptr<QueryResult> schema_result;
    try {
        schema_result = conn->Query(schema_query);
    } catch (const Exception &e) {
        throw BinderException("Table '" + result->table_name + "' does not exist or cannot be accessed: " + e.what());
    }

    if (schema_result->HasError()) {
        throw BinderException("Failed to describe table: " + schema_result->GetError());
    }

    // Store all columns and their types
    std::map<string, string> column_type_map;
    while (true) {
        auto chunk = schema_result->Fetch();
        if (!chunk || chunk->size() == 0) {
            break;
        }
        for (idx_t i = 0; i < chunk->size(); i++) {
            string col_name = chunk->GetValue(0, i).ToString();
            string col_type = chunk->GetValue(1, i).ToString();
            column_type_map[col_name] = col_type;
        }
    }

    if (column_type_map.empty()) {
        throw BinderException("Table '" + result->table_name + "' has no columns");
    }

    // Step 2: Run SUMMARIZE to get column statistics
    string summarize_query = "SUMMARIZE " + result->table_name;
    unique_ptr<QueryResult> summarize_result;
    try {
        summarize_result = conn->Query(summarize_query);
    } catch (const Exception &e) {
        throw BinderException("Failed to summarize table: " + string(e.what()));
    }

    if (summarize_result->HasError()) {
        throw BinderException("SUMMARIZE failed: " + summarize_result->GetError());
    }

    // Step 3: Parse SUMMARIZE results to find non-null columns
    // SUMMARIZE returns columns: column_name, column_type, min, max, approx_unique, avg, std, q25, q50, q75, count, null_percentage
    std::set<string> non_null_column_set;

    while (true) {
        auto chunk = summarize_result->Fetch();
        if (!chunk || chunk->size() == 0) {
            break;
        }

        for (idx_t i = 0; i < chunk->size(); i++) {
            string col_name = chunk->GetValue(0, i).ToString();

            // Get the count column (index 10) and null_percentage column (index 11)
            auto count_value = chunk->GetValue(10, i);
            auto null_pct_value = chunk->GetValue(11, i);

            // Column is non-null if:
            // - count > 0 AND null_percentage < 100.0
            // - OR if null_percentage is NULL (shouldn't happen, but be safe)
            bool has_non_null_values = false;

            if (!count_value.IsNull() && !null_pct_value.IsNull()) {
                int64_t count = count_value.GetValue<int64_t>();
                double null_pct = null_pct_value.GetValue<double>();

                if (count > 0 && null_pct < 100.0) {
                    has_non_null_values = true;
                }
            } else if (!count_value.IsNull()) {
                // Fallback: if we have count but no null_percentage
                int64_t count = count_value.GetValue<int64_t>();
                if (count > 0) {
                    has_non_null_values = true;
                }
            }

            if (has_non_null_values) {
                non_null_column_set.insert(col_name);
            }
        }
    }

    // Step 4: Build the list of non-null columns in original order
    for (const auto &kv : column_type_map) {
        if (non_null_column_set.find(kv.first) != non_null_column_set.end()) {
            result->non_null_columns.push_back(kv.first);
            // Parse the type string back to LogicalType
            // This is a simplified version - DuckDB has more complex type parsing
            string type_str = kv.second;
            LogicalType col_type;

            if (type_str == "VARCHAR") {
                col_type = LogicalType::VARCHAR;
            } else if (type_str == "BIGINT") {
                col_type = LogicalType::BIGINT;
            } else if (type_str == "INTEGER") {
                col_type = LogicalType::INTEGER;
            } else if (type_str == "DOUBLE") {
                col_type = LogicalType::DOUBLE;
            } else if (type_str == "BOOLEAN") {
                col_type = LogicalType::BOOLEAN;
            } else if (type_str == "DATE") {
                col_type = LogicalType::DATE;
            } else if (type_str == "TIMESTAMP") {
                col_type = LogicalType::TIMESTAMP;
            } else {
                // Default to VARCHAR for unknown types
                col_type = LogicalType::VARCHAR;
            }

            result->column_types.push_back(col_type);
        }
    }

    // If no non-null columns found, return empty schema
    if (result->non_null_columns.empty()) {
        // Return a single dummy column to avoid errors, but with 0 rows
        names.push_back("__empty__");
        return_types.push_back(LogicalType::VARCHAR);
        return std::move(result);
    }

    // Set output schema
    names = result->non_null_columns;
    return_types = result->column_types;

    return std::move(result);
}

// Init function for stps_drop_null_columns
static unique_ptr<GlobalTableFunctionState> DropNullColumnsInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<DropNullColumnsBindData>();
    auto result = make_uniq<DropNullColumnsGlobalState>();

    // If no non-null columns, return empty state
    if (bind_data.non_null_columns.empty()) {
        return std::move(result);
    }

    // Build SELECT query with only non-null columns
    string select_query = "SELECT ";
    for (idx_t i = 0; i < bind_data.non_null_columns.size(); i++) {
        if (i > 0) {
            select_query += ", ";
        }
        select_query += "\"" + bind_data.non_null_columns[i] + "\"";
    }
    select_query += " FROM " + bind_data.table_name;

    // Execute the query
    auto conn = make_uniq<Connection>(*context.db);
    try {
        result->result = conn->Query(select_query);
    } catch (const Exception &e) {
        throw IOException("Failed to query table: " + string(e.what()));
    }

    if (result->result->HasError()) {
        throw IOException("Query failed: " + result->result->GetError());
    }

    return std::move(result);
}

// Scan function for stps_drop_null_columns
static void DropNullColumnsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<DropNullColumnsGlobalState>();

    // If no result (empty columns case), return empty
    if (!state.result) {
        output.SetCardinality(0);
        return;
    }

    // Fetch next chunk if needed
    if (!state.current_chunk || state.chunk_offset >= state.current_chunk->size()) {
        state.current_chunk = state.result->Fetch();
        state.chunk_offset = 0;

        if (!state.current_chunk || state.current_chunk->size() == 0) {
            output.SetCardinality(0);
            return;
        }
    }

    // Copy data from current chunk to output
    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.chunk_offset < state.current_chunk->size() && count < max_count) {
        for (idx_t col = 0; col < output.ColumnCount(); col++) {
            output.SetValue(col, count, state.current_chunk->GetValue(col, state.chunk_offset));
        }
        state.chunk_offset++;
        count++;
    }

    output.SetCardinality(count);
}

// Register stps_drop_null_columns function
void RegisterDropNullColumnsFunction(ExtensionLoader &loader) {
    TableFunction func("stps_drop_null_columns", {LogicalType::VARCHAR}, DropNullColumnsScan,
                       DropNullColumnsBind, DropNullColumnsInit);
    loader.RegisterFunction(func);
}

} // namespace stps
} // namespace duckdb
