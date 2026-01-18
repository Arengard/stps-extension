#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include <algorithm>
#include <cctype>

namespace duckdb {
namespace stps {

struct SearchColumnsBindData : public TableFunctionData {
    string table_name;
    string search_pattern;
    string generated_sql;  // The dynamic SQL query to execute
    vector<LogicalType> original_column_types;  // Types from source table
    vector<string> original_column_names;  // Names from source table
};

struct SearchColumnsGlobalState : public GlobalTableFunctionState {
    unique_ptr<QueryResult> result;  // Result from executing dynamic SQL
    unique_ptr<DataChunk> current_chunk;  // Current chunk being processed
    idx_t chunk_offset = 0;  // Offset within current chunk
    bool query_executed = false;  // Track if query has been run
};

static unique_ptr<FunctionData> SearchColumnsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<SearchColumnsBindData>();

    // Get parameters - only 2 parameters now (removed case_sensitive)
    if (input.inputs.size() < 2) {
        throw BinderException("stps_search_columns requires 2 arguments: table_name, pattern");
    }

    result->table_name = input.inputs[0].GetValue<string>();
    result->search_pattern = input.inputs[1].GetValue<string>();

    // Create connection to query table schema
    Connection conn(context.db->GetDatabase(context));

    // Get table schema (column names and types) using LIMIT 0
    string schema_query = "SELECT * FROM " + result->table_name + " LIMIT 0";
    auto schema_result = conn.Query(schema_query);

    if (schema_result->HasError()) {
        throw BinderException("Table '%s' not found or inaccessible: %s",
                            result->table_name.c_str(), schema_result->GetError().c_str());
    }

    // Store original column names and types
    result->original_column_names = schema_result->names;
    result->original_column_types = schema_result->types;

    if (result->original_column_names.empty()) {
        throw BinderException("Table '%s' has no columns", result->table_name.c_str());
    }

    // Generate dynamic SQL to search all columns
    // Pattern: SELECT *, [col1_match, col2_match, ...] FROM table WHERE (col1 LIKE pattern OR col2 LIKE pattern...)

    string sql = "SELECT *, LIST_VALUE(";

    // Build list of CASE statements for matched columns
    for (idx_t i = 0; i < result->original_column_names.size(); i++) {
        if (i > 0) sql += ", ";
        string col_name = result->original_column_names[i];
        // Escape single quotes in column name
        string escaped_col = col_name;
        size_t pos = 0;
        while ((pos = escaped_col.find("'", pos)) != string::npos) {
            escaped_col.replace(pos, 1, "''");
            pos += 2;
        }
        sql += "CASE WHEN LOWER(CAST(\"" + col_name + "\" AS VARCHAR)) LIKE LOWER(?) THEN '" + escaped_col + "' END";
    }

    sql += ") FILTER (WHERE value IS NOT NULL) AS matched_columns FROM " + result->table_name + " WHERE ";

    // Build WHERE clause - at least one column must match
    for (idx_t i = 0; i < result->original_column_names.size(); i++) {
        if (i > 0) sql += " OR ";
        sql += "LOWER(CAST(\"" + result->original_column_names[i] + "\" AS VARCHAR)) LIKE LOWER(?)";
    }

    result->generated_sql = sql;

    // Define output schema: original columns + matched_columns array
    for (idx_t i = 0; i < result->original_column_names.size(); i++) {
        return_types.push_back(result->original_column_types[i]);
        names.push_back(result->original_column_names[i]);
    }

    // Add matched_columns as VARCHAR[] (list of strings)
    return_types.push_back(LogicalType::LIST(LogicalType::VARCHAR));
    names.push_back("matched_columns");

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> SearchColumnsInit(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<SearchColumnsGlobalState>();
    state->query_executed = false;
    return std::move(state);
}

static void SearchColumnsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<SearchColumnsBindData>();
    auto &state = data_p.global_state->Cast<SearchColumnsGlobalState>();

    // Execute query on first call
    if (!state.query_executed) {
        Connection conn(context.db->GetDatabase(context));

        // Create prepared statement with parameters
        auto prepared = conn.Prepare(bind_data.generated_sql);
        if (prepared->HasError()) {
            throw InvalidInputException("Failed to prepare search query: %s", prepared->GetError().c_str());
        }

        // Bind pattern parameters
        vector<Value> params;
        // Add pattern for each CASE statement in SELECT
        for (idx_t i = 0; i < bind_data.original_column_names.size(); i++) {
            params.push_back(Value(bind_data.search_pattern));
        }
        // Add pattern for each OR clause in WHERE
        for (idx_t i = 0; i < bind_data.original_column_names.size(); i++) {
            params.push_back(Value(bind_data.search_pattern));
        }

        state.result = prepared->Execute(params);

        if (state.result->HasError()) {
            throw InvalidInputException("Search query failed: %s", state.result->GetError().c_str());
        }

        state.query_executed = true;
    }

    // Fetch next chunk if needed
    if (!state.current_chunk || state.chunk_offset >= state.current_chunk->size()) {
        state.current_chunk = state.result->Fetch();
        state.chunk_offset = 0;

        // No more data
        if (!state.current_chunk || state.current_chunk->size() == 0) {
            output.SetCardinality(0);
            return;
        }
    }

    // Copy data from current chunk to output
    idx_t count = 0;
    idx_t max_count = std::min(STANDARD_VECTOR_SIZE, state.current_chunk->size() - state.chunk_offset);

    for (idx_t i = 0; i < max_count; i++) {
        idx_t source_row = state.chunk_offset + i;

        // Copy all columns (including matched_columns from result)
        for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
            auto &source_vector = state.current_chunk->data[col_idx];
            auto &dest_vector = output.data[col_idx];

            // Copy value from source to destination
            VectorOperations::Copy(source_vector, dest_vector, source_row + 1, source_row, i);
        }

        count++;
    }

    state.chunk_offset += count;
    output.SetCardinality(count);
}

void RegisterSearchColumnsFunction(ExtensionLoader& loader) {
    // Create function set to support multiple signatures
    TableFunctionSet search_columns_set("stps_search_columns");

    // Version 1: Two parameters (table_name, pattern) - case_sensitive defaults to false
    search_columns_set.AddFunction(TableFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR},
        SearchColumnsFunction, SearchColumnsBind, SearchColumnsInit
    ));

    // Version 2: Three parameters (table_name, pattern, case_sensitive)
    search_columns_set.AddFunction(TableFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN},
        SearchColumnsFunction, SearchColumnsBind, SearchColumnsInit
    ));

    loader.RegisterFunction(search_columns_set);
}

} // namespace stps
} // namespace duckdb
