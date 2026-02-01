#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include <algorithm>
#include <cctype>
#include <sstream>

namespace duckdb {
namespace stps {

struct SearchDatabaseBindData : public TableFunctionData {
    string search_pattern;
    vector<string> schema_names;
    vector<string> table_names;
    vector<vector<string>> table_columns; // columns for each table
};

struct SearchDatabaseGlobalState : public GlobalTableFunctionState {
    idx_t current_table_idx = 0;
    idx_t current_col_idx = 0;
    unique_ptr<QueryResult> current_result;
    unique_ptr<DataChunk> current_chunk;
    idx_t chunk_offset = 0;
    bool finished = false;
    string current_schema_name;
    string current_table_name;
    string current_column_name;
};

// Helper: escape identifier for SQL
static string EscapeIdentifier(const string &name) {
    return "\"" + name + "\"";
}

// Helper: case-insensitive pattern match (SQL LIKE style)
static bool MatchesPattern(const string &value, const string &pattern) {
    string lower_value, lower_pattern;
    lower_value.reserve(value.size());
    lower_pattern.reserve(pattern.size());

    for (char c : value) lower_value += std::tolower(static_cast<unsigned char>(c));
    for (char c : pattern) lower_pattern += std::tolower(static_cast<unsigned char>(c));

    size_t v = 0, p = 0;
    size_t star_p = string::npos, star_v = 0;

    while (v < lower_value.size()) {
        if (p < lower_pattern.size() && (lower_pattern[p] == lower_value[v] || lower_pattern[p] == '_')) {
            v++;
            p++;
        } else if (p < lower_pattern.size() && lower_pattern[p] == '%') {
            star_p = p++;
            star_v = v;
        } else if (star_p != string::npos) {
            p = star_p + 1;
            v = ++star_v;
        } else {
            return false;
        }
    }

    while (p < lower_pattern.size() && lower_pattern[p] == '%') p++;
    return p == lower_pattern.size();
}

// Convert a row to JSON string for context
static string RowToJson(DataChunk &chunk, idx_t row, const vector<string> &column_names) {
    std::ostringstream json;
    json << "{";
    for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
        if (col > 0) json << ", ";
        json << "\"" << column_names[col] << "\": ";
        
        if (FlatVector::IsNull(chunk.data[col], row)) {
            json << "null";
        } else {
            Value val = chunk.data[col].GetValue(row);
            string str_val = val.ToString();
            // Escape quotes in string
            string escaped;
            for (char c : str_val) {
                if (c == '"') escaped += "\\\"";
                else if (c == '\\') escaped += "\\\\";
                else if (c == '\n') escaped += "\\n";
                else if (c == '\r') escaped += "\\r";
                else if (c == '\t') escaped += "\\t";
                else escaped += c;
            }
            json << "\"" << escaped << "\"";
        }
    }
    json << "}";
    return json.str();
}

static unique_ptr<FunctionData> SearchDatabaseBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<SearchDatabaseBindData>();

    if (input.inputs.size() < 1) {
        throw BinderException("stps_search_database requires 1 argument: pattern");
    }

    result->search_pattern = input.inputs[0].GetValue<string>();

    // Get all tables from the database across all schemas
    Connection conn(context.db->GetDatabase(context));

    // Query to get all tables from all user schemas (excluding system schemas)
    auto tables_result = conn.Query(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_type = 'BASE TABLE' "
        "AND table_schema NOT IN ('information_schema', 'pg_catalog') "
        "ORDER BY table_schema, table_name"
    );

    if (tables_result->HasError()) {
        throw BinderException("Failed to get tables: %s", tables_result->GetError().c_str());
    }

    // Collect all schema + table names
    while (true) {
        auto chunk = tables_result->Fetch();
        if (!chunk || chunk->size() == 0) break;

        for (idx_t row = 0; row < chunk->size(); row++) {
            string schema_name = chunk->data[0].GetValue(row).ToString();
            string table_name = chunk->data[1].GetValue(row).ToString();
            result->schema_names.push_back(schema_name);
            result->table_names.push_back(table_name);
        }
    }

    // For each table, get column names
    for (idx_t i = 0; i < result->table_names.size(); i++) {
        const auto &schema_name = result->schema_names[i];
        const auto &table_name = result->table_names[i];
        string col_query = "SELECT column_name FROM information_schema.columns "
                          "WHERE table_schema = '" + schema_name + "' AND table_name = '" + table_name + "'";
        auto cols_result = conn.Query(col_query);

        vector<string> columns;
        if (!cols_result->HasError()) {
            while (true) {
                auto chunk = cols_result->Fetch();
                if (!chunk || chunk->size() == 0) break;

                for (idx_t row = 0; row < chunk->size(); row++) {
                    columns.push_back(chunk->data[0].GetValue(row).ToString());
                }
            }
        }
        result->table_columns.push_back(columns);
    }

    // Output schema: schema_name, table_name, column_name, matched_value, row_data (JSON)
    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("schema_name");

    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("table_name");

    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("column_name");

    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("matched_value");

    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("row_data");

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> SearchDatabaseInit(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<SearchDatabaseGlobalState>();
    state->finished = false;
    return std::move(state);
}

// Helper to start querying a specific column in a table
static bool StartColumnQuery(ClientContext &context, SearchDatabaseGlobalState &state, 
                             const SearchDatabaseBindData &bind_data) {
    if (state.current_table_idx >= bind_data.table_names.size()) {
        state.finished = true;
        return false;
    }

    const string &schema_name = bind_data.schema_names[state.current_table_idx];
    const string &table_name = bind_data.table_names[state.current_table_idx];
    const vector<string> &columns = bind_data.table_columns[state.current_table_idx];

    if (state.current_col_idx >= columns.size()) {
        // Move to next table
        state.current_table_idx++;
        state.current_col_idx = 0;
        return StartColumnQuery(context, state, bind_data);
    }

    const string &column_name = columns[state.current_col_idx];
    state.current_schema_name = schema_name;
    state.current_table_name = table_name;
    state.current_column_name = column_name;

    // Build query to search this specific column (schema-qualified)
    string sql = "SELECT * FROM " + EscapeIdentifier(schema_name) + "." + EscapeIdentifier(table_name) +
                 " WHERE LOWER(CAST(" + EscapeIdentifier(column_name) +
                 " AS VARCHAR)) LIKE LOWER(?)";

    Connection conn(context.db->GetDatabase(context));
    auto prepared = conn.Prepare(sql);
    
    if (prepared->HasError()) {
        // Skip this column if query fails
        state.current_col_idx++;
        return StartColumnQuery(context, state, bind_data);
    }

    vector<Value> params;
    params.push_back(Value(bind_data.search_pattern));
    
    state.current_result = prepared->Execute(params);
    
    if (state.current_result->HasError()) {
        // Skip this column if execution fails
        state.current_col_idx++;
        return StartColumnQuery(context, state, bind_data);
    }

    state.current_chunk.reset();
    state.chunk_offset = 0;
    return true;
}

static void SearchDatabaseFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<SearchDatabaseBindData>();
    auto &state = data_p.global_state->Cast<SearchDatabaseGlobalState>();

    if (state.finished) {
        output.SetCardinality(0);
        return;
    }

    // Initialize first query if needed
    if (!state.current_result) {
        if (!StartColumnQuery(context, state, bind_data)) {
            output.SetCardinality(0);
            return;
        }
    }

    idx_t output_idx = 0;
    
    while (output_idx < STANDARD_VECTOR_SIZE && !state.finished) {
        // Get current chunk or fetch new one
        if (!state.current_chunk || state.chunk_offset >= state.current_chunk->size()) {
            state.current_chunk = state.current_result->Fetch();
            state.chunk_offset = 0;

            if (!state.current_chunk || state.current_chunk->size() == 0) {
                // Move to next column
                state.current_col_idx++;
                if (!StartColumnQuery(context, state, bind_data)) {
                    break;
                }
                continue;
            }
        }

        // Get column names for current table
        const vector<string> &columns = bind_data.table_columns[state.current_table_idx];
        
        // Find the column index for the matched column
        idx_t matched_col_idx = 0;
        for (idx_t i = 0; i < columns.size(); i++) {
            if (columns[i] == state.current_column_name) {
                matched_col_idx = i;
                break;
            }
        }

        // Process rows from current chunk
        while (state.chunk_offset < state.current_chunk->size() && output_idx < STANDARD_VECTOR_SIZE) {
            idx_t row = state.chunk_offset;
            
            // Get the matched value
            Value matched_val;
            if (matched_col_idx < state.current_chunk->ColumnCount()) {
                matched_val = state.current_chunk->data[matched_col_idx].GetValue(row);
            }
            
            // Set output values
            // schema_name
            output.data[0].SetValue(output_idx, Value(state.current_schema_name));
            // table_name
            output.data[1].SetValue(output_idx, Value(state.current_table_name));
            // column_name
            output.data[2].SetValue(output_idx, Value(state.current_column_name));
            // matched_value
            output.data[3].SetValue(output_idx, Value(matched_val.ToString()));
            // row_data (JSON)
            string row_json = RowToJson(*state.current_chunk, row, columns);
            output.data[4].SetValue(output_idx, Value(row_json));
            
            output_idx++;
            state.chunk_offset++;
        }
    }

    output.SetCardinality(output_idx);
}

void RegisterSearchDatabaseFunction(ExtensionLoader& loader) {
    TableFunction search_database_func(
        "stps_search_database",
        {LogicalType::VARCHAR},
        SearchDatabaseFunction,
        SearchDatabaseBind,
        SearchDatabaseInit
    );

    loader.RegisterFunction(search_database_func);
}

} // namespace stps
} // namespace duckdb
