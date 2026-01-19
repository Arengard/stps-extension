#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include <algorithm>
#include <cctype>

namespace duckdb {
namespace stps {

struct SearchColumnsBindData : public TableFunctionData {
    string table_name;
    string search_pattern;
    string generated_sql;  // Simple SQL to fetch matching rows
    vector<LogicalType> original_column_types;
    vector<string> original_column_names;
};

struct SearchColumnsGlobalState : public GlobalTableFunctionState {
    unique_ptr<QueryResult> result;
    unique_ptr<DataChunk> current_chunk;
    idx_t chunk_offset = 0;
    bool query_executed = false;
};

// Helper: case-insensitive pattern match (SQL LIKE style)
static bool MatchesPattern(const string &value, const string &pattern) {
    // Convert both to lowercase for case-insensitive match
    string lower_value, lower_pattern;
    lower_value.reserve(value.size());
    lower_pattern.reserve(pattern.size());

    for (char c : value) lower_value += std::tolower(static_cast<unsigned char>(c));
    for (char c : pattern) lower_pattern += std::tolower(static_cast<unsigned char>(c));

    // Simple LIKE pattern matching with % and _
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

static unique_ptr<FunctionData> SearchColumnsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<SearchColumnsBindData>();

    if (input.inputs.size() < 2) {
        throw BinderException("stps_search_columns requires 2 arguments: table_name, pattern");
    }

    result->table_name = input.inputs[0].GetValue<string>();
    result->search_pattern = input.inputs[1].GetValue<string>();

    Connection conn(context.db->GetDatabase(context));

    string schema_query = "SELECT * FROM " + result->table_name + " LIMIT 0";
    auto schema_result = conn.Query(schema_query);

    if (schema_result->HasError()) {
        throw BinderException("Table '%s' not found or inaccessible: %s",
                            result->table_name.c_str(), schema_result->GetError().c_str());
    }

    result->original_column_names = schema_result->names;
    result->original_column_types = schema_result->types;

    if (result->original_column_names.empty()) {
        throw BinderException("Table '%s' has no columns", result->table_name.c_str());
    }

    // Generate simple SQL - just filter rows where ANY column matches
    // We'll determine WHICH columns match in C++
    string sql = "SELECT * FROM " + result->table_name + " WHERE ";

    for (idx_t i = 0; i < result->original_column_names.size(); i++) {
        if (i > 0) sql += " OR ";
        sql += "LOWER(CAST(\"" + result->original_column_names[i] + "\" AS VARCHAR)) LIKE LOWER(?)";
    }

    result->generated_sql = sql;

    // Output schema: original columns + matched_columns list
    for (idx_t i = 0; i < result->original_column_names.size(); i++) {
        return_types.push_back(result->original_column_types[i]);
        names.push_back(result->original_column_names[i]);
    }

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

        auto prepared = conn.Prepare(bind_data.generated_sql);
        if (prepared->HasError()) {
            throw InvalidInputException("Failed to prepare search query: %s", prepared->GetError().c_str());
        }

        // Bind pattern for each column in WHERE clause
        vector<Value> params;
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

        if (!state.current_chunk || state.current_chunk->size() == 0) {
            output.SetCardinality(0);
            return;
        }
    }

    idx_t remaining = state.current_chunk->size() - state.chunk_offset;
    idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);

    idx_t num_original_cols = bind_data.original_column_names.size();

    // Copy original columns from source
    for (idx_t col_idx = 0; col_idx < num_original_cols; col_idx++) {
        auto &source_vector = state.current_chunk->data[col_idx];
        auto &dest_vector = output.data[col_idx];
        VectorOperations::Copy(source_vector, dest_vector, state.chunk_offset + count, state.chunk_offset, 0);
    }

    // Build matched_columns list in C++ for each row
    auto &matched_col_vector = output.data[num_original_cols];
    auto &list_validity = FlatVector::Validity(matched_col_vector);
    auto list_entries = FlatVector::GetData<list_entry_t>(matched_col_vector);

    auto &child_vector = ListVector::GetEntry(matched_col_vector);
    idx_t current_list_offset = 0;

    for (idx_t row = 0; row < count; row++) {
        idx_t source_row = state.chunk_offset + row;
        vector<string> matched_names;

        // Check each column for this row
        for (idx_t col_idx = 0; col_idx < num_original_cols; col_idx++) {
            auto &source_vector = state.current_chunk->data[col_idx];

            if (FlatVector::IsNull(source_vector, source_row)) {
                continue;
            }

            // Get value as string
            Value val = source_vector.GetValue(source_row);
            string str_val = val.ToString();

            // Check if it matches the pattern
            if (MatchesPattern(str_val, bind_data.search_pattern)) {
                matched_names.push_back(bind_data.original_column_names[col_idx]);
            }
        }

        // Set list entry
        list_entries[row].offset = current_list_offset;
        list_entries[row].length = matched_names.size();
        list_validity.SetValid(row);

        // Add matched column names to child vector
        for (const auto &name : matched_names) {
            ListVector::PushBack(matched_col_vector, Value(name));
        }

        current_list_offset += matched_names.size();
    }

    state.chunk_offset += count;
    output.SetCardinality(count);
}

void RegisterSearchColumnsFunction(ExtensionLoader& loader) {
    TableFunction search_columns_func(
        "stps_search_columns",
        {LogicalType::VARCHAR, LogicalType::VARCHAR},
        SearchColumnsFunction,
        SearchColumnsBind,
        SearchColumnsInit
    );

    loader.RegisterFunction(search_columns_func);
}

} // namespace stps
} // namespace duckdb
