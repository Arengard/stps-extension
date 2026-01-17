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
    idx_t current_idx = 0;
};

static unique_ptr<FunctionData> SearchColumnsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<SearchColumnsBindData>();

    // Get parameters
    result->table_name = input.inputs[0].GetValue<string>();
    result->search_pattern = input.inputs[1].GetValue<string>();

    if (input.inputs.size() >= 3) {
        result->case_sensitive = input.inputs[2].GetValue<bool>();
    }

    // Get table columns from catalog
    auto &catalog = Catalog::GetCatalog(context, INVALID_CATALOG);
    auto &schema = catalog.GetSchema(context, DEFAULT_SCHEMA);
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

    try {
        auto table_entry = schema.GetEntry(transaction, CatalogType::TABLE_ENTRY, result->table_name);
        if (table_entry && table_entry->type == CatalogType::TABLE_ENTRY) {
            auto &table = table_entry->Cast<TableCatalogEntry>();
            auto &columns = table.GetColumns();

            // Find matching columns
            for (auto &col : columns.Logical()) {
                if (column_matches_pattern(col.Name(), result->search_pattern, result->case_sensitive)) {
                    result->matching_columns.push_back(col.Name());
                }
            }
        } else {
            throw BinderException("Table '%s' not found", result->table_name); // unify error handling
        }
    } catch (std::exception &e) {
        // Table not found or error accessing it
        throw BinderException("Table '%s' not found or inaccessible: %s", result->table_name, e.what());
    }

    // Define output columns
    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("column_name");

    return_types.push_back(LogicalType::INTEGER);
    names.push_back("column_index");

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> SearchColumnsInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<SearchColumnsGlobalState>();
}

static void SearchColumnsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<SearchColumnsBindData>();
    auto &state = data_p.global_state->Cast<SearchColumnsGlobalState>();

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.current_idx < bind_data.matching_columns.size() && count < max_count) {
        auto &col_name = bind_data.matching_columns[state.current_idx];

        // Set column name
        auto col_name_data = FlatVector::GetData<string_t>(output.data[0]);
        col_name_data[count] = StringVector::AddString(output.data[0], col_name);

        // Set column index (1-based for SQL convention)
        auto col_idx_data = FlatVector::GetData<int32_t>(output.data[1]);
        col_idx_data[count] = static_cast<int32_t>(state.current_idx + 1);

        state.current_idx++;
        count++;
    }

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
