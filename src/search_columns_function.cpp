#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include <algorithm>
#include <cctype>

namespace duckdb {
namespace stps {

struct SearchColumnsBindData : public TableFunctionData {
    string table_name;
    string search_pattern;
    bool case_sensitive;
    vector<string> matching_columns;

    SearchColumnsBindData() : case_sensitive(false) {}
};

struct SearchColumnsGlobalState : public GlobalTableFunctionState {
    idx_t current_idx = 0;
};

static string to_lower_str(const string& str) {
    string result = str;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return result;
}

// SQL LIKE pattern matching with % (any chars) and _ (single char) wildcards
static bool like_match(const string& str, const string& pattern, size_t str_idx = 0, size_t pat_idx = 0) {
    while (pat_idx < pattern.size()) {
        char p = pattern[pat_idx];

        if (p == '%') {
            // Skip consecutive %
            while (pat_idx < pattern.size() && pattern[pat_idx] == '%') {
                pat_idx++;
            }
            // % at end matches everything
            if (pat_idx >= pattern.size()) {
                return true;
            }
            // Try matching % with 0 or more characters
            for (size_t i = str_idx; i <= str.size(); i++) {
                if (like_match(str, pattern, i, pat_idx)) {
                    return true;
                }
            }
            return false;
        } else if (p == '_') {
            // _ matches exactly one character
            if (str_idx >= str.size()) {
                return false;
            }
            str_idx++;
            pat_idx++;
        } else {
            // Regular character - must match exactly
            if (str_idx >= str.size() || str[str_idx] != p) {
                return false;
            }
            str_idx++;
            pat_idx++;
        }
    }
    // Pattern exhausted - string must also be exhausted
    return str_idx >= str.size();
}

static bool column_matches_pattern(const string& column_name, const string& pattern, bool case_sensitive) {
    if (pattern.empty()) {
        return true;
    }

    string col = case_sensitive ? column_name : to_lower_str(column_name);
    string pat = case_sensitive ? pattern : to_lower_str(pattern);

    // Check if pattern contains SQL LIKE wildcards
    bool has_wildcards = (pat.find('%') != string::npos || pat.find('_') != string::npos);

    if (has_wildcards) {
        // Use SQL LIKE pattern matching
        return like_match(col, pat);
    } else {
        // Simple substring search (for backward compatibility)
        return col.find(pat) != string::npos;
    }
}

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
    auto transaction = CatalogTransaction::Get(context);

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
    TableFunction search_columns_func("stps_search_columns", {LogicalType::VARCHAR, LogicalType::VARCHAR},
                                      SearchColumnsFunction, SearchColumnsBind, SearchColumnsInit);

    // Optional third parameter for case sensitivity
    search_columns_func.arguments.push_back(LogicalType::BOOLEAN);


    loader.RegisterFunction(search_columns_func);
}

} // namespace stps
} // namespace duckdb
