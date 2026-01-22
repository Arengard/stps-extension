#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include <sstream>
#include <set>

namespace duckdb {
namespace stps {

//===--------------------------------------------------------------------===//
// Shared bind data and helpers for duplicate functions
//===--------------------------------------------------------------------===//

struct DuplicatesBindData : public TableFunctionData {
    string table_name;
    vector<string> check_columns;      // Columns to check for duplicates (empty = all)
    vector<string> output_columns;     // All columns to return
    vector<LogicalType> output_types;  // Types of output columns
};

struct DuplicatesGlobalState : public GlobalTableFunctionState {
    unique_ptr<QueryResult> result;
    bool finished = false;
};

// Helper to escape table/column names for SQL
static string EscapeIdentifier(const string &name) {
    // Simple escaping - wrap in quotes and escape internal quotes
    string escaped;
    escaped.reserve(name.size() + 2);
    escaped += '"';
    for (char c : name) {
        if (c == '"') {
            escaped += "\"\"";
        } else {
            escaped += c;
        }
    }
    escaped += '"';
    return escaped;
}

// Shared bind function for both drop_duplicates and show_duplicates
static unique_ptr<FunctionData> DuplicatesBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names,
                                               const string &function_name) {
    auto result = make_uniq<DuplicatesBindData>();

    // Required parameter: table_name
    if (input.inputs.empty()) {
        throw BinderException("%s requires at least one argument: table_name", function_name);
    }
    result->table_name = input.inputs[0].GetValue<string>();

    // Optional parameter: columns (as LIST or multiple VARCHAR arguments)
    if (input.inputs.size() > 1) {
        if (input.inputs[1].type().id() == LogicalTypeId::LIST) {
            auto &list_children = ListValue::GetChildren(input.inputs[1]);
            for (const auto &child : list_children) {
                result->check_columns.push_back(child.ToString());
            }
        } else {
            for (idx_t i = 1; i < input.inputs.size(); i++) {
                result->check_columns.push_back(input.inputs[i].ToString());
            }
        }
    }

    // Check for named parameter 'columns'
    for (const auto &kv : input.named_parameters) {
        if (kv.first == "columns") {
            if (kv.second.type().id() == LogicalTypeId::LIST) {
                auto &list_children = ListValue::GetChildren(kv.second);
                for (const auto &child : list_children) {
                    result->check_columns.push_back(child.ToString());
                }
            } else {
                result->check_columns.push_back(kv.second.ToString());
            }
        }
    }

    // Create a connection to get table schema
    Connection conn(context.db->GetDatabase(context));

    // Get table columns and types - use escaped table name
    string schema_query = "SELECT * FROM " + EscapeIdentifier(result->table_name) + " LIMIT 0";
    auto schema_result = conn.Query(schema_query);
    if (schema_result->HasError()) {
        throw BinderException("Table '%s' does not exist or cannot be queried: %s",
                            result->table_name, schema_result->GetError());
    }

    // Store output schema
    for (idx_t i = 0; i < schema_result->names.size(); i++) {
        result->output_columns.push_back(schema_result->names[i]);
        result->output_types.push_back(schema_result->types[i]);
    }

    // Validate check columns exist in table
    if (!result->check_columns.empty()) {
        std::set<string> table_columns(result->output_columns.begin(), result->output_columns.end());
        for (const auto &col : result->check_columns) {
            if (table_columns.find(col) == table_columns.end()) {
                throw BinderException("Column '%s' not found in table '%s'", col, result->table_name);
            }
        }
    }

    // Set output schema (all columns from the table)
    names = result->output_columns;
    return_types = result->output_types;

    return result;
}

// Shared scan function
static void DuplicatesScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<DuplicatesGlobalState>();

    if (state.finished || !state.result) {
        output.SetCardinality(0);
        return;
    }

    auto chunk = state.result->Fetch();
    if (!chunk || chunk->size() == 0) {
        state.finished = true;
        output.SetCardinality(0);
        return;
    }

    output.Reference(*chunk);
}

//===--------------------------------------------------------------------===//
// stps_drop_duplicates - Returns unique rows (removes duplicates)
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> DropDuplicatesBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
    return DuplicatesBind(context, input, return_types, names, "stps_drop_duplicates");
}

static unique_ptr<GlobalTableFunctionState> DropDuplicatesInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<DuplicatesBindData>();
    auto state = make_uniq<DuplicatesGlobalState>();

    std::ostringstream query;
    string table_escaped = EscapeIdentifier(bind_data.table_name);

    if (bind_data.check_columns.empty()) {
        // No specific columns - use DISTINCT on all columns
        query << "SELECT DISTINCT * FROM " << table_escaped;
    } else {
        // Use ROW_NUMBER() window function to get first occurrence per group
        query << "SELECT ";
        for (idx_t i = 0; i < bind_data.output_columns.size(); i++) {
            if (i > 0) query << ", ";
            query << EscapeIdentifier(bind_data.output_columns[i]);
        }
        query << " FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY ";
        for (idx_t i = 0; i < bind_data.check_columns.size(); i++) {
            if (i > 0) query << ", ";
            query << EscapeIdentifier(bind_data.check_columns[i]);
        }
        query << ") AS __rn FROM " << table_escaped;
        query << ") AS __subq WHERE __rn = 1";
    }

    Connection conn(context.db->GetDatabase(context));
    state->result = conn.Query(query.str());

    if (state->result->HasError()) {
        throw InternalException("Failed to deduplicate: %s", state->result->GetError());
    }

    return state;
}

//===--------------------------------------------------------------------===//
// stps_show_duplicates - Returns only duplicate rows (keeps all occurrences)
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> ShowDuplicatesBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
    return DuplicatesBind(context, input, return_types, names, "stps_show_duplicates");
}

static unique_ptr<GlobalTableFunctionState> ShowDuplicatesInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<DuplicatesBindData>();
    auto state = make_uniq<DuplicatesGlobalState>();

    std::ostringstream query;
    string table_escaped = EscapeIdentifier(bind_data.table_name);

    if (bind_data.check_columns.empty()) {
        // No specific columns - find rows where all columns match another row
        // Use COUNT(*) OVER (PARTITION BY all columns) > 1
        query << "SELECT ";
        for (idx_t i = 0; i < bind_data.output_columns.size(); i++) {
            if (i > 0) query << ", ";
            query << EscapeIdentifier(bind_data.output_columns[i]);
        }
        query << " FROM (SELECT *, COUNT(*) OVER (PARTITION BY ";
        for (idx_t i = 0; i < bind_data.output_columns.size(); i++) {
            if (i > 0) query << ", ";
            query << EscapeIdentifier(bind_data.output_columns[i]);
        }
        query << ") AS __cnt FROM " << table_escaped;
        query << ") AS __subq WHERE __cnt > 1";
    } else {
        // Find rows where check_columns match another row
        query << "SELECT ";
        for (idx_t i = 0; i < bind_data.output_columns.size(); i++) {
            if (i > 0) query << ", ";
            query << EscapeIdentifier(bind_data.output_columns[i]);
        }
        query << " FROM (SELECT *, COUNT(*) OVER (PARTITION BY ";
        for (idx_t i = 0; i < bind_data.check_columns.size(); i++) {
            if (i > 0) query << ", ";
            query << EscapeIdentifier(bind_data.check_columns[i]);
        }
        query << ") AS __cnt FROM " << table_escaped;
        query << ") AS __subq WHERE __cnt > 1";
    }

    Connection conn(context.db->GetDatabase(context));
    state->result = conn.Query(query.str());

    if (state->result->HasError()) {
        throw InternalException("Failed to find duplicates: %s", state->result->GetError());
    }

    return state;
}

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//

void RegisterDropDuplicatesFunction(ExtensionLoader &loader) {
    // stps_drop_duplicates: Just table name (dedup all columns)
    TableFunction drop_func1("stps_drop_duplicates", {LogicalType::VARCHAR}, DuplicatesScan,
                             DropDuplicatesBind, DropDuplicatesInit);
    drop_func1.named_parameters["columns"] = LogicalType::LIST(LogicalType::VARCHAR);
    loader.RegisterFunction(drop_func1);

    // stps_drop_duplicates: Table name + list of columns
    TableFunction drop_func2("stps_drop_duplicates", {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)},
                             DuplicatesScan, DropDuplicatesBind, DropDuplicatesInit);
    loader.RegisterFunction(drop_func2);

    // stps_show_duplicates: Just table name (check all columns)
    TableFunction show_func1("stps_show_duplicates", {LogicalType::VARCHAR}, DuplicatesScan,
                             ShowDuplicatesBind, ShowDuplicatesInit);
    show_func1.named_parameters["columns"] = LogicalType::LIST(LogicalType::VARCHAR);
    loader.RegisterFunction(show_func1);

    // stps_show_duplicates: Table name + list of columns
    TableFunction show_func2("stps_show_duplicates", {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)},
                             DuplicatesScan, ShowDuplicatesBind, ShowDuplicatesInit);
    loader.RegisterFunction(show_func2);
}

} // namespace stps
} // namespace duckdb
