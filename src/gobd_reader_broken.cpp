#include "include/gobd_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/common/file_system.hpp"
#include <sstream>

namespace duckdb {
namespace polarsgodmode {

struct GobdReaderBindData : public TableFunctionData {
    string csv_path;
    string delimiter;
    vector<string> column_names;

    GobdReaderBindData(string csv_path_p, string delimiter_p, vector<string> column_names_p)
        : csv_path(std::move(csv_path_p)),
          delimiter(std::move(delimiter_p)),
          column_names(std::move(column_names_p)) {}
};

struct GobdReaderLocalState : public LocalTableFunctionState {
    bool done = false;
};

struct GobdReaderGlobalState : public GlobalTableFunctionState {
    unique_ptr<GlobalTableFunctionState> csv_state;
    unique_ptr<TableFunction> csv_function;
    unique_ptr<FunctionData> csv_bind_data;

    idx_t MaxThreads() const override {
        return 1;
    }
};

// Helper function to extract directory from file path
static string GetDirectory(const string &filepath) {
    size_t pos = filepath.find_last_of("/\\");
    if (pos == string::npos) {
        return ".";
    }
    return filepath.substr(0, pos);
}

// Helper to escape single quotes in strings
static string EscapeString(const string &str) {
    string result;
    result.reserve(str.length());
    for (char c : str) {
        if (c == '\'') {
            result += "''";
        } else {
            result += c;
        }
    }
    return result;
}

static unique_ptr<FunctionData> GobdReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

    string index_path = input.inputs[0].ToString();
    string table_name = input.inputs[1].ToString();
    string delimiter = input.inputs.size() > 2 ? input.inputs[2].ToString() : ";";

    // Get the directory from index.xml path
    string index_dir = GetDirectory(index_path);

    // Build query to extract schema - this runs ONLY ONCE during bind
    string query = R"(
        WITH RECURSIVE xml_tree AS (
            SELECT
                NULL::VARCHAR AS parent_tag,
                data ->> '_tag' AS tag,
                data -> '_children' AS children,
                data AS node,
                0 AS depth,
                '0' AS path
            FROM (SELECT stps_read_xml_json(')" + EscapeString(index_path) + R"(')::JSON as data)
            UNION ALL
            SELECT
                t.tag AS parent_tag,
                c.value ->> '_tag' AS tag,
                c.value -> '_children' AS children,
                c.value AS node,
                t.depth + 1,
                t.path || '.' || c.key
            FROM xml_tree t
            CROSS JOIN LATERAL (
                SELECT key, value
                FROM json_each(t.children)
            ) c
            WHERE t.children IS NOT NULL AND json_type(t.children) = 'ARRAY'
        ),
        tables AS (
            SELECT
                path as table_path,
                (SELECT value ->> '_text' FROM json_each(children) WHERE value ->> '_tag' = 'Name') as table_name,
                (SELECT value ->> '_text' FROM json_each(children) WHERE value ->> '_tag' = 'URL') as table_url
            FROM xml_tree
            WHERE tag = 'Table'
        ),
        variable_length AS (
            SELECT
                t.table_name,
                t.table_url,
                xt.node as column_node,
                xt.path as column_path
            FROM tables t
            JOIN xml_tree xt ON xt.path LIKE t.table_path || '.%'
            WHERE xt.tag IN ('VariablePrimaryKey', 'VariableColumn')
        )
        SELECT
            table_url,
            (SELECT value ->> '_text' FROM json_each(column_node -> '_children') WHERE value ->> '_tag' = 'Name') as column_name,
            (string_split(column_path, '.')[-1])::INTEGER as column_order
        FROM variable_length
        WHERE table_name = ')" + EscapeString(table_name) + R"('
        ORDER BY column_order;
    )";

    // Execute the query to get schema - this is OK in bind phase
    auto query_result = context.Query(query, false);
    if (query_result->HasError()) {
        throw BinderException("Failed to parse GoBD schema: " + query_result->GetError());
    }

    // Extract results
    auto chunk = query_result->Fetch();
    if (!chunk || chunk->size() == 0) {
        throw BinderException("Table '" + table_name + "' not found in GoBD index");
    }

    // Get table URL and column names
    string table_url = chunk->data[0].GetValue(0).ToString();
    vector<string> column_names;

    for (idx_t i = 0; i < chunk->size(); i++) {
        string column_name = chunk->data[1].GetValue(i).ToString();
        column_names.push_back(column_name);
    }

    // Construct full CSV path
    string csv_path = index_dir + "/" + table_url;

    // Set return types (all VARCHAR for simplicity, CSV reader will handle conversion)
    for (const auto &col_name : column_names) {
        names.push_back(col_name);
        return_types.push_back(LogicalType::VARCHAR);
    }

    return make_uniq<GobdReaderBindData>(csv_path, delimiter, column_names);
}

static unique_ptr<GlobalTableFunctionState> GobdReaderInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<GobdReaderBindData>();
    auto result = make_uniq<GobdReaderGlobalState>();

    // Get read_csv function from catalog
    auto &catalog = Catalog::GetSystemCatalog(context);
    auto &func_catalog = catalog.GetEntry<TableFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "read_csv");

    // Find the appropriate overload
    vector<LogicalType> csv_arg_types = {LogicalType::VARCHAR};
    string error;
    idx_t best_function = Function::BindFunction(func_catalog.name, func_catalog.functions, csv_arg_types, error);
    if (best_function == DConstants::INVALID_INDEX) {
        throw BinderException("Could not find read_csv function: " + error);
    }

    result->csv_function = make_uniq<TableFunction>(func_catalog.functions.GetFunctionByOffset(best_function));

    // Build CSV options as a named_parameter_map_t
    named_parameter_map_t csv_params;
    csv_params["delim"] = Value(bind_data.delimiter);
    csv_params["header"] = Value::BOOLEAN(false);
    csv_params["auto_detect"] = Value::BOOLEAN(false);

    // Build column names array
    vector<Value> col_values;
    for (const auto &col : bind_data.column_names) {
        col_values.push_back(Value(col));
    }
    csv_params["names"] = Value::LIST(LogicalType::VARCHAR, col_values);

    // Bind CSV function
    vector<Value> csv_inputs = {Value(bind_data.csv_path)};
    TableFunctionBindInput csv_bind_input(csv_inputs, csv_params, nullptr, nullptr, nullptr, nullptr);

    vector<LogicalType> csv_return_types;
    vector<string> csv_return_names;
    result->csv_bind_data = result->csv_function->bind(context, csv_bind_input, csv_return_types, csv_return_names);

    // Initialize CSV global state
    TableFunctionInitInput csv_init_input(result->csv_bind_data.get(), {}, {});
    result->csv_state = result->csv_function->init_global(context, csv_init_input);

    return std::move(result);
}

static unique_ptr<LocalTableFunctionState> GobdReaderInitLocal(ExecutionContext &context,
                                                                 TableFunctionInitInput &input,
                                                                 GlobalTableFunctionState *global_state) {
    auto result = make_uniq<GobdReaderLocalState>();
    return std::move(result);
}

static void GobdReaderFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &global_state = data_p.global_state->Cast<GobdReaderGlobalState>();
    auto &local_state = data_p.local_state->Cast<GobdReaderLocalState>();

    if (local_state.done) {
        return;
    }

    // Call the CSV reader function directly
    if (global_state.csv_function->function) {
        TableFunctionInput csv_input(global_state.csv_bind_data.get(), local_state,
                                      global_state.csv_state.get());
        global_state.csv_function->function(context, csv_input, output);

        if (output.size() == 0) {
            local_state.done = true;
        }
    }
}

void RegisterGobdReaderFunctions(ExtensionLoader &loader) {
    TableFunction read_gobd("stps_read_gobd",
                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                           GobdReaderFunction, GobdReaderBind, GobdReaderInitGlobal, GobdReaderInitLocal);

    read_gobd.named_parameters["delimiter"] = LogicalType::VARCHAR;

    CreateTableFunctionInfo read_gobd_info(read_gobd);
    loader.RegisterFunction(read_gobd_info);
}

} // namespace polarsgodmode
} // namespace duckdb
