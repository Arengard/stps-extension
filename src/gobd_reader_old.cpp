#include "include/gobd_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include <sstream>

namespace duckdb {
namespace polarsgodmode {

struct GobdReaderBindData : public TableFunctionData {
    string index_path;
    string table_name;
    string delimiter;
    string csv_path;
    vector<string> column_names;
    vector<LogicalType> column_types;

    GobdReaderBindData(string index_path_p, string table_name_p, string delimiter_p)
        : index_path(std::move(index_path_p)),
          table_name(std::move(table_name_p)),
          delimiter(std::move(delimiter_p)) {}
};

// Helper function to extract directory from file path
static string GetDirectory(const string &filepath) {
    size_t pos = filepath.find_last_of("/\\");
    if (pos == string::npos) {
        return ".";
    }
    return filepath.substr(0, pos);
}

// Helper function to convert GoBD data type to DuckDB type
static LogicalType GobdTypeToDuckDbType(const string &gobd_type, int accuracy = -1) {
    if (gobd_type == "Numeric") {
        if (accuracy >= 0) {
            // Use DECIMAL for numeric with accuracy
            return LogicalType::DECIMAL(18, accuracy);
        }
        return LogicalType::DOUBLE;
    } else if (gobd_type == "Date") {
        return LogicalType::DATE;
    } else if (gobd_type == "AlphaNumeric") {
        return LogicalType::VARCHAR;
    }
    // Default to VARCHAR
    return LogicalType::VARCHAR;
}

static unique_ptr<FunctionData> GobdReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<GobdReaderBindData>(
        input.inputs[0].ToString(),  // index_path
        input.inputs[1].ToString(),  // table_name
        input.inputs.size() > 2 ? input.inputs[2].ToString() : ";"  // delimiter (default semicolon)
    );

    // Get the directory from index.xml path
    string index_dir = GetDirectory(result->index_path);

    // Build query to extract schema using the XML parser
    string query = R"(
        WITH RECURSIVE xml_tree AS (
            SELECT
                NULL::VARCHAR AS parent_tag,
                data ->> '_tag' AS tag,
                data -> '_children' AS children,
                data AS node,
                0 AS depth,
                '0' AS path
            FROM (SELECT stps_read_xml_json('" + result->index_path + R"(')::JSON as data)
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
                (SELECT value ->> '_text' FROM json_each(children) WHERE value ->> '_tag' = 'URL') as table_url,
                (SELECT value ->> '_text' FROM json_each(children) WHERE value ->> '_tag' = 'Description') as table_description
            FROM xml_tree
            WHERE tag = 'Table'
        ),
        variable_length AS (
            SELECT
                t.table_name,
                t.table_url,
                t.table_description,
                xt.node as column_node,
                xt.tag as column_type,
                xt.path as column_path
            FROM tables t
            JOIN xml_tree xt ON xt.path LIKE t.table_path || '.%'
            WHERE xt.tag IN ('VariablePrimaryKey', 'VariableColumn')
        ),
        with_numeric AS (
            SELECT
                *,
                (SELECT value FROM json_each(column_node -> '_children') WHERE value ->> '_tag' = 'Numeric') as numeric_node
            FROM variable_length
        )
        SELECT
            table_name,
            table_url,
            column_type,
            (SELECT value ->> '_text' FROM json_each(column_node -> '_children') WHERE value ->> '_tag' = 'Name') as column_name,
            CASE
                WHEN numeric_node IS NOT NULL THEN 'Numeric'
                WHEN EXISTS (SELECT 1 FROM json_each(column_node -> '_children') WHERE value ->> '_tag' = 'AlphaNumeric') THEN 'AlphaNumeric'
                WHEN EXISTS (SELECT 1 FROM json_each(column_node -> '_children') WHERE value ->> '_tag' = 'Date') THEN 'Date'
                ELSE 'Unknown'
            END as data_type,
            CASE WHEN numeric_node IS NOT NULL THEN
                (SELECT (value ->> '_text')::INTEGER FROM json_each(numeric_node -> '_children') WHERE value ->> '_tag' = 'Accuracy')
            END as accuracy,
            (string_split(column_path, '.')[-1])::INTEGER as column_order
        FROM with_numeric
        WHERE table_name = ')" + result->table_name + R"('
        ORDER BY column_order;
    )";

    // Execute the query to get schema
    auto query_result = context.Query(query, false);
    if (query_result->HasError()) {
        throw BinderException("Failed to parse GoBD schema: " + query_result->GetError());
    }

    // Extract table URL from first row
    auto chunk = query_result->Fetch();
    if (!chunk || chunk->size() == 0) {
        throw BinderException("Table '" + result->table_name + "' not found in GoBD index");
    }

    // Get table URL from first row
    auto table_url_vector = chunk->data[1];
    auto table_url = table_url_vector.GetValue(0).ToString();

    // Construct full CSV path
    result->csv_path = index_dir + "/" + table_url;

    // Build column names and types
    for (idx_t i = 0; i < chunk->size(); i++) {
        auto column_name = chunk->data[3].GetValue(i).ToString();
        auto data_type = chunk->data[4].GetValue(i).ToString();

        // Get accuracy if available
        int accuracy = -1;
        auto accuracy_value = chunk->data[5].GetValue(i);
        if (!accuracy_value.IsNull()) {
            accuracy = accuracy_value.GetValue<int32_t>();
        }

        result->column_names.push_back(column_name);
        result->column_types.push_back(GobdTypeToDuckDbType(data_type, accuracy));
    }

    // Set return types and names
    return_types = result->column_types;
    names = result->column_names;

    return std::move(result);
}

static void GobdReaderFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<GobdReaderBindData>();

    // Build column names list for read_csv
    string column_names_list = "[";
    for (size_t i = 0; i < bind_data.column_names.size(); i++) {
        if (i > 0) column_names_list += ", ";
        column_names_list += "'" + bind_data.column_names[i] + "'";
    }
    column_names_list += "]";

    // Build the read_csv query
    string csv_query = "SELECT * FROM read_csv('" + bind_data.csv_path +
                      "', delim='" + bind_data.delimiter +
                      "', names=" + column_names_list +
                      ", header=false)";

    // Execute read_csv query
    auto result = context.Query(csv_query, false);
    if (result->HasError()) {
        throw IOException("Failed to read CSV file: " + result->GetError());
    }

    // Fetch and return data
    auto chunk = result->Fetch();
    if (chunk) {
        output.Move(*chunk);
    }
}

static unique_ptr<NodeStatistics> GobdReaderCardinality(ClientContext &context, const FunctionData *bind_data_p) {
    // Return unknown cardinality
    return make_uniq<NodeStatistics>();
}

void RegisterGobdReaderFunctions(ExtensionLoader &loader) {
    // stps_read_gobd(index_path, table_name, delimiter='\t')
    TableFunction read_gobd("stps_read_gobd",
                           {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                           GobdReaderFunction, GobdReaderBind);

    read_gobd.cardinality = GobdReaderCardinality;
    read_gobd.named_parameters["delimiter"] = LogicalType::VARCHAR;

    CreateTableFunctionInfo read_gobd_info(read_gobd);
    loader.RegisterFunction(read_gobd_info);
}

} // namespace polarsgodmode
} // namespace duckdb
