#include "include/gobd_reader.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {
namespace polarsgodmode {

void RegisterGobdReaderFunctions(ExtensionLoader &loader) {
    // Register helper macro to extract GoBD schema
    string gobd_extract_schema_macro = R"(
        CREATE OR REPLACE MACRO gobd_extract_schema(json_data) AS TABLE
        WITH RECURSIVE xml_tree AS (
            SELECT
                NULL::VARCHAR AS parent_tag,
                json_data ->> '_tag' AS tag,
                json_data -> '_children' AS children,
                json_data AS node,
                0 AS depth,
                '0' AS path
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
                SELECT key, value FROM json_each(t.children)
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
            table_name,
            table_url,
            (SELECT value ->> '_text' FROM json_each(column_node -> '_children') WHERE value ->> '_tag' = 'Name') as column_name,
            (string_split(column_path, '.')[-1])::INTEGER as column_order
        FROM variable_length
        ORDER BY table_name, column_order;
    )";

    // Register macro to list tables
    string gobd_list_tables_macro = R"(
        CREATE OR REPLACE MACRO gobd_list_tables(index_path) AS TABLE
        WITH RECURSIVE xml_tree AS (
            SELECT
                NULL::VARCHAR AS parent_tag,
                data ->> '_tag' AS tag,
                data -> '_children' AS children,
                0 AS depth
            FROM (SELECT stps_read_xml_json(index_path)::JSON as data)
            UNION ALL
            SELECT
                t.tag AS parent_tag,
                c.value ->> '_tag' AS tag,
                c.value -> '_children' AS children,
                t.depth + 1
            FROM xml_tree t
            CROSS JOIN LATERAL (
                SELECT value FROM json_each(t.children)
            ) c
            WHERE t.children IS NOT NULL AND json_type(t.children) = 'ARRAY'
        )
        SELECT DISTINCT
            (SELECT value ->> '_text' FROM json_each(children) WHERE value ->> '_tag' = 'Name') as table_name,
            (SELECT value ->> '_text' FROM json_each(children) WHERE value ->> '_tag' = 'URL') as table_url
        FROM xml_tree
        WHERE tag = 'Table'
        ORDER BY table_name;
    )";

    // Register the main macro that wraps everything
    string gobd_read_macro = R"(
        CREATE OR REPLACE MACRO stps_read_gobd(index_path, table_name, delimiter := ';') AS TABLE
        SELECT * FROM read_csv(
            (SELECT regexp_replace(index_path, '[^/]*$', '') || table_url
             FROM gobd_extract_schema(stps_read_xml_json(index_path)::JSON)
             WHERE table_name = $table_name LIMIT 1),
            delim=delimiter,
            names=(SELECT list(column_name ORDER BY column_order)
                   FROM gobd_extract_schema(stps_read_xml_json(index_path)::JSON)
                   WHERE table_name = $table_name),
            header=false
        );
    )";

    // Note: Macros are best loaded from SQL files to avoid escaping issues
    // The actual macros are in gobd_functions.sql
    // This function now just serves as a placeholder
}

} // namespace polarsgodmode
} // namespace duckdb
