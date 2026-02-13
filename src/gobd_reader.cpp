#include "gobd_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include <fstream>
#include <sstream>
#include <algorithm>

namespace duckdb {
namespace stps {

// Helper function to extract directory from file path
static string GetDirectory(const string &filepath) {
    size_t pos = filepath.find_last_of("/\\");
    if (pos == string::npos) {
        return ".";
    }
    return filepath.substr(0, pos);
}

// Simple XML text extraction between tags
string ExtractTagValue(const string &xml, const string &tag_name, size_t start_pos) {
    string open_tag = "<" + tag_name + ">";
    string close_tag = "</" + tag_name + ">";

    size_t tag_start = xml.find(open_tag, start_pos);
    if (tag_start == string::npos) return "";

    size_t value_start = tag_start + open_tag.length();
    size_t tag_end = xml.find(close_tag, value_start);
    if (tag_end == string::npos) return "";

    return xml.substr(value_start, tag_end - value_start);
}

// Extract all occurrences of a tag
vector<string> ExtractAllTags(const string &xml, const string &tag_name) {
    vector<string> results;
    string open_tag = "<" + tag_name;
    string close_tag = "</" + tag_name + ">";

    size_t pos = 0;
    while (pos < xml.length()) {
        size_t tag_start = xml.find(open_tag, pos);
        if (tag_start == string::npos) break;

        // Find the end of opening tag (could have attributes)
        size_t open_end = xml.find(">", tag_start);
        if (open_end == string::npos) break;

        // Check if self-closing
        if (xml[open_end - 1] == '/') {
            pos = open_end + 1;
            continue;
        }

        size_t tag_end = xml.find(close_tag, open_end);
        if (tag_end == string::npos) break;

        results.push_back(xml.substr(tag_start, tag_end + close_tag.length() - tag_start));
        pos = tag_end + close_tag.length();
    }

    return results;
}

// Parse GoBD index.xml from an in-memory XML string
vector<GobdTable> ParseGobdIndexFromString(const string &xml) {
    vector<GobdTable> tables;

    // Find all Table elements
    auto table_elements = ExtractAllTags(xml, "Table");

    for (const auto &table_xml : table_elements) {
        GobdTable table;
        table.name = ExtractTagValue(table_xml, "Name");
        table.url = ExtractTagValue(table_xml, "URL");
        table.description = ExtractTagValue(table_xml, "Description");

        if (table.name.empty() || table.url.empty()) continue;

        // Extract VariablePrimaryKey and VariableColumn elements
        int col_order = 0;

        auto pk_elements = ExtractAllTags(table_xml, "VariablePrimaryKey");
        for (const auto &pk_xml : pk_elements) {
            GobdColumn col;
            col.name = ExtractTagValue(pk_xml, "Name");
            col.order = col_order++;

            if (pk_xml.find("<Numeric>") != string::npos) {
                col.data_type = "Numeric";
                string accuracy_str = ExtractTagValue(pk_xml, "Accuracy");
                if (!accuracy_str.empty()) {
                    try { col.accuracy = std::stoi(accuracy_str); } catch (...) {}
                }
            } else if (pk_xml.find("<Date>") != string::npos || pk_xml.find("<Date/>") != string::npos) {
                col.data_type = "Date";
            } else {
                col.data_type = "AlphaNumeric";
            }

            if (!col.name.empty()) {
                table.columns.push_back(col);
            }
        }

        auto col_elements = ExtractAllTags(table_xml, "VariableColumn");
        for (const auto &col_xml : col_elements) {
            GobdColumn col;
            col.name = ExtractTagValue(col_xml, "Name");
            col.order = col_order++;

            if (col_xml.find("<Numeric>") != string::npos) {
                col.data_type = "Numeric";
                string accuracy_str = ExtractTagValue(col_xml, "Accuracy");
                if (!accuracy_str.empty()) {
                    try { col.accuracy = std::stoi(accuracy_str); } catch (...) {}
                }
            } else if (col_xml.find("<Date>") != string::npos || col_xml.find("<Date/>") != string::npos) {
                col.data_type = "Date";
            } else {
                col.data_type = "AlphaNumeric";
            }

            if (!col.name.empty()) {
                table.columns.push_back(col);
            }
        }

        tables.push_back(table);
    }

    return tables;
}

// Parse GoBD index.xml from a file path (wrapper around string-based parser)
static vector<GobdTable> ParseGobdIndex(const string &filepath) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        throw IOException("Cannot open GoBD index file: " + filepath);
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    string xml = buffer.str();
    file.close();

    return ParseGobdIndexFromString(xml);
}

// Helper function to convert GoBD data type to DuckDB type
LogicalType GobdTypeToDuckDbType(const string &gobd_type, int accuracy) {
    if (gobd_type == "Numeric") {
        if (accuracy >= 0) {
            return LogicalType::DECIMAL(18, accuracy);
        }
        return LogicalType::DOUBLE;
    } else if (gobd_type == "Date") {
        return LogicalType::DATE;
    }
    return LogicalType::VARCHAR;
}

// ============ CSV Parsing Helpers ============

// Parse a single CSV line respecting quotes
vector<string> ParseCsvLine(const string &line, char delimiter) {
    vector<string> fields;
    string current_field;
    bool in_quotes = false;

    for (size_t i = 0; i < line.length(); i++) {
        char c = line[i];

        if (c == '"') {
            if (in_quotes && i + 1 < line.length() && line[i + 1] == '"') {
                // Escaped quote
                current_field += '"';
                i++;
            } else {
                in_quotes = !in_quotes;
            }
        } else if (c == delimiter && !in_quotes) {
            fields.push_back(current_field);
            current_field.clear();
        } else {
            current_field += c;
        }
    }
    fields.push_back(current_field);

    return fields;
}

// ============ stps_read_gobd ============

struct GobdReaderBindData : public TableFunctionData {
    string index_path;
    string table_name;
    char delimiter;
    string csv_path;
    vector<string> column_names;
    idx_t column_count;

    GobdReaderBindData(string index_path_p, string table_name_p, char delimiter_p)
        : index_path(std::move(index_path_p)),
          table_name(std::move(table_name_p)),
          delimiter(delimiter_p),
          column_count(0) {}
};

struct GobdReaderGlobalState : public GlobalTableFunctionState {
    std::shared_ptr<std::ifstream> file;
    bool finished = false;
    idx_t column_count = 0;
    char delimiter = ';';

    idx_t MaxThreads() const override {
        return 1;
    }
};

static unique_ptr<FunctionData> GobdReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
    char delimiter = ';';

    for (auto &kv : input.named_parameters) {
        if (kv.first == "delimiter") {
            string delim_str = StringValue::Get(kv.second);
            if (!delim_str.empty()) {
                delimiter = delim_str[0];
            }
        }
    }

    if (input.inputs.size() > 2) {
        string delim_str = input.inputs[2].ToString();
        if (!delim_str.empty()) {
            delimiter = delim_str[0];
        }
    }

    auto result = make_uniq<GobdReaderBindData>(
        input.inputs[0].ToString(),
        input.inputs[1].ToString(),
        delimiter
    );

    // Parse index.xml directly
    auto tables = ParseGobdIndex(result->index_path);

    // Find the requested table
    GobdTable* found_table = nullptr;
    for (auto &t : tables) {
        if (t.name == result->table_name) {
            found_table = &t;
            break;
        }
    }

    if (!found_table) {
        throw BinderException("Table '" + result->table_name + "' not found in GoBD index. Available tables: " +
            [&]() {
                string list;
                for (size_t i = 0; i < tables.size() && i < 10; i++) {
                    if (i > 0) list += ", ";
                    list += tables[i].name;
                }
                if (tables.size() > 10) list += ", ...";
                return list;
            }());
    }

    // Build CSV path
    string index_dir = GetDirectory(result->index_path);
    result->csv_path = index_dir + "/" + found_table->url;

    // Build column names - all VARCHAR
    for (const auto &col : found_table->columns) {
        result->column_names.push_back(col.name);
        names.push_back(col.name);
        return_types.push_back(LogicalType::VARCHAR);
    }
    result->column_count = result->column_names.size();

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> GobdReaderInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<GobdReaderBindData>();
    auto result = make_uniq<GobdReaderGlobalState>();

    result->file = std::make_shared<std::ifstream>(bind_data.csv_path);
    if (!result->file->is_open()) {
        throw IOException("Cannot open CSV file: " + bind_data.csv_path);
    }

    result->column_count = bind_data.column_count;
    result->delimiter = bind_data.delimiter;

    return std::move(result);
}

static void GobdReaderFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<GobdReaderGlobalState>();

    if (state.finished || !state.file || !state.file->is_open()) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    string line;

    while (count < STANDARD_VECTOR_SIZE && std::getline(*state.file, line)) {
        // Skip empty lines
        if (line.empty()) continue;

        // Remove trailing \r if present (Windows line endings)
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }

        auto fields = ParseCsvLine(line, state.delimiter);

        // Set values for each column
        for (idx_t col = 0; col < state.column_count; col++) {
            if (col < fields.size()) {
                output.SetValue(col, count, Value(fields[col]));
            } else {
                output.SetValue(col, count, Value());  // NULL for missing fields
            }
        }

        count++;
    }

    if (count == 0 || state.file->eof()) {
        state.finished = true;
        state.file->close();
    }

    output.SetCardinality(count);
}

static unique_ptr<NodeStatistics> GobdReaderCardinality(ClientContext &context, const FunctionData *bind_data_p) {
    return make_uniq<NodeStatistics>();
}

// ============ gobd_list_tables ============

struct GobdListTablesBindData : public TableFunctionData {
    string index_path;
    GobdListTablesBindData(string path) : index_path(std::move(path)) {}
};

struct GobdListTablesGlobalState : public GlobalTableFunctionState {
    vector<GobdTable> tables;
    idx_t offset = 0;
    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> GobdListTablesBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<GobdListTablesBindData>(input.inputs[0].ToString());

    names.emplace_back("table_name");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("table_url");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("description");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("column_count");
    return_types.emplace_back(LogicalType::INTEGER);

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> GobdListTablesInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<GobdListTablesBindData>();
    auto result = make_uniq<GobdListTablesGlobalState>();

    result->tables = ParseGobdIndex(bind_data.index_path);

    return std::move(result);
}

static void GobdListTablesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<GobdListTablesGlobalState>();

    idx_t count = 0;
    while (state.offset < state.tables.size() && count < STANDARD_VECTOR_SIZE) {
        auto &table = state.tables[state.offset];

        output.SetValue(0, count, Value(table.name));
        output.SetValue(1, count, Value(table.url));
        output.SetValue(2, count, Value(table.description));
        output.SetValue(3, count, Value::INTEGER(static_cast<int32_t>(table.columns.size())));

        state.offset++;
        count++;
    }

    output.SetCardinality(count);
}

// ============ gobd_table_schema ============

struct GobdSchemaBindData : public TableFunctionData {
    string index_path;
    string table_name;
    GobdSchemaBindData(string path, string name) : index_path(std::move(path)), table_name(std::move(name)) {}
};

struct GobdSchemaGlobalState : public GlobalTableFunctionState {
    vector<GobdColumn> columns;
    idx_t offset = 0;
    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> GobdSchemaBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<GobdSchemaBindData>(input.inputs[0].ToString(), input.inputs[1].ToString());

    names.emplace_back("column_name");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("data_type");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("accuracy");
    return_types.emplace_back(LogicalType::INTEGER);
    names.emplace_back("column_order");
    return_types.emplace_back(LogicalType::INTEGER);

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> GobdSchemaInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<GobdSchemaBindData>();
    auto result = make_uniq<GobdSchemaGlobalState>();

    auto tables = ParseGobdIndex(bind_data.index_path);
    for (auto &t : tables) {
        if (t.name == bind_data.table_name) {
            result->columns = t.columns;
            break;
        }
    }

    return std::move(result);
}

static void GobdSchemaFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<GobdSchemaGlobalState>();

    idx_t count = 0;
    while (state.offset < state.columns.size() && count < STANDARD_VECTOR_SIZE) {
        auto &col = state.columns[state.offset];

        output.SetValue(0, count, Value(col.name));
        output.SetValue(1, count, Value(col.data_type));
        output.SetValue(2, count, col.accuracy >= 0 ? Value::INTEGER(col.accuracy) : Value());
        output.SetValue(3, count, Value::INTEGER(col.order));

        state.offset++;
        count++;
    }

    output.SetCardinality(count);
}

// ============ stps_read_gobd_all ============

struct GobdReadAllBindData : public TableFunctionData {
    string index_path;
    char delimiter;
    // Unified column names across all tables (excluding _table_name)
    vector<string> column_names;
    // Per-table info
    struct TableInfo {
        string name;
        string csv_path;
        // Maps unified column index -> table's CSV column index (-1 if not present)
        vector<int> col_mapping;
    };
    vector<TableInfo> tables;

    GobdReadAllBindData(string index_path_p, char delimiter_p)
        : index_path(std::move(index_path_p)), delimiter(delimiter_p) {}
};

struct GobdReadAllGlobalState : public GlobalTableFunctionState {
    idx_t current_table = 0;
    std::shared_ptr<std::ifstream> file;
    bool finished = false;
    char delimiter = ';';

    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> GobdReadAllBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
    char delimiter = ';';

    for (auto &kv : input.named_parameters) {
        if (kv.first == "delimiter") {
            string delim_str = StringValue::Get(kv.second);
            if (!delim_str.empty()) {
                delimiter = delim_str[0];
            }
        }
    }

    auto result = make_uniq<GobdReadAllBindData>(input.inputs[0].ToString(), delimiter);

    auto tables = ParseGobdIndex(result->index_path);
    if (tables.empty()) {
        throw BinderException("No tables found in GoBD index: " + result->index_path);
    }

    string index_dir = GetDirectory(result->index_path);

    // First pass: collect all unique column names in order
    // Use a map to track insertion order
    vector<string> all_columns;
    std::unordered_map<string, idx_t> col_index_map;

    for (auto &t : tables) {
        for (auto &col : t.columns) {
            if (col_index_map.find(col.name) == col_index_map.end()) {
                col_index_map[col.name] = all_columns.size();
                all_columns.push_back(col.name);
            }
        }
    }

    result->column_names = all_columns;

    // Second pass: build per-table column mappings
    for (auto &t : tables) {
        GobdReadAllBindData::TableInfo info;
        info.name = t.name;
        info.csv_path = index_dir + "/" + t.url;
        info.col_mapping.resize(all_columns.size(), -1);

        for (idx_t csv_col = 0; csv_col < t.columns.size(); csv_col++) {
            auto it = col_index_map.find(t.columns[csv_col].name);
            if (it != col_index_map.end()) {
                info.col_mapping[it->second] = static_cast<int>(csv_col);
            }
        }

        result->tables.push_back(std::move(info));
    }

    // Output schema: _table_name + all unified columns
    names.emplace_back("_table_name");
    return_types.emplace_back(LogicalType::VARCHAR);

    for (auto &col_name : all_columns) {
        names.push_back(col_name);
        return_types.push_back(LogicalType::VARCHAR);
    }

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> GobdReadAllInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<GobdReadAllBindData>();
    auto result = make_uniq<GobdReadAllGlobalState>();
    result->delimiter = bind_data.delimiter;

    if (!bind_data.tables.empty()) {
        result->file = std::make_shared<std::ifstream>(bind_data.tables[0].csv_path);
        if (!result->file->is_open()) {
            // Skip files that can't be opened
            result->file.reset();
        }
    } else {
        result->finished = true;
    }

    return std::move(result);
}

static void GobdReadAllFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<GobdReadAllBindData>();
    auto &state = data_p.global_state->Cast<GobdReadAllGlobalState>();

    if (state.finished) {
        output.SetCardinality(0);
        return;
    }

    idx_t total_columns = bind_data.column_names.size();
    idx_t count = 0;
    string line;

    while (count < STANDARD_VECTOR_SIZE) {
        // Try to read a line from the current file
        bool got_line = false;

        while (state.current_table < bind_data.tables.size()) {
            if (state.file && state.file->is_open() && std::getline(*state.file, line)) {
                if (line.empty()) continue;
                if (!line.empty() && line.back() == '\r') {
                    line.pop_back();
                }
                if (line.empty()) continue;
                got_line = true;
                break;
            }

            // Current file exhausted or not open, move to next table
            if (state.file && state.file->is_open()) {
                state.file->close();
            }
            state.current_table++;

            if (state.current_table < bind_data.tables.size()) {
                state.file = std::make_shared<std::ifstream>(bind_data.tables[state.current_table].csv_path);
                if (!state.file->is_open()) {
                    state.file.reset();
                }
            }
        }

        if (!got_line) {
            state.finished = true;
            break;
        }

        auto &table_info = bind_data.tables[state.current_table];
        auto fields = ParseCsvLine(line, state.delimiter);

        // Column 0: _table_name
        output.SetValue(0, count, Value(table_info.name));

        // Columns 1..N: mapped data columns
        for (idx_t col = 0; col < total_columns; col++) {
            int csv_idx = table_info.col_mapping[col];
            if (csv_idx >= 0 && static_cast<idx_t>(csv_idx) < fields.size()) {
                output.SetValue(col + 1, count, Value(fields[csv_idx]));
            } else {
                output.SetValue(col + 1, count, Value());  // NULL
            }
        }

        count++;
    }

    output.SetCardinality(count);
}

static unique_ptr<NodeStatistics> GobdReadAllCardinality(ClientContext &context, const FunctionData *bind_data_p) {
    return make_uniq<NodeStatistics>();
}

// ============ Registration ============

void RegisterGobdReaderFunctions(ExtensionLoader &loader) {
    // stps_read_gobd(index_path, table_name, delimiter=';')
    TableFunction read_gobd("stps_read_gobd",
                           {LogicalType::VARCHAR, LogicalType::VARCHAR},
                           GobdReaderFunction, GobdReaderBind, GobdReaderInit);

    read_gobd.cardinality = GobdReaderCardinality;
    read_gobd.named_parameters["delimiter"] = LogicalType::VARCHAR;

    CreateTableFunctionInfo read_gobd_info(read_gobd);
    loader.RegisterFunction(read_gobd_info);

    // stps_read_gobd_all(index_path, delimiter=';')
    TableFunction read_gobd_all("stps_read_gobd_all",
                               {LogicalType::VARCHAR},
                               GobdReadAllFunction, GobdReadAllBind, GobdReadAllInit);

    read_gobd_all.cardinality = GobdReadAllCardinality;
    read_gobd_all.named_parameters["delimiter"] = LogicalType::VARCHAR;

    CreateTableFunctionInfo read_gobd_all_info(read_gobd_all);
    loader.RegisterFunction(read_gobd_all_info);

    // gobd_list_tables(index_path)
    TableFunction list_tables("gobd_list_tables",
                             {LogicalType::VARCHAR},
                             GobdListTablesFunction, GobdListTablesBind, GobdListTablesInit);

    CreateTableFunctionInfo list_tables_info(list_tables);
    loader.RegisterFunction(list_tables_info);

    // gobd_table_schema(index_path, table_name)
    TableFunction table_schema("gobd_table_schema",
                              {LogicalType::VARCHAR, LogicalType::VARCHAR},
                              GobdSchemaFunction, GobdSchemaBind, GobdSchemaInit);

    CreateTableFunctionInfo schema_info(table_schema);
    loader.RegisterFunction(schema_info);
}

} // namespace stps
} // namespace duckdb
