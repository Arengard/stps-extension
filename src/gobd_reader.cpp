#include "gobd_reader.hpp"
#include "shared/archive_utils.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <set>
#include <unordered_map>
#include <random>
#include <chrono>

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

// ============ Encoding Helpers ============

// Windows-1252 code points for bytes 0x80-0x9F
static const uint16_t CP1252_MAP[32] = {
    0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021,
    0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0x008D, 0x017D, 0x008F,
    0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
    0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x009D, 0x017E, 0x0178
};

// Windows-1250 (Central European) full mapping for bytes 0x80-0xFF
static const uint16_t CP1250_MAP[128] = {
    // 0x80-0x8F
    0x20AC, 0x0081, 0x201A, 0x0083, 0x201E, 0x2026, 0x2020, 0x2021,
    0x0088, 0x2030, 0x0160, 0x2039, 0x015A, 0x0164, 0x017D, 0x0179,
    // 0x90-0x9F
    0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
    0x0098, 0x2122, 0x0161, 0x203A, 0x015B, 0x0165, 0x017E, 0x017A,
    // 0xA0-0xAF
    0x00A0, 0x02C7, 0x02D8, 0x0141, 0x00A4, 0x0104, 0x00A6, 0x00A7,
    0x00A8, 0x00A9, 0x015E, 0x00AB, 0x00AC, 0x00AD, 0x00AE, 0x017B,
    // 0xB0-0xBF
    0x00B0, 0x00B1, 0x02DB, 0x0142, 0x00B4, 0x00B5, 0x00B6, 0x00B7,
    0x00B8, 0x0105, 0x015F, 0x00BB, 0x013D, 0x02DD, 0x013E, 0x017C,
    // 0xC0-0xCF
    0x0154, 0x00C1, 0x00C2, 0x0102, 0x00C4, 0x0139, 0x0106, 0x00C7,
    0x010C, 0x00C9, 0x0118, 0x00CB, 0x011A, 0x00CD, 0x00CE, 0x010E,
    // 0xD0-0xDF
    0x0110, 0x0143, 0x0147, 0x00D3, 0x00D4, 0x0150, 0x00D6, 0x00D7,
    0x0158, 0x016E, 0x00DA, 0x0170, 0x00DC, 0x00DD, 0x0162, 0x00DF,
    // 0xE0-0xEF
    0x0155, 0x00E1, 0x00E2, 0x0103, 0x00E4, 0x013A, 0x0107, 0x00E7,
    0x010D, 0x00E9, 0x0119, 0x00EB, 0x011B, 0x00ED, 0x00EE, 0x010F,
    // 0xF0-0xFF
    0x0111, 0x0144, 0x0148, 0x00F3, 0x00F4, 0x0151, 0x00F6, 0x00F7,
    0x0159, 0x016F, 0x00FA, 0x0171, 0x00FC, 0x00FD, 0x0163, 0x02D9
};

// ISO-8859-1 (Latin-1): bytes 0x80-0xFF map directly to Unicode code points 0x0080-0x00FF
// No table needed — the byte value IS the Unicode code point.

static void AppendUtf8Char(std::string &out, uint32_t cp) {
    if (cp <= 0x7F) {
        out.push_back(static_cast<char>(cp));
    } else if (cp <= 0x7FF) {
        out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else if (cp <= 0xFFFF) {
        out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    }
}

bool IsValidUtf8(const std::string &str) {
    const unsigned char *bytes = reinterpret_cast<const unsigned char *>(str.data());
    size_t len = str.size();
    for (size_t i = 0; i < len; ) {
        if (bytes[i] <= 0x7F) {
            i++;
        } else if ((bytes[i] & 0xE0) == 0xC0) {
            if (i + 1 >= len || (bytes[i + 1] & 0xC0) != 0x80) return false;
            i += 2;
        } else if ((bytes[i] & 0xF0) == 0xE0) {
            if (i + 2 >= len || (bytes[i + 1] & 0xC0) != 0x80 || (bytes[i + 2] & 0xC0) != 0x80) return false;
            i += 3;
        } else if ((bytes[i] & 0xF8) == 0xF0) {
            if (i + 3 >= len || (bytes[i + 1] & 0xC0) != 0x80 || (bytes[i + 2] & 0xC0) != 0x80 || (bytes[i + 3] & 0xC0) != 0x80) return false;
            i += 4;
        } else {
            return false;
        }
    }
    return true;
}

std::string ConvertWindows1252ToUtf8(const std::string &input) {
    std::string output;
    output.reserve(input.size() * 2);
    for (unsigned char c : input) {
        if (c <= 0x7F) {
            output.push_back(static_cast<char>(c));
        } else if (c >= 0x80 && c <= 0x9F) {
            AppendUtf8Char(output, CP1252_MAP[c - 0x80]);
        } else {
            AppendUtf8Char(output, c);
        }
    }
    return output;
}

std::string EnsureUtf8(const std::string &input) {
    if (IsValidUtf8(input)) {
        return input;
    }
    return ConvertWindows1252ToUtf8(input);
}

static std::string ConvertCp1250ToUtf8(const std::string &input) {
    std::string output;
    output.reserve(input.size() * 2);
    for (unsigned char c : input) {
        if (c <= 0x7F) {
            output.push_back(static_cast<char>(c));
        } else {
            AppendUtf8Char(output, CP1250_MAP[c - 0x80]);
        }
    }
    return output;
}

static std::string ConvertLatin1ToUtf8(const std::string &input) {
    std::string output;
    output.reserve(input.size() * 2);
    for (unsigned char c : input) {
        if (c <= 0x7F) {
            output.push_back(static_cast<char>(c));
        } else {
            AppendUtf8Char(output, static_cast<uint32_t>(c));
        }
    }
    return output;
}

static std::string NormalizeEncodingName(const std::string &enc) {
    std::string lower;
    lower.reserve(enc.size());
    for (char c : enc) {
        if (c != '-' && c != '_') {
            lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        }
    }
    return lower;
}

std::string ConvertToUtf8(const std::string &input, const std::string &encoding) {
    std::string enc = NormalizeEncodingName(encoding);
    if (enc == "utf8" || enc == "utf8bom" || enc.empty()) {
        return input;  // already UTF-8 or no conversion needed
    }
    if (enc == "cp1252" || enc == "windows1252" || enc == "win1252") {
        return ConvertWindows1252ToUtf8(input);
    }
    if (enc == "cp1250" || enc == "windows1250" || enc == "win1250") {
        return ConvertCp1250ToUtf8(input);
    }
    if (enc == "latin1" || enc == "iso88591" || enc == "iso885915") {
        return ConvertLatin1ToUtf8(input);
    }
    throw std::runtime_error("Unsupported encoding: " + encoding + ". Supported: utf-8, cp1250, cp1252, latin1/iso-8859-1");
}

// ============ Shared Import Pipeline ============

// Convert a name to snake_case: lowercase, replace non-alphanumeric with _, collapse multiples
static string ToSnakeCase(const string &input) {
    string result;
    result.reserve(input.size());
    bool last_was_underscore = false;

    for (size_t i = 0; i < input.size(); i++) {
        char c = input[i];
        if (std::isalnum(static_cast<unsigned char>(c))) {
            // Insert _ before uppercase if preceded by lowercase
            if (std::isupper(static_cast<unsigned char>(c)) && !result.empty() && !last_was_underscore) {
                char prev = result.back();
                if (std::islower(static_cast<unsigned char>(prev)) || std::isdigit(static_cast<unsigned char>(prev))) {
                    result += '_';
                }
            }
            result += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            last_was_underscore = false;
        } else {
            // Replace special chars with _
            if (!result.empty() && !last_was_underscore) {
                result += '_';
                last_was_underscore = true;
            }
        }
    }

    // Trim trailing _
    while (!result.empty() && result.back() == '_') {
        result.pop_back();
    }

    return result.empty() ? "column" : result;
}

// Escape a SQL identifier (double-quote)
static string EscapeIdentifier(const string &name) {
    string escaped;
    for (char c : name) {
        if (c == '"') escaped += "\"\"";
        else escaped += c;
    }
    return "\"" + escaped + "\"";
}

// Escape a SQL string literal (single-quote)
static string EscapeStringLiteral(const string &value) {
    string escaped;
    for (char c : value) {
        if (c == '\'') escaped += "''";
        else escaped += c;
    }
    return "'" + escaped + "'";
}

// Generate unique temp filename for bulk CSV loading
static std::string GenerateGobdTempFilename() {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1000, 9999);

    std::string temp_dir = GetTempDirectory();
    return temp_dir + "gobd_import_" + std::to_string(ms) + "_" + std::to_string(dis(gen)) + ".csv";
}

// Escape a path for use in SQL (replace backslashes with forward slashes for DuckDB compatibility)
static string NormalizeSqlPath(const string &path) {
    string result = path;
    for (auto &c : result) {
        if (c == '\\') c = '/';
    }
    return result;
}

vector<GobdImportResult> ExecuteGobdImportPipeline(ClientContext &context,
                                                    const GobdImportData &data,
                                                    char delimiter,
                                                    bool overwrite) {
    vector<GobdImportResult> results;
    Connection conn(context.db->GetDatabase(context));

    for (auto &table : data.tables) {
        GobdImportResult result;
        result.rows_imported = 0;
        result.columns_created = 0;

        // Get CSV content for this table
        auto csv_it = data.csv_contents.find(table.url);
        if (csv_it == data.csv_contents.end() || csv_it->second.empty()) {
            result.table_name = ToSnakeCase(table.name);
            result.error = "CSV content not found for table: " + table.name;
            results.push_back(result);
            continue;
        }

        // Normalize table name
        string table_name = ToSnakeCase(table.name);
        result.table_name = table_name;
        string escaped_table = EscapeIdentifier(table_name);

        // Normalize column names, handle duplicates
        vector<string> normalized_cols;
        std::set<string> used_names;
        for (auto &col : table.columns) {
            string name = ToSnakeCase(col.name);
            string base_name = name;
            int suffix = 2;
            while (used_names.count(name)) {
                name = base_name + "_" + std::to_string(suffix++);
            }
            used_names.insert(name);
            normalized_cols.push_back(name);
        }

        if (normalized_cols.empty()) {
            result.error = "No columns found for table: " + table.name;
            results.push_back(result);
            continue;
        }

        // Check if table exists
        {
            auto check = conn.Query("SELECT 1 FROM information_schema.tables WHERE table_name = " +
                                    EscapeStringLiteral(table_name) + " LIMIT 1");
            if (check && check->RowCount() > 0) {
                if (overwrite) {
                    conn.Query("DROP TABLE IF EXISTS " + escaped_table);
                } else {
                    result.error = "Table already exists: " + table_name + " (use overwrite := true to replace)";
                    results.push_back(result);
                    continue;
                }
            }
        }

        // Step 1+2: Bulk load CSV via temp file + DuckDB's read_csv (vectorized, ~100x faster)
        std::string csv_content = EnsureUtf8(csv_it->second);
        std::string temp_path = GenerateGobdTempFilename();
        bool bulk_loaded = false;

        {
            std::ofstream out(temp_path, std::ios::binary);
            if (out) {
                out.write(csv_content.data(), csv_content.size());
                out.close();

                // Build columns={col1: 'VARCHAR', col2: 'VARCHAR', ...} for read_csv
                string columns_spec = "{";
                for (size_t i = 0; i < normalized_cols.size(); i++) {
                    if (i > 0) columns_spec += ", ";
                    columns_spec += EscapeStringLiteral(normalized_cols[i]) + ": 'VARCHAR'";
                }
                columns_spec += "}";

                string delim_escaped = EscapeStringLiteral(string(1, delimiter));
                string sql_path = EscapeStringLiteral(NormalizeSqlPath(temp_path));

                string create_sql = "CREATE TABLE " + escaped_table +
                                    " AS SELECT * FROM read_csv(" + sql_path +
                                    ", delim=" + delim_escaped +
                                    ", header=false, all_varchar=true, columns=" + columns_spec +
                                    ", ignore_errors=true, null_padding=true)";

                auto create_result = conn.Query(create_sql);
                if (create_result && !create_result->HasError()) {
                    bulk_loaded = true;
                }

                std::remove(temp_path.c_str());
            }
        }

        // Fallback: batched multi-row INSERT if read_csv failed
        if (!bulk_loaded) {
            // Create the table first
            {
                string create_sql = "CREATE TABLE " + escaped_table + " (";
                for (size_t i = 0; i < normalized_cols.size(); i++) {
                    if (i > 0) create_sql += ", ";
                    create_sql += EscapeIdentifier(normalized_cols[i]) + " VARCHAR";
                }
                create_sql += ")";

                auto create_result = conn.Query(create_sql);
                if (create_result->HasError()) {
                    result.error = "Failed to create table: " + create_result->GetError();
                    results.push_back(result);
                    continue;
                }
            }

            // Batched INSERT (1000 rows per statement instead of 1 row per statement)
            std::istringstream stream(csv_content);
            string line;
            const size_t BATCH_SIZE = 1000;
            string batch_sql;
            size_t batch_count = 0;

            string insert_prefix = "INSERT INTO " + escaped_table + " VALUES ";

            while (std::getline(stream, line)) {
                if (line.empty()) continue;
                if (!line.empty() && line.back() == '\r') line.pop_back();
                if (line.empty()) continue;

                auto fields = ParseCsvLine(line, delimiter);

                if (batch_count > 0) batch_sql += ", ";
                batch_sql += "(";
                for (size_t i = 0; i < normalized_cols.size(); i++) {
                    if (i > 0) batch_sql += ", ";
                    if (i < fields.size() && !fields[i].empty()) {
                        batch_sql += EscapeStringLiteral(fields[i]);
                    } else {
                        batch_sql += "NULL";
                    }
                }
                batch_sql += ")";
                batch_count++;

                if (batch_count >= BATCH_SIZE) {
                    conn.Query(insert_prefix + batch_sql);
                    batch_sql.clear();
                    batch_count = 0;
                }
            }

            // Flush remaining rows
            if (batch_count > 0) {
                conn.Query(insert_prefix + batch_sql);
            }
        }

        // Get row count
        {
            auto count_result = conn.Query("SELECT COUNT(*) FROM " + escaped_table);
            if (count_result && count_result->RowCount() > 0) {
                result.rows_imported = count_result->GetValue(0, 0).GetValue<int64_t>();
            }
        }

        // Step 3: Drop empty columns - single query checks all columns at once
        {
            vector<string> cols_to_drop;

            // Build one query that counts non-empty values for ALL columns
            string check_sql = "SELECT ";
            for (size_t i = 0; i < normalized_cols.size(); i++) {
                if (i > 0) check_sql += ", ";
                check_sql += "COUNT(CASE WHEN " + EscapeIdentifier(normalized_cols[i]) +
                             " IS NOT NULL AND " + EscapeIdentifier(normalized_cols[i]) +
                             " <> '' THEN 1 END)";
            }
            check_sql += " FROM " + escaped_table;

            auto check_result = conn.Query(check_sql);
            if (check_result && !check_result->HasError() && check_result->RowCount() > 0) {
                for (size_t i = 0; i < normalized_cols.size(); i++) {
                    auto count_val = check_result->GetValue(i, 0).GetValue<int64_t>();
                    if (count_val == 0) {
                        cols_to_drop.push_back(normalized_cols[i]);
                    }
                }
            }

            for (auto &col_name : cols_to_drop) {
                conn.Query("ALTER TABLE " + escaped_table + " DROP COLUMN " + EscapeIdentifier(col_name));
            }

            result.columns_created = static_cast<int32_t>(normalized_cols.size() - cols_to_drop.size());
        }

        // Step 4: Smart cast types
        if (result.rows_imported > 0) {
            string cast_sql = "CREATE OR REPLACE TABLE " + escaped_table +
                              " AS SELECT * FROM stps_smart_cast(" + EscapeStringLiteral(table_name) + ")";
            auto cast_result = conn.Query(cast_sql);
            if (cast_result && cast_result->HasError()) {
                // Smart cast failed - keep VARCHAR table, not a fatal error
            }
        }

        results.push_back(result);
    }

    return results;
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

// ============ stps_gobd_list_tables ============

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

// ============ stps_gobd_table_schema ============

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

// ============ stps_read_gobd_all (table-creating pipeline) ============

struct GobdReadAllBindData : public TableFunctionData {
    vector<GobdImportResult> import_results;
};

struct GobdReadAllGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> GobdReadAllBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<GobdReadAllBindData>();

    string index_path = input.inputs[0].ToString();
    char delimiter = ';';
    bool overwrite = false;

    for (auto &kv : input.named_parameters) {
        if (kv.first == "delimiter") {
            string delim_str = StringValue::Get(kv.second);
            if (!delim_str.empty()) {
                delimiter = delim_str[0];
            }
        } else if (kv.first == "overwrite") {
            overwrite = BooleanValue::Get(kv.second);
        }
    }

    // Parse index.xml from local file
    auto tables = ParseGobdIndex(index_path);
    if (tables.empty()) {
        throw BinderException("No tables found in GoBD index: " + index_path);
    }

    string index_dir = GetDirectory(index_path);

    // Build import data: read each CSV file from disk
    GobdImportData import_data;
    import_data.tables = tables;

    for (auto &table : tables) {
        string csv_path = index_dir + "/" + table.url;
        std::ifstream file(csv_path);
        if (file.is_open()) {
            std::stringstream buf;
            buf << file.rdbuf();
            import_data.csv_contents[table.url] = buf.str();
            file.close();
        }
    }

    // Execute the shared pipeline
    result->import_results = ExecuteGobdImportPipeline(context, import_data, delimiter, overwrite);

    // Output schema: summary table
    names.emplace_back("table_name");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("rows_imported");
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("columns_created");
    return_types.emplace_back(LogicalType::INTEGER);
    names.emplace_back("error");
    return_types.emplace_back(LogicalType::VARCHAR);

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> GobdReadAllInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<GobdReadAllGlobalState>();
}

static void GobdReadAllFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<GobdReadAllBindData>();
    auto &state = data_p.global_state->Cast<GobdReadAllGlobalState>();

    idx_t count = 0;
    while (state.offset < bind_data.import_results.size() && count < STANDARD_VECTOR_SIZE) {
        auto &r = bind_data.import_results[state.offset];

        output.SetValue(0, count, Value(r.table_name));
        output.SetValue(1, count, Value::BIGINT(r.rows_imported));
        output.SetValue(2, count, Value::INTEGER(r.columns_created));
        output.SetValue(3, count, r.error.empty() ? Value() : Value(r.error));

        state.offset++;
        count++;
    }

    output.SetCardinality(count);
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

    // stps_read_gobd_all(index_path, delimiter=';', overwrite=false)
    TableFunction read_gobd_all("stps_read_gobd_all",
                               {LogicalType::VARCHAR},
                               GobdReadAllFunction, GobdReadAllBind, GobdReadAllInit);

    read_gobd_all.named_parameters["delimiter"] = LogicalType::VARCHAR;
    read_gobd_all.named_parameters["overwrite"] = LogicalType::BOOLEAN;

    CreateTableFunctionInfo read_gobd_all_info(read_gobd_all);
    loader.RegisterFunction(read_gobd_all_info);

    // stps_gobd_list_tables(index_path)
    TableFunction list_tables("stps_gobd_list_tables",
                             {LogicalType::VARCHAR},
                             GobdListTablesFunction, GobdListTablesBind, GobdListTablesInit);

    CreateTableFunctionInfo list_tables_info(list_tables);
    loader.RegisterFunction(list_tables_info);

    // stps_gobd_table_schema(index_path, table_name)
    TableFunction table_schema("stps_gobd_table_schema",
                              {LogicalType::VARCHAR, LogicalType::VARCHAR},
                              GobdSchemaFunction, GobdSchemaBind, GobdSchemaInit);

    CreateTableFunctionInfo schema_info(table_schema);
    loader.RegisterFunction(schema_info);
}

} // namespace stps
} // namespace duckdb
