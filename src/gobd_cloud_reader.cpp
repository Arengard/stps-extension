#include "gobd_cloud_reader.hpp"
#include "gobd_reader.hpp"
#include "webdav_utils.hpp"
#include "curl_utils.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include <sstream>
#include <algorithm>

namespace duckdb {
namespace stps {

// ============================================================================
// Shared helpers
// ============================================================================

// Ensure URL ends with /
static std::string EnsureTrailingSlash(const std::string &url) {
    if (!url.empty() && url.back() != '/') {
        return url + "/";
    }
    return url;
}

// Extract server-relative path from a full URL, percent-decoded
// e.g. "https://host/remote.php/dav/files/User%40org/test/" -> "/remote.php/dav/files/User@org/test/"
static std::string ExtractDecodedPath(const std::string &url) {
    std::string decoded = PercentDecodePath(url);
    size_t scheme_end = decoded.find("://");
    if (scheme_end == std::string::npos) return decoded;
    size_t path_start = decoded.find('/', scheme_end + 3);
    if (path_start == std::string::npos) return "/";
    return decoded.substr(path_start);
}

// Normalize a path for comparison: decode + strip trailing slashes
static std::string NormalizePath(const std::string &path) {
    std::string decoded = PercentDecodePath(path);
    while (!decoded.empty() && decoded.back() == '/') decoded.pop_back();
    return decoded;
}

// Check if a PROPFIND entry's href matches the parent folder (should be skipped)
static bool IsParentEntry(const std::string &entry_href, const std::string &parent_url) {
    return NormalizePath(entry_href) == NormalizePath(ExtractDecodedPath(parent_url));
}

// Download a file from URL, returns body string. Empty on failure.
static std::string DownloadFile(const std::string &url, const std::string &username, const std::string &password,
                                long *http_code_out = nullptr) {
    CurlHeaders headers;
    BuildAuthHeaders(headers, username, password);

    long http_code = 0;
    std::string request_url = NormalizeRequestUrl(url);
    std::string body = curl_get(request_url, headers, &http_code);

    if (http_code_out) *http_code_out = http_code;

    if (body.find("ERROR:") == 0 || http_code >= 400 || body.empty()) {
        return "";
    }
    return body;
}

// PROPFIND a folder, return entries
static std::vector<PropfindEntry> PropfindFolder(const std::string &url, const std::string &username,
                                                  const std::string &password) {
    CurlHeaders headers;
    BuildAuthHeaders(headers, username, password);
    headers.append("Depth: 1");
    headers.append("Content-Type: application/xml");

    long http_code = 0;
    std::string request_url = NormalizeRequestUrl(url);
    std::string response = curl_propfind(request_url, PROPFIND_BODY, headers, &http_code);

    if (response.find("ERROR:") == 0 || http_code >= 400) {
        return {};
    }

    return ParsePropfindResponse(response);
}

// Check if a filename matches "index.xml" (case-insensitive)
static bool IsIndexXml(const std::string &name) {
    std::string lower = name;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
    return lower == "index.xml";
}

// Discover index.xml in a cloud folder and download its content.
// Sets index_base_url to the folder containing index.xml (for resolving relative CSV paths).
// Discovery strategy:
//   1. Try direct GET on folder_url/index.xml
//   2. If 404, PROPFIND folder at Depth:1, look for index.xml in listing
//   3. If not at root, search one level of subfolders
// Returns empty string on failure.
static std::string DiscoverAndDownloadIndexXml(const std::string &folder_url,
                                                const std::string &username,
                                                const std::string &password,
                                                std::string &index_base_url) {
    std::string base = EnsureTrailingSlash(folder_url);
    std::string server_base = GetBaseUrl(folder_url);

    // Strategy 1: Direct GET on folder_url/index.xml
    {
        long http_code = 0;
        std::string content = DownloadFile(base + "index.xml", username, password, &http_code);
        if (!content.empty()) {
            index_base_url = base;
            return content;
        }
    }

    // Strategy 2: PROPFIND the folder and look for index.xml
    auto entries = PropfindFolder(base, username, password);
    if (entries.empty()) {
        return "";
    }

    // Look for index.xml among files in this folder
    for (auto &entry : entries) {
        if (entry.is_collection) continue;
        std::string decoded = PercentDecodePath(entry.href);
        std::string filename = GetLastPathSegment(decoded);
        if (IsIndexXml(filename)) {
            std::string file_url = server_base + entry.href;
            std::string content = DownloadFile(file_url, username, password);
            if (!content.empty()) {
                index_base_url = base;
                return content;
            }
        }
    }

    // Strategy 3: Search one level of subfolders
    for (auto &entry : entries) {
        if (!entry.is_collection) continue;

        // Skip the parent folder itself (PROPFIND Depth:1 always includes it)
        if (IsParentEntry(entry.href, base)) continue;

        // PROPFIND this subfolder
        std::string subfolder_url = server_base + entry.href;
        if (subfolder_url.back() != '/') subfolder_url += '/';

        auto sub_entries = PropfindFolder(subfolder_url, username, password);
        for (auto &sub_entry : sub_entries) {
            if (sub_entry.is_collection) continue;
            std::string sub_decoded = PercentDecodePath(sub_entry.href);
            std::string sub_filename = GetLastPathSegment(sub_decoded);
            if (IsIndexXml(sub_filename)) {
                std::string file_url = server_base + sub_entry.href;
                std::string content = DownloadFile(file_url, username, password);
                if (!content.empty()) {
                    index_base_url = subfolder_url;
                    return content;
                }
            }
        }
    }

    return "";
}

// ============================================================================
// stps_read_gobd_cloud
// ============================================================================

struct GobdCloudReaderBindData : public TableFunctionData {
    std::string csv_content;
    vector<string> column_names;
    idx_t column_count;
    char delimiter;

    GobdCloudReaderBindData() : column_count(0), delimiter(';') {}
};

struct GobdCloudReaderGlobalState : public GlobalTableFunctionState {
    std::istringstream stream;
    bool finished = false;
    idx_t column_count = 0;
    char delimiter = ';';

    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> GobdCloudReaderBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<GobdCloudReaderBindData>();

    std::string folder_url = input.inputs[0].ToString();
    std::string table_name = input.inputs[1].ToString();
    std::string username, password;
    result->delimiter = ';';

    for (auto &kv : input.named_parameters) {
        if (kv.first == "username") {
            username = kv.second.ToString();
        } else if (kv.first == "password") {
            password = kv.second.ToString();
        } else if (kv.first == "delimiter") {
            string delim_str = kv.second.ToString();
            if (!delim_str.empty()) {
                result->delimiter = delim_str[0];
            }
        }
    }

    // Discover and download index.xml
    std::string index_base_url;
    std::string xml_content = DiscoverAndDownloadIndexXml(folder_url, username, password, index_base_url);

    if (xml_content.empty()) {
        throw IOException("Could not find or download index.xml from: " + folder_url);
    }

    // Parse tables from index.xml
    auto tables = ParseGobdIndexFromString(xml_content);

    // Find requested table
    GobdTable* found_table = nullptr;
    for (auto &t : tables) {
        if (t.name == table_name) {
            found_table = &t;
            break;
        }
    }

    if (!found_table) {
        throw BinderException("Table '" + table_name + "' not found in cloud GoBD index. Available tables: " +
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

    // Download the CSV file
    std::string csv_url = EnsureTrailingSlash(index_base_url) + found_table->url;
    result->csv_content = DownloadFile(csv_url, username, password);

    if (result->csv_content.empty()) {
        throw IOException("Could not download CSV file: " + csv_url);
    }

    // Build schema - all VARCHAR (matching local reader)
    for (const auto &col : found_table->columns) {
        result->column_names.push_back(col.name);
        names.push_back(col.name);
        return_types.push_back(LogicalType::VARCHAR);
    }
    result->column_count = result->column_names.size();

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> GobdCloudReaderInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<GobdCloudReaderBindData>();
    auto result = make_uniq<GobdCloudReaderGlobalState>();

    result->stream = std::istringstream(bind_data.csv_content);
    result->column_count = bind_data.column_count;
    result->delimiter = bind_data.delimiter;

    return std::move(result);
}

static void GobdCloudReaderScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<GobdCloudReaderGlobalState>();

    if (state.finished) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    string line;

    while (count < STANDARD_VECTOR_SIZE && std::getline(state.stream, line)) {
        if (line.empty()) continue;

        // Remove trailing \r
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }

        if (line.empty()) continue;

        auto fields = ParseCsvLine(line, state.delimiter);

        for (idx_t col = 0; col < state.column_count; col++) {
            if (col < fields.size()) {
                output.SetValue(col, count, Value(fields[col]));
            } else {
                output.SetValue(col, count, Value());
            }
        }

        count++;
    }

    if (count == 0 || state.stream.eof()) {
        state.finished = true;
    }

    output.SetCardinality(count);
}

// ============================================================================
// stps_read_gobd_cloud_folder
// ============================================================================

struct GobdCloudFolderBindData : public TableFunctionData {
    // Materialized rows: parent_folder | child_folder | [data columns...]
    vector<vector<Value>> all_rows;
    vector<string> column_names;
    idx_t data_col_count = 0;
    char delimiter;

    GobdCloudFolderBindData() : delimiter(';') {}
};

struct GobdCloudFolderGlobalState : public GlobalTableFunctionState {
    idx_t current_row = 0;
    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> GobdCloudFolderBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<GobdCloudFolderBindData>();

    std::string parent_url = input.inputs[0].ToString();
    std::string table_name = input.inputs[1].ToString();
    std::string child_folder, username, password;
    result->delimiter = ';';

    for (auto &kv : input.named_parameters) {
        if (kv.first == "child_folder") {
            child_folder = kv.second.ToString();
        } else if (kv.first == "username") {
            username = kv.second.ToString();
        } else if (kv.first == "password") {
            password = kv.second.ToString();
        } else if (kv.first == "delimiter") {
            string delim_str = kv.second.ToString();
            if (!delim_str.empty()) {
                result->delimiter = delim_str[0];
            }
        }
    }

    parent_url = EnsureTrailingSlash(parent_url);
    std::string server_base = GetBaseUrl(parent_url);

    // PROPFIND parent folder to list mandant subfolders
    auto entries = PropfindFolder(parent_url, username, password);

    if (entries.empty()) {
        throw IOException("Could not list parent folder (PROPFIND failed or empty): " + parent_url);
    }

    bool schema_detected = false;
    idx_t mandants_scanned = 0;
    idx_t mandants_with_index = 0;
    idx_t mandants_with_table = 0;

    for (auto &entry : entries) {
        if (!entry.is_collection) continue;

        // Skip the parent folder itself (PROPFIND Depth:1 always includes it)
        if (IsParentEntry(entry.href, parent_url)) continue;

        std::string decoded_href = PercentDecodePath(entry.href);
        std::string mandant_name = GetLastPathSegment(decoded_href);
        if (mandant_name.empty()) continue;

        mandants_scanned++;

        // Build target folder URL
        std::string target_url = server_base + entry.href;
        if (target_url.back() != '/') target_url += '/';
        if (!child_folder.empty()) {
            target_url += child_folder + "/";
        }

        // Try to discover and download index.xml
        std::string index_base_url;
        std::string xml_content = DiscoverAndDownloadIndexXml(target_url, username, password, index_base_url);
        if (xml_content.empty()) continue;

        mandants_with_index++;

        // Parse tables
        auto tables = ParseGobdIndexFromString(xml_content);

        // Find requested table
        GobdTable* found_table = nullptr;
        for (auto &t : tables) {
            if (t.name == table_name) {
                found_table = &t;
                break;
            }
        }
        if (!found_table) continue;

        mandants_with_table++;

        // On first success, establish schema
        if (!schema_detected) {
            for (const auto &col : found_table->columns) {
                result->column_names.push_back(col.name);
            }
            result->data_col_count = result->column_names.size();
            schema_detected = true;
        }

        // Check column count matches (schema consistency)
        if (found_table->columns.size() != result->data_col_count) continue;

        // Download CSV
        std::string csv_url = EnsureTrailingSlash(index_base_url) + found_table->url;
        std::string csv_content = DownloadFile(csv_url, username, password);
        if (csv_content.empty()) continue;

        // Parse CSV lines
        std::istringstream stream(csv_content);
        string line;
        while (std::getline(stream, line)) {
            if (line.empty()) continue;
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (line.empty()) continue;

            auto fields = ParseCsvLine(line, result->delimiter);

            vector<Value> row;
            row.push_back(Value(mandant_name));
            row.push_back(Value(child_folder));

            for (idx_t col = 0; col < result->data_col_count; col++) {
                if (col < fields.size()) {
                    row.push_back(Value(fields[col]));
                } else {
                    row.push_back(Value());
                }
            }

            result->all_rows.push_back(std::move(row));
        }
    }

    // If nothing was found, throw a helpful error instead of silently returning empty
    if (!schema_detected) {
        std::string detail = "Scanned " + std::to_string(mandants_scanned) + " subfolders";
        if (mandants_scanned == 0) {
            detail += " (no subfolders found in parent folder)";
        } else if (mandants_with_index == 0) {
            detail += ", none contained index.xml";
            if (!child_folder.empty()) {
                detail += " (child_folder='" + child_folder + "')";
            }
        } else if (mandants_with_table == 0) {
            detail += ", " + std::to_string(mandants_with_index) + " had index.xml but none contained table '" + table_name + "'";
            // Try to list available tables from first index.xml found
            // (re-scan to give helpful error)
            for (auto &entry : entries) {
                if (!entry.is_collection) continue;
                if (IsParentEntry(entry.href, parent_url)) continue;

                std::string target_url = server_base + entry.href;
                if (target_url.back() != '/') target_url += '/';
                if (!child_folder.empty()) target_url += child_folder + "/";

                std::string index_base_url;
                std::string xml_content = DiscoverAndDownloadIndexXml(target_url, username, password, index_base_url);
                if (xml_content.empty()) continue;

                auto tables = ParseGobdIndexFromString(xml_content);
                if (!tables.empty()) {
                    detail += ". Available tables: ";
                    for (size_t i = 0; i < tables.size() && i < 10; i++) {
                        if (i > 0) detail += ", ";
                        detail += tables[i].name;
                    }
                    if (tables.size() > 10) detail += ", ...";
                    break;
                }
            }
        }
        throw IOException("No GoBD data found. " + detail + ". URL: " + parent_url);
    }

    // Build return schema
    names.push_back("parent_folder");
    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("child_folder");
    return_types.push_back(LogicalType::VARCHAR);

    for (const auto &col_name : result->column_names) {
        names.push_back(col_name);
        return_types.push_back(LogicalType::VARCHAR);
    }

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> GobdCloudFolderInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<GobdCloudFolderGlobalState>();
}

static void GobdCloudFolderScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<GobdCloudFolderBindData>();
    auto &state = data_p.global_state->Cast<GobdCloudFolderGlobalState>();

    idx_t count = 0;
    while (state.current_row < bind_data.all_rows.size() && count < STANDARD_VECTOR_SIZE) {
        auto &row = bind_data.all_rows[state.current_row];
        for (idx_t col = 0; col < row.size() && col < output.ColumnCount(); col++) {
            output.SetValue(col, count, row[col]);
        }
        state.current_row++;
        count++;
    }

    output.SetCardinality(count);
}

// ============================================================================
// gobd_list_tables_cloud
// ============================================================================

struct GobdListTablesCloudBindData : public TableFunctionData {
    vector<GobdTable> tables;
};

struct GobdListTablesCloudGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> GobdListTablesCloudBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<GobdListTablesCloudBindData>();

    std::string folder_url = input.inputs[0].ToString();
    std::string username, password;

    for (auto &kv : input.named_parameters) {
        if (kv.first == "username") {
            username = kv.second.ToString();
        } else if (kv.first == "password") {
            password = kv.second.ToString();
        }
    }

    std::string index_base_url;
    std::string xml_content = DiscoverAndDownloadIndexXml(folder_url, username, password, index_base_url);

    if (xml_content.empty()) {
        throw IOException("Could not find or download index.xml from: " + folder_url);
    }

    result->tables = ParseGobdIndexFromString(xml_content);

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

static unique_ptr<GlobalTableFunctionState> GobdListTablesCloudInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<GobdListTablesCloudGlobalState>();
}

static void GobdListTablesCloudScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<GobdListTablesCloudBindData>();
    auto &state = data_p.global_state->Cast<GobdListTablesCloudGlobalState>();

    idx_t count = 0;
    while (state.offset < bind_data.tables.size() && count < STANDARD_VECTOR_SIZE) {
        auto &table = bind_data.tables[state.offset];

        output.SetValue(0, count, Value(table.name));
        output.SetValue(1, count, Value(table.url));
        output.SetValue(2, count, Value(table.description));
        output.SetValue(3, count, Value::INTEGER(static_cast<int32_t>(table.columns.size())));

        state.offset++;
        count++;
    }

    output.SetCardinality(count);
}

// ============================================================================
// gobd_table_schema_cloud
// ============================================================================

struct GobdSchemaCloudBindData : public TableFunctionData {
    vector<GobdColumn> columns;
};

struct GobdSchemaCloudGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> GobdSchemaCloudBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<GobdSchemaCloudBindData>();

    std::string folder_url = input.inputs[0].ToString();
    std::string table_name = input.inputs[1].ToString();
    std::string username, password;

    for (auto &kv : input.named_parameters) {
        if (kv.first == "username") {
            username = kv.second.ToString();
        } else if (kv.first == "password") {
            password = kv.second.ToString();
        }
    }

    std::string index_base_url;
    std::string xml_content = DiscoverAndDownloadIndexXml(folder_url, username, password, index_base_url);

    if (xml_content.empty()) {
        throw IOException("Could not find or download index.xml from: " + folder_url);
    }

    auto tables = ParseGobdIndexFromString(xml_content);
    for (auto &t : tables) {
        if (t.name == table_name) {
            result->columns = t.columns;
            break;
        }
    }

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

static unique_ptr<GlobalTableFunctionState> GobdSchemaCloudInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<GobdSchemaCloudGlobalState>();
}

static void GobdSchemaCloudScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<GobdSchemaCloudBindData>();
    auto &state = data_p.global_state->Cast<GobdSchemaCloudGlobalState>();

    idx_t count = 0;
    while (state.offset < bind_data.columns.size() && count < STANDARD_VECTOR_SIZE) {
        auto &col = bind_data.columns[state.offset];

        output.SetValue(0, count, Value(col.name));
        output.SetValue(1, count, Value(col.data_type));
        output.SetValue(2, count, col.accuracy >= 0 ? Value::INTEGER(col.accuracy) : Value());
        output.SetValue(3, count, Value::INTEGER(col.order));

        state.offset++;
        count++;
    }

    output.SetCardinality(count);
}

// ============================================================================
// Registration
// ============================================================================

void RegisterGobdCloudReaderFunctions(ExtensionLoader &loader) {
    // stps_read_gobd_cloud(url, table_name, username, password, delimiter)
    {
        TableFunction func("stps_read_gobd_cloud",
                          {LogicalType::VARCHAR, LogicalType::VARCHAR},
                          GobdCloudReaderScan, GobdCloudReaderBind, GobdCloudReaderInit);
        func.named_parameters["username"] = LogicalType::VARCHAR;
        func.named_parameters["password"] = LogicalType::VARCHAR;
        func.named_parameters["delimiter"] = LogicalType::VARCHAR;

        CreateTableFunctionInfo info(func);
        loader.RegisterFunction(info);
    }

    // stps_read_gobd_cloud_folder(url, table_name, child_folder, username, password, delimiter)
    {
        TableFunction func("stps_read_gobd_cloud_folder",
                          {LogicalType::VARCHAR, LogicalType::VARCHAR},
                          GobdCloudFolderScan, GobdCloudFolderBind, GobdCloudFolderInit);
        func.named_parameters["child_folder"] = LogicalType::VARCHAR;
        func.named_parameters["username"] = LogicalType::VARCHAR;
        func.named_parameters["password"] = LogicalType::VARCHAR;
        func.named_parameters["delimiter"] = LogicalType::VARCHAR;

        CreateTableFunctionInfo info(func);
        loader.RegisterFunction(info);
    }

    // gobd_list_tables_cloud(url, username, password)
    {
        TableFunction func("gobd_list_tables_cloud",
                          {LogicalType::VARCHAR},
                          GobdListTablesCloudScan, GobdListTablesCloudBind, GobdListTablesCloudInit);
        func.named_parameters["username"] = LogicalType::VARCHAR;
        func.named_parameters["password"] = LogicalType::VARCHAR;

        CreateTableFunctionInfo info(func);
        loader.RegisterFunction(info);
    }

    // gobd_table_schema_cloud(url, table_name, username, password)
    {
        TableFunction func("gobd_table_schema_cloud",
                          {LogicalType::VARCHAR, LogicalType::VARCHAR},
                          GobdSchemaCloudScan, GobdSchemaCloudBind, GobdSchemaCloudInit);
        func.named_parameters["username"] = LogicalType::VARCHAR;
        func.named_parameters["password"] = LogicalType::VARCHAR;

        CreateTableFunctionInfo info(func);
        loader.RegisterFunction(info);
    }
}

} // namespace stps
} // namespace duckdb
