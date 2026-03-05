#include "gobd_cloud_reader.hpp"
#include "gobd_reader.hpp"
#include "webdav_utils.hpp"
#include "curl_utils.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "../miniz/miniz.h"
#include <sstream>
#include <algorithm>
#include "shared/archive_utils.hpp"
#include <fstream>
#include <cstdio>
#include <cstdlib>
#include <ctime>

#ifdef _WIN32
#define NOMINMAX
#include <windows.h>
#else
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#endif

namespace duckdb {
namespace stps {

// ============================================================================
// 7z archive support helpers
// ============================================================================

// Find the 7z binary on the system. Returns empty string if not found.
static std::string Find7zBinary() {
#ifdef _WIN32
    // Check common install locations
    const char *candidates[] = {
        "C:\\Program Files\\7-Zip\\7z.exe",
        "C:\\Program Files (x86)\\7-Zip\\7z.exe"
    };
    for (auto &path : candidates) {
        DWORD attrs = GetFileAttributesA(path);
        if (attrs != INVALID_FILE_ATTRIBUTES && !(attrs & FILE_ATTRIBUTE_DIRECTORY)) {
            return std::string(path);
        }
    }
    // Try PATH via 'where'
    FILE *pipe = _popen("where 7z.exe 2>nul", "r");
    if (pipe) {
        char buf[512];
        std::string result;
        while (fgets(buf, sizeof(buf), pipe)) {
            result += buf;
        }
        _pclose(pipe);
        // Take first line
        size_t nl = result.find_first_of("\r\n");
        if (nl != std::string::npos) result = result.substr(0, nl);
        if (!result.empty()) return result;
    }
#else
    // Try PATH via 'which'
    FILE *pipe = popen("which 7z 2>/dev/null", "r");
    if (pipe) {
        char buf[512];
        std::string result;
        while (fgets(buf, sizeof(buf), pipe)) {
            result += buf;
        }
        pclose(pipe);
        size_t nl = result.find_first_of("\r\n");
        if (nl != std::string::npos) result = result.substr(0, nl);
        if (!result.empty()) return result;
    }
#endif
    return "";
}

// Extract a 7z archive to a unique temporary directory. Returns the directory path.
// Throws IOException if 7z is not found or extraction fails.
static std::string Extract7zToDirectory(const std::string &archive_path) {
    std::string seven_z = Find7zBinary();
    if (seven_z.empty()) {
        throw IOException("7z not found. Install 7-Zip to handle .7z archives. "
                          "Checked: C:\\Program Files\\7-Zip\\7z.exe, C:\\Program Files (x86)\\7-Zip\\7z.exe, and PATH.");
    }

    // Create a unique temp directory
    std::string temp_base = GetTempDirectory();
    std::string extract_dir = temp_base + "gobd_7z_" + std::to_string(std::time(nullptr)) + "_" +
                              std::to_string(reinterpret_cast<uintptr_t>(&archive_path));

#ifdef _WIN32
    CreateDirectoryA(extract_dir.c_str(), nullptr);
#else
    mkdir(extract_dir.c_str(), 0700);
#endif

    // Build command: 7z x archive.7z -ooutput_dir -y
    // Quote paths to handle spaces
    std::string cmd = "\"" + seven_z + "\" x \"" + archive_path + "\" -o\"" + extract_dir + "\" -y";
#ifdef _WIN32
    // On Windows, wrap entire command in quotes for system()
    cmd = "\"" + cmd + "\"";
#endif
    cmd += " > ";
#ifdef _WIN32
    cmd += "nul 2>&1";
#else
    cmd += "/dev/null 2>&1";
#endif

    int ret = std::system(cmd.c_str());
    if (ret != 0) {
        // Clean up the empty directory on failure
#ifdef _WIN32
        RemoveDirectoryA(extract_dir.c_str());
#else
        rmdir(extract_dir.c_str());
#endif
        throw IOException("7z extraction failed (exit code " + std::to_string(ret) + ") for: " + archive_path);
    }

    return extract_dir;
}

// Recursively remove a directory and all its contents.
static void CleanupDirectory(const std::string &dir_path) {
    if (dir_path.empty()) return;
#ifdef _WIN32
    std::string cmd = "rmdir /s /q \"" + dir_path + "\" > nul 2>&1";
    std::system(cmd.c_str());
#else
    std::string cmd = "rm -rf \"" + dir_path + "\" > /dev/null 2>&1";
    std::system(cmd.c_str());
#endif
}

// Read an entire file into a string.
static std::string ReadFileToString(const std::string &path) {
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) return "";
    std::ostringstream oss;
    oss << ifs.rdbuf();
    return oss.str();
}

// Recursively collect all files in a directory.
// Returns vector of pairs: (relative_name, full_path).
static void CollectFiles(const std::string &dir, std::vector<std::pair<std::string, std::string>> &files) {
#ifdef _WIN32
    std::string search_path = dir + "\\*";
    WIN32_FIND_DATAA fd;
    HANDLE hFind = FindFirstFileA(search_path.c_str(), &fd);
    if (hFind == INVALID_HANDLE_VALUE) return;
    do {
        std::string name = fd.cFileName;
        if (name == "." || name == "..") continue;
        std::string full_path = dir + "\\" + name;
        if (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
            CollectFiles(full_path, files);
        } else {
            files.push_back({name, full_path});
        }
    } while (FindNextFileA(hFind, &fd));
    FindClose(hFind);
#else
    DIR *dp = opendir(dir.c_str());
    if (!dp) return;
    struct dirent *entry;
    while ((entry = readdir(dp)) != nullptr) {
        std::string name = entry->d_name;
        if (name == "." || name == "..") continue;
        std::string full_path = dir + "/" + name;
        struct stat st;
        if (stat(full_path.c_str(), &st) != 0) continue;
        if (S_ISDIR(st.st_mode)) {
            CollectFiles(full_path, files);
        } else {
            files.push_back({name, full_path});
        }
    }
    closedir(dp);
#endif
}

// Read GoBD data from an extracted directory (finds index.xml, parses it, reads CSVs).
// Returns GobdImportData ready for ExecuteGobdImportPipeline.
static GobdImportData ReadGobdFromDirectory(const std::string &dir_path, const std::string &url_for_errors, int32_t read_folder = 0) {
    // Collect all files recursively
    std::vector<std::pair<std::string, std::string>> all_files;
    CollectFiles(dir_path, all_files);

    // Find ALL index.xml files (case-insensitive)
    std::vector<std::pair<std::string, std::string>> index_xmls; // (full_path, dir containing it)
    for (auto &f : all_files) {
        std::string lower_name = f.first;
        std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
        if (lower_name == "index.xml") {
            std::string xml_dir;
            size_t sep = f.second.find_last_of("/\\");
            if (sep != std::string::npos) {
                xml_dir = f.second.substr(0, sep);
            } else {
                xml_dir = dir_path;
            }
            index_xmls.push_back({f.second, xml_dir});
        }
    }

    if (index_xmls.empty()) {
        throw IOException("No index.xml found in extracted 7z archive: " + url_for_errors);
    }

    // Sort alphabetically by path
    std::sort(index_xmls.begin(), index_xmls.end());

    // Select the appropriate index.xml
    size_t selected = 0;
    if (read_folder > 0) {
        if (static_cast<size_t>(read_folder) > index_xmls.size()) {
            throw IOException("read_folder=" + std::to_string(read_folder) +
                              " but only " + std::to_string(index_xmls.size()) +
                              " folders with index.xml found in: " + url_for_errors);
        }
        selected = static_cast<size_t>(read_folder - 1);
    }

    std::string index_xml_path = index_xmls[selected].first;
    std::string index_xml_dir = index_xmls[selected].second;

    std::string xml_content = ReadFileToString(index_xml_path);
    if (xml_content.empty()) {
        throw IOException("index.xml is empty in extracted 7z archive: " + url_for_errors);
    }

    xml_content = EnsureUtf8(xml_content);

    auto tables = ParseGobdIndexFromString(xml_content);
    if (tables.empty()) {
        throw BinderException("No tables found in GoBD index within 7z archive: " + url_for_errors);
    }

    GobdImportData import_data;
    import_data.tables = tables;

    // Read each CSV referenced by the tables
    for (auto &table : tables) {
        // Try direct path relative to index.xml directory
        std::string csv_path = index_xml_dir;
#ifdef _WIN32
        csv_path += "\\" + table.url;
#else
        csv_path += "/" + table.url;
#endif
        std::string csv_content = ReadFileToString(csv_path);

        // Fallback: search by filename (case-insensitive) among all collected files
        if (csv_content.empty()) {
            std::string target_lower = table.url;
            std::transform(target_lower.begin(), target_lower.end(), target_lower.begin(), ::tolower);
            for (auto &f : all_files) {
                std::string lower_name = f.first;
                std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
                if (lower_name == target_lower) {
                    csv_content = ReadFileToString(f.second);
                    break;
                }
            }
        }

        if (!csv_content.empty()) {
            import_data.csv_contents[table.url] = csv_content;
        }
    }

    return import_data;
}

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

// Download a file from a folder with case-insensitive filename fallback.
// First tries direct GET. If that fails (404 / empty), does a PROPFIND of the
// parent folder and looks for a filename match ignoring case.
static std::string DownloadFileCaseInsensitive(const std::string &folder_url,
                                                const std::string &filename,
                                                const std::string &username,
                                                const std::string &password) {
    std::string base = EnsureTrailingSlash(folder_url);
    std::string server_base = GetBaseUrl(folder_url);

    // Try exact name first
    long http_code = 0;
    std::string content = DownloadFile(base + filename, username, password, &http_code);
    if (!content.empty()) return content;

    // Fallback: PROPFIND and match case-insensitively
    std::string target_lower = filename;
    std::transform(target_lower.begin(), target_lower.end(), target_lower.begin(), ::tolower);

    CurlHeaders headers;
    BuildAuthHeaders(headers, username, password);
    headers.append("Depth: 1");
    headers.append("Content-Type: application/xml");

    http_code = 0;
    std::string request_url = NormalizeRequestUrl(base);
    std::string response = curl_propfind(request_url, PROPFIND_BODY, headers, &http_code);
    if (response.find("ERROR:") == 0 || http_code >= 400) return "";

    auto entries = ParsePropfindResponse(response);
    for (auto &entry : entries) {
        if (entry.is_collection) continue;
        std::string decoded = PercentDecodePath(entry.href);
        std::string entry_name = GetLastPathSegment(decoded);
        std::string entry_lower = entry_name;
        std::transform(entry_lower.begin(), entry_lower.end(), entry_lower.begin(), ::tolower);
        if (entry_lower == target_lower) {
            return DownloadFile(server_base + entry.href, username, password);
        }
    }
    return "";
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

    // Download the CSV file (case-insensitive fallback for mismatched filenames)
    result->csv_content = DownloadFileCaseInsensitive(index_base_url, found_table->url, username, password);

    if (result->csv_content.empty()) {
        throw IOException("Could not download CSV file: " + EnsureTrailingSlash(index_base_url) + found_table->url);
    }

    // Convert encoding to UTF-8 if needed (GoBD exports are often Windows-1252)
    result->csv_content = EnsureUtf8(result->csv_content);

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

        // Download CSV (case-insensitive fallback for mismatched filenames)
        std::string csv_content = DownloadFileCaseInsensitive(index_base_url, found_table->url, username, password);
        if (csv_content.empty()) continue;

        // Convert encoding to UTF-8 if needed (GoBD exports are often Windows-1252)
        csv_content = EnsureUtf8(csv_content);

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
// stps_gobd_list_tables_cloud
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
// stps_gobd_table_schema_cloud
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
// stps_read_gobd_cloud_all (table-creating pipeline from cloud)
// ============================================================================

struct GobdCloudAllBindData : public TableFunctionData {
    vector<GobdImportResult> import_results;
};

struct GobdCloudAllGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> GobdCloudAllBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<GobdCloudAllBindData>();

    std::string folder_url = input.inputs[0].ToString();
    std::string username, password;
    char delimiter = ';';
    bool overwrite = false;

    for (auto &kv : input.named_parameters) {
        if (kv.first == "username") {
            username = kv.second.ToString();
        } else if (kv.first == "password") {
            password = kv.second.ToString();
        } else if (kv.first == "delimiter") {
            string delim_str = kv.second.ToString();
            if (!delim_str.empty()) delimiter = delim_str[0];
        } else if (kv.first == "overwrite") {
            overwrite = BooleanValue::Get(kv.second);
        }
    }

    // Discover and download index.xml
    std::string index_base_url;
    std::string xml_content = DiscoverAndDownloadIndexXml(folder_url, username, password, index_base_url);
    if (xml_content.empty()) {
        throw IOException("Could not find or download index.xml from: " + folder_url);
    }

    // Parse tables
    auto tables = ParseGobdIndexFromString(xml_content);
    if (tables.empty()) {
        throw BinderException("No tables found in GoBD index at: " + folder_url);
    }

    // Build import data: download each CSV
    GobdImportData import_data;
    import_data.tables = tables;

    for (auto &table : tables) {
        std::string csv_content = DownloadFileCaseInsensitive(index_base_url, table.url, username, password);
        if (!csv_content.empty()) {
            import_data.csv_contents[table.url] = csv_content;
        }
    }

    // Execute shared pipeline
    result->import_results = ExecuteGobdImportPipeline(context, import_data, delimiter, overwrite);

    // Output schema
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

static unique_ptr<GlobalTableFunctionState> GobdCloudAllInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<GobdCloudAllGlobalState>();
}

static void GobdCloudAllScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<GobdCloudAllBindData>();
    auto &state = data_p.global_state->Cast<GobdCloudAllGlobalState>();

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

// ============================================================================
// stps_read_gobd_cloud_zip_all (table-creating pipeline from cloud ZIP)
// ============================================================================

struct GobdCloudZipAllBindData : public TableFunctionData {
    vector<GobdImportResult> import_results;
};

struct GobdCloudZipAllGlobalState : public GlobalTableFunctionState {
    idx_t offset = 0;
    idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> GobdCloudZipAllBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<GobdCloudZipAllBindData>();

    std::string zip_url = input.inputs[0].ToString();
    std::string username, password;
    char delimiter = ';';
    bool overwrite = false;
    int32_t read_folder = 0;

    for (auto &kv : input.named_parameters) {
        if (kv.first == "username") {
            username = kv.second.ToString();
        } else if (kv.first == "password") {
            password = kv.second.ToString();
        } else if (kv.first == "delimiter") {
            string delim_str = kv.second.ToString();
            if (!delim_str.empty()) delimiter = delim_str[0];
        } else if (kv.first == "overwrite") {
            overwrite = BooleanValue::Get(kv.second);
        } else if (kv.first == "read_folder") {
            read_folder = IntegerValue::Get(kv.second);
        }
    }

    // Detect archive type from URL extension
    std::string url_lower = zip_url;
    std::transform(url_lower.begin(), url_lower.end(), url_lower.begin(), ::tolower);
    bool is_7z = (url_lower.size() >= 3 && url_lower.substr(url_lower.size() - 3) == ".7z");

    GobdImportData import_data;

    if (is_7z) {
        // ---- 7z path: download to temp file, extract with 7z CLI ----
        std::string archive_data = DownloadFile(zip_url, username, password);
        if (archive_data.empty()) {
            throw IOException("Could not download 7z file from: " + zip_url);
        }

        // Write to temp file
        std::string temp_dir = GetTempDirectory();
        std::string temp_file = temp_dir + "gobd_download_" + std::to_string(std::time(nullptr)) + ".7z";
        {
            std::ofstream ofs(temp_file, std::ios::binary);
            if (!ofs) {
                throw IOException("Failed to create temp file for 7z download: " + temp_file);
            }
            ofs.write(archive_data.data(), archive_data.size());
            ofs.close();
        }

        // Free download memory early
        archive_data.clear();
        archive_data.shrink_to_fit();

        std::string extract_dir;
        try {
            extract_dir = Extract7zToDirectory(temp_file);
            // Remove the temp archive file (no longer needed)
            std::remove(temp_file.c_str());

            import_data = ReadGobdFromDirectory(extract_dir, zip_url, read_folder);

            // Clean up extracted directory
            CleanupDirectory(extract_dir);
        } catch (...) {
            // Clean up on any error
            std::remove(temp_file.c_str());
            if (!extract_dir.empty()) {
                CleanupDirectory(extract_dir);
            }
            throw;
        }
    } else {
        // ---- ZIP path: in-memory extraction with miniz (existing logic) ----
        std::string zip_data = DownloadFile(zip_url, username, password);
        if (zip_data.empty()) {
            throw IOException("Could not download ZIP file from: " + zip_url);
        }

        // Open ZIP from memory
        mz_zip_archive zip_archive;
        memset(&zip_archive, 0, sizeof(zip_archive));

        if (!mz_zip_reader_init_mem(&zip_archive, zip_data.data(), zip_data.size(), 0)) {
            throw IOException("Failed to open ZIP archive from: " + zip_url);
        }

        // Find ALL index.xml files in the ZIP
        struct IndexXmlEntry {
            int file_index;
            std::string full_path;
            std::string dir_prefix;
        };
        std::vector<IndexXmlEntry> index_entries;
        int num_files = mz_zip_reader_get_num_files(&zip_archive);

        for (int i = 0; i < num_files; i++) {
            mz_zip_archive_file_stat file_stat;
            if (!mz_zip_reader_file_stat(&zip_archive, i, &file_stat)) continue;
            if (mz_zip_reader_is_file_a_directory(&zip_archive, i)) continue;

            std::string filename = file_stat.m_filename;
            std::string basename = filename;
            size_t slash_pos = filename.find_last_of("/\\");
            if (slash_pos != std::string::npos) {
                basename = filename.substr(slash_pos + 1);
            }

            std::string lower_basename = basename;
            std::transform(lower_basename.begin(), lower_basename.end(), lower_basename.begin(), ::tolower);

            if (lower_basename == "index.xml") {
                IndexXmlEntry entry;
                entry.file_index = i;
                entry.full_path = filename;
                entry.dir_prefix = (slash_pos != std::string::npos) ? filename.substr(0, slash_pos + 1) : "";
                index_entries.push_back(entry);
            }
        }

        if (index_entries.empty()) {
            mz_zip_reader_end(&zip_archive);
            throw IOException("No index.xml found in ZIP archive: " + zip_url);
        }

        // Sort alphabetically by path
        std::sort(index_entries.begin(), index_entries.end(),
                  [](const IndexXmlEntry &a, const IndexXmlEntry &b) {
                      return a.full_path < b.full_path;
                  });

        // Select the appropriate index.xml
        size_t selected = 0;
        if (read_folder > 0) {
            if (static_cast<size_t>(read_folder) > index_entries.size()) {
                mz_zip_reader_end(&zip_archive);
                throw IOException("read_folder=" + std::to_string(read_folder) +
                                  " but only " + std::to_string(index_entries.size()) +
                                  " folders with index.xml found in: " + zip_url);
            }
            selected = static_cast<size_t>(read_folder - 1);
        }

        // Extract the selected index.xml
        std::string xml_content;
        std::string index_dir = index_entries[selected].dir_prefix;
        {
            size_t file_size = 0;
            void *file_data = mz_zip_reader_extract_to_heap(&zip_archive, index_entries[selected].file_index, &file_size, 0);
            if (file_data) {
                xml_content = std::string(static_cast<char*>(file_data), file_size);
                mz_free(file_data);
            }
        }

        if (xml_content.empty()) {
            mz_zip_reader_end(&zip_archive);
            throw IOException("No index.xml found in ZIP archive: " + zip_url);
        }

        xml_content = EnsureUtf8(xml_content);

        // Parse tables
        auto tables = ParseGobdIndexFromString(xml_content);
        if (tables.empty()) {
            mz_zip_reader_end(&zip_archive);
            throw BinderException("No tables found in GoBD index within ZIP: " + zip_url);
        }

        // Build import data: extract each CSV from ZIP
        import_data.tables = tables;

        for (auto &table : tables) {
            // The CSV path in index.xml is relative to the index.xml location
            std::string zip_path = index_dir + table.url;

            int file_index = mz_zip_reader_locate_file(&zip_archive, zip_path.c_str(), nullptr, 0);
            if (file_index < 0) {
                // Try without directory prefix
                file_index = mz_zip_reader_locate_file(&zip_archive, table.url.c_str(), nullptr, 0);
            }

            if (file_index >= 0) {
                size_t file_size = 0;
                void *file_data = mz_zip_reader_extract_to_heap(&zip_archive, file_index, &file_size, 0);
                if (file_data) {
                    import_data.csv_contents[table.url] = std::string(static_cast<char*>(file_data), file_size);
                    mz_free(file_data);
                }
            }
        }

        mz_zip_reader_end(&zip_archive);
    }

    // Execute shared pipeline (both paths converge here)
    result->import_results = ExecuteGobdImportPipeline(context, import_data, delimiter, overwrite);

    // Output schema
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

static unique_ptr<GlobalTableFunctionState> GobdCloudZipAllInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<GobdCloudZipAllGlobalState>();
}

static void GobdCloudZipAllScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<GobdCloudZipAllBindData>();
    auto &state = data_p.global_state->Cast<GobdCloudZipAllGlobalState>();

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

    // stps_gobd_list_tables_cloud(url, username, password)
    {
        TableFunction func("stps_gobd_list_tables_cloud",
                          {LogicalType::VARCHAR},
                          GobdListTablesCloudScan, GobdListTablesCloudBind, GobdListTablesCloudInit);
        func.named_parameters["username"] = LogicalType::VARCHAR;
        func.named_parameters["password"] = LogicalType::VARCHAR;

        CreateTableFunctionInfo info(func);
        loader.RegisterFunction(info);
    }

    // stps_gobd_table_schema_cloud(url, table_name, username, password)
    {
        TableFunction func("stps_gobd_table_schema_cloud",
                          {LogicalType::VARCHAR, LogicalType::VARCHAR},
                          GobdSchemaCloudScan, GobdSchemaCloudBind, GobdSchemaCloudInit);
        func.named_parameters["username"] = LogicalType::VARCHAR;
        func.named_parameters["password"] = LogicalType::VARCHAR;

        CreateTableFunctionInfo info(func);
        loader.RegisterFunction(info);
    }

    // stps_read_gobd_cloud_all(url, username, password, delimiter, overwrite)
    {
        TableFunction func("stps_read_gobd_cloud_all",
                          {LogicalType::VARCHAR},
                          GobdCloudAllScan, GobdCloudAllBind, GobdCloudAllInit);
        func.named_parameters["username"] = LogicalType::VARCHAR;
        func.named_parameters["password"] = LogicalType::VARCHAR;
        func.named_parameters["delimiter"] = LogicalType::VARCHAR;
        func.named_parameters["overwrite"] = LogicalType::BOOLEAN;

        CreateTableFunctionInfo info(func);
        loader.RegisterFunction(info);
    }

    // stps_read_gobd_cloud_zip_all(url, username, password, delimiter, overwrite)
    {
        TableFunction func("stps_read_gobd_cloud_zip_all",
                          {LogicalType::VARCHAR},
                          GobdCloudZipAllScan, GobdCloudZipAllBind, GobdCloudZipAllInit);
        func.named_parameters["username"] = LogicalType::VARCHAR;
        func.named_parameters["password"] = LogicalType::VARCHAR;
        func.named_parameters["delimiter"] = LogicalType::VARCHAR;
        func.named_parameters["overwrite"] = LogicalType::BOOLEAN;
        func.named_parameters["read_folder"] = LogicalType::INTEGER;

        CreateTableFunctionInfo info(func);
        loader.RegisterFunction(info);
    }
}

} // namespace stps
} // namespace duckdb
