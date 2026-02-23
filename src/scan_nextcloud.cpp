#include "scan_nextcloud.hpp"
#include "webdav_utils.hpp"
#include "curl_utils.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include <algorithm>
#include <ctime>
#include <cstring>

namespace duckdb {
namespace stps {

// ─── Options ────────────────────────────────────────────────────────────────

struct ScanNextcloudOptions {
    std::string url;
    std::string username;
    std::string password;
    bool recursive = false;
    std::string file_type;       // filter by extension (e.g. "csv")
    std::string pattern;         // glob pattern on name
    int32_t max_depth = -1;      // -1 = unlimited
    bool include_hidden = false;
    int64_t min_size = -1;
    int64_t max_size = -1;
    int64_t min_date = -1;       // unix timestamp
    int64_t max_date = -1;       // unix timestamp
};

// ─── Result entry ───────────────────────────────────────────────────────────

struct ScanNextcloudEntry {
    std::string name;
    std::string path;
    std::string type;            // "file" or "directory"
    int64_t size;                // -1 if unknown
    int64_t modified_time;       // unix timestamp, 0 if unknown
    std::string extension;
    std::string parent_directory;
};

// ─── Helpers ────────────────────────────────────────────────────────────────

// Parse HTTP-date (RFC 7231) like "Sat, 01 Jan 2022 12:00:00 GMT" → unix ts
static int64_t ParseHttpDate(const std::string &date_str) {
    if (date_str.empty()) return 0;
    struct tm tm_val;
    memset(&tm_val, 0, sizeof(tm_val));

    // Try RFC 7231 format: "Day, DD Mon YYYY HH:MM:SS GMT"
    static const char *months[] = {"Jan","Feb","Mar","Apr","May","Jun",
                                   "Jul","Aug","Sep","Oct","Nov","Dec"};
    // Manual parse to avoid locale issues with strptime (not available on MSVC)
    // Expected: "Sat, 01 Jan 2022 12:00:00 GMT"
    //            0123456789...
    if (date_str.size() < 25) return 0;

    size_t comma = date_str.find(',');
    if (comma == std::string::npos) return 0;

    // Parse after ", "
    size_t p = comma + 2;
    if (p >= date_str.size()) return 0;

    // day
    tm_val.tm_mday = std::atoi(date_str.c_str() + p);
    p = date_str.find(' ', p);
    if (p == std::string::npos) return 0;
    p++;

    // month
    std::string mon = date_str.substr(p, 3);
    tm_val.tm_mon = -1;
    for (int i = 0; i < 12; i++) {
        if (mon == months[i]) { tm_val.tm_mon = i; break; }
    }
    if (tm_val.tm_mon < 0) return 0;
    p += 4;

    // year
    tm_val.tm_year = std::atoi(date_str.c_str() + p) - 1900;
    p = date_str.find(' ', p);
    if (p == std::string::npos) return 0;
    p++;

    // HH:MM:SS
    tm_val.tm_hour = std::atoi(date_str.c_str() + p);
    p = date_str.find(':', p);
    if (p == std::string::npos) return 0;
    p++;
    tm_val.tm_min = std::atoi(date_str.c_str() + p);
    p = date_str.find(':', p);
    if (p == std::string::npos) return 0;
    p++;
    tm_val.tm_sec = std::atoi(date_str.c_str() + p);

    // Convert to UTC timestamp
#ifdef _WIN32
    return static_cast<int64_t>(_mkgmtime(&tm_val));
#else
    return static_cast<int64_t>(timegm(&tm_val));
#endif
}

// Simple glob match (supports * and ?)
static bool GlobMatch(const std::string &pattern, const std::string &text) {
    size_t p = 0, t = 0;
    size_t star_p = std::string::npos, star_t = 0;

    while (t < text.size()) {
        if (p < pattern.size() && (pattern[p] == text[t] || pattern[p] == '?')) {
            p++; t++;
        } else if (p < pattern.size() && pattern[p] == '*') {
            star_p = p++;
            star_t = t;
        } else if (star_p != std::string::npos) {
            p = star_p + 1;
            t = ++star_t;
        } else {
            return false;
        }
    }
    while (p < pattern.size() && pattern[p] == '*') p++;
    return p == pattern.size();
}

// Extract file extension from name
static std::string GetExtension(const std::string &name) {
    auto dot = name.rfind('.');
    if (dot == std::string::npos || dot == 0) return "";
    return name.substr(dot + 1);
}

// Get parent directory from a path
static std::string GetParentDir(const std::string &path) {
    std::string clean = path;
    while (!clean.empty() && clean.back() == '/') clean.pop_back();
    auto slash = clean.rfind('/');
    if (slash == std::string::npos) return "/";
    return clean.substr(0, slash);
}

// Case-insensitive string comparison
static std::string ToLower(const std::string &s) {
    std::string out = s;
    std::transform(out.begin(), out.end(), out.begin(), ::tolower);
    return out;
}

// ─── Recursive PROPFIND scanner ─────────────────────────────────────────────

static void ScanNextcloudRecursive(
    const std::string &dir_url,
    const std::string &dir_path,
    const ScanNextcloudOptions &opts,
    CurlHeaders &auth_headers,
    const std::string &base_url,
    int current_depth,
    std::vector<ScanNextcloudEntry> &results
) {
    // Check max depth
    if (opts.max_depth >= 0 && current_depth > opts.max_depth) return;

    // Perform PROPFIND
    long http_code = 0;
    std::string normalized_url = NormalizeRequestUrl(dir_url);
    std::string response = curl_propfind(normalized_url, PROPFIND_BODY_EXTENDED,
                                         auth_headers, &http_code);

    if (response.substr(0, 6) == "ERROR:") {
        throw IOException("scan_nextcloud: PROPFIND failed for " + dir_url + ": " + response);
    }
    if (http_code != 207 && http_code != 200) {
        throw IOException("scan_nextcloud: " + GetHttpErrorMessage(http_code, dir_url));
    }

    auto entries = ParsePropfindResponseExtended(response);
    if (entries.empty()) return;

    // The first entry is usually the directory itself — skip it
    std::string dir_href_decoded = PercentDecodePath(entries[0].href);

    for (size_t i = 0; i < entries.size(); i++) {
        auto &entry = entries[i];
        std::string decoded_href = PercentDecodePath(entry.href);

        // Skip the directory itself
        if (decoded_href == dir_href_decoded && i == 0) continue;
        // Also skip if trailing-slash variants match
        std::string a = decoded_href, b = dir_href_decoded;
        while (!a.empty() && a.back() == '/') a.pop_back();
        while (!b.empty() && b.back() == '/') b.pop_back();
        if (a == b) continue;

        std::string name = PercentDecodePath(GetLastPathSegment(entry.href));
        if (name.empty()) continue;

        // Hidden file filter
        if (!opts.include_hidden && !name.empty() && name[0] == '.') continue;

        std::string entry_path = dir_path;
        if (!entry_path.empty() && entry_path.back() != '/') entry_path += "/";
        entry_path += name;

        if (entry.is_collection) {
            // It's a directory
            ScanNextcloudEntry result;
            result.name = name;
            result.path = entry_path;
            result.type = "directory";
            result.size = -1;
            result.modified_time = ParseHttpDate(entry.last_modified);
            result.extension = "";
            result.parent_directory = dir_path;

            // Apply pattern filter to directories too
            bool passes_pattern = opts.pattern.empty() || GlobMatch(opts.pattern, name);
            // Only add directories if no file_type filter is set (or pattern matches)
            if (opts.file_type.empty() && passes_pattern) {
                // Date filters for directories
                bool passes_date = true;
                if (opts.min_date >= 0 && result.modified_time < opts.min_date) passes_date = false;
                if (opts.max_date >= 0 && result.modified_time > opts.max_date) passes_date = false;
                if (passes_date) {
                    results.push_back(result);
                }
            }

            // Recurse into subdirectory
            if (opts.recursive) {
                std::string sub_url = base_url + PercentEncodePath(decoded_href);
                ScanNextcloudRecursive(sub_url, entry_path, opts, auth_headers,
                                       base_url, current_depth + 1, results);
            }
        } else {
            // It's a file
            std::string ext = GetExtension(name);

            // File type filter
            if (!opts.file_type.empty() && ToLower(ext) != ToLower(opts.file_type)) continue;

            // Pattern filter
            if (!opts.pattern.empty() && !GlobMatch(opts.pattern, name)) continue;

            // Size filters
            if (entry.content_length >= 0) {
                if (opts.min_size >= 0 && entry.content_length < opts.min_size) continue;
                if (opts.max_size >= 0 && entry.content_length > opts.max_size) continue;
            }

            // Date filter
            int64_t mod_time = ParseHttpDate(entry.last_modified);
            if (opts.min_date >= 0 && mod_time < opts.min_date) continue;
            if (opts.max_date >= 0 && mod_time > opts.max_date) continue;

            ScanNextcloudEntry result;
            result.name = name;
            result.path = entry_path;
            result.type = "file";
            result.size = entry.content_length;
            result.modified_time = mod_time;
            result.extension = ext;
            result.parent_directory = dir_path;
            results.push_back(result);
        }
    }
}

// ─── DuckDB Table Function ──────────────────────────────────────────────────

struct ScanNextcloudBindData : public TableFunctionData {
    ScanNextcloudOptions options;
};

struct ScanNextcloudGlobalState : public GlobalTableFunctionState {
    std::vector<ScanNextcloudEntry> entries;
    idx_t position = 0;
};

static unique_ptr<FunctionData> ScanNextcloudBind(
    ClientContext &context, TableFunctionBindInput &input,
    vector<LogicalType> &return_types, vector<string> &names
) {
    auto result = make_uniq<ScanNextcloudBindData>();

    if (input.inputs.empty()) {
        throw BinderException("scan_nextcloud requires at least one argument: url");
    }
    result->options.url = input.inputs[0].GetValue<string>();

    for (auto &kv : input.named_parameters) {
        if (kv.first == "username") {
            result->options.username = kv.second.GetValue<string>();
        } else if (kv.first == "password") {
            result->options.password = kv.second.GetValue<string>();
        } else if (kv.first == "recursive") {
            result->options.recursive = kv.second.GetValue<bool>();
        } else if (kv.first == "file_type") {
            result->options.file_type = kv.second.GetValue<string>();
        } else if (kv.first == "pattern") {
            result->options.pattern = kv.second.GetValue<string>();
        } else if (kv.first == "max_depth") {
            result->options.max_depth = kv.second.GetValue<int32_t>();
        } else if (kv.first == "include_hidden") {
            result->options.include_hidden = kv.second.GetValue<bool>();
        } else if (kv.first == "min_size") {
            result->options.min_size = kv.second.GetValue<int64_t>();
        } else if (kv.first == "max_size") {
            result->options.max_size = kv.second.GetValue<int64_t>();
        } else if (kv.first == "min_date") {
            result->options.min_date = kv.second.GetValue<int64_t>();
        } else if (kv.first == "max_date") {
            result->options.max_date = kv.second.GetValue<int64_t>();
        }
    }

    // Output schema matches stps_scan
    names = {"name", "path", "type", "size", "modified_time", "extension", "parent_directory"};
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                    LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR};

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ScanNextcloudInit(
    ClientContext &context, TableFunctionInitInput &input
) {
    auto &bind_data = input.bind_data->Cast<ScanNextcloudBindData>();
    auto result = make_uniq<ScanNextcloudGlobalState>();
    auto &opts = bind_data.options;

    // Ensure URL ends with /
    std::string url = opts.url;
    if (!url.empty() && url.back() != '/') url += '/';

    // Build auth headers
    CurlHeaders headers;
    BuildAuthHeaders(headers, opts.username, opts.password);
    headers.append("Depth: 1");
    headers.append("Content-Type: application/xml");

    std::string base_url = GetBaseUrl(url);

    // Extract the initial path for display (decoded)
    std::string normalized = NormalizeRequestUrl(url);
    // Use the URL path as the root display path
    size_t scheme_end = url.find("://");
    std::string root_path = "/";
    if (scheme_end != std::string::npos) {
        size_t host_end = url.find('/', scheme_end + 3);
        if (host_end != std::string::npos) {
            root_path = PercentDecodePath(url.substr(host_end));
        }
    }
    // Clean trailing slash for display
    while (root_path.size() > 1 && root_path.back() == '/') root_path.pop_back();

    try {
        ScanNextcloudRecursive(url, root_path, opts, headers, base_url, 0, result->entries);
    } catch (const std::exception &e) {
        throw IOException("scan_nextcloud error: " + string(e.what()));
    }

    return std::move(result);
}

static void ScanNextcloudScan(
    ClientContext &context, TableFunctionInput &data_p, DataChunk &output
) {
    auto &state = data_p.global_state->Cast<ScanNextcloudGlobalState>();

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.position < state.entries.size() && count < max_count) {
        auto &entry = state.entries[state.position];

        output.SetValue(0, count, Value(entry.name));
        output.SetValue(1, count, Value(entry.path));
        output.SetValue(2, count, Value(entry.type));
        output.SetValue(3, count, entry.size >= 0 ? Value::BIGINT(entry.size) : Value(LogicalType::BIGINT));
        output.SetValue(4, count, entry.modified_time > 0 ? Value::BIGINT(entry.modified_time) : Value(LogicalType::BIGINT));
        output.SetValue(5, count, Value(entry.extension));
        output.SetValue(6, count, Value(entry.parent_directory));

        state.position++;
        count++;
    }

    output.SetCardinality(count);
}

// ─── Registration ───────────────────────────────────────────────────────────

void RegisterScanNextcloudFunction(ExtensionLoader &loader) {
    TableFunction func("scan_nextcloud", {LogicalType::VARCHAR},
                       ScanNextcloudScan, ScanNextcloudBind, ScanNextcloudInit);

    func.named_parameters["username"] = LogicalType::VARCHAR;
    func.named_parameters["password"] = LogicalType::VARCHAR;
    func.named_parameters["recursive"] = LogicalType::BOOLEAN;
    func.named_parameters["file_type"] = LogicalType::VARCHAR;
    func.named_parameters["pattern"] = LogicalType::VARCHAR;
    func.named_parameters["max_depth"] = LogicalType::INTEGER;
    func.named_parameters["include_hidden"] = LogicalType::BOOLEAN;
    func.named_parameters["min_size"] = LogicalType::BIGINT;
    func.named_parameters["max_size"] = LogicalType::BIGINT;
    func.named_parameters["min_date"] = LogicalType::BIGINT;
    func.named_parameters["max_date"] = LogicalType::BIGINT;

    loader.RegisterFunction(func);
}

} // namespace stps
} // namespace duckdb
