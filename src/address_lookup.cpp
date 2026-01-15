#include "address_lookup.hpp"
#include "street_split.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <mutex>
#include <unordered_map>

#ifdef _WIN32
#include <windows.h>
#include <io.h>
#define popen _popen
#define pclose _pclose
#else
#include <unistd.h>
#endif

namespace duckdb {
namespace stps {

// ============================================================================
// Rate limiting and caching
// ============================================================================

// Simple in-memory cache with TTL
struct CacheEntry {
    AddressResult result;
    std::chrono::steady_clock::time_point timestamp;
};

static std::mutex cache_mutex;
static std::unordered_map<std::string, CacheEntry> address_cache;
static std::chrono::steady_clock::time_point last_request_time;
static const int MIN_REQUEST_INTERVAL_MS = 2000; // 2 seconds between requests
static const int CACHE_TTL_SECONDS = 3600; // 1 hour cache

static void rate_limit_delay() {
    std::lock_guard<std::mutex> lock(cache_mutex);

    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_request_time).count();

    if (elapsed < MIN_REQUEST_INTERVAL_MS) {
        int sleep_time = MIN_REQUEST_INTERVAL_MS - static_cast<int>(elapsed);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
    }

    last_request_time = std::chrono::steady_clock::now();
}

static bool get_cached_address(const std::string& company_name, AddressResult& result) {
    std::lock_guard<std::mutex> lock(cache_mutex);

    auto it = address_cache.find(company_name);
    if (it != address_cache.end()) {
        auto now = std::chrono::steady_clock::now();
        auto age = std::chrono::duration_cast<std::chrono::seconds>(now - it->second.timestamp).count();

        if (age < CACHE_TTL_SECONDS) {
            result = it->second.result;
            return true;
        } else {
            // Expired, remove from cache
            address_cache.erase(it);
        }
    }

    return false;
}

static void cache_address_result(const std::string& company_name, const AddressResult& result) {
    std::lock_guard<std::mutex> lock(cache_mutex);

    CacheEntry entry;
    entry.result = result;
    entry.timestamp = std::chrono::steady_clock::now();

    address_cache[company_name] = entry;

    // Limit cache size to 1000 entries
    if (address_cache.size() > 1000) {
        // Remove oldest entry (simple LRU)
        auto oldest = address_cache.begin();
        for (auto it = address_cache.begin(); it != address_cache.end(); ++it) {
            if (it->second.timestamp < oldest->second.timestamp) {
                oldest = it;
            }
        }
        address_cache.erase(oldest);
    }
}

// ============================================================================
// String utilities
// ============================================================================

static std::string trim(const std::string& str) {
    size_t start = str.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) return "";
    size_t end = str.find_last_not_of(" \t\n\r");
    return str.substr(start, end - start + 1);
}

static std::string to_lower(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return result;
}

// ============================================================================
// URL encoding/decoding
// ============================================================================

static std::string url_encode(const std::string& text) {
    std::ostringstream encoded;
    encoded.fill('0');
    encoded << std::hex;

    for (unsigned char c : text) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            encoded << c;
        } else if (c == ' ') {
            encoded << '+';
        } else {
            encoded << '%' << std::setw(2) << std::uppercase << static_cast<int>(c);
        }
    }

    return encoded.str();
}

static int hex_char_to_int(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
}

static std::string url_decode(const std::string& text) {
    std::string decoded;
    decoded.reserve(text.size());

    for (size_t i = 0; i < text.size(); i++) {
        if (text[i] == '%' && i + 2 < text.size()) {
            int high = hex_char_to_int(text[i + 1]);
            int low = hex_char_to_int(text[i + 2]);
            if (high >= 0 && low >= 0) {
                decoded += static_cast<char>((high << 4) | low);
                i += 2;
                continue;
            }
        } else if (text[i] == '+') {
            decoded += ' ';
            continue;
        }
        decoded += text[i];
    }

    return decoded;
}

// ============================================================================
// HTTP fetching via curl/wget
// ============================================================================

static std::string get_temp_filename() {
#ifdef _WIN32
    char temp_path[MAX_PATH];
    char temp_file[MAX_PATH];
    GetTempPathA(MAX_PATH, temp_path);
    GetTempFileNameA(temp_path, "stps", 0, temp_file);
    return std::string(temp_file);
#else
    char temp_file[] = "/tmp/stps_XXXXXX";
    int fd = mkstemp(temp_file);
    if (fd != -1) {
        close(fd);
    }
    return std::string(temp_file);
#endif
}

static std::string read_file_content(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) return "";

    std::ostringstream content;
    content << file.rdbuf();
    return content.str();
}

static std::string fetch_url(const std::string& url) {
    // Apply rate limiting before making request
    rate_limit_delay();

    std::string temp_file = get_temp_filename();

    // Build curl command with proper User-Agent for Nominatim API
    // Nominatim requires a descriptive User-Agent per their usage policy
    // See: https://operations.osmfoundation.org/policies/nominatim/
    std::string cmd = "curl -s -L -m 15 "
                      "-H \"User-Agent: DuckDB-STPS-Extension/1.0 (contact: github.com/Arengard/stps-extension)\" "
                      "-H \"Accept: application/json\" "
                      "-o \"" + temp_file + "\" "
                      "\"" + url + "\" 2>/dev/null";

#ifdef _WIN32
    cmd = "curl -s -L -m 15 "
          "-H \"User-Agent: DuckDB-STPS-Extension/1.0 (contact: github.com/Arengard/stps-extension)\" "
          "-H \"Accept: application/json\" "
          "-o \"" + temp_file + "\" "
          "\"" + url + "\" 2>nul";
#endif

    int result = system(cmd.c_str());

    if (result != 0) {
        // Fallback to wget
#ifdef _WIN32
        cmd = "wget -q -O \"" + temp_file + "\" "
              "--user-agent=\"DuckDB-STPS-Extension/1.0 (contact: github.com/Arengard/stps-extension)\" "
              "--header=\"Accept: application/json\" "
              "\"" + url + "\" 2>nul";
#else
        cmd = "wget -q -O \"" + temp_file + "\" "
              "--user-agent=\"DuckDB-STPS-Extension/1.0 (contact: github.com/Arengard/stps-extension)\" "
              "--header=\"Accept: application/json\" "
              "\"" + url + "\" 2>/dev/null";
#endif
        result = system(cmd.c_str());

        if (result != 0) {
            std::remove(temp_file.c_str());
            return "";
        }
    }

    std::string content = read_file_content(temp_file);
    std::remove(temp_file.c_str());

    return content;
}

// ============================================================================
// JSON parsing utilities (simple, no external dependencies)
// ============================================================================

static std::string extract_json_string(const std::string& json, const std::string& key) {
    // Find "key":"value" pattern
    std::string search = "\"" + key + "\":";
    size_t pos = json.find(search);
    if (pos == std::string::npos) {
        return "";
    }

    pos += search.length();

    // Skip whitespace
    while (pos < json.length() && std::isspace(json[pos])) {
        pos++;
    }

    // Check if value is a string (starts with ")
    if (pos >= json.length() || json[pos] != '"') {
        return "";
    }

    pos++; // Skip opening quote
    size_t end = pos;

    // Find closing quote (handle escaped quotes)
    while (end < json.length()) {
        if (json[end] == '"' && (end == 0 || json[end - 1] != '\\')) {
            break;
        }
        end++;
    }

    if (end >= json.length()) {
        return "";
    }

    return json.substr(pos, end - pos);
}

// ============================================================================
// OpenStreetMap Nominatim API parsing
// ============================================================================

static AddressResult parse_nominatim_response(const std::string& json_response) {
    AddressResult result;
    result.found = false;

    // Check if response is empty or error
    if (json_response.empty() || json_response.find('[') == std::string::npos) {
        return result;
    }

    // Find first result in array (we want the first match)
    size_t first_obj = json_response.find('{');
    if (first_obj == std::string::npos) {
        return result;
    }

    // Extract address components from JSON
    // Nominatim returns: {"address": {"road": "...", "house_number": "...", "postcode": "...", "city": "..."}}

    // First try to find "road" for street name
    result.street_name = extract_json_string(json_response, "road");

    // Try "house_number" for street number
    result.street_number = extract_json_string(json_response, "house_number");

    // Try "postcode" for PLZ
    result.plz = extract_json_string(json_response, "postcode");

    // Try multiple keys for city (Nominatim can return city, town, village, municipality)
    result.city = extract_json_string(json_response, "city");
    if (result.city.empty()) {
        result.city = extract_json_string(json_response, "town");
    }
    if (result.city.empty()) {
        result.city = extract_json_string(json_response, "village");
    }
    if (result.city.empty()) {
        result.city = extract_json_string(json_response, "municipality");
    }

    // Build full address from components
    if (!result.street_name.empty()) {
        result.full_address = result.street_name;
        if (!result.street_number.empty()) {
            result.full_address += " " + result.street_number;
        }
    }

    // Mark as found if we have at least city or postcode
    if (!result.city.empty() || !result.plz.empty()) {
        result.found = true;
    }

    return result;
}

// ============================================================================
// Main lookup function
// ============================================================================

AddressResult lookup_company_address(const std::string& company_name) {
    AddressResult result;
    result.found = false;

    if (company_name.empty()) {
        return result;
    }

    // Step 0: Check cache first
    if (get_cached_address(company_name, result)) {
        return result;
    }

    // Step 1: Use OpenStreetMap Nominatim API for geocoding
    // Nominatim is a free geocoding API with fair use policy:
    // - Max 1 request per second
    // - Must include User-Agent
    // - Returns JSON with detailed address information
    //
    // API Documentation: https://nominatim.org/release-docs/develop/api/Search/
    //
    // Example response:
    // [{"address": {"road": "Hauptstraße", "house_number": "1",
    //               "postcode": "10115", "city": "Berlin", ...}}]

    std::string search_url = "https://nominatim.openstreetmap.org/search?format=json&addressdetails=1&limit=1&q=" + url_encode(company_name);

    // Step 2: Fetch geocoding results (with rate limiting - Nominatim requires 1 req/sec)
    std::string json_response = fetch_url(search_url);

    if (json_response.empty()) {
        // URL fetch failed - curl/wget not available or request blocked
        cache_address_result(company_name, result);
        return result;
    }

    // Step 3: Parse JSON response from Nominatim
    result = parse_nominatim_response(json_response);

    // Step 4: Cache result (positive or negative) to avoid repeated lookups
    cache_address_result(company_name, result);

    return result;
}

// ============================================================================
// DuckDB function wrapper
// ============================================================================

static void StpsGetAddressFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input = args.data[0];

    // Get struct entries for output
    auto &struct_entries = StructVector::GetEntries(result);
    auto &city_vec = *struct_entries[0];
    auto &plz_vec = *struct_entries[1];
    auto &address_vec = *struct_entries[2];
    auto &street_name_vec = *struct_entries[3];
    auto &street_number_vec = *struct_entries[4];

    auto &result_validity = FlatVector::Validity(result);

    // Process each row
    for (idx_t i = 0; i < count; i++) {
        if (FlatVector::IsNull(input, i)) {
            result_validity.SetInvalid(i);
            continue;
        }

        string_t company_name_str = FlatVector::GetData<string_t>(input)[i];
        std::string company_name = company_name_str.GetString();

        if (company_name.empty()) {
            result_validity.SetInvalid(i);
            continue;
        }

        AddressResult addr = lookup_company_address(company_name);

        if (!addr.found) {
            result_validity.SetInvalid(i);
            continue;
        }

        // Set struct fields
        if (addr.city.empty()) {
            FlatVector::SetNull(city_vec, i, true);
        } else {
            FlatVector::GetData<string_t>(city_vec)[i] = StringVector::AddString(city_vec, addr.city);
        }

        if (addr.plz.empty()) {
            FlatVector::SetNull(plz_vec, i, true);
        } else {
            FlatVector::GetData<string_t>(plz_vec)[i] = StringVector::AddString(plz_vec, addr.plz);
        }

        if (addr.full_address.empty()) {
            FlatVector::SetNull(address_vec, i, true);
        } else {
            FlatVector::GetData<string_t>(address_vec)[i] = StringVector::AddString(address_vec, addr.full_address);
        }

        if (addr.street_name.empty()) {
            FlatVector::SetNull(street_name_vec, i, true);
        } else {
            FlatVector::GetData<string_t>(street_name_vec)[i] = StringVector::AddString(street_name_vec, addr.street_name);
        }

        if (addr.street_number.empty()) {
            FlatVector::SetNull(street_number_vec, i, true);
        } else {
            FlatVector::GetData<string_t>(street_number_vec)[i] = StringVector::AddString(street_number_vec, addr.street_number);
        }
    }
}

void RegisterAddressLookupFunctions(ExtensionLoader& loader) {
    // Define STRUCT return type with named fields
    child_list_t<LogicalType> struct_children;
    struct_children.push_back(make_pair("city", LogicalType::VARCHAR));
    struct_children.push_back(make_pair("plz", LogicalType::VARCHAR));
    struct_children.push_back(make_pair("address", LogicalType::VARCHAR));
    struct_children.push_back(make_pair("street_name", LogicalType::VARCHAR));
    struct_children.push_back(make_pair("street_number", LogicalType::VARCHAR));
    auto return_type = LogicalType::STRUCT(std::move(struct_children));

    ScalarFunctionSet get_address_set("stps_get_address");
    get_address_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR},
        return_type,
        StpsGetAddressFunction
    ));

    loader.RegisterFunction(get_address_set);
}

} // namespace stps
} // namespace duckdb
