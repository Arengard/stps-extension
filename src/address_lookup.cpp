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

    // Build curl command with realistic browser headers to avoid bot detection
    // Updated User-Agent to latest Chrome version
    // Added Accept-Language for German locale
    // Added DNT (Do Not Track) header
    std::string cmd = "curl -s -L -m 15 "
                      "-H \"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36\" "
                      "-H \"Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\" "
                      "-H \"Accept-Language: de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7\" "
                      "-H \"Accept-Encoding: gzip, deflate, br\" "
                      "-H \"DNT: 1\" "
                      "-H \"Connection: keep-alive\" "
                      "-H \"Upgrade-Insecure-Requests: 1\" "
                      "--compressed "
                      "-o \"" + temp_file + "\" "
                      "\"" + url + "\" 2>/dev/null";

#ifdef _WIN32
    cmd = "curl -s -L -m 15 "
          "-H \"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36\" "
          "-H \"Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8\" "
          "-H \"Accept-Language: de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7\" "
          "-H \"Accept-Encoding: gzip, deflate, br\" "
          "-H \"DNT: 1\" "
          "-H \"Connection: keep-alive\" "
          "-H \"Upgrade-Insecure-Requests: 1\" "
          "--compressed "
          "-o \"" + temp_file + "\" "
          "\"" + url + "\" 2>nul";
#endif

    int result = system(cmd.c_str());

    if (result != 0) {
        // Fallback to wget with similar headers
#ifdef _WIN32
        cmd = "wget -q -O \"" + temp_file + "\" "
              "--user-agent=\"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36\" "
              "--header=\"Accept-Language: de-DE,de;q=0.9\" "
              "\"" + url + "\" 2>nul";
#else
        cmd = "wget -q -O \"" + temp_file + "\" "
              "--user-agent=\"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36\" "
              "--header=\"Accept-Language: de-DE,de;q=0.9\" "
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
// HTML processing
// ============================================================================

static std::string decode_html_entities(const std::string& text) {
    std::string result = text;

    // Named entities
    const std::vector<std::pair<std::string, std::string>> entities = {
        {"&amp;", "&"}, {"&lt;", "<"}, {"&gt;", ">"},
        {"&quot;", "\""}, {"&apos;", "'"},
        {"&nbsp;", " "}, {"&ndash;", "-"}, {"&mdash;", "-"},
        {"&auml;", "ä"}, {"&ouml;", "ö"}, {"&uuml;", "ü"},
        {"&Auml;", "Ä"}, {"&Ouml;", "Ö"}, {"&Uuml;", "Ü"},
        {"&szlig;", "ß"}
    };

    for (const auto& e : entities) {
        size_t pos = 0;
        while ((pos = result.find(e.first, pos)) != std::string::npos) {
            result.replace(pos, e.first.length(), e.second);
            pos += e.second.length();
        }
    }

    // Numeric entities (&#123; or &#x7B;)
    size_t pos = 0;
    while ((pos = result.find("&#", pos)) != std::string::npos) {
        size_t end = result.find(';', pos);
        if (end == std::string::npos || end > pos + 10) {
            pos++;
            continue;
        }

        std::string entity = result.substr(pos + 2, end - pos - 2);
        int code = 0;

        if (!entity.empty() && (entity[0] == 'x' || entity[0] == 'X')) {
            // Hex: &#xAB;
            try {
                code = std::stoi(entity.substr(1), nullptr, 16);
            } catch (...) {
                pos++;
                continue;
            }
        } else {
            // Decimal: &#123;
            try {
                code = std::stoi(entity);
            } catch (...) {
                pos++;
                continue;
            }
        }

        if (code > 0 && code < 128) {
            result.replace(pos, end - pos + 1, std::string(1, static_cast<char>(code)));
        } else {
            pos = end + 1;
        }
    }

    return result;
}

static std::string strip_html_tags(const std::string& html) {
    std::string result;
    result.reserve(html.size());

    bool in_tag = false;
    bool in_script = false;
    bool in_style = false;

    for (size_t i = 0; i < html.size(); i++) {
        char c = html[i];

        if (c == '<') {
            in_tag = true;

            // Check for <script> or <style>
            if (i + 7 < html.size()) {
                std::string upcoming = to_lower(html.substr(i, 8));
                if (upcoming.find("<script") == 0) in_script = true;
                if (upcoming.find("<style") == 0) in_style = true;
            }
            if (i + 8 < html.size()) {
                std::string upcoming = to_lower(html.substr(i, 9));
                if (upcoming.find("</script") == 0) in_script = false;
                if (upcoming.find("</style") == 0) in_style = false;
            }
            continue;
        }

        if (c == '>') {
            in_tag = false;
            result += ' ';
            continue;
        }

        if (!in_tag && !in_script && !in_style) {
            result += c;
        }
    }

    result = decode_html_entities(result);

    // Collapse whitespace
    std::string collapsed;
    collapsed.reserve(result.size());
    bool last_was_space = false;
    for (char c : result) {
        if (std::isspace(static_cast<unsigned char>(c))) {
            if (!last_was_space) {
                collapsed += ' ';
                last_was_space = true;
            }
        } else {
            collapsed += c;
            last_was_space = false;
        }
    }

    return collapsed;
}

static std::vector<std::string> split_lines(const std::string& text) {
    std::vector<std::string> lines;
    std::istringstream stream(text);
    std::string line;

    // Split by newlines and also by double spaces (common in stripped HTML)
    while (std::getline(stream, line)) {
        // Also split by common separators that become boundaries in stripped HTML
        size_t pos = 0;
        size_t prev = 0;
        while ((pos = line.find("  ", prev)) != std::string::npos) {
            std::string part = trim(line.substr(prev, pos - prev));
            if (!part.empty()) {
                lines.push_back(part);
            }
            prev = pos + 2;
        }
        std::string part = trim(line.substr(prev));
        if (!part.empty()) {
            lines.push_back(part);
        }
    }

    return lines;
}

// ============================================================================
// Handelsregister parsing - Forward declarations
// ============================================================================

static bool parse_plz_city(const std::string& line, std::string& plz, std::string& city);
static bool looks_like_street_address(const std::string& line);

// ============================================================================
// Handelsregister parsing
// ============================================================================

static AddressResult parse_handelsregister_company_page(const std::string& html) {
    AddressResult result;
    result.found = false;

    // Strip HTML and get plain text
    std::string text = strip_html_tags(html);
    std::vector<std::string> lines = split_lines(text);

    // Handelsregister has address in various formats, look for PLZ + City pattern
    for (size_t i = 0; i < lines.size(); i++) {
        std::string plz, city;
        if (parse_plz_city(lines[i], plz, city)) {
            result.plz = plz;
            result.city = city;

            // Street is typically the line BEFORE PLZ+City
            if (i > 0) {
                std::string prev_line = lines[i - 1];
                if (looks_like_street_address(prev_line)) {
                    result.full_address = prev_line;
                } else if (i > 1 && looks_like_street_address(lines[i - 2])) {
                    result.full_address = lines[i - 2];
                }
            }

            // Also check if street+number has house number at end
            if (result.full_address.empty() && i > 0) {
                // Try previous line even if it doesn't match street patterns
                std::string prev_line = lines[i - 1];
                // Check if it has a number at the end (likely house number)
                if (!prev_line.empty() && prev_line.length() > 2) {
                    char last = prev_line.back();
                    if (std::isdigit(last) || last == 'a' || last == 'b' || last == 'c') {
                        result.full_address = prev_line;
                    }
                }
            }

            result.found = true;
            break;
        }
    }

    // Split street address into name and number
    if (!result.full_address.empty()) {
        auto parsed = parse_street_address(result.full_address);
        result.street_name = parsed.street_name;
        result.street_number = parsed.street_number;
    }

    return result;
}

// ============================================================================
// Address parsing utilities
// ============================================================================

static bool parse_plz_city(const std::string& line, std::string& plz, std::string& city) {
    std::string work = line;

    // Skip country prefix if present (D-, DE-, A-, AT-, CH-)
    size_t start = 0;
    if (work.size() > 2) {
        std::string prefix = work.substr(0, 3);
        if (prefix == "D- " || prefix == "D-" || work.substr(0, 2) == "D ") {
            start = work.find_first_of("0123456789");
        } else if (work.size() > 3 && (work.substr(0, 3) == "DE-" || work.substr(0, 3) == "DE ")) {
            start = work.find_first_of("0123456789");
        }
    }

    // Look for 5 consecutive digits
    size_t digit_start = work.find_first_of("0123456789", start);
    if (digit_start == std::string::npos || digit_start + 5 > work.size()) {
        return false;
    }

    // Check if we have exactly 5 digits followed by non-digit
    std::string potential_plz = work.substr(digit_start, 5);
    bool all_digits = true;
    for (char c : potential_plz) {
        if (!std::isdigit(static_cast<unsigned char>(c))) {
            all_digits = false;
            break;
        }
    }

    if (!all_digits) {
        return false;
    }

    // Check that there's no 6th digit (PLZ is exactly 5 digits)
    if (digit_start + 5 < work.size() &&
        std::isdigit(static_cast<unsigned char>(work[digit_start + 5]))) {
        return false;
    }

    // Validate PLZ range (01000-99999)
    int plz_value = std::stoi(potential_plz);
    if (plz_value < 1000 || plz_value > 99999) {
        return false;
    }

    plz = potential_plz;

    // City is everything after PLZ, trimmed
    if (digit_start + 5 < work.size()) {
        city = trim(work.substr(digit_start + 5));
        // Remove trailing punctuation
        while (!city.empty() && (city.back() == ',' || city.back() == '.')) {
            city.pop_back();
        }
        city = trim(city);
    }

    return !plz.empty();
}

static bool looks_like_street_address(const std::string& line) {
    std::string lower = to_lower(line);

    // Common German street suffixes
    static const std::vector<std::string> suffixes = {
        "straße", "strasse", "str.", "str ",
        "weg", "platz", "allee", "ring", "damm",
        "ufer", "gasse", "steig", "pfad", "chaussee"
    };

    for (const auto& suffix : suffixes) {
        if (lower.find(suffix) != std::string::npos) {
            return true;
        }
    }

    // Check for "Am/An/Auf/Im/In" prefixes common in German street names
    static const std::vector<std::string> prefixes = {
        "am ", "an der ", "auf der ", "im ", "in der ", "zur ", "zum "
    };

    for (const auto& prefix : prefixes) {
        if (lower.find(prefix) == 0) {
            return true;
        }
    }

    return false;
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

    // NOTE: This function attempts to fetch company addresses from Handelsregister.de
    // It requires curl or wget to be installed and may not work reliably due to:
    // - Website may block automated requests
    // - curl/wget may not be installed
    // - Website requires JavaScript which curl doesn't support
    // Consider using an API service instead for production use.

    // Step 1: Search on Handelsregister.de
    // URL format: https://www.handelsregister.de/rp_web/search.xhtml
    // Uses POST request with search parameters
    std::string search_url = "https://www.handelsregister.de/rp_web/normalesuche.xhtml?form=simple&query=" + url_encode(company_name);

    // Step 2: Fetch search results (with rate limiting)
    std::string search_html = fetch_url(search_url);
    if (search_html.empty()) {
        // URL fetch failed - curl/wget not available or request blocked
        cache_address_result(company_name, result);
        return result;
    }

    // Step 3: Parse the search results page directly for address info
    // Handelsregister often shows basic info in search results
    result = parse_handelsregister_company_page(search_html);

    if (result.found) {
        cache_address_result(company_name, result);
        return result;
    }

    // Step 4: Try to find detail page link
    // Look for links to individual company pages
    size_t pos = 0;
    std::string marker = "href=\"";

    while ((pos = search_html.find(marker, pos)) != std::string::npos) {
        pos += marker.length();
        size_t end = search_html.find("\"", pos);
        if (end == std::string::npos) break;

        std::string url = search_html.substr(pos, end - pos);

        // Look for detail page URLs (typically contain "ergebnisse" or company identifiers)
        if (url.find("ergebnisse") != std::string::npos ||
            url.find("?id=") != std::string::npos ||
            url.find("detailansicht") != std::string::npos) {

            // Make URL absolute if relative
            if (url[0] == '/') {
                url = "https://www.handelsregister.de" + url;
            } else if (url.find("http") != 0) {
                url = "https://www.handelsregister.de/rp_web/" + url;
            }

            std::string detail_html = fetch_url(url);
            if (!detail_html.empty()) {
                result = parse_handelsregister_company_page(detail_html);
                if (result.found) {
                    cache_address_result(company_name, result);
                    return result;
                }
            }
            break; // Only try first detail link
        }

        pos = end;
    }

    // Step 5: Cache negative result to avoid repeated failed lookups
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
