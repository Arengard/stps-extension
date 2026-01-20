#include "ai_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <fstream>
#include <sstream>
#include <cstdlib>
#include <cctype>
#include <iomanip>
#include <mutex>
#include <regex>
#include "curl_utils.hpp"

#ifdef _WIN32
#include <windows.h>
#define popen _popen
#define pclose _pclose
#else
#include <unistd.h>
#endif

namespace duckdb {
namespace stps {

// ============================================================================
// Global API key and model storage (thread-safe)
// ============================================================================

static std::mutex ai_config_mutex;
static std::string anthropic_api_key;
static std::string anthropic_model = "claude-sonnet-4-5-20250929";  // Default model (Claude Sonnet 4.5)
static std::string brave_api_key;
static std::string google_api_key;
static std::string google_cse_id;  // Google Custom Search Engine ID
static std::string search_provider = "brave";  // Default: "brave" or "google"

void SetAnthropicApiKey(const std::string& key) {
    std::lock_guard<std::mutex> lock(ai_config_mutex);
    anthropic_api_key = key;
}

void SetAnthropicModel(const std::string& model) {
    std::lock_guard<std::mutex> lock(ai_config_mutex);
    anthropic_model = model;
}

void SetBraveApiKey(const std::string& key) {
    std::lock_guard<std::mutex> lock(ai_config_mutex);
    brave_api_key = key;
}

void SetGoogleApiKey(const std::string& key) {
    std::lock_guard<std::mutex> lock(ai_config_mutex);
    google_api_key = key;
}

void SetGoogleCseId(const std::string& id) {
    std::lock_guard<std::mutex> lock(ai_config_mutex);
    google_cse_id = id;
}

void SetSearchProvider(const std::string& provider) {
    std::lock_guard<std::mutex> lock(ai_config_mutex);
    // Normalize to lowercase
    std::string p = provider;
    for (auto& c : p) c = std::tolower(static_cast<unsigned char>(c));
    if (p == "google" || p == "brave") {
        search_provider = p;
    }
}

std::string GetSearchProvider() {
    std::lock_guard<std::mutex> lock(ai_config_mutex);
    return search_provider;
}

std::string GetGoogleApiKey() {
    std::lock_guard<std::mutex> lock(ai_config_mutex);
    return google_api_key;
}

std::string GetGoogleCseId() {
    std::lock_guard<std::mutex> lock(ai_config_mutex);
    return google_cse_id;
}

std::string GetAnthropicModel() {
    std::lock_guard<std::mutex> lock(ai_config_mutex);
    return anthropic_model;
}

std::string GetAnthropicApiKey() {
    // Check if key was set via stps_set_api_key()
    {
        std::lock_guard<std::mutex> lock(ai_config_mutex);
        if (!anthropic_api_key.empty()) {
            return anthropic_api_key;
        }
    }

    // Check environment variable ANTHROPIC_API_KEY
    const char* env_key = std::getenv("ANTHROPIC_API_KEY");
    if (env_key != nullptr) {
        return std::string(env_key);
    }

    // Check ~/.stps/anthropic_api_key file
    const char* home = std::getenv("HOME");
    if (!home) {
#ifdef _WIN32
        home = std::getenv("USERPROFILE");
#endif
    }

    if (home) {
        std::string key_file = std::string(home) + "/.stps/anthropic_api_key";
        std::ifstream file(key_file);
        if (file.is_open()) {
            std::string key;
            std::getline(file, key);
            // Trim whitespace
            key.erase(0, key.find_first_not_of(" \t\n\r"));
            key.erase(key.find_last_not_of(" \t\n\r") + 1);
            if (!key.empty()) {
                return key;
            }
        }
    }

    return "";
}

std::string GetBraveApiKey() {
    // Check if key was set via stps_set_brave_api_key()
    {
        std::lock_guard<std::mutex> lock(ai_config_mutex);
        if (!brave_api_key.empty()) {
            return brave_api_key;
        }
    }

    // Check environment variable BRAVE_API_KEY
    const char* env_key = std::getenv("BRAVE_API_KEY");
    if (env_key != nullptr) {
        return std::string(env_key);
    }

    // Check ~/.stps/brave_api_key file
    const char* home = std::getenv("HOME");
    if (!home) {
#ifdef _WIN32
        home = std::getenv("USERPROFILE");
#endif
    }

    if (home) {
        std::string key_file = std::string(home) + "/.stps/brave_api_key";
        std::ifstream file(key_file);
        if (file.is_open()) {
            std::string key;
            std::getline(file, key);
            // Trim whitespace
            key.erase(0, key.find_first_not_of(" \t\n\r"));
            key.erase(key.find_last_not_of(" \t\n\r") + 1);
            if (!key.empty()) {
                return key;
            }
        }
    }

    return "";
}

// ============================================================================
// Helper functions
// ============================================================================

static std::string escape_json_string(const std::string& str) {
    std::ostringstream escaped;
    for (char c : str) {
        switch (c) {
            case '"':  escaped << "\\\""; break;
            case '\\': escaped << "\\\\"; break;
            case '\b': escaped << "\\b"; break;
            case '\f': escaped << "\\f"; break;
            case '\n': escaped << "\\n"; break;
            case '\r': escaped << "\\r"; break;
            case '\t': escaped << "\\t"; break;
            default:
                if (c < 32) {
                    escaped << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int)c;
                } else {
                    escaped << c;
                }
        }
    }
    return escaped.str();
}

static std::string extract_json_content(const std::string& json, const std::string& key) {
    // First, strip markdown code blocks if present (```json ... ``` or ``` ... ```)
    std::string cleaned = json;

    // Remove ```json or ``` at start
    size_t code_start = cleaned.find("```");
    if (code_start != std::string::npos) {
        size_t line_end = cleaned.find('\n', code_start);
        if (line_end != std::string::npos) {
            cleaned = cleaned.substr(line_end + 1);
        }
    }
    // Remove trailing ```
    size_t code_end = cleaned.rfind("```");
    if (code_end != std::string::npos) {
        cleaned = cleaned.substr(0, code_end);
    }

    // Trim whitespace
    size_t start = cleaned.find_first_not_of(" \t\n\r");
    size_t end = cleaned.find_last_not_of(" \t\n\r");
    if (start != std::string::npos && end != std::string::npos) {
        cleaned = cleaned.substr(start, end - start + 1);
    }

    // Find "key":"value" or "key": "value" pattern
    std::string search = "\"" + key + "\"";
    size_t pos = cleaned.find(search);
    if (pos == std::string::npos) {
        return "";
    }

    pos += search.length();

    // Skip to colon
    size_t colon = cleaned.find(':', pos);
    if (colon == std::string::npos) {
        return "";
    }

    pos = colon + 1;

    // Skip whitespace
    while (pos < cleaned.length() && std::isspace(cleaned[pos])) {
        pos++;
    }

    // Check if value is a string (starts with ")
    if (pos >= cleaned.length() || cleaned[pos] != '"') {
        return "";
    }

    pos++; // Skip opening quote
    end = pos;

    // Find closing quote (handle escaped quotes)
    while (end < cleaned.length()) {
        if (cleaned[end] == '"' && (end == 0 || cleaned[end - 1] != '\\')) {
            break;
        }
        end++;
    }

    if (end >= cleaned.length()) {
        return "";
    }

    std::string result = cleaned.substr(pos, end - pos);

    // Unescape basic sequences
    // Process \\ first to avoid double-processing
    size_t esc_pos = 0;
    while ((esc_pos = result.find("\\\\", esc_pos)) != std::string::npos) {
        result.replace(esc_pos, 2, "\\");
        esc_pos += 1;
    }
    // Then \" to avoid conflicts
    esc_pos = 0;
    while ((esc_pos = result.find("\\\"", esc_pos)) != std::string::npos) {
        result.replace(esc_pos, 2, "\"");
        esc_pos += 1;
    }
    // Then \n
    esc_pos = 0;
    while ((esc_pos = result.find("\\n", esc_pos)) != std::string::npos) {
        result.replace(esc_pos, 2, "\n");
        esc_pos += 1;
    }
    // Finally \t
    esc_pos = 0;
    while ((esc_pos = result.find("\\t", esc_pos)) != std::string::npos) {
        result.replace(esc_pos, 2, "\t");
        esc_pos += 1;
    }

    return result;
}

// ============================================================================
// German Address Parser - extracts address components from text using regex
// ============================================================================


struct ParsedAddress {
    std::string city;
    std::string postal_code;
    std::string street_name;
    std::string street_nr;
};

// Strip HTML tags from text
static std::string strip_html_tags(const std::string& input) {
    std::string result;
    result.reserve(input.length());
    bool in_tag = false;

    for (size_t i = 0; i < input.length(); i++) {
        if (input[i] == '<') {
            in_tag = true;
        } else if (input[i] == '>') {
            in_tag = false;
        } else if (!in_tag) {
            result += input[i];
        }
    }
    return result;
}

// Trim whitespace from string
static std::string trim_string(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\n\r,.");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\n\r,.");
    return s.substr(start, end - start + 1);
}

// Clean city name - remove phone, country, and other suffixes
static std::string clean_city_name(const std::string& city) {
    std::string result = city;

    // Convert to check for stop words (case insensitive)
    std::string lower = result;
    for (auto& c : lower) c = std::tolower(static_cast<unsigned char>(c));

    // Stop words - truncate at these
    std::vector<std::string> stop_words = {
        "germany", "deutschland", "phone", "tel", "fax", "telefon",
        "email", "e-mail", "www.", "http", "+49", "(+49", "gmbh", "ag "
    };

    for (const auto& stop : stop_words) {
        size_t pos = lower.find(stop);
        if (pos != std::string::npos && pos > 0) {
            result = result.substr(0, pos);
            lower = lower.substr(0, pos);
        }
    }

    return trim_string(result);
}

// Clean street name - extract only the street part, not company names
static std::string clean_street_name(const std::string& street) {
    std::string result = street;

    // Find the last occurrence of a street suffix
    std::string lower = result;
    for (auto& c : lower) c = std::tolower(static_cast<unsigned char>(c));

    std::vector<std::string> suffixes = {
        "straße", "strasse", "str.", "weg", "platz", "allee",
        "ring", "gasse", "damm", "ufer", "chaussee"
    };

    size_t last_suffix_pos = std::string::npos;
    size_t last_suffix_len = 0;

    for (const auto& suffix : suffixes) {
        size_t pos = lower.rfind(suffix);
        if (pos != std::string::npos) {
            if (last_suffix_pos == std::string::npos || pos > last_suffix_pos) {
                last_suffix_pos = pos;
                last_suffix_len = suffix.length();
            }
        }
    }

    if (last_suffix_pos != std::string::npos) {
        // Find the start of this street name (go backwards from suffix)
        size_t street_start = last_suffix_pos;

        // Skip back over the street name word(s)
        while (street_start > 0) {
            char prev = result[street_start - 1];
            // Stop at delimiters that separate company name from street
            if (prev == ',' || prev == ';' || prev == '|' || prev == '\n') {
                break;
            }
            // Stop at company indicators
            if (street_start >= 4) {
                std::string prev4 = lower.substr(street_start - 4, 4);
                if (prev4 == "gmbh" || prev4 == " ag " || prev4 == " kg ") {
                    break;
                }
            }
            if (street_start >= 3) {
                std::string prev3 = lower.substr(street_start - 3, 3);
                if (prev3 == "mbh" || prev3 == " se") {
                    break;
                }
            }
            street_start--;
        }

        // Skip leading spaces/punctuation
        while (street_start < result.length() &&
               (result[street_start] == ' ' || result[street_start] == ',' ||
                result[street_start] == ';')) {
            street_start++;
        }

        result = result.substr(street_start, last_suffix_pos + last_suffix_len - street_start);
    }

    return trim_string(result);
}

static ParsedAddress parse_german_address(const std::string& text) {
    ParsedAddress addr;

    // First, strip HTML tags
    std::string cleaned = strip_html_tags(text);

    // Step 1: Find 5-digit German postal code (PLZ)
    std::regex plz_pattern(R"((\d{5}))");
    std::smatch plz_match;

    if (std::regex_search(cleaned, plz_match, plz_pattern)) {
        // Verify it's not a phone number (check context)
        size_t plz_pos = plz_match.position();
        bool is_phone = false;

        // Check if preceded by +, (, Tel, Fax
        if (plz_pos > 0) {
            char prev = cleaned[plz_pos - 1];
            if (prev == '+' || prev == '(' || prev == '-') {
                is_phone = true;
            }
            if (plz_pos >= 3) {
                std::string prev3 = cleaned.substr(plz_pos - 3, 3);
                for (auto& c : prev3) c = std::tolower(static_cast<unsigned char>(c));
                if (prev3 == "tel" || prev3 == "fax" || prev3 == "+49") {
                    is_phone = true;
                }
            }
        }

        // Check if followed by more digits (phone number)
        size_t after_plz = plz_pos + 5;
        if (after_plz < cleaned.length() && std::isdigit(cleaned[after_plz])) {
            is_phone = true;
        }

        if (!is_phone) {
            addr.postal_code = plz_match[1].str();

            // Step 2: Extract city (text after PLZ until delimiter)
            size_t city_start = after_plz;
            while (city_start < cleaned.length() &&
                   (cleaned[city_start] == ' ' || cleaned[city_start] == ',')) {
                city_start++;
            }

            size_t city_end = city_start;
            while (city_end < cleaned.length()) {
                char c = cleaned[city_end];
                if (c == ',' || c == '\n' || c == '|' || c == ';' || c == '(') {
                    break;
                }
                city_end++;
            }

            if (city_end > city_start) {
                addr.city = clean_city_name(cleaned.substr(city_start, city_end - city_start));
            }
        }
    }

    // Step 3: Find street with number
    // Pattern: StreetName + suffix + optional space + number
    std::regex street_pattern(
        R"(([A-Za-z][A-Za-z\-\.\s]*(?:stra(?:ss|ß)e|str\.|weg|platz|allee|ring|gasse|damm|ufer|chaussee))\s*(\d+[a-zA-Z]?))",
        std::regex_constants::icase
    );

    std::smatch street_match;
    if (std::regex_search(cleaned, street_match, street_pattern)) {
        addr.street_name = clean_street_name(street_match[1].str());
        addr.street_nr = trim_string(street_match[2].str());
    }

    return addr;
}

// ============================================================================
// Web Search Support
// ============================================================================

// URL-encode a string for use in HTTP GET parameters
static std::string url_encode(const std::string& value) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;

    for (char c : value) {
        // Keep alphanumeric and other safe characters
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
        } else {
            // Percent-encode everything else
            escaped << '%' << std::setw(2) << int((unsigned char)c);
        }
    }

    return escaped.str();
}

// Format Brave Search API results into a readable string
static std::string format_search_results(const std::string& json_response) {
    std::ostringstream formatted;

    // Extract query from response (optional, for context)
    std::string query = extract_json_content(json_response, "query");

    // Parse web results array
    // Look for "web":{"results":[...]} pattern
    size_t web_pos = json_response.find("\"web\"");
    if (web_pos == std::string::npos) {
        return "No search results found.";
    }

    size_t results_pos = json_response.find("\"results\"", web_pos);
    if (results_pos == std::string::npos) {
        return "No search results found.";
    }

    // Find the opening bracket of results array
    size_t array_start = json_response.find('[', results_pos);
    if (array_start == std::string::npos) {
        return "No search results found.";
    }

    // Simple parsing: find each result object
    formatted << "Search Results:\n\n";

    size_t pos = array_start + 1;
    int result_num = 1;

    while (pos < json_response.length() && result_num <= 5) {
        // Find next result object
        size_t obj_start = json_response.find('{', pos);
        if (obj_start == std::string::npos) break;

        size_t obj_end = json_response.find('}', obj_start);
        if (obj_end == std::string::npos) break;

        std::string result_obj = json_response.substr(obj_start, obj_end - obj_start + 1);

        // Extract title, url, description
        std::string title = extract_json_content(result_obj, "title");
        std::string url = extract_json_content(result_obj, "url");
        std::string description = extract_json_content(result_obj, "description");

        if (!title.empty() && !url.empty()) {
            formatted << result_num << ". " << title << "\n";
            formatted << "   URL: " << url << "\n";
            if (!description.empty()) {
                formatted << "   " << description << "\n";
            }
            formatted << "\n";
            result_num++;
        }

        pos = obj_end + 1;

        // Break if we hit the end of the array
        if (json_response.find(',', pos) == std::string::npos ||
            json_response.find(']', pos) < json_response.find('{', pos)) {
            break;
        }
    }

    if (result_num == 1) {
        return "No search results found.";
    }

    return formatted.str();
}

// Execute a Brave Search API query
static std::string execute_brave_search(const std::string& query) {
    std::string api_key = GetBraveApiKey();

    if (api_key.empty()) {
        return "ERROR: Brave API key not configured. Cannot perform web search.";
    }

    // Build Brave Search API URL
    std::string encoded_query = url_encode(query);
    std::string url = "https://api.search.brave.com/res/v1/web/search?q=" + encoded_query + "&count=5";

    // Set up headers with API key
    CurlHeaders headers;
    headers.append("Accept: application/json");
    headers.append("X-Subscription-Token: " + api_key);

    // Make the request
    long http_code = 0;
    std::string response = curl_get(url, headers, &http_code);

    // Check for errors
    if (response.find("ERROR:") == 0) {
        return "Search failed: " + response;
    }

    // Format and return results
    return format_search_results(response);
}

// Structure to hold parsed address from Brave Local Search
struct BraveLocalAddress {
    std::string city;
    std::string postal_code;
    std::string street_name;
    std::string street_nr;
    bool found;
};

// Parse German street address like "Brauerstraße 12" into street_name and street_nr
static void parse_street_address(const std::string& street_full, std::string& street_name, std::string& street_nr) {
    street_name.clear();
    street_nr.clear();

    if (street_full.empty()) return;

    // Find last space followed by number
    size_t last_space = std::string::npos;
    for (size_t i = street_full.length() - 1; i > 0; i--) {
        if (street_full[i] == ' ' && i + 1 < street_full.length() && std::isdigit(street_full[i + 1])) {
            last_space = i;
            break;
        }
    }

    if (last_space != std::string::npos) {
        street_name = street_full.substr(0, last_space);
        street_nr = street_full.substr(last_space + 1);
    } else {
        street_name = street_full;
    }
}

// Execute Brave Local Search API for business address lookup
// Returns structured address data directly from Brave's POI/locations data
static BraveLocalAddress execute_brave_local_search(const std::string& company_name) {
    BraveLocalAddress result = {"", "", "", "", false};

    std::string api_key = GetBraveApiKey();
    if (api_key.empty()) {
        return result;
    }

    // Build optimized search query for German business addresses
    std::string query = company_name + " Adresse Deutschland";
    std::string encoded_query = url_encode(query);

    // Use Brave web search with result_filter=locations to get POI data
    // Also request extra_snippets for more address details
    std::string url = "https://api.search.brave.com/res/v1/web/search?q=" + encoded_query +
                      "&count=10&result_filter=locations,web&extra_snippets=true";

    CurlHeaders headers;
    headers.append("Accept: application/json");
    headers.append("X-Subscription-Token: " + api_key);

    long http_code = 0;
    std::string response = curl_get(url, headers, &http_code);

    if (response.find("ERROR:") == 0 || response.empty()) {
        return result;
    }

    // First try: Look for "locations" object with structured address data
    // Brave returns: "locations": {"results": [{"address": {...}, "postal_code": "...", ...}]}
    size_t locations_pos = response.find("\"locations\"");
    if (locations_pos != std::string::npos) {
        size_t results_pos = response.find("\"results\"", locations_pos);
        if (results_pos != std::string::npos) {
            // Find first location result
            size_t obj_start = response.find('{', results_pos + 10);
            if (obj_start != std::string::npos) {
                // Find matching closing brace
                int brace_count = 1;
                size_t obj_end = obj_start + 1;
                while (obj_end < response.length() && brace_count > 0) {
                    if (response[obj_end] == '{') brace_count++;
                    else if (response[obj_end] == '}') brace_count--;
                    obj_end++;
                }

                std::string location_obj = response.substr(obj_start, obj_end - obj_start);

                // Extract address fields from location object
                // Brave Local returns: city, postal_code, street_address or address_line
                std::string city = extract_json_content(location_obj, "city");
                std::string postal = extract_json_content(location_obj, "postal_code");
                std::string street = extract_json_content(location_obj, "street_address");
                if (street.empty()) {
                    street = extract_json_content(location_obj, "address_line");
                }

                if (!city.empty() || !postal.empty() || !street.empty()) {
                    result.city = city;
                    result.postal_code = postal;
                    parse_street_address(street, result.street_name, result.street_nr);
                    result.found = true;
                    return result;
                }
            }
        }
    }

    // Second try: Parse from web results - look for German address patterns
    // Pattern: "Street Nr, PLZ City" or "PLZ City, Street Nr"
    size_t web_pos = response.find("\"web\"");
    if (web_pos != std::string::npos) {
        size_t results_pos = response.find("\"results\"", web_pos);
        if (results_pos != std::string::npos) {
            // Search through web results for address patterns
            size_t search_start = results_pos;

            // Look for 5-digit German postal code pattern
            for (int attempt = 0; attempt < 10 && search_start < response.length(); attempt++) {
                // Find description or snippet text
                size_t desc_pos = response.find("\"description\"", search_start);
                if (desc_pos == std::string::npos) break;

                std::string desc = extract_json_content(response.substr(desc_pos), "description");
                if (!desc.empty()) {
                    // Use existing parse_german_address function
                    ParsedAddress parsed = parse_german_address(desc);
                    if (!parsed.city.empty() || !parsed.postal_code.empty()) {
                        result.city = parsed.city;
                        result.postal_code = parsed.postal_code;
                        result.street_name = parsed.street_name;
                        result.street_nr = parsed.street_nr;
                        result.found = true;
                        return result;
                    }
                }
                search_start = desc_pos + 15;
            }
        }
    }

    return result;
}

// Format Google Custom Search API results into a readable string
static std::string format_google_search_results(const std::string& json_response) {
    std::ostringstream formatted;

    // Look for "items":[...] pattern (Google CSE uses "items" not "results")
    size_t items_pos = json_response.find("\"items\"");
    if (items_pos == std::string::npos) {
        return "No search results found.";
    }

    // Find the opening bracket of items array
    size_t array_start = json_response.find('[', items_pos);
    if (array_start == std::string::npos) {
        return "No search results found.";
    }

    formatted << "Search Results:\n\n";

    size_t pos = array_start + 1;
    int result_num = 1;

    while (pos < json_response.length() && result_num <= 5) {
        // Find next result object
        size_t obj_start = json_response.find('{', pos);
        if (obj_start == std::string::npos) break;

        // Find matching closing brace (handle nested objects)
        int brace_count = 1;
        size_t obj_end = obj_start + 1;
        while (obj_end < json_response.length() && brace_count > 0) {
            if (json_response[obj_end] == '{') brace_count++;
            else if (json_response[obj_end] == '}') brace_count--;
            obj_end++;
        }
        if (brace_count != 0) break;

        std::string result_obj = json_response.substr(obj_start, obj_end - obj_start);

        // Extract title, link (url), snippet (description) - Google uses different field names
        std::string title = extract_json_content(result_obj, "title");
        std::string url = extract_json_content(result_obj, "link");
        std::string description = extract_json_content(result_obj, "snippet");

        if (!title.empty() && !url.empty()) {
            formatted << result_num << ". " << title << "\n";
            formatted << "   URL: " << url << "\n";
            if (!description.empty()) {
                formatted << "   " << description << "\n";
            }
            formatted << "\n";
            result_num++;
        }

        pos = obj_end;

        // Check if we've reached the end of the array
        size_t next_comma = json_response.find(',', pos);
        size_t array_end = json_response.find(']', pos);
        if (next_comma == std::string::npos || (array_end != std::string::npos && array_end < next_comma)) {
            break;
        }
    }

    if (result_num == 1) {
        return "No search results found.";
    }

    return formatted.str();
}

// Execute a Google Custom Search API query
static std::string execute_google_search(const std::string& query) {
    std::string api_key = GetGoogleApiKey();
    std::string cse_id = GetGoogleCseId();

    if (api_key.empty()) {
        return "ERROR: Google API key not configured. Use stps_set_google_api_key() first.";
    }
    if (cse_id.empty()) {
        return "ERROR: Google CSE ID not configured. Use stps_set_google_cse_id() first.";
    }

    // Build Google Custom Search API URL
    // https://www.googleapis.com/customsearch/v1?key=API_KEY&cx=CSE_ID&q=QUERY
    std::string encoded_query = url_encode(query);
    std::string url = "https://www.googleapis.com/customsearch/v1?key=" + api_key +
                      "&cx=" + cse_id +
                      "&q=" + encoded_query +
                      "&num=5";

    // Set up headers
    CurlHeaders headers;
    headers.append("Accept: application/json");

    // Make the request
    long http_code = 0;
    std::string response = curl_get(url, headers, &http_code);

    // Check for errors
    if (response.find("ERROR:") == 0) {
        return "Search failed: " + response;
    }

    // Check for API error in response
    std::string error_msg = extract_json_content(response, "message");
    if (!error_msg.empty() && response.find("\"error\"") != std::string::npos) {
        return "ERROR: Google API error: " + error_msg;
    }

    // Format and return results
    return format_google_search_results(response);
}

// Unified web search function - uses configured provider
static std::string execute_web_search(const std::string& query) {
    std::string provider = GetSearchProvider();

    if (provider == "google") {
        // Check if Google is configured
        if (!GetGoogleApiKey().empty() && !GetGoogleCseId().empty()) {
            return execute_google_search(query);
        }
        // Fall back to Brave if Google not configured
        if (!GetBraveApiKey().empty()) {
            return execute_brave_search(query);
        }
        return "ERROR: Google search not configured and no fallback available.";
    } else {
        // Default: Brave
        if (!GetBraveApiKey().empty()) {
            return execute_brave_search(query);
        }
        // Fall back to Google if Brave not configured
        if (!GetGoogleApiKey().empty() && !GetGoogleCseId().empty()) {
            return execute_google_search(query);
        }
        return "ERROR: Brave search not configured and no fallback available.";
    }
}

// Check if any search provider is available
static bool is_search_available() {
    return !GetBraveApiKey().empty() ||
           (!GetGoogleApiKey().empty() && !GetGoogleCseId().empty());
}

// ============================================================================
// Anthropic API call
// ============================================================================

static std::string call_anthropic_api(const std::string& context, const std::string& prompt,
                                      const std::string& model, int max_tokens,
                                      const std::string& custom_system_message = "") {
    std::string api_key = GetAnthropicApiKey();

    if (api_key.empty()) {
        return "ERROR: Anthropic API key not configured. Use stps_set_api_key() or set ANTHROPIC_API_KEY environment variable.";
    }

    // Check if we should enable tools (only if no custom system message and search is available)
    bool tools_enabled = is_search_available() && custom_system_message.empty();

    // Build JSON request payload for Anthropic Messages API
    std::string system_message;
    if (!custom_system_message.empty()) {
        system_message = custom_system_message;
    } else {
        system_message = tools_enabled
            ? "You are a helpful assistant with access to web search. When asked about current information, real-time data, business address, recent events, stock prices, or anything that requires up-to-date information, you MUST use the web_search tool. Always search first before saying you don't have access to current data."
            : "You are a helpful assistant.";
    }
    std::string user_message = "Context: " + escape_json_string(context) +
                              "\\n\\nQuestion: " + escape_json_string(prompt);

    std::string json_payload = "{"
        "\"model\":\"" + model + "\","
        "\"max_tokens\":" + std::to_string(max_tokens) + ",";

    // Add tools array if Brave key is configured
    if (tools_enabled) {
        json_payload += "\"tools\":[{"
            "\"name\":\"web_search\","
            "\"description\":\"Search the web for current information\","
            "\"input_schema\":{"
                "\"type\":\"object\","
                "\"properties\":{"
                    "\"query\":{\"type\":\"string\",\"description\":\"Search query\"}"
                "},"
                "\"required\":[\"query\"]"
            "}"
        "}],";
    }

    json_payload += "\"system\":\"" + system_message + "\","
        "\"messages\":["
            "{\"role\":\"user\",\"content\":\"" + user_message + "\"}"
        "],"
        "\"temperature\":0.7"
    "}";

    // Build headers
    CurlHeaders headers;
    headers.append("Content-Type: application/json");
    headers.append("x-api-key: " + api_key);
    headers.append("anthropic-version: 2023-06-01");

    // Make first API call
    long http_code = 0;
    std::string response = curl_post_json(
        "https://api.anthropic.com/v1/messages",
        json_payload,
        headers,
        &http_code
    );

    // Check for errors
    if (response.find("ERROR:") == 0) {
        return response;
    }

    // Check for API error in response
    std::string error_type = extract_json_content(response, "type");
    if (error_type == "error") {
        std::string error_msg = extract_json_content(response, "message");
        return "ERROR: Anthropic API returned error: " + error_msg;
    }

    // Check stop_reason
    std::string stop_reason = extract_json_content(response, "stop_reason");

    // If no tool use, return the text response
    if (stop_reason != "tool_use" || !tools_enabled) {
        // Extract text from content array: content:[{"type":"text","text":"..."}]
        // First find the content array
        size_t content_pos = response.find("\"content\"");
        if (content_pos != std::string::npos) {
            // Find the text field within content array (search for "text": to avoid matching "type":"text")
            size_t text_start = response.find("\"text\":", content_pos);
            if (text_start != std::string::npos) {
                std::string content = extract_json_content(response.substr(text_start), "text");
                if (!content.empty()) {
                    return content;
                }
            }
        }
        // Fallback: try to find text directly
        std::string content = extract_json_content(response, "text");
        if (content.empty()) {
            return "ERROR: Could not parse response from Anthropic API. Response: " + response.substr(0, 500);
        }
        return content;
    }

    // Tool use detected - extract tool request
    // Find the tool_use block in content array
    size_t tool_use_pos = response.find("\"type\":\"tool_use\"");
    if (tool_use_pos == std::string::npos) {
        // No tool use found, return text if available
        std::string content = extract_json_content(response, "text");
        return content.empty() ? "No response from API" : content;
    }

    // Extract tool_use_id and input
    std::string tool_id = extract_json_content(response.substr(tool_use_pos, 500), "id");
    std::string tool_name = extract_json_content(response.substr(tool_use_pos, 500), "name");

    // Find the input object
    size_t input_start = response.find("\"input\"", tool_use_pos);
    std::string search_query = extract_json_content(response.substr(input_start, 500), "query");

    if (tool_name != "web_search" || search_query.empty()) {
        // Unexpected tool or malformed request
        return "ERROR: Unexpected tool request";
    }

    // Execute the search using configured provider
    std::string search_results = execute_web_search(search_query);

    bool search_failed = search_results.find("ERROR:") == 0 || search_results.find("Search failed") == 0;

    // Build second API request with tool result
    std::string tool_result_content = search_failed
        ? "Search unavailable. Please answer from your knowledge."
        : search_results;

    std::string second_payload = "{"
        "\"model\":\"" + model + "\","
        "\"max_tokens\":" + std::to_string(max_tokens) + ","
        "\"system\":\"" + system_message + "\","
        "\"messages\":["
            "{\"role\":\"user\",\"content\":\"" + user_message + "\"},"
            "{\"role\":\"assistant\",\"content\":["
                "{\"type\":\"tool_use\",\"id\":\"" + tool_id + "\",\"name\":\"web_search\","
                "\"input\":{\"query\":\"" + escape_json_string(search_query) + "\"}}"
            "]},"
            "{\"role\":\"user\",\"content\":["
                "{\"type\":\"tool_result\",\"tool_use_id\":\"" + tool_id + "\","
                "\"content\":\"" + escape_json_string(tool_result_content) + "\""
                + (search_failed ? ",\"is_error\":true" : "") + "}"
            "]}"
        "],"
        "\"temperature\":0.3"
    "}";

    // Make second API call
    response = curl_post_json(
        "https://api.anthropic.com/v1/messages",
        second_payload,
        headers,
        &http_code
    );

    // Check for errors
    if (response.find("ERROR:") == 0) {
        return response;
    }

    error_type = extract_json_content(response, "type");
    if (error_type == "error") {
        std::string error_msg = extract_json_content(response, "message");
        return "ERROR: Anthropic API returned error: " + error_msg;
    }

    // Check for empty content array - this means no response text
    if (response.find("\"content\":[]") != std::string::npos) {
        // Return the search results directly as fallback
        return search_results.empty() ? "No response from API" : search_results;
    }

    // Extract final text response from content array
    size_t content_pos = response.find("\"content\"");
    if (content_pos != std::string::npos) {
        size_t text_start = response.find("\"text\":", content_pos);
        if (text_start != std::string::npos) {
            std::string content = extract_json_content(response.substr(text_start), "text");
            if (!content.empty()) {
                return content;
            }
        }
    }
    // Fallback
    std::string content = extract_json_content(response, "text");
    if (content.empty()) {
        // If we have search results, return them as the response
        if (!search_results.empty() && search_results.find("ERROR:") != 0) {
            return search_results;
        }
        return "ERROR: Could not parse response from Anthropic API. Response: " + response.substr(0, 500);
    }

    return content;
}

// ============================================================================
// DuckDB function wrappers
// ============================================================================

static void StpsAskAIFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &context_vec = args.data[0];
    auto &prompt_vec = args.data[1];

    // Use configured model as default, allow override via parameter
    string model = GetAnthropicModel();
    int max_tokens = 1000;

    if (args.ColumnCount() >= 3 && !FlatVector::IsNull(args.data[2], 0)) {
        string_t model_str = FlatVector::GetData<string_t>(args.data[2])[0];
        model = model_str.GetString();
    }

    if (args.ColumnCount() >= 4 && !FlatVector::IsNull(args.data[3], 0)) {
        max_tokens = FlatVector::GetData<int32_t>(args.data[3])[0];
    }

    for (idx_t i = 0; i < count; i++) {
        if (FlatVector::IsNull(context_vec, i) || FlatVector::IsNull(prompt_vec, i)) {
            FlatVector::SetNull(result, i, true);
            continue;
        }

        string_t context_str = FlatVector::GetData<string_t>(context_vec)[i];
        string_t prompt_str = FlatVector::GetData<string_t>(prompt_vec)[i];

        std::string context = context_str.GetString();
        std::string prompt = prompt_str.GetString();

        std::string response = call_anthropic_api(context, prompt, model, max_tokens);

        FlatVector::GetData<string_t>(result)[i] = StringVector::AddString(result, response);
    }
}

static void StpsSetApiKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &key_vec = args.data[0];

    if (!FlatVector::IsNull(key_vec, 0)) {
        string_t key_str = FlatVector::GetData<string_t>(key_vec)[0];
        std::string key = key_str.GetString();
        SetAnthropicApiKey(key);

        FlatVector::GetData<string_t>(result)[0] = StringVector::AddString(result, "API key configured successfully");
        FlatVector::SetNull(result, 0, false);
    } else {
        FlatVector::GetData<string_t>(result)[0] = StringVector::AddString(result, "ERROR: API key cannot be NULL");
        FlatVector::SetNull(result, 0, false);
    }
}

static void StpsSetModelFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &model_vec = args.data[0];

    if (!FlatVector::IsNull(model_vec, 0)) {
        string_t model_str = FlatVector::GetData<string_t>(model_vec)[0];
        std::string model = model_str.GetString();
        SetAnthropicModel(model);

        std::string msg = "Model set to: " + model;
        FlatVector::GetData<string_t>(result)[0] = StringVector::AddString(result, msg);
        FlatVector::SetNull(result, 0, false);
    } else {
        FlatVector::GetData<string_t>(result)[0] = StringVector::AddString(result, "ERROR: Model cannot be NULL");
        FlatVector::SetNull(result, 0, false);
    }
}

static void StpsSetBraveApiKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &key_vec = args.data[0];

    if (!FlatVector::IsNull(key_vec, 0)) {
        string_t key_str = FlatVector::GetData<string_t>(key_vec)[0];
        std::string key = key_str.GetString();
        SetBraveApiKey(key);

        FlatVector::GetData<string_t>(result)[0] = StringVector::AddString(result, "Brave API key configured successfully");
        FlatVector::SetNull(result, 0, false);
    } else {
        FlatVector::GetData<string_t>(result)[0] = StringVector::AddString(result, "ERROR: API key cannot be NULL");
        FlatVector::SetNull(result, 0, false);
    }
}

static void StpsGetModelFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    std::string model = GetAnthropicModel();
    FlatVector::GetData<string_t>(result)[0] = StringVector::AddString(result, model);
    FlatVector::SetNull(result, 0, false);
}

static void StpsSetGoogleApiKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &key_vec = args.data[0];

    if (!FlatVector::IsNull(key_vec, 0)) {
        string_t key_str = FlatVector::GetData<string_t>(key_vec)[0];
        std::string key = key_str.GetString();
        SetGoogleApiKey(key);

        FlatVector::GetData<string_t>(result)[0] = StringVector::AddString(result, "Google API key configured successfully");
        FlatVector::SetNull(result, 0, false);
    } else {
        FlatVector::GetData<string_t>(result)[0] = StringVector::AddString(result, "ERROR: API key cannot be NULL");
        FlatVector::SetNull(result, 0, false);
    }
}

static void StpsSetGoogleCseIdFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &id_vec = args.data[0];

    if (!FlatVector::IsNull(id_vec, 0)) {
        string_t id_str = FlatVector::GetData<string_t>(id_vec)[0];
        std::string id = id_str.GetString();
        SetGoogleCseId(id);

        FlatVector::GetData<string_t>(result)[0] = StringVector::AddString(result, "Google CSE ID configured successfully");
        FlatVector::SetNull(result, 0, false);
    } else {
        FlatVector::GetData<string_t>(result)[0] = StringVector::AddString(result, "ERROR: CSE ID cannot be NULL");
        FlatVector::SetNull(result, 0, false);
    }
}

static void StpsSetSearchProviderFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &provider_vec = args.data[0];

    if (!FlatVector::IsNull(provider_vec, 0)) {
        string_t provider_str = FlatVector::GetData<string_t>(provider_vec)[0];
        std::string provider = provider_str.GetString();
        SetSearchProvider(provider);

        std::string current = GetSearchProvider();
        std::string msg = "Search provider set to: " + current;
        FlatVector::GetData<string_t>(result)[0] = StringVector::AddString(result, msg);
        FlatVector::SetNull(result, 0, false);
    } else {
        FlatVector::GetData<string_t>(result)[0] = StringVector::AddString(result, "ERROR: Provider cannot be NULL (use 'brave' or 'google')");
        FlatVector::SetNull(result, 0, false);
    }
}

static void StpsGetSearchProviderFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    std::string provider = GetSearchProvider();
    FlatVector::GetData<string_t>(result)[0] = StringVector::AddString(result, provider);
    FlatVector::SetNull(result, 0, false);
}

// ============================================================================
// Address-specific AI function with structured output
// ============================================================================

static void StpsAskAIAddressFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &company_name_vec = args.data[0];

    string model = GetAnthropicModel();
    if (args.ColumnCount() >= 2 && !FlatVector::IsNull(args.data[1], 0)) {
        string_t model_str = FlatVector::GetData<string_t>(args.data[1])[0];
        model = model_str.GetString();
    }

    auto &struct_entries = StructVector::GetEntries(result);
    auto &city_vec = *struct_entries[0];
    auto &postal_code_vec = *struct_entries[1];
    auto &street_name_vec = *struct_entries[2];
    auto &street_nr_vec = *struct_entries[3];

    auto &result_validity = FlatVector::Validity(result);

    for (idx_t i = 0; i < count; i++) {
        if (FlatVector::IsNull(company_name_vec, i)) {
            result_validity.SetInvalid(i);
            continue;
        }

        string_t company_name_str = FlatVector::GetData<string_t>(company_name_vec)[i];
        std::string company_name = company_name_str.GetString();

        std::string city, postal_code, street_name, street_nr;
        bool has_data = false;

        // STEP 1: Try direct Brave Local Search first (faster, no Claude API cost)
        if (!GetBraveApiKey().empty()) {
            BraveLocalAddress local_result = execute_brave_local_search(company_name);
            if (local_result.found) {
                city = local_result.city;
                postal_code = local_result.postal_code;
                street_name = local_result.street_name;
                street_nr = local_result.street_nr;
                has_data = !city.empty() || !postal_code.empty() ||
                           !street_name.empty() || !street_nr.empty();
            }
        }

        // STEP 2: Fall back to Claude AI with web search if direct search failed
        if (!has_data && !GetAnthropicApiKey().empty()) {
            std::string prompt = "Search for the official business address of \"" + company_name +
                "\" in Germany. After finding the address, respond with ONLY this JSON format, no other text:\n"
                "{\"city\":\"<city name>\",\"postal_code\":\"<5 digit PLZ>\",\"street_name\":\"<street name>\",\"street_nr\":\"<house number>\"}";

            std::string response = call_anthropic_api(
                company_name,
                prompt,
                model,
                800,
                ""  // Empty = tools enabled for web search
            );

            if (response.find("ERROR:") != 0 && !response.empty()) {
                // Try JSON extraction first
                city = extract_json_content(response, "city");
                postal_code = extract_json_content(response, "postal_code");
                street_name = extract_json_content(response, "street_name");
                street_nr = extract_json_content(response, "street_nr");

                has_data = !city.empty() || !postal_code.empty() ||
                           !street_name.empty() || !street_nr.empty();

                // If JSON extraction failed, try parsing address from plain text
                if (!has_data) {
                    ParsedAddress parsed = parse_german_address(response);
                    city = parsed.city;
                    postal_code = parsed.postal_code;
                    street_name = parsed.street_name;
                    street_nr = parsed.street_nr;
                    has_data = !city.empty() || !postal_code.empty() ||
                               !street_name.empty() || !street_nr.empty();
                }
            }
        }

        if (!has_data) {
            result_validity.SetInvalid(i);
            continue;
        }

        result_validity.SetValid(i);

        // Helper lambda to set field or null
        auto set_or_null = [&](Vector &vec, const std::string &value) {
            if (value.empty()) {
                FlatVector::SetNull(vec, i, true);
            } else {
                FlatVector::GetData<string_t>(vec)[i] = StringVector::AddString(vec, value);
                FlatVector::SetNull(vec, i, false);
            }
        };

        set_or_null(city_vec, city);
        set_or_null(postal_code_vec, postal_code);
        set_or_null(street_name_vec, street_name);
        set_or_null(street_nr_vec, street_nr);
    }
}

// ============================================================================
// Gender detection from first name
// ============================================================================

static void StpsAskAIGenderFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &name_vec = args.data[0];

    string model = GetAnthropicModel();
    if (args.ColumnCount() >= 2 && !FlatVector::IsNull(args.data[1], 0)) {
        string_t model_str = FlatVector::GetData<string_t>(args.data[1])[0];
        model = model_str.GetString();
    }

    // System message for gender detection - no web search needed
    const std::string system_message =
        "You are a gender classification assistant. Given a first name, determine the most likely gender. "
        "Respond with ONLY one of these values: 'male', 'female', or 'unknown'. "
        "Do not include any explanation or additional text.";

    for (idx_t i = 0; i < count; i++) {
        if (FlatVector::IsNull(name_vec, i)) {
            FlatVector::SetNull(result, i, true);
            continue;
        }

        string_t name_str = FlatVector::GetData<string_t>(name_vec)[i];
        std::string name = name_str.GetString();

        // Trim the name
        size_t start = name.find_first_not_of(" \t\n\r");
        size_t end = name.find_last_not_of(" \t\n\r");
        if (start == std::string::npos) {
            FlatVector::SetNull(result, i, true);
            continue;
        }
        name = name.substr(start, end - start + 1);

        // Extract first name only (in case full name is provided)
        size_t space_pos = name.find(' ');
        if (space_pos != std::string::npos) {
            name = name.substr(0, space_pos);
        }

        std::string prompt = "What is the gender of the first name: " + name;

        // Call API with custom system message (no web search needed for names)
        std::string response = call_anthropic_api(
            name,
            prompt,
            model,
            50,  // Very short response expected
            system_message
        );

        if (response.find("ERROR:") == 0 || response.empty()) {
            FlatVector::GetData<string_t>(result)[i] = StringVector::AddString(result, "unknown");
            FlatVector::SetNull(result, i, false);
            continue;
        }

        // Normalize response to lowercase and extract gender
        std::string lower_response = response;
        for (auto& c : lower_response) c = std::tolower(static_cast<unsigned char>(c));

        std::string gender = "unknown";
        if (lower_response.find("female") != std::string::npos ||
            lower_response.find("weiblich") != std::string::npos ||
            lower_response.find("woman") != std::string::npos ||
            lower_response.find("girl") != std::string::npos) {
            gender = "female";
        } else if (lower_response.find("male") != std::string::npos ||
                   lower_response.find("männlich") != std::string::npos ||
                   lower_response.find("man") != std::string::npos ||
                   lower_response.find("boy") != std::string::npos) {
            // Check it's not "female" (which contains "male")
            if (lower_response.find("female") == std::string::npos) {
                gender = "male";
            }
        }

        FlatVector::GetData<string_t>(result)[i] = StringVector::AddString(result, gender);
        FlatVector::SetNull(result, i, false);
    }
}

void RegisterAIFunctions(ExtensionLoader& loader) {
    // Register stps_ask_ai function
    ScalarFunctionSet ask_ai_set("stps_ask_ai");

    // stps_ask_ai(context VARCHAR, prompt VARCHAR) -> VARCHAR
    ask_ai_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        StpsAskAIFunction
    ));

    // stps_ask_ai(context VARCHAR, prompt VARCHAR, model VARCHAR) -> VARCHAR
    ask_ai_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        StpsAskAIFunction
    ));

    // stps_ask_ai(context VARCHAR, prompt VARCHAR, model VARCHAR, max_tokens INTEGER) -> VARCHAR
    ask_ai_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER},
        LogicalType::VARCHAR,
        StpsAskAIFunction
    ));

    loader.RegisterFunction(ask_ai_set);

    // Register stps_ask_ai_address function for structured address output
    // Define STRUCT return type with address fields
    child_list_t<LogicalType> address_struct_children;
    address_struct_children.push_back(make_pair("city", LogicalType::VARCHAR));
    address_struct_children.push_back(make_pair("postal_code", LogicalType::VARCHAR));
    address_struct_children.push_back(make_pair("street_name", LogicalType::VARCHAR));
    address_struct_children.push_back(make_pair("street_nr", LogicalType::VARCHAR));
    auto address_return_type = LogicalType::STRUCT(std::move(address_struct_children));

    ScalarFunctionSet ask_ai_address_set("stps_ask_ai_address");

    // stps_ask_ai_address(company_name VARCHAR) -> STRUCT(...)
    ask_ai_address_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR},
        address_return_type,
        StpsAskAIAddressFunction
    ));

    // stps_ask_ai_address(company_name VARCHAR, model VARCHAR) -> STRUCT(...)
    ask_ai_address_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR},
        address_return_type,
        StpsAskAIAddressFunction
    ));

    loader.RegisterFunction(ask_ai_address_set);

    // Register stps_set_api_key function
    ScalarFunctionSet set_api_key_set("stps_set_api_key");
    set_api_key_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        StpsSetApiKeyFunction
    ));
    loader.RegisterFunction(set_api_key_set);

    // Register stps_set_model function
    ScalarFunctionSet set_model_set("stps_set_model");
    set_model_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        StpsSetModelFunction
    ));
    loader.RegisterFunction(set_model_set);

    // Register stps_get_model function
    ScalarFunctionSet get_model_set("stps_get_model");
    get_model_set.AddFunction(ScalarFunction(
        {},
        LogicalType::VARCHAR,
        StpsGetModelFunction
    ));
    loader.RegisterFunction(get_model_set);

    // Register stps_set_brave_api_key function
    ScalarFunctionSet set_brave_key_set("stps_set_brave_api_key");
    set_brave_key_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        StpsSetBraveApiKeyFunction
    ));
    loader.RegisterFunction(set_brave_key_set);

    // Register stps_set_google_api_key function
    ScalarFunctionSet set_google_key_set("stps_set_google_api_key");
    set_google_key_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        StpsSetGoogleApiKeyFunction
    ));
    loader.RegisterFunction(set_google_key_set);

    // Register stps_set_google_cse_id function
    ScalarFunctionSet set_google_cse_set("stps_set_google_cse_id");
    set_google_cse_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        StpsSetGoogleCseIdFunction
    ));
    loader.RegisterFunction(set_google_cse_set);

    // Register stps_set_search_provider function
    ScalarFunctionSet set_search_provider_set("stps_set_search_provider");
    set_search_provider_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        StpsSetSearchProviderFunction
    ));
    loader.RegisterFunction(set_search_provider_set);

    // Register stps_get_search_provider function
    ScalarFunctionSet get_search_provider_set("stps_get_search_provider");
    get_search_provider_set.AddFunction(ScalarFunction(
        {},
        LogicalType::VARCHAR,
        StpsGetSearchProviderFunction
    ));
    loader.RegisterFunction(get_search_provider_set);

    // Register stps_ask_ai_gender function
    ScalarFunctionSet ask_ai_gender_set("stps_ask_ai_gender");

    // stps_ask_ai_gender(name VARCHAR) -> VARCHAR ('male', 'female', 'unknown')
    ask_ai_gender_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        StpsAskAIGenderFunction
    ));

    // stps_ask_ai_gender(name VARCHAR, model VARCHAR) -> VARCHAR
    ask_ai_gender_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        StpsAskAIGenderFunction
    ));

    loader.RegisterFunction(ask_ai_gender_set);
}

} // namespace stps
} // namespace duckdb
