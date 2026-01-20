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
// German Address Parser - extracts address components from text
// ============================================================================

struct ParsedAddress {
    std::string city;
    std::string postal_code;
    std::string street_name;
    std::string street_nr;
};

static ParsedAddress parse_german_address(const std::string& text) {
    ParsedAddress addr;

    // Find 5-digit German postal code (PLZ)
    for (size_t i = 0; i + 4 < text.length(); i++) {
        if (std::isdigit(text[i]) && std::isdigit(text[i+1]) &&
            std::isdigit(text[i+2]) && std::isdigit(text[i+3]) &&
            std::isdigit(text[i+4])) {
            // Check it's not part of a longer number
            bool valid = (i == 0 || !std::isdigit(text[i-1])) &&
                        (i + 5 >= text.length() || !std::isdigit(text[i+5]));
            if (valid) {
                addr.postal_code = text.substr(i, 5);

                // City is usually right after the postal code
                size_t city_start = i + 5;
                while (city_start < text.length() && (text[city_start] == ' ' || text[city_start] == ',')) {
                    city_start++;
                }
                size_t city_end = city_start;
                while (city_end < text.length() && text[city_end] != ',' && text[city_end] != '\n' &&
                       text[city_end] != '*' && text[city_end] != ')') {
                    city_end++;
                }
                if (city_end > city_start) {
                    addr.city = text.substr(city_start, city_end - city_start);
                    // Trim trailing spaces and "Germany"
                    while (!addr.city.empty() && (addr.city.back() == ' ' || addr.city.back() == '*')) {
                        addr.city.pop_back();
                    }
                    // Remove " Germany" suffix if present
                    if (addr.city.length() > 8 && addr.city.substr(addr.city.length() - 7) == "Germany") {
                        addr.city = addr.city.substr(0, addr.city.length() - 7);
                        while (!addr.city.empty() && (addr.city.back() == ' ' || addr.city.back() == ',')) {
                            addr.city.pop_back();
                        }
                    }
                }
                break;
            }
        }
    }

    // Find street pattern: word(s) ending in typical German street suffixes followed by number
    // Common patterns: "Straße", "Str.", "straße", "weg", "platz", "allee", "ring", "gasse"
    std::vector<std::string> street_suffixes = {"straße", "strasse", "str.", "weg", "platz", "allee", "ring", "gasse", "damm", "ufer"};

    std::string lower_text = text;
    for (auto& c : lower_text) c = std::tolower(static_cast<unsigned char>(c));

    for (const auto& suffix : street_suffixes) {
        size_t pos = lower_text.find(suffix);
        if (pos != std::string::npos) {
            // Find start of street name (go back to find beginning of word)
            size_t street_start = pos;
            while (street_start > 0 && text[street_start - 1] != '\n' && text[street_start - 1] != ',' &&
                text[street_start - 1] != '*' && text[street_start - 1] != '(') {
                street_start--;
            }
            // Skip leading ** if markdown
            while (street_start < text.length() && text[street_start] == '*') {
                street_start++;
            }

            size_t street_end = pos + suffix.length();
            addr.street_name = text.substr(street_start, street_end - street_start);

            // Find street number after the street name
            size_t nr_start = street_end;
            while (nr_start < text.length() && (text[nr_start] == ' ' || text[nr_start] == '.')) {
                nr_start++;
            }
            size_t nr_end = nr_start;
            while (nr_end < text.length() && (std::isdigit(text[nr_end]) || std::isalpha(text[nr_end]))) {
                // Allow letter suffix like "12a" but stop at comma/space after number
                if (std::isalpha(text[nr_end]) && nr_end > nr_start && !std::isdigit(text[nr_end-1])) {
                    break;
                }
                nr_end++;
            }
            if (nr_end > nr_start) {
                addr.street_nr = text.substr(nr_start, nr_end - nr_start);
                // Trim trailing non-digits except single letter suffix
                while (addr.street_nr.length() > 1 && !std::isdigit(addr.street_nr.back()) &&
                       !std::isdigit(addr.street_nr[addr.street_nr.length()-2])) {
                    addr.street_nr.pop_back();
                }
            }
            break;
        }
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

    // Check if we should enable tools (only if no custom system message - custom means structured task)
    bool tools_enabled = !GetBraveApiKey().empty() && custom_system_message.empty();

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

    // Execute the search
    std::string search_results = execute_brave_search(search_query);

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

        // Include JSON format instructions in the prompt so tools stay enabled
        // (passing empty custom_system_message keeps tools_enabled = true)
        std::string prompt = "Search for the official business address of \"" + company_name +
            "\" in Germany. After finding the address, respond with ONLY this JSON format, no other text:\n"
            "{\"city\":\"<city name>\",\"postal_code\":\"<5 digit PLZ>\",\"street_name\":\"<street name>\",\"street_nr\":\"<house number>\"}";

        // Call API with empty system message to keep web search tools enabled
        std::string response = call_anthropic_api(
            company_name,
            prompt,
            model,
            800,
            ""  // Empty = tools enabled for web search
        );

        if (response.find("ERROR:") == 0 || response.empty()) {
            result_validity.SetInvalid(i);
            continue;
        }

        // Extract structured fields directly from JSON response
        std::string city = extract_json_content(response, "city");
        std::string postal_code = extract_json_content(response, "postal_code");
        std::string street_name = extract_json_content(response, "street_name");
        std::string street_nr = extract_json_content(response, "street_nr");

        bool has_data = !city.empty() || !postal_code.empty() ||
                        !street_name.empty() || !street_nr.empty();

        // Fallback: if JSON extraction failed, try parsing address from plain text
        if (!has_data) {
            ParsedAddress parsed = parse_german_address(response);
            city = parsed.city;
            postal_code = parsed.postal_code;
            street_name = parsed.street_name;
            street_nr = parsed.street_nr;
            has_data = !city.empty() || !postal_code.empty() ||
                       !street_name.empty() || !street_nr.empty();
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
}

} // namespace stps
} // namespace duckdb
