#include "ai_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <fstream>
#include <sstream>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include <cctype>

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
// Global API key storage
// ============================================================================

static std::string openai_api_key;

void SetOpenAIApiKey(const std::string& key) {
    openai_api_key = key;
}

std::string GetOpenAIApiKey() {
    // Check if key was set via stps_set_api_key()
    if (!openai_api_key.empty()) {
        return openai_api_key;
    }

    // Check environment variable OPENAI_API_KEY
    const char* env_key = std::getenv("OPENAI_API_KEY");
    if (env_key != nullptr) {
        return std::string(env_key);
    }

    // Check ~/.stps/openai_api_key file
    const char* home = std::getenv("HOME");
    if (!home) {
#ifdef _WIN32
        home = std::getenv("USERPROFILE");
#endif
    }

    if (home) {
        std::string key_file = std::string(home) + "/.stps/openai_api_key";
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
    // Find "key":"value" or "key": "value" pattern
    std::string search = "\"" + key + "\"";
    size_t pos = json.find(search);
    if (pos == std::string::npos) {
        return "";
    }

    pos += search.length();

    // Skip to colon
    size_t colon = json.find(':', pos);
    if (colon == std::string::npos) {
        return "";
    }

    pos = colon + 1;

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

    std::string result = json.substr(pos, end - pos);

    // Unescape basic sequences
    size_t esc_pos = 0;
    while ((esc_pos = result.find("\\n", esc_pos)) != std::string::npos) {
        result.replace(esc_pos, 2, "\n");
        esc_pos += 1;
    }
    esc_pos = 0;
    while ((esc_pos = result.find("\\t", esc_pos)) != std::string::npos) {
        result.replace(esc_pos, 2, "\t");
        esc_pos += 1;
    }
    esc_pos = 0;
    while ((esc_pos = result.find("\\\"", esc_pos)) != std::string::npos) {
        result.replace(esc_pos, 2, "\"");
        esc_pos += 1;
    }
    esc_pos = 0;
    while ((esc_pos = result.find("\\\\", esc_pos)) != std::string::npos) {
        result.replace(esc_pos, 2, "\\");
        esc_pos += 1;
    }

    return result;
}

static std::string get_temp_filename() {
#ifdef _WIN32
    char temp_path[MAX_PATH];
    char temp_file[MAX_PATH];
    GetTempPathA(MAX_PATH, temp_path);
    GetTempFileNameA(temp_path, "stps", 0, temp_file);
    return std::string(temp_file);
#else
    char temp_file[] = "/tmp/stps_ai_XXXXXX";
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

// ============================================================================
// OpenAI API call
// ============================================================================

static std::string call_openai_api(const std::string& context, const std::string& prompt,
                                   const std::string& model, int max_tokens) {
    std::string api_key = GetOpenAIApiKey();

    if (api_key.empty()) {
        return "ERROR: OpenAI API key not configured. Use stps_set_api_key() or set OPENAI_API_KEY environment variable.";
    }

    // Build JSON request payload
    std::string json_payload = "{"
        "\"model\":\"" + model + "\","
        "\"messages\":["
            "{\"role\":\"system\",\"content\":\"You are a helpful assistant.\"},"
            "{\"role\":\"user\",\"content\":\"Context: " + escape_json_string(context) + "\\n\\nQuestion: " + escape_json_string(prompt) + "\"}"
        "],"
        "\"max_tokens\":" + std::to_string(max_tokens) + ","
        "\"temperature\":0.7"
    "}";

    // Write payload to temp file
    std::string payload_file = get_temp_filename() + ".json";
    std::ofstream out(payload_file);
    if (!out) {
        return "ERROR: Failed to create temporary file for API request.";
    }
    out << json_payload;
    out.close();

    // Write response to temp file
    std::string response_file = get_temp_filename() + ".json";

    // Build curl command
    std::string cmd = "curl -s -X POST \"https://api.openai.com/v1/chat/completions\" "
                      "-H \"Content-Type: application/json\" "
                      "-H \"Authorization: Bearer " + api_key + "\" "
                      "-d @\"" + payload_file + "\" "
                      "-o \"" + response_file + "\" "
                      "2>/dev/null";

#ifdef _WIN32
    cmd = "curl -s -X POST \"https://api.openai.com/v1/chat/completions\" "
          "-H \"Content-Type: application/json\" "
          "-H \"Authorization: Bearer " + api_key + "\" "
          "-d @\"" + payload_file + "\" "
          "-o \"" + response_file + "\" "
          "2>nul";
#endif

    int result = system(cmd.c_str());

    // Clean up payload file
    std::remove(payload_file.c_str());

    if (result != 0) {
        std::remove(response_file.c_str());
        return "ERROR: Failed to execute curl command. Make sure curl is installed.";
    }

    // Read response
    std::string response = read_file_content(response_file);
    std::remove(response_file.c_str());

    if (response.empty()) {
        return "ERROR: Empty response from OpenAI API.";
    }

    // Check for API errors
    std::string error_msg = extract_json_content(response, "message");
    if (!error_msg.empty() && response.find("\"error\"") != std::string::npos) {
        return "ERROR: OpenAI API returned error: " + error_msg;
    }

    // Extract the assistant's message content
    std::string content = extract_json_content(response, "content");
    if (content.empty()) {
        return "ERROR: Could not parse response from OpenAI API. Response: " + response.substr(0, 200);
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

    // Optional parameters with defaults
    string model = "gpt-5.1";
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

        std::string response = call_openai_api(context, prompt, model, max_tokens);

        FlatVector::GetData<string_t>(result)[i] = StringVector::AddString(result, response);
    }
}

static void StpsSetApiKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &key_vec = args.data[0];

    if (!FlatVector::IsNull(key_vec, 0)) {
        string_t key_str = FlatVector::GetData<string_t>(key_vec)[0];
        std::string key = key_str.GetString();
        SetOpenAIApiKey(key);

        FlatVector::GetData<string_t>(result)[0] = StringVector::AddString(result, "API key configured successfully");
    } else {
        FlatVector::GetData<string_t>(result)[0] = StringVector::AddString(result, "ERROR: API key cannot be NULL");
    }

    result.SetCardinality(1);
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

    // Register stps_set_api_key function
    ScalarFunctionSet set_api_key_set("stps_set_api_key");
    set_api_key_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        StpsSetApiKeyFunction
    ));

    loader.RegisterFunction(set_api_key_set);
}

} // namespace stps
} // namespace duckdb
