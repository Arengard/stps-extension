# Web Search Tool Execution - Implementation Plan

**Related Design:** [2026-01-17-web-search-tool-execution-design.md](2026-01-17-web-search-tool-execution-design.md)
**Estimated Effort:** 4-6 hours
**Dependencies:** Design document approved ✅

## Overview

Step-by-step implementation plan for adding web search tool execution to `stps_ask_ai` and `stps_ask_ai_address`. Each step includes specific code changes with line numbers and validation steps.

**Important:** Since both functions use the same underlying `call_anthropic_api()` function, modifying it in Phase 3 will automatically enable web search for both:
- `stps_ask_ai()` - General AI queries
- `stps_ask_ai_address()` - Structured address lookups

---

## Phase 1: Foundation (HTTP GET + Key Management)

### Step 1.1: Add curl_get Function

**File:** `src/curl_utils.hpp`

**Action:** Add function declaration after curl_post_json (around line 30)

```cpp
// Existing: curl_post_json declaration
std::string curl_post_json(const std::string& url,
                           const std::string& json_payload,
                           const CurlHeaders& headers,
                           long* http_code_out = nullptr);

// ADD THIS:
std::string curl_get(const std::string& url,
                     const CurlHeaders& headers,
                     long* http_code_out = nullptr);
```

---

**File:** `src/curl_utils.cpp`

**Action:** Implement curl_get function after curl_post_json (after line 89)

```cpp
std::string curl_get(const std::string& url,
                     const CurlHeaders& headers,
                     long* http_code_out) {
    CurlHandle handle;
    if (!handle.handle()) {
        return "ERROR: Failed to initialize curl";
    }

    std::string response;

    // Set options
    curl_easy_setopt(handle.handle(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(handle.handle(), CURLOPT_HTTPHEADER, headers.list());
    curl_easy_setopt(handle.handle(), CURLOPT_WRITEFUNCTION, curl_write_callback);
    curl_easy_setopt(handle.handle(), CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(handle.handle(), CURLOPT_TIMEOUT, 30L);  // 30s timeout for searches
    curl_easy_setopt(handle.handle(), CURLOPT_FOLLOWLOCATION, 1L);

    // Perform request
    CURLcode res = curl_easy_perform(handle.handle());

    if (res != CURLE_OK) {
        std::ostringstream err;
        err << "ERROR: curl request failed: " << curl_easy_strerror(res);
        return err.str();
    }

    // Get HTTP status code
    long http_code = 0;
    curl_easy_getinfo(handle.handle(), CURLINFO_RESPONSE_CODE, &http_code);

    if (http_code_out) {
        *http_code_out = http_code;
    }

    if (http_code < 200 || http_code >= 300) {
        std::ostringstream err;
        err << "ERROR: HTTP " << http_code << " - " << response;
        return err.str();
    }

    return response;
}
```

**Validation:**
```bash
# Rebuild extension
make clean && make

# Test in DuckDB (create simple test)
# Will test properly in Phase 3
```

---

### Step 1.2: Add Brave API Key Management

**File:** `src/ai_functions.cpp`

**Action 1:** Add global variable (after line 30, near anthropic_api_key)

```cpp
static std::mutex ai_config_mutex;
static std::string anthropic_api_key;
static std::string anthropic_model = "claude-sonnet-4-5-20250929";
static std::string brave_api_key;  // ADD THIS
```

**Action 2:** Add GetBraveApiKey function (after GetAnthropicApiKey, around line 87)

```cpp
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
```

**Action 3:** Add SetBraveApiKey function (after SetAnthropicModel, around line 40)

```cpp
void SetBraveApiKey(const std::string& key) {
    std::lock_guard<std::mutex> lock(ai_config_mutex);
    brave_api_key = key;
}
```

**Action 4:** Add SQL function wrapper (after StpsSetModelFunction, around line 344)

```cpp
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
```

**Action 5:** Register function (in RegisterAIFunctions, after stps_set_model registration, around line 518)

```cpp
    // Register stps_set_brave_api_key function
    ScalarFunctionSet set_brave_key_set("stps_set_brave_api_key");
    set_brave_key_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        StpsSetBraveApiKeyFunction
    ));
    loader.RegisterFunction(set_brave_key_set);
```

**Validation:**
```sql
-- Rebuild extension, then test in DuckDB
LOAD stps;

-- Test setting key
SELECT stps_set_brave_api_key('test-key-123');
-- Should return: 'Brave API key configured successfully'

-- Verify it doesn't break existing functions
SELECT stps_ask_ai('Test', 'Say hello');
-- Should still work normally
```

---

## Phase 2: Search Execution

### Step 2.1: Add URL Encoding Helper

**File:** `src/ai_functions.cpp`

**Action:** Add url_encode function before execute_brave_search (around line 90, after GetBraveApiKey)

```cpp
static std::string url_encode(const std::string& str) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;

    for (char c : str) {
        // Keep alphanumeric and safe chars
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
        } else if (c == ' ') {
            escaped << '+';
        } else {
            escaped << '%' << std::setw(2) << int((unsigned char)c);
        }
    }

    return escaped.str();
}
```

---

### Step 2.2: Add Search Results Formatter

**File:** `src/ai_functions.cpp`

**Action:** Add format_search_results function after url_encode

```cpp
static std::string format_search_results(const std::string& json) {
    std::ostringstream formatted;
    formatted << "Search Results:\n\n";

    // Simple JSON parsing for Brave Search results
    // Look for "web" -> "results" array

    size_t results_start = json.find("\"results\"");
    if (results_start == std::string::npos) {
        return "No search results found.";
    }

    // Find the array start
    size_t array_start = json.find("[", results_start);
    if (array_start == std::string::npos) {
        return "Error parsing search results.";
    }

    int result_count = 0;
    size_t pos = array_start + 1;

    // Extract up to 5 results
    while (result_count < 5 && pos < json.length()) {
        // Find next result object
        size_t obj_start = json.find("{", pos);
        if (obj_start == std::string::npos || obj_start > json.find("]", pos)) {
            break;
        }

        // Extract title
        std::string title = extract_json_content(json.substr(obj_start, 1000), "title");
        // Extract url
        std::string url = extract_json_content(json.substr(obj_start, 1000), "url");
        // Extract description
        std::string description = extract_json_content(json.substr(obj_start, 1000), "description");

        if (!title.empty() && !url.empty()) {
            result_count++;
            formatted << result_count << ". " << title << "\n";
            formatted << "   " << url << "\n";

            if (!description.empty()) {
                // Truncate description to 200 chars
                if (description.length() > 200) {
                    description = description.substr(0, 197) + "...";
                }
                formatted << "   " << description << "\n";
            }
            formatted << "\n";
        }

        // Move to next result
        pos = json.find("}", obj_start) + 1;
    }

    if (result_count == 0) {
        return "No search results found.";
    }

    return formatted.str();
}
```

---

### Step 2.3: Add Brave Search Execution

**File:** `src/ai_functions.cpp`

**Action:** Add execute_brave_search function after format_search_results

```cpp
static std::string execute_brave_search(const std::string& query) {
    std::string api_key = GetBraveApiKey();

    if (api_key.empty()) {
        return "ERROR: Brave API key not configured";
    }

    // URL encode the query
    std::string encoded_query = url_encode(query);

    // Build Brave Search API URL
    std::string url = "https://api.search.brave.com/res/v1/web/search?q=" + encoded_query;

    // Build headers
    CurlHeaders headers;
    headers.append("Accept: application/json");
    headers.append("X-Subscription-Token: " + api_key);

    // Make GET request
    long http_code = 0;
    std::string response = curl_get(url, headers, &http_code);

    // Check for errors
    if (response.find("ERROR:") == 0) {
        return "Search failed: " + response;
    }

    if (http_code != 200) {
        return "Search API returned error (HTTP " + std::to_string(http_code) + ")";
    }

    // Format the results for Claude
    return format_search_results(response);
}
```

**Validation:**
```sql
-- Can't test directly yet (no SQL wrapper)
-- Will test in Phase 3 when integrated into call_anthropic_api
```

---

## Phase 3: Tool Execution Integration

### Step 3.1: Modify call_anthropic_api for Tool Support

**File:** `src/ai_functions.cpp`

**Action:** Replace call_anthropic_api function (lines 212-271) with tool-aware version

**Current function structure:**
1. Build JSON request
2. Make API call
3. Extract text response
4. Return

**New function structure:**
1. Build JSON request **with tools array if Brave key exists**
2. Make API call
3. Check stop_reason:
   - If "end_turn": Extract text, return
   - If "tool_use": Extract tool request, execute, make second API call
4. Extract final text response
5. Return

**Implementation:**

```cpp
static std::string call_anthropic_api(const std::string& context, const std::string& prompt,
                                      const std::string& model, int max_tokens) {
    std::string api_key = GetAnthropicApiKey();

    if (api_key.empty()) {
        return "ERROR: Anthropic API key not configured. Use stps_set_api_key() or set ANTHROPIC_API_KEY environment variable.";
    }

    // Check if we should enable tools
    bool tools_enabled = !GetBraveApiKey().empty();

    // Build JSON request payload for Anthropic Messages API
    std::string system_message = "You are a helpful assistant.";
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
        "\"temperature\":0.7"
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

    // Extract final text response
    std::string content = extract_json_content(response, "text");
    if (content.empty()) {
        return "ERROR: Could not parse response from Anthropic API. Response: " + response.substr(0, 500);
    }

    return content;
}
```

**Validation:**
```sql
-- Rebuild extension
make clean && make

-- Test without Brave key (should work as before)
LOAD stps;
SELECT stps_set_api_key('sk-ant-...');
SELECT stps_ask_ai('Python', 'What is a list?');
-- Should return normal answer without searching

-- Test with Brave key (should search when needed)
SELECT stps_set_brave_api_key('BSA...');
SELECT stps_ask_ai('Bitcoin', 'What is the current price?');
-- Should search and return current price

-- Test query that doesn't need search
SELECT stps_ask_ai('Python', 'What is a list comprehension?');
-- Should answer from knowledge without searching
```

---

## Phase 4: Documentation

### Step 4.1: Update README.md

**File:** `README.md`

**Action:** Add section after AI Functions intro (around line 300)

```markdown
#### Brave Search Integration

Enable Claude to search the web for current information:

```sql
-- Configure Brave API key (get from https://brave.com/search/api/)
SELECT stps_set_brave_api_key('BSA-your-key-here');

-- Now queries automatically search when needed
SELECT stps_ask_ai('TSLA', 'What is the current stock price?');
-- Claude will search the web and return current price

SELECT stps_ask_ai('Germany', 'Who is the current chancellor?');
-- Searches for up-to-date political information

-- Works with address lookups too
SELECT stps_ask_ai_address('Anthropic PBC');
-- Searches for current Anthropic address, returns structured data
```

**Brave API Key Configuration:**
```sql
-- Option 1: SQL function
SELECT stps_set_brave_api_key('BSA-...');

-- Option 2: Environment variable
-- export BRAVE_API_KEY='BSA-...'

-- Option 3: Config file
-- echo "BSA-..." > ~/.stps/brave_api_key
```

**Cost:** Queries using web search cost ~2x (two Claude API calls + Brave search at $0.003)

**Free Tier:** Brave provides 2,000 searches/month free
```

---

### Step 4.2: Update AI_FUNCTIONS_GUIDE.md

**File:** `AI_FUNCTIONS_GUIDE.md`

**Action:** Add section after Model Selection (around line 70)

```markdown
## Web Search Integration

### Overview

When configured with a Brave Search API key, both `stps_ask_ai` and `stps_ask_ai_address` can automatically search the web for current information.

### Setup

1. Get Brave API key from https://brave.com/search/api/
2. Configure using one of three methods (same as Anthropic key)
3. Queries automatically use search when Claude determines it's needed

### Examples

**Real-time Financial Data:**
```sql
SELECT stps_set_brave_api_key('BSA-...');

SELECT
    ticker,
    stps_ask_ai(ticker, 'Current stock price?') as price
FROM stocks;
```

**Current Events:**
```sql
SELECT stps_ask_ai('Ukraine', 'What is the latest news today?');
```

**Company Information:**
```sql
SELECT
    company_name,
    stps_ask_ai(company_name, 'Latest quarterly revenue?') as revenue
FROM companies;
```

**Address Lookups (Structured Output):**
```sql
-- stps_ask_ai_address also benefits from web search
SELECT
    company,
    (stps_ask_ai_address(company)).city,
    (stps_ask_ai_address(company)).postal_code,
    (stps_ask_ai_address(company)).street_name
FROM new_companies
-- For recently founded companies, Claude will search for current address
```

### Cost Implications

- **Without search:** 1 Claude API call per query
- **With search:** 2 Claude API calls + 1 Brave search
- **Cost:** Approximately 2x Claude cost + $0.003/search
- **Free tier:** 2,000 searches/month from Brave

### When Search is Used

Claude automatically decides when to search based on the query:
- ✅ "Current price of Bitcoin" → Searches
- ✅ "Latest news about X" → Searches
- ✅ "Who is the current CEO of Y" → Searches
- ❌ "What is a database?" → Uses knowledge, no search
- ❌ "Explain Python lists" → Uses knowledge, no search
```

---

## Phase 5: Testing & Validation

### Test Plan

**Test 1: Basic Search**
```sql
SELECT stps_set_api_key('sk-ant-...');
SELECT stps_set_brave_api_key('BSA-...');
SELECT stps_ask_ai('Bitcoin', 'What is the current price in USD?');
```
✅ Expected: Returns actual current Bitcoin price from web

**Test 2: No Search Needed**
```sql
SELECT stps_ask_ai('Python', 'What is a list comprehension?');
```
✅ Expected: Answers from knowledge without searching

**Test 3: Without Brave Key**
```sql
-- Unset BRAVE_API_KEY, restart DuckDB
SELECT stps_ask_ai('Bitcoin', 'What is the current price?');
```
✅ Expected: Returns answer from training data (works but might be outdated)

**Test 4: Batch Queries**
```sql
CREATE TABLE test_tickers AS SELECT * FROM (VALUES ('AAPL'), ('MSFT'), ('GOOGL')) t(ticker);

SELECT
    ticker,
    stps_ask_ai(ticker, 'Current stock price?') as price
FROM test_tickers;
```
✅ Expected: Each row triggers search, returns current prices

**Test 5: Search Failure Handling**
```sql
-- Use invalid Brave API key
SELECT stps_set_brave_api_key('invalid-key');
SELECT stps_ask_ai('Bitcoin', 'Current price?');
```
✅ Expected: Gracefully falls back to knowledge-based answer

**Test 6: Address Function (Basic)**
```sql
SELECT stps_ask_ai_address('Deutsche Bank AG');
```
✅ Expected: Returns structured address (may use web search if needed)

**Test 7: Address Function with Current Company**
```sql
-- Test with recently founded company not in training data
SELECT stps_ask_ai_address('Anthropic PBC');
```
✅ Expected: Searches web for current address, returns structured data

**Test 8: Address Function Batch Processing**
```sql
CREATE TABLE test_companies AS
SELECT * FROM (VALUES ('Apple Inc'), ('Microsoft Corporation'), ('Tesla Inc')) t(company);

SELECT
    company,
    (stps_ask_ai_address(company)).city AS city,
    (stps_ask_ai_address(company)).postal_code AS postal_code,
    (stps_ask_ai_address(company)).street_name AS street
FROM test_companies;
```
✅ Expected: Each company address looked up (with web search if needed), returns structured data

---

## Phase 6: Final Checklist

Before marking complete:

- [ ] All code compiles without warnings
- [ ] All 8 test cases pass
- [ ] Documentation updated (README + AI_FUNCTIONS_GUIDE)
- [ ] Commit messages follow format
- [ ] Design document matches implementation
- [ ] No breaking changes to existing API
- [ ] Error messages are clear and helpful
- [ ] Brave API key configuration mirrors Anthropic pattern exactly

---

## Estimated Timeline

- **Phase 1:** 1 hour (curl_get + key management)
- **Phase 2:** 1.5 hours (search execution + formatting)
- **Phase 3:** 2 hours (tool integration into call_anthropic_api)
- **Phase 4:** 0.5 hours (documentation)
- **Phase 5:** 1 hour (testing)

**Total:** 6 hours

---

## Rollback Plan

If issues are discovered:

1. Revert to commit before implementation
2. The design is preserved in docs/plans/
3. Can resume implementation from any phase
4. No breaking changes, so users unaffected

## Notes

- Start with Phase 1, validate before proceeding
- Each phase is independently testable
- Brave API has rate limits (2 req/sec), but shouldn't be an issue for SQL use case
- Consider adding debug logging for tool execution in future iteration
