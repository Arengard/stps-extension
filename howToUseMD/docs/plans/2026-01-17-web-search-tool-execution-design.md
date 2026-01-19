# Web Search Tool Execution for stps_ask_ai

**Date:** 2026-01-17
**Status:** Approved
**Objective:** Enable Claude to execute web searches from SQL queries via stps_ask_ai()

## Overview

Extend `stps_ask_ai` to handle tool execution, specifically web search via Brave Search API. When Claude determines it needs current information, it will automatically search the web and incorporate results into its answer.

**Key Constraints:**
- Single tool call limit (max 1 search per query) for predictable cost/latency
- Transparent to users (no API changes)
- Graceful degradation when Brave API key not configured

## Architecture

### Core Concept

The tool execution flow adds a conditional second API call to Claude:

```
SQL Query → call_anthropic_api() with tools=[web_search] → Claude response
                ↓ (if stop_reason = "tool_use")
         Extract web_search request
                ↓
         Execute via Brave Search API
                ↓
         Send tool result back to Claude
                ↓
         Return final text answer to SQL
```

### Configuration (3 Methods)

Following the exact pattern as Anthropic API key:

**1. SQL Function**
```sql
SELECT stps_set_brave_api_key('BSA...your-key-here');
```

**2. Environment Variable**
```bash
export BRAVE_API_KEY='BSA...your-key-here'
```

**3. Config File**
```bash
echo "BSA...your-key-here" > ~/.stps/brave_api_key
```

## Implementation Details

### 1. Tool Definition in Request

Add to Claude API request payload when Brave key is configured:

```json
{
  "model": "claude-sonnet-4-5-20250929",
  "max_tokens": 1000,
  "tools": [{
    "name": "web_search",
    "description": "Search the web for current information",
    "input_schema": {
      "type": "object",
      "properties": {
        "query": {
          "type": "string",
          "description": "Search query"
        }
      },
      "required": ["query"]
    }
  }],
  "messages": [...]
}
```

### 2. Response Handling

After first Claude API call, check `stop_reason`:

- `"end_turn"` → Extract text content, return immediately (no tool use)
- `"tool_use"` → Extract tool request, execute search, make second API call
- `"max_tokens"` → Return partial response (edge case)

### 3. Brave Search Execution

**New function:** `execute_brave_search(const std::string& query)`

```cpp
std::string execute_brave_search(const std::string& query) {
    std::string api_key = GetBraveApiKey();
    if (api_key.empty()) {
        return "ERROR: Brave API key not configured";
    }

    // URL encode query
    std::string encoded_query = url_encode(query);
    std::string url = "https://api.search.brave.com/res/v1/web/search?q=" + encoded_query;

    // Build headers
    CurlHeaders headers;
    headers.append("Accept: application/json");
    headers.append("X-Subscription-Token: " + api_key);

    // Make GET request (need new curl_get function)
    long http_code = 0;
    std::string response = curl_get(url, headers, &http_code);

    if (http_code != 200) {
        return "ERROR: Search failed";
    }

    return format_search_results(response);
}
```

**New function:** `format_search_results(const std::string& json)`

Parse Brave JSON and extract:
- Top 5 web results
- For each: title, URL, description (truncate to ~200 chars)
- Format as readable text for Claude

Example output:
```
Search Results:

1. Bitcoin Price Today - $95,234 USD
   https://coinmarketcap.com/...
   Current Bitcoin price is $95,234.15, up 2.3% in the last 24 hours...

2. BTC Live Price Chart
   https://www.coingecko.com/...
   Real-time Bitcoin trading at $95,240 with market cap of $1.88T...

[3 more results...]
```

### 4. Second API Call with Tool Result

```json
{
  "model": "claude-sonnet-4-5-20250929",
  "max_tokens": 1000,
  "messages": [
    {
      "role": "user",
      "content": "Context: Bitcoin\n\nQuestion: What is the current price?"
    },
    {
      "role": "assistant",
      "content": [
        {
          "type": "tool_use",
          "id": "toolu_01ABC123",
          "name": "web_search",
          "input": {"query": "Bitcoin price USD"}
        }
      ]
    },
    {
      "role": "user",
      "content": [
        {
          "type": "tool_result",
          "tool_use_id": "toolu_01ABC123",
          "content": "[Formatted search results here]"
        }
      ]
    }
  ]
}
```

### 5. New Global State & Functions

Add to `ai_functions.cpp`:

```cpp
// Global storage (near line 28-30)
static std::string brave_api_key;

// Key management functions
std::string GetBraveApiKey();  // Mirrors GetAnthropicApiKey() logic
void SetBraveApiKey(const std::string& key);

// Tool execution functions
std::string execute_brave_search(const std::string& query);
std::string format_search_results(const std::string& json);
std::string url_encode(const std::string& str);

// DuckDB wrapper
static void StpsSetBraveApiKeyFunction(DataChunk &args, ExpressionState &state, Vector &result);
```

### 6. Curl Utilities

Add to `curl_utils.cpp`:

```cpp
std::string curl_get(const std::string& url,
                     const CurlHeaders& headers,
                     long* http_code_out);
```

Currently only `curl_post_json` exists.

## Error Handling

### 1. Missing Brave API Key
- Skip tool definition in request
- Claude proceeds without web search capability
- Functions normally (current behavior)

### 2. Brave API Failures
```cpp
// Network error, auth failure, rate limit
tool_result = {
    "type": "tool_result",
    "tool_use_id": tool_id,
    "content": "Search unavailable. Please answer from your knowledge.",
    "is_error": true
}
```
Send error result to Claude, let it answer without search data.

### 3. Malformed Tool Requests
- Unknown tool name: Ignore, return text response
- Missing query parameter: Send error tool_result

### 4. Timeout Protection
- Current total timeout: 90s
- Budget: 30s Claude + 10s Brave + 30s Claude = 70s
- If Brave exceeds 10s: Cancel, send error tool_result

### 5. Large Search Results
- Limit to top 5 results
- Truncate each description to ~200 chars
- Max total: ~1500 tokens

## Cost Implications

**Before tool support:**
- 1 Claude API call per query

**After tool support (when tool used):**
- 2 Claude API calls + 1 Brave search
- Claude cost: ~2x (two API calls instead of one)
- Brave cost: $0.003 per search (after free tier)

**Recommendation:** Document clearly in README that queries using web search will cost approximately 2x.

## Behavior Matrix

| Brave Key | Needs Current Info | Result |
|-----------|-------------------|--------|
| ✅ Yes | ✅ Yes | Web search executed, current answer |
| ✅ Yes | ❌ No | Claude answers from knowledge, no search |
| ❌ No | ✅ Yes | Claude answers from training (might be outdated) |
| ❌ No | ❌ No | Claude answers from knowledge |

## Files to Modify

1. **src/ai_functions.cpp** - Core implementation
2. **src/ai_functions.hpp** - Function declarations
3. **src/curl_utils.cpp** - Add curl_get function
4. **src/curl_utils.hpp** - Add curl_get declaration
5. **README.md** - Document Brave API key setup
6. **AI_FUNCTIONS_GUIDE.md** - Examples and best practices

## Testing Strategy

**Manual testing:**
```sql
-- 1. Configure keys
SELECT stps_set_api_key('sk-ant-...');
SELECT stps_set_brave_api_key('BSA...');

-- 2. Test current info query
SELECT stps_ask_ai('Bitcoin', 'What is the current price?');
-- Should return actual current price from web

-- 3. Test knowledge query
SELECT stps_ask_ai('Python', 'What is a list comprehension?');
-- Should answer from knowledge without searching

-- 4. Test without Brave key
-- Unset BRAVE_API_KEY, restart
SELECT stps_ask_ai('Bitcoin', 'What is the current price?');
-- Should still work, answer from training data

-- 5. Test batch queries
SELECT
    ticker,
    stps_ask_ai(ticker, 'Current stock price?') as price
FROM stocks
LIMIT 5;
```

## Success Criteria

- ✅ Existing queries work unchanged
- ✅ Brave key configuration mirrors Anthropic key
- ✅ Web searches execute when Claude determines it's needed
- ✅ Graceful degradation without Brave key
- ✅ No breaking changes to current API
- ✅ Clear documentation with examples
- ✅ Total query time < 10 seconds for tool-using queries

## Future Enhancements (Out of Scope)

- Support multiple tool calls per query (agentic behavior)
- Support additional tools (bash, file operations)
- Pluggable search providers (Google, Bing, DuckDuckGo)
- Caching of search results
- Query cost tracking/reporting
