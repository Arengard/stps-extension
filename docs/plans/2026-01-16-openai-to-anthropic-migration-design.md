# OpenAI to Anthropic Migration Design

## Overview

Complete replacement of OpenAI integration with Anthropic Claude API in the stps-extension AI functions. This migration maintains the same DuckDB function interface while switching to Claude models and using libcurl for more robust HTTP handling.

## Architecture & Overall Approach

### API Endpoint & Authentication
- **Endpoint**: `https://api.openai.com/v1/chat/completions` → `https://api.anthropic.com/v1/messages`
- **Authentication**: `Authorization: Bearer <key>` → `x-api-key: <key>` header
- **Required Headers**: Add `anthropic-version: 2023-06-01`

### Model Configuration
- **Default model**: `gpt-4o-mini` → `claude-3-5-sonnet-20241022`
- Keep existing model selection functions (stps_set_model/stps_get_model)
- Remove OpenAI-specific token parameter logic (max_tokens vs max_completion_tokens)
- Anthropic always uses `max_tokens` as a required parameter

### Request/Response Format

**OpenAI Request**:
```json
{
  "model": "gpt-4o-mini",
  "messages": [
    {"role": "system", "content": "You are a helpful assistant."},
    {"role": "user", "content": "Context: ...\n\nQuestion: ..."}
  ],
  "max_tokens": 1000,
  "temperature": 0.7
}
```

**Anthropic Request**:
```json
{
  "model": "claude-3-5-sonnet-20241022",
  "max_tokens": 1000,
  "system": "You are a helpful assistant.",
  "messages": [
    {"role": "user", "content": "Context: ...\n\nQuestion: ..."}
  ],
  "temperature": 0.7
}
```

**Response Parsing**:
- OpenAI: `choices[0].message.content`
- Anthropic: `content[0].text`

### Naming & Branding
- Variables: `openai_api_key` → `anthropic_api_key`, `openai_model` → `anthropic_model`
- Config file: `~/.stps/openai_api_key` → `~/.stps/anthropic_api_key`
- Environment: `OPENAI_API_KEY` → `ANTHROPIC_API_KEY`
- DuckDB function names remain unchanged (stps_ask_ai, stps_set_api_key, etc.) to avoid breaking SQL queries

## HTTP Client Integration with libcurl

### Dependency Management
- Add libcurl to build dependencies in CMakeLists.txt
- Link against libcurl: `target_link_libraries(stps_extension PRIVATE CURL::libcurl)`
- Include `<curl/curl.h>` header

### Implementation Pattern
- Initialize curl handle with `curl_easy_init()`
- Use callback function to capture response data into string buffer
- Set curl options: URL, headers (curl_slist), POST data, write callback
- Execute with `curl_easy_perform()`
- Check for errors and clean up resources
- Replace all `system()` curl calls

### Error Handling Improvements
- Capture HTTP status codes (200 for success, 4xx/5xx for errors)
- Distinguish between network errors and API errors
- Return specific error messages (connection failed vs API rate limit vs invalid key)
- Handle timeouts gracefully

### Memory Management
- Create helper functions to manage curl handle lifecycle
- Ensure proper cleanup even on error paths
- Reuse curl handles where beneficial
- Handle response data in memory (eliminate temp files for API calls)

### Cross-Platform Benefits
- Remove Windows-specific `_popen` vs Unix `popen` conditionals
- libcurl works cross-platform without platform-specific code
- Keep temp file helpers only if needed for debugging

## Function-Level Changes

### Global Configuration (ai_functions.cpp:29-46)
```cpp
// Before
static std::string openai_api_key;
static std::string openai_model = "gpt-4o-mini";

// After
static std::string anthropic_api_key;
static std::string anthropic_model = "claude-3-5-sonnet-20241022";
```

Rename all related functions:
- `SetOpenAIApiKey()` → `SetAnthropicApiKey()`
- `GetOpenAIApiKey()` → `GetAnthropicApiKey()`
- `SetOpenAIModel()` → `SetAnthropicModel()`
- `GetOpenAIModel()` → `GetAnthropicModel()`

### API Key Retrieval (ai_functions.cpp:48-87)
- Change environment variable: `OPENAI_API_KEY` → `ANTHROPIC_API_KEY`
- Change file path: `~/.stps/openai_api_key` → `~/.stps/anthropic_api_key`
- Keep three-tier fallback logic: function call → environment → config file

### Core API Call Function (ai_functions.cpp:239-326)
- Rename: `call_openai_api()` → `call_anthropic_api()`
- Remove conditional logic for max_completion_tokens (lines 247-253)
- Rewrite JSON payload construction for Anthropic format (system as separate field)
- Replace system curl calls with libcurl implementation
- Update response parsing to extract `content[0].text`
- Update all error messages to reference Anthropic

### libcurl Implementation Details

**Write Callback Function**:
```cpp
static size_t write_callback(void* contents, size_t size, size_t nmemb, std::string* userp) {
    size_t total_size = size * nmemb;
    userp->append((char*)contents, total_size);
    return total_size;
}
```

**API Call Structure**:
1. Initialize curl handle
2. Build headers list (x-api-key, anthropic-version, content-type)
3. Set URL, POST data, headers, write callback
4. Perform request
5. Check HTTP status code
6. Parse response
7. Cleanup curl handle and headers

### Address-Specific Function (ai_functions.cpp:411-503)
- Update function to use `call_anthropic_api()`
- Modify prompt at line 441: Remove "USE WEB SEARCH" instruction
- Claude doesn't have built-in web search; update prompt to be realistic about capabilities
- Consider updating prompt to: "Find the registered business address for this company based on your knowledge. Only return information you are confident about."
- Keep struct return type and parsing logic unchanged

### Utility Functions
- `escape_json_string()` - Keep unchanged, still needed
- `extract_json_content()` - Update to handle Anthropic's response structure
- `get_temp_filename()` - Can remove if going fully in-memory with libcurl
- `read_file_content()` - Can remove if going fully in-memory with libcurl

## Build Configuration

### CMakeLists.txt Updates
```cmake
find_package(CURL REQUIRED)
target_link_libraries(stps_extension PRIVATE CURL::libcurl)
```

### Platform-Specific Setup
- **macOS**: Pre-installed or via Homebrew (`brew install curl`)
- **Linux**: Package manager (`apt-get install libcurl4-openssl-dev` or similar)
- **Windows**: May need to specify curl library path or bundle

### Documentation Updates
- Update `AI_FUNCTIONS_GUIDE.md` with Claude model examples
- Update `README.md` to reference Anthropic instead of OpenAI
- Update configuration instructions for API keys
- Include example model names:
  - `claude-3-5-sonnet-20241022` (default, recommended)
  - `claude-3-5-haiku-20241022` (fast, cost-effective)
  - `claude-opus-4-5-20251101` (most capable)

## Testing & Migration Strategy

### Testing Checklist
- [ ] API key configuration via `stps_set_api_key()`
- [ ] API key configuration via `ANTHROPIC_API_KEY` environment variable
- [ ] API key configuration via `~/.stps/anthropic_api_key` file
- [ ] Model switching with `stps_set_model()` and `stps_get_model()`
- [ ] `stps_ask_ai()` with various context/prompt combinations
- [ ] `stps_ask_ai_address()` struct return values
- [ ] Error handling: invalid API key
- [ ] Error handling: network failures
- [ ] Error handling: invalid model names
- [ ] Error handling: rate limits
- [ ] Cross-platform builds (Linux, macOS, Windows)

### Migration Considerations
- **Breaking change**: Users must replace OpenAI API keys with Anthropic API keys
- **Model names**: Users with hardcoded model names in SQL must update them
- **Function names**: SQL queries won't break - function names remain the same
- **Address function**: Results may differ since Claude uses training data instead of web search

### Address Function Behavior Change
Current prompt instructs: "USE WEB SEARCH to find the registered business address"

Claude models don't have built-in web search. Options:
1. **Simple fix** (recommended for v1): Update prompt to use training data, remove web search references
2. **Tool-use pattern**: Integrate Anthropic's tool-use API with actual web search tool
3. **Hybrid approach**: Document limitation and suggest external web search integration

For initial migration, recommend option 1 with clear documentation about the limitation.

### Default Parameters
- Keep `max_tokens` default at 1000 for general queries
- Address function uses 250 tokens - appropriate for structured output
- All Anthropic requests require `max_tokens` (cannot be omitted)

## Implementation Phases

### Phase 1: Core API Migration
1. Add libcurl dependency to build system
2. Rename all OpenAI-related variables and functions to Anthropic
3. Implement libcurl wrapper with write callback
4. Update `call_anthropic_api()` with new request format
5. Update response parsing for Anthropic format
6. Update error handling

### Phase 2: Function Updates
1. Update all wrapper functions to use new API
2. Update address function prompt (remove web search references)
3. Update default model to Claude 3.5 Sonnet
4. Test all DuckDB function variants

### Phase 3: Documentation & Testing
1. Update README.md and AI_FUNCTIONS_GUIDE.md
2. Add example queries with Claude models
3. Run comprehensive test suite
4. Test cross-platform builds

## Files to Modify

1. `src/ai_functions.cpp` - Main implementation (complete rewrite of API calls)
2. `src/include/ai_functions.hpp` - Update function declarations
3. `CMakeLists.txt` or equivalent - Add libcurl dependency
4. `README.md` - Update documentation
5. `AI_FUNCTIONS_GUIDE.md` - Update examples and model names

## Success Criteria

- [ ] All DuckDB AI functions work with Anthropic API
- [ ] API key configuration works in all three modes
- [ ] Model selection functions work correctly
- [ ] Error handling is robust and informative
- [ ] Cross-platform builds succeed
- [ ] Documentation is updated and accurate
- [ ] No OpenAI code or references remain in codebase
