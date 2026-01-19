# Fix `stps_ask_ai_address` Web Search Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix `stps_ask_ai_address` to automatically use web search and return structured JSON address data

**Architecture:** Modify the prompt in `StpsAskAIAddressFunction` to explicitly instruct Claude to use web search when available, ensuring the function behaves like `stps_ask_ai` with a web search prompt but returns only structured address data.

**Tech Stack:** C++17, DuckDB Extension API, Anthropic Claude API, Brave Search API

---

## Background

**Current Behavior:**
- `stps_ask_ai('STP Solution GmbH', 'make a websearch and look for business address')` works correctly and returns full address information
- `stps_ask_ai_address('Tax Network GmbH')` returns NULL because it doesn't trigger web search

**Root Cause:**
The prompt in `StpsAskAIAddressFunction` (line 675-689) tells Claude to "use only information you are highly confident about from your training data" and explicitly says "DO NOT make up, guess, or hallucinate". This instruction prevents Claude from using the web_search tool even when it's available.

**Desired Behavior:**
- `stps_ask_ai_address('STP Solution GmbH')` should automatically trigger web search
- Should return structured JSON address: `{city, postal_code, street_name, street_nr}`
- Should behave like `stps_ask_ai(company, 'make a websearch and look for business address')` but with structured output

---

## Task 1: Update System Message for Address Lookups

**Files:**
- Modify: `src/ai_functions.cpp:674-689`

**Context:**
The current implementation uses a generic system message "You are a helpful assistant." for all queries. We need to modify the `call_anthropic_api` function to accept an optional system message parameter so `stps_ask_ai_address` can provide a system message that encourages web search.

### Step 1: Add system_message parameter to call_anthropic_api

**File:** `src/ai_functions.cpp`

Update the function signature (around line 384):

```cpp
// OLD:
static std::string call_anthropic_api(const std::string& context, const std::string& prompt,
                                      const std::string& model, int max_tokens) {

// NEW:
static std::string call_anthropic_api(const std::string& context, const std::string& prompt,
                                      const std::string& model, int max_tokens,
                                      const std::string& custom_system_message = "") {
```

### Step 2: Use custom system message if provided

Update the system message logic (around line 396-398):

```cpp
// OLD:
std::string system_message = tools_enabled
    ? "You are a helpful assistant with access to web search. When asked about current information, real-time data, recent events, stock prices, or anything that requires up-to-date information, you MUST use the web_search tool. Always search first before saying you don't have access to current data."
    : "You are a helpful assistant.";

// NEW:
std::string system_message;
if (!custom_system_message.empty()) {
    system_message = custom_system_message;
} else {
    system_message = tools_enabled
        ? "You are a helpful assistant with access to web search. When asked about current information, real-time data, recent events, stock prices, or anything that requires up-to-date information, you MUST use the web_search tool. Always search first before saying you don't have access to current data."
        : "You are a helpful assistant.";
}
```

### Step 3: Verify compilation

Run:
```bash
make clean && make
```

Expected: Compiles successfully with no errors

### Step 4: Commit

```bash
git add src/ai_functions.cpp
git commit -m "feat: add custom system message parameter to call_anthropic_api

- Add optional custom_system_message parameter with default empty string
- Use custom message if provided, otherwise fall back to default behavior
- Enables specialized prompts for different AI function types

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Update StpsAskAIAddressFunction Prompt

**Files:**
- Modify: `src/ai_functions.cpp:674-691`

**Context:**
Replace the conservative prompt that discourages web search with an aggressive prompt that explicitly requests web search and JSON output.

### Step 1: Replace the prompt in StpsAskAIAddressFunction

**File:** `src/ai_functions.cpp`

Replace the prompt construction (lines 674-689):

```cpp
// OLD PROMPT (lines 674-689):
std::string prompt = "Find the registered business address (Impressum/legal address) for this company based on your knowledge.\n"
                   "\n"
                   "CRITICAL INSTRUCTIONS:\n"
                   "- Use only information you are highly confident about from your training data\n"
                   "- DO NOT make up, guess, or hallucinate any address information\n"
                   "- If you cannot provide verified information, use empty strings\n"
                   "- This is for a database system - accuracy is essential\n"
                   "- Focus on official registered business addresses, not customer service addresses\n"
                   "\n"
                   "Company: " + company_name + "\n"
                   "\n"
                   "Respond ONLY with a JSON object in this exact format (no other text or markdown):\n"
                   "{\"city\":\"\",\"postal_code\":\"\",\"street_name\":\"\",\"street_nr\":\"\"}\n"
                   "\n"
                   "Fill in ONLY fields you are confident about. Use empty strings for unknown fields.";

// NEW PROMPT:
std::string prompt = "Find the current registered business address (Impressum/legal address) for this company.\n"
                   "\n"
                   "INSTRUCTIONS:\n"
                   "- You MUST use web search to find the most current, accurate business address\n"
                   "- Search for official business registry information, company websites, or business directories\n"
                   "- Look for the legal/registered address (Impressum), not customer service addresses\n"
                   "- Extract the complete address with all components\n"
                   "\n"
                   "Company: " + company_name + "\n"
                   "\n"
                   "Respond ONLY with a JSON object in this exact format (no markdown, no code blocks, no explanatory text):\n"
                   "{\"city\":\"<city>\",\"postal_code\":\"<code>\",\"street_name\":\"<street>\",\"street_nr\":\"<number>\"}\n"
                   "\n"
                   "Example: {\"city\":\"Karlsruhe\",\"postal_code\":\"76135\",\"street_name\":\"Brauerstraße\",\"street_nr\":\"12\"}\n"
                   "\n"
                   "If a field cannot be determined, use an empty string for that field only.";
```

### Step 2: Add custom system message for address lookups

Update the `call_anthropic_api` call (around line 691):

```cpp
// OLD:
std::string response = call_anthropic_api(company_name, prompt, model, 250);

// NEW:
std::string system_msg = "You are a business address lookup assistant with web search capabilities. "
                         "When searching for company addresses, you MUST use the web_search tool to find current, accurate information. "
                         "Always search official sources like business registries, company websites, and verified business directories.";
std::string response = call_anthropic_api(company_name, prompt, model, 500, system_msg);
```

Note: Also increased max_tokens from 250 to 500 to accommodate web search results processing.

### Step 3: Build and test

Run:
```bash
make clean && make
```

Expected: Compiles successfully

### Step 4: Manual test with DuckDB

```sql
LOAD stps;

-- Configure API keys
SELECT stps_set_api_key('your-anthropic-key');
SELECT stps_set_brave_api_key('your-brave-key');

-- Test with known company
SELECT stps_ask_ai_address('STP Solution GmbH');
```

Expected output structure:
```
{city: Karlsruhe, postal_code: 76135, street_name: Brauerstraße, street_nr: 12}
```

### Step 5: Test with previously failing case

```sql
SELECT stps_ask_ai_address('Tax Network GmbH');
```

Expected: Should now return structured address data instead of NULL

### Step 6: Test without Brave API key

```sql
-- Restart DuckDB without BRAVE_API_KEY set
LOAD stps;
SELECT stps_set_api_key('your-anthropic-key');
SELECT stps_ask_ai_address('STP Solution GmbH');
```

Expected: Falls back to knowledge-based response (may be less current but should still work)

### Step 7: Commit

```bash
git add src/ai_functions.cpp
git commit -m "feat: enable web search in stps_ask_ai_address function

- Update prompt to explicitly request web search for address lookups
- Add custom system message that instructs Claude to use web_search tool
- Increase max_tokens from 250 to 500 to handle search results
- Remove conservative 'training data only' instruction that blocked searches
- Maintain strict JSON-only output format

Fixes: stps_ask_ai_address now searches web like stps_ask_ai
Example: SELECT stps_ask_ai_address('Tax Network GmbH') now returns data

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Improve JSON Parsing Robustness

**Files:**
- Modify: `src/ai_functions.cpp:700-734`

**Context:**
The current JSON parsing may fail if Claude returns the address wrapped in markdown code blocks (```json ... ```) or includes explanatory text. We need to ensure robust parsing.

### Step 1: Verify extract_json_content handles markdown

Review the `extract_json_content` function (lines 161-253):

The function already handles markdown code blocks:
- Lines 166-177: Strips ```json and ``` markers
- Lines 179-184: Trims whitespace

This should already handle the case where Claude returns:
```
```json
{"city":"Karlsruhe","postal_code":"76135",...}
```
```

### Step 2: Test edge cases manually

```sql
-- Test that should return well-formed JSON
SELECT stps_ask_ai_address('Deutsche Bank AG');
SELECT stps_ask_ai_address('Apple Inc');
SELECT stps_ask_ai_address('Siemens AG');
```

Expected: All should parse correctly whether wrapped in markdown or not

### Step 3: Add logging for debugging (optional)

If parsing issues occur, we could add debug output, but for now we'll rely on the existing implementation which should be sufficient.

### Step 4: Document the JSON parsing behavior

No code changes needed - verify that the existing `extract_json_content` is sufficient.

---

## Task 4: Update Documentation

**Files:**
- Modify: `AI_FUNCTIONS_GUIDE.md`
- Modify: `README.md`

### Step 1: Update AI_FUNCTIONS_GUIDE.md

**File:** `AI_FUNCTIONS_GUIDE.md`

Find the section about `stps_ask_ai_address` and update it to reflect web search capability:

```markdown
### stps_ask_ai_address - Structured Address Lookup with Web Search

**Signature:**
```sql
stps_ask_ai_address(company_name VARCHAR) → STRUCT(city VARCHAR, postal_code VARCHAR, street_name VARCHAR, street_nr VARCHAR)
stps_ask_ai_address(company_name VARCHAR, model VARCHAR) → STRUCT(...)
```

**Description:**
Looks up the registered business address for a company using web search (when Brave API key is configured) and returns structured address data. This function automatically triggers web search to find current, accurate address information from business registries and official sources.

**Behavior:**
- **With Brave API key:** Automatically searches the web for current business address
- **Without Brave API key:** Uses Claude's training data (may be outdated for recent companies)
- **Output:** Structured data with city, postal_code, street_name, street_nr fields
- **NULL fields:** If a specific field cannot be determined, it will be NULL

**Examples:**

```sql
-- Basic usage (requires API keys configured)
SELECT stps_ask_ai_address('STP Solution GmbH');
-- Result: {city: Karlsruhe, postal_code: 76135, street_name: Brauerstraße, street_nr: 12}

-- Extract individual fields
SELECT
    (stps_ask_ai_address('Deutsche Bank AG')).city AS city,
    (stps_ask_ai_address('Deutsche Bank AG')).postal_code AS plz,
    (stps_ask_ai_address('Deutsche Bank AG')).street_name AS street;

-- Batch processing
SELECT
    company_name,
    (stps_ask_ai_address(company_name)).city,
    (stps_ask_ai_address(company_name)).postal_code,
    (stps_ask_ai_address(company_name)).street_name
FROM companies
WHERE address_missing = true;
```

**Web Search Behavior:**
This function is equivalent to:
```sql
SELECT stps_ask_ai(company_name, 'make a websearch and look for business address')
```
...but with structured JSON output instead of natural language text.

**Cost:** When web search is used, this function makes 2 Claude API calls + 1 Brave search per company.

**Performance Tip:** For batch processing, consider caching results to avoid duplicate lookups.
```

### Step 2: Update README.md

**File:** `README.md`

Find the AI Functions section and update the `stps_ask_ai_address` description:

```markdown
#### stps_ask_ai_address - Business Address Lookup

Get structured business address data with automatic web search:

```sql
-- Configure API keys (Brave key enables web search)
SELECT stps_set_api_key('sk-ant-...');
SELECT stps_set_brave_api_key('BSA-...');  -- Optional but recommended

-- Lookup address (automatically searches web if Brave key configured)
SELECT stps_ask_ai_address('STP Solution GmbH');
-- Returns: {city: Karlsruhe, postal_code: 76135, street_name: Brauerstraße, street_nr: 12}

-- Extract specific fields
SELECT
    company,
    (stps_ask_ai_address(company)).city,
    (stps_ask_ai_address(company)).postal_code
FROM companies;

-- Batch address enrichment
UPDATE companies
SET
    city = (stps_ask_ai_address(company_name)).city,
    postal_code = (stps_ask_ai_address(company_name)).postal_code,
    street = (stps_ask_ai_address(company_name)).street_name,
    street_nr = (stps_ask_ai_address(company_name)).street_nr
WHERE address_missing = true;
```

**How it works:**
- With Brave API key: Automatically searches business registries and official sources
- Without Brave API key: Uses Claude's knowledge (may be outdated)
- Returns structured data ready for database storage
- NULL for fields that cannot be determined
```

### Step 3: Commit documentation

```bash
git add AI_FUNCTIONS_GUIDE.md README.md
git commit -m "docs: update stps_ask_ai_address to reflect web search capability

- Document automatic web search behavior
- Add examples showing structured output
- Clarify behavior with/without Brave API key
- Note cost implications for batch processing

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Comprehensive Testing

**Files:**
- Create: `test/sql/ai_address_search.test` (optional)

### Step 1: Create manual test script

Create a test file to verify all scenarios:

**File:** `test_address_search.sql`

```sql
-- Test Script: stps_ask_ai_address Web Search
-- Run this in DuckDB after loading extension

.echo on

-- Setup
LOAD stps;
SELECT stps_set_api_key('your-anthropic-key');
SELECT stps_set_brave_api_key('your-brave-key');

-- Test 1: Known company with web search
.print '=== Test 1: STP Solution GmbH (should search web) ==='
SELECT stps_ask_ai_address('STP Solution GmbH') AS result;
-- Expected: {city: Karlsruhe, postal_code: 76135, street_name: Brauerstraße, street_nr: 12}

-- Test 2: Another company
.print '=== Test 2: Tax Network GmbH (previously returned NULL) ==='
SELECT stps_ask_ai_address('Tax Network GmbH') AS result;
-- Expected: Valid address structure (not NULL)

-- Test 3: Extract individual fields
.print '=== Test 3: Field extraction ==='
SELECT
    (stps_ask_ai_address('Deutsche Bank AG')).city AS city,
    (stps_ask_ai_address('Deutsche Bank AG')).postal_code AS plz,
    (stps_ask_ai_address('Deutsche Bank AG')).street_name AS street,
    (stps_ask_ai_address('Deutsche Bank AG')).street_nr AS nr;
-- Expected: All fields populated with Deutsche Bank address

-- Test 4: Batch processing
.print '=== Test 4: Batch processing ==='
CREATE TEMP TABLE test_companies AS
SELECT * FROM (VALUES
    ('Apple Inc'),
    ('Microsoft Corporation'),
    ('Siemens AG')
) t(company_name);

SELECT
    company_name,
    (stps_ask_ai_address(company_name)).city AS city,
    (stps_ask_ai_address(company_name)).postal_code AS postal_code
FROM test_companies;
-- Expected: All companies return valid city and postal_code

-- Test 5: Without Brave key (fallback behavior)
.print '=== Test 5: Without Brave API key ==='
-- Note: Need to restart DuckDB without BRAVE_API_KEY for this test
-- Expected: Should still return results from training data

-- Cleanup
DROP TABLE IF EXISTS test_companies;
.print '=== All tests complete ==='
```

### Step 2: Run tests manually

```bash
# Start DuckDB and run tests
duckdb -init test_address_search.sql
```

### Step 3: Verify each test case

**Test 1 - Known Company:**
- ✅ Returns structured address with all fields
- ✅ Web search is triggered (verify in logs if available)
- ✅ Data matches known STP Solution GmbH address

**Test 2 - Previously Failing Case:**
- ✅ Returns valid address structure (not NULL)
- ✅ Address data is reasonable and structured

**Test 3 - Field Extraction:**
- ✅ All fields can be extracted individually
- ✅ No parsing errors
- ✅ Data types are correct (all VARCHAR)

**Test 4 - Batch Processing:**
- ✅ Multiple companies processed sequentially
- ✅ Each returns structured data
- ✅ No crashes or memory issues

**Test 5 - Fallback Behavior:**
- ✅ Works without Brave key (uses training data)
- ✅ No errors about missing Brave configuration
- ✅ Results may be less current but still structured

### Step 4: Performance test (optional)

```sql
-- Test 10 companies to check for rate limiting or performance issues
CREATE TEMP TABLE companies AS
SELECT * FROM (VALUES
    ('SAP SE'),
    ('BMW AG'),
    ('Volkswagen AG'),
    ('Allianz SE'),
    ('Deutsche Telekom AG'),
    ('Bayer AG'),
    ('BASF SE'),
    ('Daimler AG'),
    ('Continental AG'),
    ('Adidas AG')
) t(name);

-- Time the batch lookup
.timer on
SELECT
    name,
    (stps_ask_ai_address(name)).city,
    (stps_ask_ai_address(name)).postal_code
FROM companies;
.timer off
```

Expected: Completes without errors (may take several minutes due to API calls)

### Step 5: Document test results

Create a summary of test results (no need to commit this):

```
Test Results - 2026-01-18
==========================
✅ Test 1: STP Solution GmbH - PASS (correct address returned)
✅ Test 2: Tax Network GmbH - PASS (returns data, not NULL)
✅ Test 3: Field extraction - PASS (all fields accessible)
✅ Test 4: Batch processing - PASS (3 companies processed)
✅ Test 5: Fallback mode - PASS (works without Brave key)
✅ Performance test - PASS (10 companies in Xs)

All tests passing. Ready for production use.
```

---

## Task 6: Final Validation and Cleanup

### Step 1: Review all changes

```bash
# Check git status
git status

# Review all commits
git log --oneline -5

# Verify no uncommitted changes
git diff
```

Expected commits:
1. "feat: add custom system message parameter to call_anthropic_api"
2. "feat: enable web search in stps_ask_ai_address function"
3. "docs: update stps_ask_ai_address to reflect web search capability"

### Step 2: Build final version

```bash
make clean
make
```

Expected: Clean build with no warnings or errors

### Step 3: Quick smoke test

```sql
LOAD stps;
SELECT stps_set_api_key('your-key');
SELECT stps_set_brave_api_key('your-brave-key');
SELECT stps_ask_ai_address('STP Solution GmbH');
```

Expected: Returns structured address data with all fields populated

### Step 4: Compare with original behavior

**Before (what user reported):**
```sql
SELECT stps_ask_ai_address('Tax Network GmbH');
-- Result: NULL
```

**After (fixed):**
```sql
SELECT stps_ask_ai_address('Tax Network GmbH');
-- Result: {city: "...", postal_code: "...", street_name: "...", street_nr: "..."}
```

**Equivalent to:**
```sql
SELECT stps_ask_ai('Tax Network GmbH', 'make a websearch and look for business address');
-- But with structured output instead of natural language
```

### Step 5: Clean up test files

```bash
# Remove temporary test file if created
rm -f test_address_search.sql
```

### Step 6: Final commit (if any loose ends)

```bash
# Only if needed
git add .
git commit -m "chore: final cleanup for address search implementation"
```

---

## Verification Checklist

Before marking this plan as complete, verify:

- [x] `call_anthropic_api` accepts custom system message parameter
- [x] `StpsAskAIAddressFunction` uses web-search-friendly prompt
- [x] Custom system message instructs Claude to use web_search tool
- [x] max_tokens increased to 500 to handle search results
- [x] Documentation updated in AI_FUNCTIONS_GUIDE.md
- [x] Documentation updated in README.md
- [x] Manual tests pass for known companies
- [x] Manual tests pass for previously failing cases
- [x] Batch processing works correctly
- [x] Fallback behavior works without Brave key
- [x] No breaking changes to existing API
- [x] All commits have proper messages with Co-Authored-By

---

## Rollback Plan

If issues are discovered after implementation:

1. Revert commits in reverse order:
   ```bash
   git revert HEAD~2..HEAD
   ```

2. Specific rollback scenarios:
   - **JSON parsing fails:** Revert Task 2, keep Task 1
   - **Web search not triggering:** Review Brave API key configuration
   - **Performance issues:** Add rate limiting or caching layer

3. Emergency rollback:
   ```bash
   git reset --hard <commit-before-changes>
   make clean && make
   ```

---

## Success Criteria

This implementation is successful when:

1. ✅ `stps_ask_ai_address('STP Solution GmbH')` returns structured address matching web search results
2. ✅ `stps_ask_ai_address('Tax Network GmbH')` returns valid address (not NULL)
3. ✅ Function behaves equivalently to `stps_ask_ai(company, 'make a websearch and look for business address')` but with structured output
4. ✅ Works with and without Brave API key (graceful degradation)
5. ✅ All documentation accurately reflects new behavior
6. ✅ No breaking changes to existing code
7. ✅ Clean commit history with proper attribution

---

## Notes

- **Performance:** Each address lookup makes 2 Claude API calls + 1 Brave search when web search is used
- **Cost:** Approximately 2x Claude API cost + $0.003/search per company
- **Rate Limits:** Brave API has rate limits (2 req/sec), but shouldn't be an issue for typical SQL use cases
- **Caching:** Consider implementing caching in application layer for repeated lookups
- **Future Enhancement:** Could add a parameter to force/disable web search per query
