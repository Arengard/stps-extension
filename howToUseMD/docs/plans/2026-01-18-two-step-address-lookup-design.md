# Two-Step Address Lookup Design

**Date:** 2026-01-18
**Status:** Design Complete
**Related Issue:** `stps_ask_ai_address` returns NULL instead of structured address data

---

## Problem Statement

The `stps_ask_ai_address` function returns NULL when querying for company addresses, despite having both Anthropic and Brave API keys configured.

**Root Cause:**
The function uses a strict prompt demanding "Respond ONLY with a JSON object (no markdown, no code blocks, no explanatory text)" combined with a custom system message requiring "you MUST use the web_search tool". These constraints are incompatible:

- Claude's natural web search behavior includes explanatory text like "Based on my web search, I found..."
- The strict JSON-only constraint prevents Claude from explaining its tool use
- Result: Claude returns empty content (0 tokens) → API error → NULL

**Evidence:**
```json
{
  "content": [],
  "stop_reason": "end_turn",
  "output_tokens": 8
}
```

**Working Comparison:**
```sql
-- This works (returns natural language with address):
SELECT stps_ask_ai('Tax Network GmbH', 'make a websearch and look for business address');
-- Returns: "Based on my web search, I found the business address for Tax Network GmbH: Carl-Metz-Straße 17, 76185 Karlsruhe, Germany..."

-- This fails (returns NULL):
SELECT stps_ask_ai_address('Tax Network GmbH');
-- Returns: NULL
```

---

## Solution: Two-Step Approach

Separate the search operation from the parsing operation to work with Claude's natural behavior instead of constraining it.

### Architecture

```
┌─────────────────┐
│ Company Name    │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│ STEP 1: Natural Search              │
│                                     │
│ • Prompt: "Find the registered     │
│   business address for {company}"  │
│ • System: Default (allows web      │
│   search with explanations)        │
│ • max_tokens: 500                  │
└────────┬────────────────────────────┘
         │
         │ Natural language result:
         │ "Based on my web search,
         │  I found the address..."
         │
         ▼
┌─────────────────────────────────────┐
│ STEP 2: Structure Parsing           │
│                                     │
│ • Prompt: "Extract address         │
│   components into JSON"            │
│ • Input: Step 1 result             │
│ • max_tokens: 200                  │
└────────┬────────────────────────────┘
         │
         │ JSON result:
         │ {"city":"Karlsruhe",...}
         │
         ▼
┌─────────────────────────────────────┐
│ Parse & Return Struct               │
└─────────────────────────────────────┘
```

---

## Implementation

### Step 1: Natural Search

```cpp
// Check if web search is available
bool tools_enabled = !GetBraveApiKey().empty();

std::string search_prompt;
if (tools_enabled) {
    // With Brave API key: request web search
    search_prompt = "Find the current registered business address (Impressum/legal address) for this company: " + company_name + ". "
                   "Search for official business registry information, company websites, or business directories. "
                   "Focus on the legal/registered address, not customer service addresses.";
} else {
    // Without Brave API key: fallback to training data
    search_prompt = "What is the registered business address (Impressum/legal address) for " + company_name + "? "
                   "Provide the legal/registered address if you know it from your training data.";
}

// Use default system message (empty string = natural behavior)
std::string address_text = call_anthropic_api(company_name, search_prompt, model, 500, "");

// Check for errors
if (address_text.find("ERROR:") == 0) {
    result_validity.SetInvalid(i);
    continue;
}
```

**Key Points:**
- No custom system message (uses default which supports web search)
- Natural prompt allows Claude to explain its findings
- Conditional prompt based on Brave API key availability
- 500 tokens for search results processing

### Step 2: Structure Parsing

```cpp
// Parse the natural language response into structured JSON
std::string parse_prompt = "Extract the address components from the following text and respond with ONLY a JSON object in this format: "
                          "{\"city\":\"...\",\"postal_code\":\"...\",\"street_name\":\"...\",\"street_nr\":\"...\"}. "
                          "Use empty strings for any fields you cannot determine. "
                          "Text to parse: " + address_text;

std::string response = call_anthropic_api("", parse_prompt, model, 200, "");

// Check for errors
if (response.find("ERROR:") == 0) {
    result_validity.SetInvalid(i);
    continue;
}

// Parse JSON fields (existing code)
std::string city = extract_json_content(response, "city");
std::string postal_code = extract_json_content(response, "postal_code");
std::string street_name = extract_json_content(response, "street_name");
std::string street_nr = extract_json_content(response, "street_nr");
```

**Key Points:**
- Simple parsing task (no web search needed)
- Empty context parameter (first argument)
- 200 tokens sufficient for JSON response
- Leverages existing `extract_json_content` parser

---

## Trade-offs Analysis

### Cost Impact

| Scenario | Before | After | Difference |
|----------|--------|-------|------------|
| With web search | 2 calls | 2 calls | No change |
| Without web search | 1 call | 2 calls | +1 call |
| **Per lookup cost** | ~$0.001 | ~$0.002 | +$0.001 |

**Analysis:**
- When web search is triggered, cost is identical (2 calls both before and after)
- When web search is NOT triggered, we add 1 parsing call
- Additional cost is negligible: ~$0.001 per lookup
- For batch operations (1000 companies): +$1 total

**Verdict:** Acceptable cost increase for reliable functionality.

### Latency Impact

| Operation | Time |
|-----------|------|
| Step 1: Search | 2-3 seconds |
| Step 2: Parse | 0.5-1 second |
| **Total** | **3-4 seconds** |

**Before:** 2-4 seconds (when working)
**After:** 3-4 seconds (always)

**Verdict:** Minimal increase, acceptable for batch address enrichment.

### Reliability Improvement

**Before:**
- Strict constraint → Claude refuses to respond → NULL
- Single point of failure
- No intermediate data if parsing fails

**After:**
- Natural behavior → Claude responds successfully
- Two-stage error handling
- Even if Step 2 fails, Step 1 data available for debugging

**Verdict:** Significant reliability improvement.

### Fallback Behavior

**With Brave API key:**
- Step 1: Web search for current address
- Step 2: Parse into structure
- Result: Current, accurate data

**Without Brave API key:**
- Step 1: Uses training data
- Step 2: Parse into structure
- Result: Historical data (may be outdated but better than NULL)

**Verdict:** Graceful degradation maintained.

---

## Error Handling

### Error Flow

```
Step 1: Search
    ↓
   ERROR? → SetInvalid(i) → Return NULL
    ↓ NO
Step 2: Parse
    ↓
   ERROR? → SetInvalid(i) → Return NULL
    ↓ NO
Parse JSON fields
    ↓
   Empty field? → Set field to NULL
    ↓ NO
   Set field value
```

### Failure Modes

1. **API Error in Step 1**
   - Cause: API key invalid, rate limit, network error
   - Result: Entire struct set to NULL
   - Detection: `response.find("ERROR:") == 0`

2. **API Error in Step 2**
   - Cause: Same as Step 1
   - Result: Entire struct set to NULL
   - Note: Step 1 data is lost but not accessible at SQL level

3. **JSON Parsing Failure**
   - Cause: Malformed JSON, missing fields
   - Result: Empty string for field → NULL in struct
   - Detection: `extract_json_content` returns ""

4. **Empty Response**
   - Cause: Claude returns no content
   - Result: Empty strings → all fields NULL
   - Graceful degradation

---

## Testing Plan

### Test Cases

**1. Known Company with Web Search**
```sql
SELECT stps_ask_ai_address('Tax Network GmbH');
-- Expected: {city: Karlsruhe, postal_code: 76185, street_name: Carl-Metz-Straße, street_nr: 17}
```

**2. Field Extraction**
```sql
SELECT
    (stps_ask_ai_address('STP Solution GmbH')).city,
    (stps_ask_ai_address('STP Solution GmbH')).postal_code,
    (stps_ask_ai_address('STP Solution GmbH')).street_name,
    (stps_ask_ai_address('STP Solution GmbH')).street_nr;
-- Expected: All fields populated with correct data
```

**3. Fallback Without Brave Key**
```sql
-- Restart DuckDB without BRAVE_API_KEY environment variable
SELECT stps_ask_ai_address('Deutsche Bank AG');
-- Expected: Still returns structured address (from training data)
```

**4. Non-existent Company**
```sql
SELECT stps_ask_ai_address('Fake Company XYZ 99999');
-- Expected: NULL or partial data with empty fields
```

**5. Batch Processing**
```sql
CREATE TEMP TABLE companies AS
SELECT * FROM (VALUES
    ('Apple Inc'),
    ('Microsoft Corporation'),
    ('Siemens AG')
) t(name);

SELECT
    name,
    (stps_ask_ai_address(name)).city,
    (stps_ask_ai_address(name)).postal_code
FROM companies;
-- Expected: All 3 companies return valid addresses
```

### Verification Checklist

- [ ] Step 1 triggers web search (check Brave API usage)
- [ ] Step 2 successfully parses natural language
- [ ] Total API calls: 2 per lookup
- [ ] Response time: 3-5 seconds per lookup
- [ ] NULL returned on API errors (not crashes)
- [ ] Empty fields set to NULL (not empty strings)
- [ ] Works without Brave key (training data fallback)

---

## Documentation Updates

### AI_FUNCTIONS_GUIDE.md

Add section explaining the two-step process:

```markdown
### stps_ask_ai_address - How It Works

This function uses a two-step approach for reliability:

1. **Search Step:** Retrieves address information using web search (if Brave API key configured) or training data. Claude responds naturally with context.

2. **Parsing Step:** Extracts structured components (city, postal_code, street_name, street_nr) from the natural language result.

**Why Two Steps?**
This approach works with Claude's natural behavior instead of constraining it with strict formatting requirements, resulting in higher success rates.

**Cost:** 2 Claude API calls per lookup
**Latency:** Typically 3-5 seconds per address
```

### README.md

Update cost information:

```markdown
**Cost:** Each address lookup makes 2 Claude API calls:
- First call: Search for address (with web search if Brave key configured)
- Second call: Parse natural language into structured format
- Total cost: ~$0.002 per lookup with Claude Sonnet 4.5
```

### test_address_search.sql

Add note about two-step behavior:

```sql
-- Note: stps_ask_ai_address makes 2 API calls per lookup:
-- 1. Search for address information (natural language)
-- 2. Parse into structured JSON format
-- This is intentional for reliability and works with Claude's natural behavior
```

---

## Implementation Checklist

### Code Changes

- [ ] Replace `StpsAskAIAddressFunction` implementation (src/ai_functions.cpp lines 684-735)
- [ ] Remove custom system message logic for address function
- [ ] Add Step 1: natural search with default system message
- [ ] Add Step 2: structured parsing
- [ ] Maintain existing error handling patterns
- [ ] Keep existing JSON parsing logic (extract_json_content)

### Testing

- [ ] Manual test with Brave API key configured
- [ ] Manual test without Brave API key (fallback)
- [ ] Test with known companies (STP Solution GmbH, Tax Network GmbH)
- [ ] Test with non-existent company
- [ ] Batch processing test (3+ companies)
- [ ] Verify 2 API calls per lookup in Anthropic dashboard
- [ ] Measure latency (should be 3-5 seconds)

### Documentation

- [ ] Update AI_FUNCTIONS_GUIDE.md with two-step explanation
- [ ] Update README.md with cost information
- [ ] Update test_address_search.sql with notes
- [ ] Commit design document
- [ ] Update IMPLEMENTATION_SUMMARY.md if exists

---

## Success Criteria

Implementation is successful when:

1. ✅ `stps_ask_ai_address('Tax Network GmbH')` returns structured address (not NULL)
2. ✅ All test cases pass
3. ✅ Cost is predictable (2 calls per lookup)
4. ✅ Latency is acceptable (3-5 seconds)
5. ✅ Fallback works without Brave key
6. ✅ Error handling prevents crashes
7. ✅ Documentation accurately reflects behavior

---

## Alternatives Considered

### Alternative 1: Relax JSON-Only Constraint
**Approach:** Allow explanatory text in single-step prompt
**Pros:** Only 1 API call
**Cons:** Still fighting Claude's natural behavior, unpredictable format
**Rejected:** Less reliable than two-step separation

### Alternative 2: Custom Parsing Logic
**Approach:** Parse natural language directly with regex/heuristics
**Pros:** No second API call needed
**Cons:** Brittle, language-dependent, hard to maintain
**Rejected:** LLM parsing is more robust and flexible

### Alternative 3: Structured Output API
**Approach:** Use Anthropic's structured output feature (if available)
**Pros:** Native JSON generation, no parsing needed
**Cons:** May still conflict with web search tool use
**Status:** Consider for future optimization if Anthropic adds this feature

---

## Next Steps

1. **Implementation:** Use `superpowers:using-git-worktrees` to create isolated workspace
2. **Planning:** Use `superpowers:writing-plans` to create detailed implementation plan
3. **Execution:** Implement changes following test-driven development
4. **Testing:** Execute manual test suite with API keys
5. **Documentation:** Update all affected docs
6. **Deployment:** Build and deploy updated extension

---

## References

- Original issue: `stps_ask_ai_address` returns NULL
- Working reference: `stps_ask_ai` with natural web search prompt
- Related files:
  - `src/ai_functions.cpp` (lines 645-775)
  - `docs/plans/2026-01-18-fix-address-web-search.md` (previous implementation)
  - `test_address_search.sql` (test suite)
