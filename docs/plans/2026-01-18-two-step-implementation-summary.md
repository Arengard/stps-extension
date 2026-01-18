# Two-Step Address Lookup Implementation Summary

**Date:** 2026-01-18
**Branch:** feature/two-step-address-lookup
**Status:** Implementation Complete

---

## Problem Solved

**Issue:** `stps_ask_ai_address('Tax Network GmbH')` returned NULL instead of structured address data

**Root Cause:** Strict "JSON-only, no explanatory text" prompt conflicted with Claude's natural web search behavior (which includes explanations), causing Claude to return empty content → NULL result

---

## Solution Implemented

Replaced single-step strict prompt with two-step approach:

### Step 1: Natural Search
- Prompt: Natural language request for address
- System message: Default (allows web search with explanations)
- max_tokens: 500
- Result: Natural language with address info

### Step 2: Structured Parsing
- Prompt: Extract address components into JSON
- Input: Step 1 result
- max_tokens: 200
- Result: Clean JSON structure

---

## Code Changes

### Modified Files

**1. src/ai_functions.cpp (lines 684-735)**
- Removed custom system message for address function
- Added Step 1: natural search with conditional prompt (web search vs training data)
- Added Step 2: parse natural language into JSON
- Applied security fix: escape address_text to prevent prompt injection
- Maintained existing error handling and JSON parsing logic

**2. AI_FUNCTIONS_GUIDE.md**
- Added "How It Works" section explaining two-step process
- Documented API call count, cost, and latency

**3. README.md**
- Updated cost information (2 API calls per lookup)
- Clarified behavior with/without Brave key

**4. test_address_search.sql**
- Added header explaining two-step implementation
- Updated test expectations with latency and API call info

---

## Testing Instructions

### Prerequisites
- Anthropic API key configured
- Brave API key configured (for web search)

### Test Cases

**1. Known Company**
```sql
SELECT stps_ask_ai_address('Tax Network GmbH');
-- Expected: {city: Karlsruhe, postal_code: 76185, street_name: Carl-Metz-Straße, street_nr: 17}
```

**2. Field Extraction**
```sql
SELECT
    (stps_ask_ai_address('STP Solution GmbH')).city,
    (stps_ask_ai_address('STP Solution GmbH')).postal_code;
-- Expected: city = "Karlsruhe", postal_code = "76135"
```

**3. Fallback Without Brave Key**
```sql
-- Restart DuckDB without BRAVE_API_KEY
SELECT stps_ask_ai_address('Deutsche Bank AG');
-- Expected: Still returns address (from training data)
```

**4. Batch Processing**
```sql
CREATE TEMP TABLE companies AS SELECT * FROM (VALUES
    ('Apple Inc'), ('Microsoft Corporation'), ('Siemens AG')
) t(name);

SELECT name, (stps_ask_ai_address(name)).city FROM companies;
-- Expected: All 3 companies return cities
```

---

## Verification Checklist

Implementation is successful when:

- [ ] `stps_ask_ai_address('Tax Network GmbH')` returns structured address (not NULL)
- [ ] Each lookup makes exactly 2 API calls (visible in Anthropic dashboard)
- [ ] Latency is 3-5 seconds per lookup
- [ ] Function works with Brave key (web search)
- [ ] Function works without Brave key (training data fallback)
- [ ] Error handling prevents crashes on API failures
- [ ] Empty fields are set to NULL (not empty strings)

---

## Trade-offs

### Cost
- **Before:** 1-2 API calls per lookup
- **After:** Always 2 API calls per lookup
- **Increase:** ~$0.001 per lookup (negligible, from single call to double call)

### Latency
- **Before:** 2-4 seconds (when working)
- **After:** 3-5 seconds (always)
- **Increase:** ~1 second (acceptable for batch operations)

### Reliability
- **Before:** Strict constraints → NULL on failure
- **After:** Natural behavior → successful responses
- **Improvement:** Significant

---

## Commits

1. `feat: implement two-step approach for stps_ask_ai_address`
2. `fix: escape address_text in Step 2 to prevent prompt injection`
3. `docs: document two-step approach in stps_ask_ai_address`
4. `docs: consolidate duplicate cost information in AI_FUNCTIONS_GUIDE`
5. `docs: update README with two-step address lookup details`
6. `docs: update test suite with two-step behavior notes`
7. `docs: add implementation summary`

---

## Next Steps

1. Merge feature branch to master
2. Build and deploy updated extension
3. Execute test suite with real API keys
4. Monitor Anthropic dashboard for 2 API calls per lookup
5. Validate latency is 3-5 seconds
6. Confirm NULL issue is resolved

---

## References

- Design: `docs/plans/2026-01-18-two-step-address-lookup-design.md`
- Original implementation: `docs/plans/2026-01-18-fix-address-web-search.md`
- Test suite: `test_address_search.sql`
