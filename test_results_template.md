# Test Results - stps_ask_ai_address Web Search

**Date:** [Fill in date]
**Tester:** [Your name]
**Environment:** [DuckDB version, OS]

## Configuration

- [ ] Anthropic API key configured
- [ ] Brave API key configured
- [ ] Extension built successfully
- [ ] Extension loaded in DuckDB

## Test Results

### Test 1: Known Company (STP Solution GmbH)
- **Status:** [ ] PASS [ ] FAIL
- **Expected:** {city: Karlsruhe, postal_code: 76135, street_name: Brauerstraße, street_nr: 12}
- **Actual:**
- **Notes:**

### Test 2: Previously Failing Case (Tax Network GmbH)
- **Status:** [ ] PASS [ ] FAIL
- **Expected:** Valid address structure (not NULL)
- **Actual:**
- **Notes:**

### Test 3: Field Extraction (Deutsche Bank AG)
- **Status:** [ ] PASS [ ] FAIL
- **Expected:** All fields populated
- **Actual:**
- **Notes:**

### Test 4: Batch Processing
- **Status:** [ ] PASS [ ] FAIL
- **Expected:** 3 companies processed successfully
- **Actual:**
- **Notes:**

### Test 5: Equivalence Check
- **Status:** [ ] PASS [ ] FAIL
- **Expected:** Structured and natural language outputs match
- **Actual:**
- **Notes:**

### Test 6: Fallback Without Brave Key
- **Status:** [ ] PASS [ ] FAIL [ ] SKIPPED
- **Expected:** Uses training data, still returns addresses
- **Actual:**
- **Notes:**

### Test 7: NULL Input Handling
- **Status:** [ ] PASS [ ] FAIL
- **Expected:** Returns NULL
- **Actual:**
- **Notes:**

### Test 8: Empty String Input
- **Status:** [ ] PASS [ ] FAIL
- **Expected:** Returns NULL or error
- **Actual:**
- **Notes:**

### Test 9: Non-existent Company
- **Status:** [ ] PASS [ ] FAIL
- **Expected:** Graceful handling (NULL or empty fields)
- **Actual:**
- **Notes:**

## Summary

**Tests Passed:** __ / 9
**Tests Failed:** __ / 9
**Tests Skipped:** __ / 9

## Issues Found

[List any issues discovered during testing]

## Recommendations

[Any recommendations for improvements]

## Sign-off

**Ready for Production:** [ ] YES [ ] NO

**Tester Signature:** ___________________________  **Date:** __________
