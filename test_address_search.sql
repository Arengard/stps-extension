-- ============================================================================
-- Test Script: stps_ask_ai_address Web Search Functionality
-- ============================================================================
-- Prerequisites:
--   1. Build and load the extension: LOAD stps;
--   2. Configure Anthropic API key: SELECT stps_set_api_key('sk-ant-...');
--   3. Configure Brave API key: SELECT stps_set_brave_api_key('BSA-...');
--
-- Run this in DuckDB after configuring API keys
-- ============================================================================

.echo on

-- Setup: Configure API keys
-- IMPORTANT: Replace with your actual API keys before running
LOAD stps;

.print '=== Configuring API keys ==='
SELECT stps_set_api_key('YOUR-ANTHROPIC-API-KEY-HERE');
SELECT stps_set_brave_api_key('YOUR-BRAVE-API-KEY-HERE');

-- ============================================================================
-- Test 1: Known company with web search
-- ============================================================================
.print ''
.print '=== Test 1: STP Solution GmbH (should search web) ==='
.print 'Expected: {city: Karlsruhe, postal_code: 76135, street_name: Brauerstraße, street_nr: 12}'
SELECT stps_ask_ai_address('STP Solution GmbH') AS result;

-- ============================================================================
-- Test 2: Previously failing case (from user report)
-- ============================================================================
.print ''
.print '=== Test 2: Tax Network GmbH (previously returned NULL) ==='
.print 'Expected: Valid address structure (not NULL)'
SELECT stps_ask_ai_address('Tax Network GmbH') AS result;

-- ============================================================================
-- Test 3: Extract individual fields
-- ============================================================================
.print ''
.print '=== Test 3: Field extraction - Deutsche Bank AG ==='
.print 'Expected: All fields populated with Deutsche Bank address'
SELECT
    (stps_ask_ai_address('Deutsche Bank AG')).city AS city,
    (stps_ask_ai_address('Deutsche Bank AG')).postal_code AS postal_code,
    (stps_ask_ai_address('Deutsche Bank AG')).street_name AS street_name,
    (stps_ask_ai_address('Deutsche Bank AG')).street_nr AS street_nr;

-- ============================================================================
-- Test 4: Batch processing
-- ============================================================================
.print ''
.print '=== Test 4: Batch processing (3 companies) ==='
.print 'Expected: All companies return valid city and postal_code'
CREATE TEMP TABLE test_companies AS
SELECT * FROM (VALUES
    ('Apple Inc'),
    ('Microsoft Corporation'),
    ('Siemens AG')
) t(company_name);

SELECT
    company_name,
    (stps_ask_ai_address(company_name)).city AS city,
    (stps_ask_ai_address(company_name)).postal_code AS postal_code,
    (stps_ask_ai_address(company_name)).street_name AS street_name
FROM test_companies;

DROP TABLE test_companies;

-- ============================================================================
-- Test 5: Comparison with stps_ask_ai
-- ============================================================================
.print ''
.print '=== Test 5: Verify equivalence to stps_ask_ai with search prompt ==='
.print 'Both should return same address data (structured vs. natural language)'

-- Structured output
.print 'Structured output (stps_ask_ai_address):'
SELECT stps_ask_ai_address('STP Solution GmbH') AS structured;

-- Natural language output
.print 'Natural language output (stps_ask_ai):'
SELECT stps_ask_ai('STP Solution GmbH', 'make a websearch and look for business address') AS natural_language;

-- ============================================================================
-- Test 6: Without Brave API key (fallback behavior)
-- ============================================================================
.print ''
.print '=== Test 6: Fallback behavior (requires restarting DuckDB without Brave key) ==='
.print 'To test:'
.print '  1. Close DuckDB'
.print '  2. Restart DuckDB'
.print '  3. LOAD stps;'
.print '  4. SELECT stps_set_api_key(''your-anthropic-key'');'
.print '  5. SELECT stps_ask_ai_address(''STP Solution GmbH'');'
.print 'Expected: Returns address from training data (may be less current but should work)'
.print 'Note: This test cannot be automated - must be run manually'

-- ============================================================================
-- Test 7: NULL handling
-- ============================================================================
.print ''
.print '=== Test 7: NULL input handling ==='
.print 'Expected: Returns NULL'
SELECT stps_ask_ai_address(NULL) AS result;

-- ============================================================================
-- Test 8: Empty string handling
-- ============================================================================
.print ''
.print '=== Test 8: Empty string input ==='
.print 'Expected: Returns NULL or error'
SELECT stps_ask_ai_address('') AS result;

-- ============================================================================
-- Test 9: Non-existent company
-- ============================================================================
.print ''
.print '=== Test 9: Non-existent company ==='
.print 'Expected: Returns NULL or empty fields'
SELECT stps_ask_ai_address('Fake Company XYZ That Does Not Exist 12345') AS result;

-- ============================================================================
-- Test Results Summary
-- ============================================================================
.print ''
.print '============================================================================'
.print 'Test Execution Complete'
.print '============================================================================'
.print ''
.print 'Review the results above and verify:'
.print '  ✓ Test 1: Correct address for STP Solution GmbH'
.print '  ✓ Test 2: Tax Network GmbH returns data (not NULL)'
.print '  ✓ Test 3: All fields can be extracted individually'
.print '  ✓ Test 4: Batch processing works for 3 companies'
.print '  ✓ Test 5: Structured output matches natural language results'
.print '  ✓ Test 6: Manual test - fallback works without Brave key'
.print '  ✓ Test 7: NULL input handled correctly'
.print '  ✓ Test 8: Empty string handled correctly'
.print '  ✓ Test 9: Non-existent company handled gracefully'
.print ''
.print 'If all tests pass, the implementation is ready for production use.'
.print '============================================================================'
