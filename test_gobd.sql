-- Test script for GoBD Reader functionality
-- This file tests the stps_read_gobd function and related macros

-- Enable auto-load for json extension
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

-- Load the extension
LOAD 'build/release/extension/polarsgodmode/polarsgodmode.duckdb_extension';

-- Test 1: List all tables in the GoBD index
.print '=== Test 1: List all tables in GoBD index ==='
SELECT * FROM gobd_list_tables('/Users/ramonljevo/Downloads/siebert/index.xml');

-- Test 2: Extract schema for all tables
.print ''
.print '=== Test 2: Extract schema for all tables ==='
SELECT
    table_name,
    column_name,
    data_type,
    accuracy,
    column_order
FROM gobd_extract_schema(stps_read_xml_json('/Users/ramonljevo/Downloads/siebert/index.xml')::JSON)
ORDER BY table_name, column_order
LIMIT 20;

-- Test 3: Extract schema for Debitor table specifically
.print ''
.print '=== Test 3: Extract schema for Debitor table ==='
SELECT
    column_name,
    data_type,
    column_description,
    accuracy
FROM gobd_extract_schema(stps_read_xml_json('/Users/ramonljevo/Downloads/siebert/index.xml')::JSON)
WHERE table_name = 'Debitor'
ORDER BY column_order;

-- Test 4: Read actual data from Debitor table
.print ''
.print '=== Test 4: Read data from Debitor table (first 10 rows) ==='
SELECT *
FROM stps_read_gobd('/Users/ramonljevo/Downloads/siebert/index.xml', 'Debitor', '\t')
LIMIT 10;

-- Test 5: Count rows in Debitor table
.print ''
.print '=== Test 5: Count rows in Debitor table ==='
SELECT COUNT(*) as total_rows
FROM stps_read_gobd('/Users/ramonljevo/Downloads/siebert/index.xml', 'Debitor', '\t');

-- Test 6: Aggregate data from Debitor table
.print ''
.print '=== Test 6: Aggregate by Country/Region ==='
SELECT
    CountryRegionCode,
    COUNT(*) as customer_count,
    SUM(CAST(CreditLimitLCY AS DOUBLE)) as total_credit_limit
FROM stps_read_gobd('/Users/ramonljevo/Downloads/siebert/index.xml', 'Debitor', '\t')
WHERE CountryRegionCode IS NOT NULL AND CountryRegionCode != ''
GROUP BY CountryRegionCode
ORDER BY customer_count DESC;

-- Test 7: Test custom delimiter (if you have a semicolon-delimited file)
-- Uncomment and adjust if you have such a file:
-- .print ''
-- .print '=== Test 7: Read with custom delimiter ==='
-- SELECT * FROM stps_read_gobd('/path/to/index.xml', 'TableName', ';') LIMIT 5;

.print ''
.print '=== All tests completed ==='
