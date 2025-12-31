LOAD '/Users/ramonljevo/Desktop/fuck/stps extension/build/release/extension/polarsgodmode/polarsgodmode.duckdb_extension';

-- Test existing function
SELECT 'Testing stps_is_valid_iban:' as test;
SELECT stps_is_valid_iban('DE89370400440532013000') as is_valid;

-- List all functions
SELECT 'All stps functions:' as test;
SELECT function_name
FROM duckdb_functions()
WHERE function_name LIKE 'stps%'
ORDER BY function_name;

-- Try new functions
SELECT 'Testing stps_format_iban:' as test;
SELECT stps_format_iban('DE89370400440532013000') as formatted;
