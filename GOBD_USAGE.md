# GoBD Reader Functions

This document describes the GoBD (Grundsätze zur ordnungsmäßigen Führung und Aufbewahrung von Büchern) reader functions added to the polarsgodmode extension.

## Overview

GoBD is a German standard for accounting data archival. The functions in this module help you work with GoBD-compliant XML index files and their associated CSV data files.

## Functions

### `stps_read_gobd(index_path, table_name, delimiter='\t')`

A table function that reads GoBD CSV data files using the schema information from a GoBD index.xml file.

**Parameters:**
- `index_path` (VARCHAR): Path to the GoBD index.xml file
- `table_name` (VARCHAR): Name of the table to load (as defined in the XML schema)
- `delimiter` (VARCHAR, optional): CSV delimiter character (default: `\t` for tab-delimited)

**Returns:** A table with columns dynamically determined from the GoBD schema

**Type Mapping:**
- GoBD `Numeric` → DuckDB `DECIMAL(18, accuracy)` or `DOUBLE`
- GoBD `Date` → DuckDB `DATE`
- GoBD `AlphaNumeric` → DuckDB `VARCHAR`

**Example:**
```sql
-- Load data from the Debitor table
SELECT * FROM stps_read_gobd(
    '/Users/data/siebert/index.xml',
    'Debitor'
);

-- Load data with custom delimiter
SELECT * FROM stps_read_gobd(
    '/Users/data/siebert/index.xml',
    'Kreditor',
    ';'
);

-- Filter and aggregate
SELECT
    CountryRegionCode,
    COUNT(*) as customer_count,
    SUM(CreditLimitLCY) as total_credit_limit
FROM stps_read_gobd('/Users/data/siebert/index.xml', 'Debitor')
GROUP BY CountryRegionCode;
```

## SQL Macros

### `gobd_extract_schema(json_data)`

A macro that extracts the table schema from GoBD XML JSON data.

**Parameters:**
- `json_data` (JSON): JSON representation of the GoBD XML (from `stps_read_xml_json`)

**Returns:** Table with columns:
- `table_name` (VARCHAR)
- `table_url` (VARCHAR)
- `table_description` (VARCHAR)
- `column_type` (VARCHAR): Either 'VariablePrimaryKey' or 'VariableColumn'
- `column_name` (VARCHAR)
- `column_description` (VARCHAR)
- `data_type` (VARCHAR): 'Numeric', 'AlphaNumeric', 'Date', or 'Unknown'
- `accuracy` (INTEGER): For Numeric types, the decimal precision
- `column_order` (INTEGER): Position of column in the table

**Example:**
```sql
-- Load the macros
.read gobd_macros.sql

-- Extract schema for all tables
SELECT * FROM gobd_extract_schema(
    stps_read_xml_json('/Users/data/siebert/index.xml')::JSON
);

-- Extract schema for a specific table
SELECT
    column_name,
    data_type,
    column_description
FROM gobd_extract_schema(
    stps_read_xml_json('/Users/data/siebert/index.xml')::JSON
)
WHERE table_name = 'Debitor'
ORDER BY column_order;
```

### `gobd_list_tables(index_path)`

A macro that lists all tables defined in a GoBD index file.

**Parameters:**
- `index_path` (VARCHAR): Path to the GoBD index.xml file

**Returns:** Table with columns:
- `table_name` (VARCHAR)
- `table_url` (VARCHAR): Relative path to the CSV file
- `table_description` (VARCHAR)

**Example:**
```sql
-- Load the macros
.read gobd_macros.sql

-- List all available tables
SELECT * FROM gobd_list_tables('/Users/data/siebert/index.xml');
```

## How It Works

1. The `stps_read_gobd` function first parses the index.xml using the existing `stps_read_xml_json` function
2. It extracts the table schema using a recursive CTE that navigates the XML structure
3. It finds the table's CSV file path (table_url) and combines it with the index.xml directory
4. It maps GoBD data types to DuckDB types
5. It generates and executes a `read_csv` call with the appropriate column names and types

## Path Resolution

The function automatically resolves the CSV file path by:
1. Extracting the directory from the index.xml path
2. Reading the `URL` field from the table definition in the XML
3. Combining them to form the full path to the CSV file

For example:
- Index path: `/Users/data/siebert/index.xml`
- Table URL (from XML): `Debitor.txt`
- Resolved CSV path: `/Users/data/siebert/Debitor.txt`

## Error Handling

The function will throw errors in these cases:
- Index XML file cannot be parsed
- Specified table_name not found in the schema
- CSV file cannot be read
- Schema extraction fails

## Performance Tips

1. If you're loading multiple tables, consider materializing the schema extraction:
   ```sql
   CREATE TABLE gobd_schema AS
   SELECT * FROM gobd_extract_schema(
       stps_read_xml_json('/Users/data/siebert/index.xml')::JSON
   );
   ```

2. Use column filters to reduce data transfer:
   ```sql
   SELECT CustomerNo, Name, CreditLimit
   FROM stps_read_gobd('/Users/data/siebert/index.xml', 'Debitor')
   WHERE CountryRegionCode = 'DE';
   ```

3. For very large CSV files, consider using DuckDB's parallel CSV reader (enabled by default)

## Limitations

1. The function currently only supports single-file tables (no multi-file table support)
2. Complex GoBD schema features (like foreign keys, constraints) are not enforced
3. The delimiter parameter must be a single character
4. All CSV files are assumed to have no header row (as per GoBD standard)
