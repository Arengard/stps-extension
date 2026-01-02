# GoBD Reader Implementation

## Overview

Successfully implemented a custom DuckDB function `stps_read_gobd` for reading GoBD (Grundsätze zur ordnungsmäßigen Führung und Aufbewahrung von Büchern) accounting data files.

## What Was Built

### Core Function
- **`stps_read_gobd(index_path, table_name, delimiter='\t')`** - Table function that:
  1. Parses GoBD index.xml files
  2. Extracts table schemas (columns, types)
  3. Maps GoBD types to DuckDB types
  4. Dynamically loads CSV data with proper schema

### SQL Macros
- **`gobd_extract_schema(json_data)`** - Extracts full schema from XML
- **`gobd_list_tables(index_path)`** - Lists all available tables

### Files Created/Modified

#### New Files:
1. `src/include/gobd_reader.hpp` - Header file
2. `src/gobd_reader.cpp` - Implementation (~230 lines)
3. `gobd_macros.sql` - SQL helper macros
4. `GOBD_USAGE.md` - Comprehensive documentation
5. `GOBD_README.md` - This file
6. `test_gobd.sql` - Test script

#### Modified Files:
1. `src/stps_extension.cpp` - Registered new function
2. `CMakeLists.txt` - Added gobd_reader.cpp to build

## Usage

### Basic Usage

```sql
-- Load the extension
LOAD 'build/release/extension/stps/stps.duckdb_extension';

-- Load macros (optional)
.read gobd_macros.sql

-- Read data from a table
SELECT * FROM stps_read_gobd(
    '/Users/ramonljevo/Downloads/siebert/index.xml',
    'Debitor'
);

-- With custom delimiter
SELECT * FROM stps_read_gobd(
    '/path/to/index.xml',
    'TableName',
    ';'
);
```

### List Available Tables

```sql
SELECT * FROM gobd_list_tables('/Users/ramonljevo/Downloads/siebert/index.xml');
```

### Extract Schema

```sql
SELECT
    table_name,
    column_name,
    data_type,
    column_description
FROM gobd_extract_schema(
    stps_read_xml_json('/Users/ramonljevo/Downloads/siebert/index.xml')::JSON
)
WHERE table_name = 'Debitor'
ORDER BY column_order;
```

## Type Mapping

| GoBD Type | DuckDB Type |
|-----------|-------------|
| Numeric | DECIMAL(18, accuracy) or DOUBLE |
| Date | DATE |
| AlphaNumeric | VARCHAR |

## Testing

Run the test script:

```bash
# From DuckDB CLI
.read test_gobd.sql
```

Or test manually:

```bash
# Start DuckDB
./build/release/duckdb

# Load extension
LOAD 'build/release/extension/stps/stps.duckdb_extension';

# Test the function
SELECT * FROM stps_read_gobd(
    '/Users/ramonljevo/Downloads/siebert/index.xml',
    'Debitor'
) LIMIT 10;
```

## Build Status

✅ **Successfully Built**
- Extension file: `build/release/extension/stps/stps.duckdb_extension` (26MB)
- Build completed at 100%
- All compilation errors fixed

## Implementation Details

### Architecture
1. **Bind Phase**: Parses XML, extracts schema, prepares column definitions
2. **Execution Phase**: Generates and executes `read_csv` with dynamic schema
3. **Path Resolution**: Automatically combines index directory with table URL

### Key Features
- Dynamic schema extraction from XML
- Automatic type mapping
- Configurable delimiter
- Proper error handling
- Integration with existing `stps_read_xml_json` function

## Next Steps

1. **Test with your actual data**:
   ```sql
   SELECT * FROM stps_read_gobd(
       '/Users/ramonljevo/Downloads/siebert/index.xml',
       'Debitor'
   );
   ```

2. **Verify schema extraction**:
   ```sql
   .read gobd_macros.sql
   SELECT * FROM gobd_list_tables('/Users/ramonljevo/Downloads/siebert/index.xml');
   ```

3. **Run comprehensive tests**:
   ```bash
   .read test_gobd.sql
   ```

## Documentation

- **GOBD_USAGE.md** - Detailed usage guide with examples
- **STPS_FUNCTIONS.md** - All extension functions (update this to include GoBD functions)
- **gobd_macros.sql** - SQL macro definitions with comments

## Notes

- The function assumes tab-delimited CSV files by default (GoBD standard)
- CSV files should have no header row
- The index.xml path and CSV file paths are resolved automatically
- Error messages provide clear feedback for missing tables or files

## Compilation Fixes Applied

During development, fixed:
1. Removed unused `<filesystem>` header (C++17 → C++11 compatibility)
2. Added second parameter to `Query()` calls (API change)
3. Removed `RowCount()` call (not available in QueryResult)

All issues resolved, extension builds successfully.
