# STPS DuckDB Extension

A DuckDB extension providing STPS-specific functions for data transformation, validation, and file operations.

## Features

- **Text Transformations** – Case conversion, normalization, and text processing
- **Data Validation** – IBAN validation and other data quality checks
- **UUID Functions** – UUID generation and manipulation
- **Null Handling** – Enhanced null value processing
- **XML Parsing** – XML data parsing and extraction
- **File Operations** – Filesystem scanning and path manipulation
- **GOBD Reader** – German GOBD standard compliance tools
- **Smart Cast** – Automatic type detection and casting for VARCHAR columns

## Quick Start (Prebuilt Binaries)

**No build tools required!** Download prebuilt binaries from GitHub Actions:

### Step 1: Download the Extension

1. Go to [GitHub Actions](https://github.com/Arengard/stps-extension/actions)
2. Click on the latest successful "Windows Build on Push" workflow (green checkmark ✅)
3. Scroll to the **Artifacts** section at the bottom
4. Download `stps-windows-amd64-latest-master` (or your preferred version)
5. Extract the ZIP file to get `stps.duckdb_extension`

### Step 2: Load in DuckDB

```sql
-- Start DuckDB (unsigned mode required for loading extensions)
duckdb -unsigned

-- Install the extension
INSTALL './stps.duckdb_extension';

-- Load it
LOAD stps;

-- Test it works
SELECT stps_is_valid_iban('DE89370400440532013000') as is_valid;
```

### Step 3: Use the Functions

```sql
-- Case transformations
SELECT stps_to_snake_case('HelloWorld') as snake_case;
SELECT stps_to_camel_case('hello_world') as camel_case;

-- IBAN validation
SELECT stps_is_valid_iban('DE89370400440532013000') as valid_iban;
SELECT stps_format_iban('DE89370400440532013000') as formatted_iban;

-- UUID generation
SELECT stps_uuid() as new_uuid;

-- File operations
SELECT * FROM stps_scan('C:/path/to/folder');
SELECT * FROM stps_path('C:/base/path', 'subdir', 'file.txt');
```

## Available Functions

### 🔥 Lambda Functions (NEW!)

Apply transformations to all columns at once using lambda-like expressions. Perfect for bulk data cleaning.

#### `stps_lambda(table_name, lambda_expr, [varchar_only], [column_pattern])`

**Quick Examples:**
```sql
-- Trim all VARCHAR columns
SELECT * FROM stps_lambda('my_table', 'c -> trim(c)');

-- Convert all text to uppercase
SELECT * FROM stps_lambda('my_table', 'c -> upper(c)');

-- Apply STPS functions to all columns
SELECT * FROM stps_lambda('my_table', 'c -> stps_to_snake_case(c)');

-- Only columns containing 'name'
SELECT * FROM stps_lambda('my_table', 'c -> upper(c)', true, 'name');
```

**Parameters:**
- `table_name` - Table to transform
- `lambda_expr` - Transformation expression (`c -> function(c)` or just `function(c)`)
- `varchar_only` - Only apply to VARCHAR columns (default: true)
- `column_pattern` - Only columns matching this pattern

**Supported Transformations:**
- Built-in SQL: `TRIM`, `UPPER`, `LOWER`, `SUBSTR`, etc.
- All STPS functions: `stps_normalize`, `stps_clean_string`, case conversions, etc.
- Custom SQL expressions: `CASE WHEN c IS NULL THEN 'empty' ELSE c END`

### Case Transformation Functions
- `stps_to_snake_case(text)` - Convert to snake_case
- `stps_to_camel_case(text)` - Convert to camelCase
- `stps_to_pascal_case(text)` - Convert to PascalCase
- `stps_to_kebab_case(text)` - Convert to kebab-case
- `stps_to_const_case(text)` - Convert to CONST_CASE
- `stps_to_sentence_case(text)` - Convert to Sentence case
- `stps_to_title_case(text)` - Convert to Title Case

### Text Normalization Functions
- `stps_remove_accents(text, [keep_umlauts])` - Remove accents from text
- `stps_restore_umlauts(text)` - Restore German umlauts
- `stps_clean_string(text)` - Clean and normalize string
- `stps_normalize(text)` - Full text normalization

### IBAN Validation Functions
- `stps_is_valid_iban(iban)` - Validate IBAN format
- `stps_format_iban(iban)` - Format IBAN with spaces
- `stps_get_iban_country_code(iban)` - Extract country code
- `stps_get_iban_check_digits(iban)` - Extract check digits
- `stps_get_bban(iban)` - Extract BBAN

### UUID Functions
- `stps_uuid()` - Generate random UUID v4
- `stps_uuid_from_string(text)` - Generate deterministic UUID v5
- `stps_get_guid(...)` - Generate GUID from parts
- `stps_guid_to_path(guid)` - Convert GUID to folder path

### Null Handling Functions
- `stps_map_empty_to_null(text)` - Convert empty strings to NULL
- `stps_map_null_to_empty(text)` - Convert NULL to empty string

### I/O Operations
- `stps_copy_io(source, dest)` - Copy file or directory
- `stps_move_io(source, dest)` - Move file or directory
- `stps_delete_io(path)` - Delete file or directory
- `stps_io_rename(old_name, new_name)` - Rename file or directory

### File System Table Functions
- `stps_scan(path, [recursive], [pattern])` - Scan directory contents
- `stps_path(base_path, ...)` - Build and list file paths
- `stps_read_folders(path)` - Read folder structure

### XML Functions
- `stps_read_xml(filepath)` - Read XML file as JSON string
- `stps_read_xml_json(filepath)` - Read XML file as DuckDB JSON

### GOBD Functions
- `stps_read_gobd(index_path, table_name, [delimiter])` - Read GOBD files

### Smart Cast Functions

Smart cast provides automatic type detection and casting for VARCHAR columns. It supports:
- **Boolean**: true/false, yes/no, ja/nein, 1/0
- **Integer**: Numbers with locale-aware thousands separators
- **Double**: Decimal numbers in German (1.234,56) or US (1,234.56) format
- **Date**: Multiple formats including ISO, German dot (15.01.2024), US slash (01/15/2024)
- **Timestamp**: Date + time combinations
- **UUID**: Standard UUID format

#### Scalar Function

```sql
-- Auto-detect type and cast
SELECT stps_smart_cast('123');           -- Returns: 123 (detected as INTEGER)
SELECT stps_smart_cast('1.234,56');      -- Returns: 1234.56 (German DOUBLE)
SELECT stps_smart_cast('2024-01-15');    -- Returns: 2024-01-15 (DATE)
SELECT stps_smart_cast('true');          -- Returns: true (BOOLEAN)

-- Cast to explicit type
SELECT stps_smart_cast('1.234,56', 'DOUBLE');   -- Force DOUBLE parsing
SELECT stps_smart_cast('123', 'INTEGER');       -- Force INTEGER parsing
```

#### Table Functions

```sql
-- Analyze a table to see detected types for each column
SELECT * FROM stps_smart_cast_analyze('my_table');
-- Returns: column_name, original_type, detected_type, total_rows, null_count,
--          cast_success_count, cast_failure_count

-- Cast all VARCHAR columns in a table to their detected types
SELECT * FROM stps_smart_cast('my_table');
-- Returns the table with columns cast to detected types
```

#### Named Parameters

Both table functions support optional named parameters:

```sql
-- Set minimum success rate for type detection (default: 0.9 = 90%)
SELECT * FROM stps_smart_cast('my_table', min_success_rate := 0.8);

-- Force German locale for number parsing
SELECT * FROM stps_smart_cast('my_table', locale := 'de');

-- Force US locale for number parsing
SELECT * FROM stps_smart_cast('my_table', locale := 'us');

-- Force date format: 'dmy' (European), 'mdy' (US), or 'ymd' (ISO)
SELECT * FROM stps_smart_cast('my_table', date_format := 'mdy');

-- Combine parameters
SELECT * FROM stps_smart_cast_analyze('my_table',
    min_success_rate := 0.95,
    locale := 'de',
    date_format := 'dmy'
);
```

#### Example Workflow

```sql
-- 1. Create a table with string data
CREATE TABLE raw_data AS SELECT
    '123' as amount,
    'true' as active,
    '2024-01-15' as created_date,
    '1.234,56' as price;

-- 2. Analyze to see what types are detected
SELECT * FROM stps_smart_cast_analyze('raw_data');
-- Shows: amount->INTEGER, active->BOOLEAN, created_date->DATE, price->DOUBLE

-- 3. Cast the table to proper types
CREATE TABLE typed_data AS SELECT * FROM stps_smart_cast('raw_data');

-- 4. Verify the types
DESCRIBE typed_data;
-- amount: BIGINT, active: BOOLEAN, created_date: DATE, price: DOUBLE
```

## Building from Source

If you want to build the extension yourself:

### Prerequisites
- CMake 3.15+
- C++17 compatible compiler
- Git

### Build Steps

```bash
# Clone the repository
git clone https://github.com/Arengard/stps-extension.git
cd stps-extension

# Initialize submodules
git submodule update --init --recursive

# Build the extension
make release
```

### Windows Build
```batch
# Windows Command Prompt or PowerShell
git submodule update --init --recursive
make release
```

The built extension will be in `build/release/extension/stps/stps.duckdb_extension`

## Automated Builds

Every push to any branch automatically builds Windows binaries via GitHub Actions. Artifacts are retained for:
- **90 days** for commit-specific builds
- **30 days** for latest branch builds

Tagged releases (e.g., `v1.0.0`) create permanent GitHub releases with attached binaries.

## Development

### Running Tests

```bash
# Run extension tests
make test
```

### Project Structure

```
stps-extension/
├── src/                          # Source files
│   ├── stps_unified_extension.cpp  # Main extension entry
│   ├── case_transform.cpp        # Text transformations
│   ├── iban_validation.cpp       # IBAN validation
│   ├── uuid_functions.cpp        # UUID operations
│   ├── filesystem_functions.cpp  # File operations
│   ├── smart_cast_utils.cpp      # Smart cast parsing utilities
│   ├── smart_cast_scalar.cpp     # Smart cast scalar function
│   └── smart_cast_function.cpp   # Smart cast table functions
├── test/sql/                     # SQL test files
├── CMakeLists.txt               # Build configuration
└── .github/workflows/           # CI/CD workflows
```

## Compatibility

- **DuckDB Version**: v1.4.3
- **Platform**: Windows x64 (binaries provided)
- **Build System**: Linux, macOS, Windows (source builds)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `make test`
5. Submit a pull request

## License

See LICENSE file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/Arengard/stps-extension/issues)
- **Documentation**: See test files in `test/sql/` for usage examples

## Version History

- **Latest**: Automated builds on every commit
- Check [GitHub Releases](https://github.com/Arengard/stps-extension/releases) for tagged versions
