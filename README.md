# STPS DuckDB Extension

A comprehensive DuckDB extension providing 45+ functions for data transformation, validation, file operations, and German business data processing.

## 📦 Installation

### Quick Start (Prebuilt Binaries - Recommended)

1. **Download** from [GitHub Actions](https://github.com/Arengard/stps-extension/actions)
   - Click latest successful workflow (green ✅)
   - Download `stps-windows-amd64-latest-master`
   - Extract `stps.duckdb_extension`

2. **Load in DuckDB**
```sql
duckdb -unsigned
INSTALL './stps.duckdb_extension';
LOAD stps;
```

3. **Verify**
```sql
SELECT stps_is_valid_iban('DE89370400440532013000');
```

## 📚 Complete Function Reference

### 📝 Text Processing & Normalization

#### `stps_clean_string(text VARCHAR) → VARCHAR`
Remove non-printable characters, normalize whitespace, handle special Unicode characters.
```sql
SELECT stps_clean_string('Hello   World​') AS cleaned;
-- Result: 'Hello World'

SELECT stps_clean_string('Text with\t\ttabs and\n\nnewlines') AS cleaned;
-- Result: 'Text with tabs and newlines'
```

#### `stps_normalize(text VARCHAR) → VARCHAR`
Normalize text: collapse whitespace, trim, optionally lowercase.
```sql
SELECT stps_normalize('  Multiple   Spaces  ') AS normalized;
-- Result: 'Multiple Spaces'
```

#### `stps_remove_accents(text VARCHAR, keep_umlauts BOOLEAN DEFAULT false) → VARCHAR`
Remove accents from text, optionally preserve German umlauts.
```sql
SELECT stps_remove_accents('Café München', false) AS no_accents;
-- Result: 'Cafe Muenchen'

SELECT stps_remove_accents('Café München', true) AS keep_umlauts;
-- Result: 'Cafe München'
```

#### `stps_restore_umlauts(text VARCHAR) → VARCHAR`
Convert ASCII representations back to German umlauts (ae→ä, oe→ö, ue→ü, ss→ß).
```sql
SELECT stps_restore_umlauts('Muenchen') AS restored;
-- Result: 'München'

SELECT stps_restore_umlauts('Strasse') AS restored;
-- Result: 'Straße'
```

---

### 🔤 Case Transformations

#### `stps_to_snake_case(text VARCHAR) → VARCHAR`
Convert to snake_case.
```sql
SELECT stps_to_snake_case('HelloWorld') AS snake;
-- Result: 'hello_world'
```

#### `stps_to_camel_case(text VARCHAR) → VARCHAR`
Convert to camelCase.
```sql
SELECT stps_to_camel_case('hello_world') AS camel;
-- Result: 'helloWorld'
```

#### `stps_to_pascal_case(text VARCHAR) → VARCHAR`
Convert to PascalCase.
```sql
SELECT stps_to_pascal_case('hello_world') AS pascal;
-- Result: 'HelloWorld'
```

#### `stps_to_kebab_case(text VARCHAR) → VARCHAR`
Convert to kebab-case.
```sql
SELECT stps_to_kebab_case('HelloWorld') AS kebab;
-- Result: 'hello-world'
```

#### `stps_to_const_case(text VARCHAR) → VARCHAR`
Convert to CONST_CASE.
```sql
SELECT stps_to_const_case('helloWorld') AS const;
-- Result: 'HELLO_WORLD'
```

#### `stps_to_title_case(text VARCHAR) → VARCHAR`
Convert to Title Case.
```sql
SELECT stps_to_title_case('hello world') AS title;
-- Result: 'Hello World'
```

#### `stps_to_sentence_case(text VARCHAR) → VARCHAR`
Convert to Sentence case.
```sql
SELECT stps_to_sentence_case('HELLO WORLD') AS sentence;
-- Result: 'Hello world'
```

---

### ✅ Data Validation

#### `stps_is_valid_iban(iban VARCHAR) → BOOLEAN`
Validate IBAN (International Bank Account Number) using mod-97 algorithm.
```sql
SELECT stps_is_valid_iban('DE89370400440532013000') AS valid;
-- Result: true

SELECT stps_is_valid_iban('DE12345678901234567890') AS valid;
-- Result: false
```

#### `stps_is_valid_german_iban(iban VARCHAR) → BOOLEAN`
Validate German IBAN (DE country code + 20 characters).
```sql
SELECT stps_is_valid_german_iban('DE89370400440532013000') AS valid;
-- Result: true
```

#### `stps_get_iban_country_code(iban VARCHAR) → VARCHAR`
Extract country code from IBAN.
```sql
SELECT stps_get_iban_country_code('DE89370400440532013000') AS country;
-- Result: 'DE'
```

#### `stps_get_iban_check_digits(iban VARCHAR) → VARCHAR`
Extract check digits from IBAN.
```sql
SELECT stps_get_iban_check_digits('DE89370400440532013000') AS check_digits;
-- Result: '89'
```

#### `stps_get_bban(iban VARCHAR) → VARCHAR`
Extract BBAN (Basic Bank Account Number) from IBAN.
```sql
SELECT stps_get_bban('DE89370400440532013000') AS bban;
-- Result: '370400440532013000'
```

#### `stps_format_iban(iban VARCHAR) → VARCHAR`
Format IBAN with spaces (4-character groups).
```sql
SELECT stps_format_iban('DE89370400440532013000') AS formatted;
-- Result: 'DE89 3704 0044 0532 0130 00'
```

#### `stps_is_valid_plz(plz VARCHAR) → BOOLEAN`
Validate German postal code (5 digits).
```sql
SELECT stps_is_valid_plz('10115') AS valid;
-- Result: true

SELECT stps_is_valid_plz('123') AS valid;
-- Result: false
```

#### `stps_validate_account_number(account VARCHAR, bank_code VARCHAR) → BOOLEAN`
Validate German bank account number using check digit algorithms.
```sql
SELECT stps_validate_account_number('532013000', '37040044') AS valid;
```

#### `stps_validate_account_result(account VARCHAR, bank_code VARCHAR) → VARCHAR`
Get detailed validation result.
```sql
SELECT stps_validate_account_result('532013000', '37040044') AS result;
-- Result: 'VALID' or 'INVALID'
```

---

### 🏠 Address Processing

#### `stps_split_street(address VARCHAR) → STRUCT(street_name VARCHAR, street_number VARCHAR)`
Split German street address into name and number. Abbreviates compound street names.
```sql
SELECT (stps_split_street('Siemensstraße 2')).street_name AS street,
       (stps_split_street('Siemensstraße 2')).street_number AS number;
-- street: 'Siemensstr.', number: '2'

SELECT (stps_split_street('Lange Straße 15')).street_name AS street;
-- street: 'Lange Straße' (not abbreviated - separate words)

SELECT * FROM (SELECT stps_split_street('Hauptstraße 100') AS addr);
-- Returns: {street_name: 'Hauptstr.', street_number: '100'}
```

#### `stps_get_address(company_name VARCHAR) → STRUCT(...)`
Look up company address via Google Impressum search (requires internet).
```sql
SELECT * FROM (SELECT stps_get_address('Deutsche Bank AG') AS addr);
-- Returns: {city, plz, address, street_name, street_number}
```

---

### 🔢 Smart Type Casting

#### `stps_smart_cast(value VARCHAR) → <detected type>`
Automatically detect and cast VARCHAR to appropriate type (BOOLEAN, INTEGER, DOUBLE, DATE, TIMESTAMP, UUID, VARCHAR).
```sql
SELECT stps_smart_cast('123') AS value;
-- Result: 123 (INTEGER)

SELECT stps_smart_cast('123.45') AS value;
-- Result: 123.45 (DOUBLE)

SELECT stps_smart_cast('2024-01-15') AS value;
-- Result: 2024-01-15 (DATE)

SELECT stps_smart_cast('true') AS value;
-- Result: true (BOOLEAN)

SELECT stps_smart_cast('15.01.2024') AS value;
-- Result: 2024-01-15 (DATE, German format)

SELECT stps_smart_cast('1.234,56') AS value;
-- Result: 1234.56 (DOUBLE, German number format)
```

#### `stps_smart_cast_analyze(table_name VARCHAR) → TABLE`
Analyze a table and recommend type conversions for VARCHAR columns.
```sql
CREATE TABLE my_data AS SELECT '123' as col1, '2024-01-15' as col2;

SELECT * FROM stps_smart_cast_analyze('my_data');
-- Returns: column_name, detected_type, total_rows, null_count, cast_success_count, cast_failure_count
```

---

### 🆔 UUID Functions

#### `stps_uuid() → VARCHAR`
Generate random UUID v4.
```sql
SELECT stps_uuid() AS id;
-- Result: '550e8400-e29b-41d4-a716-446655440000'
```

#### `stps_uuid_from_string(text VARCHAR) → VARCHAR`
Generate deterministic UUID v5 from string.
```sql
SELECT stps_uuid_from_string('my-unique-key') AS id;
-- Result: always same UUID for same input
```

#### `stps_get_guid(col1 VARCHAR, col2 VARCHAR, ...) → VARCHAR`
Generate deterministic UUID from multiple columns (composite key).
```sql
SELECT stps_get_guid('customer', 'order123', '2024-01-15') AS guid;
-- Result: deterministic UUID based on all inputs
```

#### `stps_guid_to_path(guid VARCHAR) → VARCHAR`
Convert GUID to 4-level folder path (0-255 decimal values).
```sql
SELECT stps_guid_to_path('550e8400-e29b-41d4-a716-446655440000') AS path;
-- Result: '85/14/132/0'
```

---

### 🗃️ Archive Functions (ZIP & 7-Zip)

#### `stps_zip(archive_path VARCHAR, inner_filename VARCHAR) → TABLE`
Extract and parse CSV/TXT file from ZIP archive.
```sql
SELECT * FROM stps_zip('data.zip', 'customers.csv');
-- Returns: parsed CSV as table with auto-detected columns

SELECT * FROM stps_zip('C:/data/archive.zip', 'report.txt');
```

#### `stps_view_zip(archive_path VARCHAR) → TABLE`
List files in ZIP archive.
```sql
SELECT * FROM stps_view_zip('data.zip');
-- Returns: filename, uncompressed_size, is_directory, index
```

#### `stps_7zip(archive_path VARCHAR, inner_filename VARCHAR) → TABLE`
Extract and parse CSV/TXT file from 7-Zip archive.
```sql
SELECT * FROM stps_7zip('data.7z', 'customers.csv');
-- Returns: parsed CSV as table

-- Auto-detect first file
SELECT * FROM stps_7zip('data.7z');
```

#### `stps_view_7zip(archive_path VARCHAR) → TABLE`
List files in 7-Zip archive.
```sql
SELECT * FROM stps_view_7zip('data.7z');
-- Returns: filename, uncompressed_size, is_directory, index
```

---

### 📁 Filesystem Functions

#### `stps_path(path VARCHAR) → TABLE`
Recursively scan directory and return file paths.
```sql
SELECT * FROM stps_path('C:/data/');
-- Returns: full_path for each file

SELECT * FROM stps_path('/home/user/documents/');
```

#### `stps_read_folders(path VARCHAR) → TABLE`
List directories only (non-recursive).
```sql
SELECT * FROM stps_read_folders('C:/data/');
-- Returns: folder_name, full_path
```

#### `stps_scan(path VARCHAR) → TABLE`
Scan directory structure.
```sql
SELECT * FROM stps_scan('C:/data/');
```

#### `stps_copy_io(source VARCHAR, destination VARCHAR) → VARCHAR`
Copy file.
```sql
SELECT stps_copy_io('data.csv', 'backup/data.csv') AS result;
-- Result: 'SUCCESS' or error message
```

#### `stps_move_io(source VARCHAR, destination VARCHAR) → VARCHAR`
Move file.
```sql
SELECT stps_move_io('old_path.csv', 'new_path.csv') AS result;
```

#### `stps_io_rename(old_path VARCHAR, new_path VARCHAR) → VARCHAR`
Rename file.
```sql
SELECT stps_io_rename('old_name.csv', 'new_name.csv') AS result;
```

#### `stps_delete_io(path VARCHAR) → VARCHAR`
Delete file.
```sql
SELECT stps_delete_io('temp_file.csv') AS result;
```

---

### 📄 XML Parsing

#### `stps_read_xml(file_path VARCHAR) → VARCHAR`
Read XML file as text.
```sql
SELECT stps_read_xml('data.xml') AS content;
```

#### `stps_read_xml_json(file_path VARCHAR) → JSON`
Parse XML file and convert to JSON.
```sql
SELECT stps_read_xml_json('data.xml') AS json_data;
```

---

### 📊 GOBD Functions (German Business Data)

#### `stps_read_gobd(file_path VARCHAR) → TABLE`
Parse GoBD (Grundsätze zur ordnungsmäßigen Führung und Aufbewahrung von Büchern) XML files.
```sql
SELECT * FROM stps_read_gobd('index.xml');
-- Returns: parsed business data according to GoBD standard
```

#### `gobd_list_tables(file_path VARCHAR) → TABLE`
List tables defined in GoBD file.
```sql
SELECT * FROM gobd_list_tables('index.xml');
-- Returns: name, url, description, column_count
```

#### `gobd_table_schema(file_path VARCHAR, table_name VARCHAR) → TABLE`
Get schema of specific GoBD table.
```sql
SELECT * FROM gobd_table_schema('index.xml', 'transactions');
-- Returns: column_name, data_type, description
```

---

### 🔄 NULL Handling

#### `stps_map_null_to_empty(value VARCHAR) → VARCHAR`
Convert NULL to empty string.
```sql
SELECT stps_map_null_to_empty(NULL) AS result;
-- Result: ''

SELECT stps_map_null_to_empty('text') AS result;
-- Result: 'text'
```

#### `stps_map_empty_to_null(value VARCHAR) → VARCHAR`
Convert empty string to NULL.
```sql
SELECT stps_map_empty_to_null('') AS result;
-- Result: NULL

SELECT stps_map_empty_to_null('text') AS result;
-- Result: 'text'
```

#### `stps_drop_null_columns(table_name VARCHAR) → TABLE`
Remove columns that are entirely NULL.
```sql
CREATE TABLE test AS SELECT 1 as a, NULL as b, 2 as c;
SELECT * FROM stps_drop_null_columns('test');
-- Returns: only columns 'a' and 'c'
```

---

### 🔧 Advanced Functions

#### `stps_lambda(input ARRAY, function VARCHAR) → ARRAY`
Apply lambda function to array elements.
```sql
SELECT stps_lambda([1, 2, 3], 'x -> x * 2') AS doubled;
-- Result: [2, 4, 6]
```

---

## 🚀 Common Use Cases

### Data Cleaning Pipeline
```sql
-- Clean and standardize text data
SELECT
    stps_clean_string(raw_text) AS cleaned,
    stps_normalize(raw_text) AS normalized,
    stps_remove_accents(raw_text, true) AS no_accents
FROM raw_data;
```

### IBAN Validation & Formatting
```sql
-- Validate and format IBANs
SELECT
    iban,
    stps_is_valid_iban(iban) AS is_valid,
    stps_format_iban(iban) AS formatted,
    stps_get_iban_country_code(iban) AS country
FROM accounts
WHERE stps_is_valid_iban(iban);
```

### Type Detection & Casting
```sql
-- Automatically detect types
CREATE TABLE cleaned AS
SELECT
    stps_smart_cast(col1) AS col1_typed,
    stps_smart_cast(col2) AS col2_typed,
    stps_smart_cast(col3) AS col3_typed
FROM raw_varchar_table;
```

### Archive Processing
```sql
-- Extract CSV from 7z archive
SELECT customer_id, amount, date
FROM stps_7zip('monthly_reports.7z', 'january.csv')
WHERE amount > 1000;
```

### Address Parsing
```sql
-- Parse German addresses
SELECT
    address,
    (stps_split_street(address)).street_name AS street,
    (stps_split_street(address)).street_number AS number
FROM locations;
```

---

## 📖 Documentation

- **[Complete Function List](STPS_FUNCTIONS.md)** - Detailed function reference
- **[Troubleshooting 7-Zip](TROUBLESHOOTING_7ZIP.md)** - Common 7z issues
- **[How to Update](HOW_TO_UPDATE_EXTENSION.md)** - Get latest version
- **[Build Process](BUILD_PROCESS.md)** - GitHub Actions build info

---

## ⚙️ Requirements

- **DuckDB** 0.10.0 or later
- **Windows** (x64)
- **Unsigned mode** for loading extensions: `duckdb -unsigned`

---

## 🐛 Troubleshooting

### Function Not Found
```sql
-- Make sure extension is loaded
LOAD stps;

-- Check loaded functions
SELECT * FROM duckdb_functions() WHERE function_name LIKE 'stps_%';
```

### Old Version Issues
If you see outdated error messages, download the latest build from GitHub Actions:
https://github.com/Arengard/stps-extension/actions

### Path Issues on Windows
Use forward slashes or escape backslashes:
```sql
-- Good: forward slashes
SELECT * FROM stps_7zip('C:/data/archive.7z', 'file.csv');

-- Good: escaped backslashes
SELECT * FROM stps_7zip('C:\\data\\archive.7z', 'file.csv');

-- Bad: unescaped backslashes
SELECT * FROM stps_7zip('C:\data\archive.7z', 'file.csv');  -- ERROR
```

---

## 🤝 Contributing

Issues and pull requests welcome!
https://github.com/Arengard/stps-extension/issues

---

## 📝 License

See [LICENSE](LICENSE) file.

---

## 🔗 Links

- **GitHub Repository**: https://github.com/Arengard/stps-extension
- **Latest Builds**: https://github.com/Arengard/stps-extension/actions
- **DuckDB**: https://duckdb.org/
