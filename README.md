# STPS DuckDB Extension

A comprehensive DuckDB extension providing 50+ functions for data transformation, validation, file operations, German business data processing, and **AI-powered data enhancement with Anthropic Claude**.

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

**Features:**
- ✅ Automatic rate limiting (2 seconds between requests)
- ✅ In-memory caching (1 hour TTL, up to 1000 entries)
- ✅ Realistic browser headers to avoid bot detection
- ✅ Automatic retry with fallback to wget

```sql
SELECT * FROM (SELECT stps_get_address('Deutsche Bank AG') AS addr);
-- Returns: {city, plz, address, street_name, street_number}

-- Works with multiple companies (cached for performance)
SELECT
    company_name,
    (stps_get_address(company_name)).city AS city,
    (stps_get_address(company_name)).plz AS plz
FROM companies;

-- Note: First call takes 4-6 seconds (web scraping + rate limit)
--       Subsequent calls for same company are instant (cached)
```

**Requirements:**
- Internet connection
- `curl` or `wget` installed
- Respectful use (rate limited to avoid Google blocks)

---

### 🤖 AI Functions (Anthropic Claude Integration)

#### `stps_ask_ai_address(company_name VARCHAR[, model VARCHAR]) → STRUCT`

Get structured address data using AI - returns organized address components.

**Quick Example:**
```sql
-- Set API key first
SELECT stps_set_api_key('sk-your-key-here');

-- Get structured address
SELECT stps_ask_ai_address('Tax Network GmbH');
-- Returns: {city: 'München', postal_code: '80331', street_name: 'Musterstraße', street_nr: '123'}

-- Access individual fields
SELECT
    company_name,
    (stps_ask_ai_address(company_name)).city AS city,
    (stps_ask_ai_address(company_name)).postal_code AS plz,
    (stps_ask_ai_address(company_name)).street_name AS street,
    (stps_ask_ai_address(company_name)).street_nr AS nr
FROM companies;

-- Use Claude Opus for better accuracy
SELECT stps_ask_ai_address('Deutsche Bank AG', 'claude-opus-4-5-20251101');
```

**Returns STRUCT with:**
- `city` - City name
- `postal_code` - Postal/ZIP code
- `street_name` - Street name
- `street_nr` - Street number

---

#### `stps_ask_ai(context VARCHAR, prompt VARCHAR[, model VARCHAR][, max_tokens INTEGER]) → VARCHAR`

Query Anthropic's Claude directly from SQL for data enhancement, classification, summarization, and more.

**Quick Start:**
```sql
-- 1. Set your Anthropic API key (get from console.anthropic.com)
SELECT stps_set_api_key('sk-ant-your-api-key-here');

-- 2. Query Claude
SELECT stps_ask_ai('Tax Network GmbH', 'What industry is this company in?');
-- Returns: "Tax Network GmbH operates in the financial services and tax consulting industry..."

-- 3. Use with your data
SELECT
    company_name,
    stps_ask_ai(
        company_name,
        'Classify this company into one category: Technology, Finance, Manufacturing, Retail, or Other'
    ) AS industry
FROM companies;
```

**Parameters:**
- `context` - Background data or information
- `prompt` - Your question or instruction
- `model` - Optional: 'claude-sonnet-4-5-20250929' (default), 'claude-3-7-sonnet-20250219', 'claude-opus-4-5-20251101'
- `max_tokens` - Optional: Max response length (default: 1000)

**Common Use Cases:**
```sql
-- Data Classification
SELECT stps_ask_ai(product_name, 'Is this a food or drink item? Answer with one word.');

-- Text Summarization
SELECT stps_ask_ai(long_description, 'Summarize in one sentence (max 15 words).');

-- Sentiment Analysis
SELECT stps_ask_ai(review_text, 'Sentiment: POSITIVE, NEGATIVE, or NEUTRAL?', 'claude-3-7-sonnet-20250219', 10);

-- Data Validation
SELECT stps_ask_ai(email, 'Is this email format valid? YES or NO only.');

-- Translation
SELECT stps_ask_ai(text_de, 'Translate to English:', 'claude-sonnet-4-5-20250929', 500);

-- Using Claude Opus for complex analysis
SELECT stps_ask_ai(
    'Revenue 2023: $5M, Revenue 2022: $3M, Margin: 25%',
    'Analyze financial health (2 sentences).',
    'claude-opus-4-5-20251101',
    150
);
```

#### `stps_set_api_key(api_key VARCHAR) → VARCHAR`

Configure Anthropic API key for the session.

**API Key Configuration Options:**
```sql
-- Option 1: Set for current session
SELECT stps_set_api_key('sk-ant-...');

-- Option 2: Environment variable (recommended for production)
-- In terminal: export ANTHROPIC_API_KEY='sk-ant-...'

-- Option 3: Config file (recommended for persistent use)
-- Create: ~/.stps/anthropic_api_key with your key
```

**📖 Full Documentation:** See [AI_FUNCTIONS_GUIDE.md](AI_FUNCTIONS_GUIDE.md) for:
- Detailed examples (9+ use cases)
- Cost optimization tips
- Best practices & prompt engineering
- Error handling & troubleshooting
- Security recommendations
- Token pricing & cost calculator

**Requirements:**
- Anthropic API key ([get one here](https://console.anthropic.com))
- Internet connection
- `curl` installed
- Anthropic account with credits

**Cost Example:** Processing 1,000 rows with claude-sonnet-4-5-20250929 ≈ $0.34

#### Brave Search Integration

Enable Claude to search the web for current information:

```sql
-- Configure Brave API key (get from https://brave.com/search/api/)
SELECT stps_set_brave_api_key('BSA-your-key-here');

-- Now queries automatically search when needed
SELECT stps_ask_ai('TSLA', 'What is the current stock price?');
-- Claude will search the web and return current price

SELECT stps_ask_ai('Germany', 'Who is the current chancellor?');
-- Searches for up-to-date political information

-- Works with address lookups too
SELECT stps_ask_ai_address('Anthropic PBC');
-- Searches for current Anthropic address, returns structured data
```

**Brave API Key Configuration:**
```sql
-- Option 1: SQL function
SELECT stps_set_brave_api_key('BSA-...');

-- Option 2: Environment variable
-- export BRAVE_API_KEY='BSA-...'

-- Option 3: Config file
-- echo "BSA-..." > ~/.stps/brave_api_key
```

**Cost:** Queries using web search cost ~2x (two Claude API calls + Brave search at $0.003)

**Free Tier:** Brave provides 2,000 searches/month free

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

#### `stps_uuid() → UUID`
Generate random UUID v4.
```sql
SELECT stps_uuid() AS id;
-- Result: 550e8400-e29b-41d4-a716-446655440000 (UUID type)
```

#### `stps_uuid_from_string(text VARCHAR) → UUID`
Generate deterministic UUID v5 from string.
```sql
SELECT stps_uuid_from_string('my-unique-key') AS id;
-- Result: always same UUID for same input (UUID type)
```

#### `stps_get_guid(col1 VARCHAR, col2 VARCHAR, ...) → UUID`
Generate deterministic UUID from multiple columns (composite key).
```sql
SELECT stps_get_guid('customer', 'order123', '2024-01-15') AS guid;
-- Result: deterministic UUID based on all inputs (UUID type)
```

#### `stps_guid_to_path(guid UUID) → VARCHAR`
Convert GUID/UUID to 4-level folder path (0-255 decimal values).
```sql
SELECT stps_guid_to_path('550e8400-e29b-41d4-a716-446655440000'::UUID) AS path;
-- Result: '85/14/132/0'

-- Also accepts VARCHAR input
SELECT stps_guid_to_path('550e8400-e29b-41d4-a716-446655440000') AS path;
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

#### `stps_search_columns(table_name VARCHAR, pattern VARCHAR) → TABLE`
Search for pattern matches in data values across all columns of a table.

**Breaking Change:** This function previously searched column names. It now searches data values.

```sql
-- Find all rows where any column contains 'Hoeger'
SELECT * FROM stps_search_columns('customers', '%Hoeger%');
-- Returns: all original columns + matched_columns (VARCHAR[])

-- Find rows with numeric value 123 in any column
SELECT * FROM stps_search_columns('orders', '%123%');

-- Case-insensitive pattern matching (always)
SELECT * FROM stps_search_columns('products', '%widget%');

-- Get just the matched column names
SELECT id, name, matched_columns
FROM stps_search_columns('users', '%@gmail.com%');
```

**Parameters:**
- `table_name` - Table to search
- `pattern` - SQL LIKE pattern (% = any chars, _ = single char)

**Returns:** Table with:
- All original columns from the source table
- `matched_columns` (VARCHAR[]) - List of column names where pattern was found

**Behavior:**
- Searches across ALL columns (converts all types to VARCHAR)
- Case-insensitive matching
- NULL values are not matched
- Pattern uses SQL LIKE syntax

**Migration from old version:**
If you were using `stps_search_columns` to find columns by name, use DuckDB's built-in:
```sql
-- Old way (no longer works):
SELECT * FROM stps_search_columns('my_table', '%date%');

-- New way (find columns by name):
DESCRIBE my_table;
-- or
SELECT column_name FROM information_schema.columns
WHERE table_name = 'my_table' AND column_name LIKE '%date%';
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
