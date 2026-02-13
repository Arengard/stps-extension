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

#### stps_ask_ai_address - Business Address Lookup

Get structured business address data with automatic web search:

```sql
-- Configure API keys (Brave key enables web search)
SELECT stps_set_api_key('sk-ant-...');
SELECT stps_set_brave_api_key('BSA-...');  -- Optional but recommended

-- Lookup address (automatically searches web if Brave key configured)
SELECT stps_ask_ai_address('STP Solution GmbH');
-- Returns: {city: Karlsruhe, postal_code: 76135, street_name: Brauerstraße, street_nr: 12}

-- Extract specific fields
SELECT
    company,
    (stps_ask_ai_address(company)).city,
    (stps_ask_ai_address(company)).postal_code
FROM companies;

-- Batch address enrichment
UPDATE companies
SET
    city = (stps_ask_ai_address(company_name)).city,
    postal_code = (stps_ask_ai_address(company_name)).postal_code,
    street = (stps_ask_ai_address(company_name)).street_name,
    street_nr = (stps_ask_ai_address(company_name)).street_nr
WHERE address_missing = true;
```

**How it works:**
- Makes 2 Claude API calls per lookup:
  1. Search for address (with web search if Brave key configured)
  2. Parse natural language into structured format
- With Brave API key: Searches business registries and official sources
- Without Brave API key: Uses Claude's knowledge (may be outdated)
- Returns structured data ready for database storage
- NULL for fields that cannot be determined

**Cost:** ~$0.002 per lookup (2 API calls)
**Latency:** 3-5 seconds per address

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

**📖 Full Documentation:** See [AI_FUNCTIONS_GUIDE.md](howToUseMD/AI_FUNCTIONS_GUIDE.md) for:
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

#### `dguid(json JSON) → UUID`
Generate deterministic UUID from JSON data. Useful for creating stable IDs from row data.
```sql
-- From JSON object (keys are sorted for deterministic output)
SELECT dguid('{"name": "John", "id": 123}'::JSON) AS guid;
-- Result: always same UUID for same JSON content

-- Works with to_json() for row-based GUIDs
SELECT dguid(to_json(t.*)) AS row_guid, * FROM my_table t;

-- Create deterministic GUID from specific columns
SELECT dguid(to_json({id: id, name: name})) AS guid FROM customers;

-- Also accepts VARCHAR (parsed as JSON)
SELECT dguid('{"a": 1, "b": 2}') AS guid;
```

**Key Features:**
- Deterministic: same JSON content always produces same UUID
- Key-order independent: `{"a":1,"b":2}` equals `{"b":2,"a":1}`
- Handles nested objects and arrays
- NULL values are ignored (like stps_get_guid)

---

### 🗃️ Archive Functions (ZIP)

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

---

### 📁 Filesystem Functions

#### `stps_path(path VARCHAR [, named params]) → TABLE`
Scan directory and return file information.

**Returns:** `name`, `path`, `type`, `size`, `modified_time`, `extension`, `parent_directory`

**Named Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `recursive` | BOOLEAN | false | Scan subdirectories |
| `file_type` | VARCHAR | | Filter by extension (e.g. `'csv'`) |
| `pattern` | VARCHAR | | Glob pattern for filenames |
| `max_depth` | INTEGER | | Max recursion depth |
| `include_hidden` | BOOLEAN | false | Include hidden files |

```sql
SELECT * FROM stps_path('C:/data/');
-- Returns: name, path, type, size, modified_time, extension, parent_directory

SELECT name, path, size FROM stps_path('.', recursive := true, file_type := 'csv');
```

#### `stps_scan(path VARCHAR [, named params]) → TABLE`
Advanced directory scan with size, date, and content filtering.

**Returns:** `name`, `path`, `type`, `size`, `modified_time`, `extension`, `parent_directory`

**Named Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `recursive` | BOOLEAN | false | Scan subdirectories |
| `file_type` | VARCHAR | | Filter by extension |
| `pattern` | VARCHAR | | Glob pattern for filenames |
| `max_depth` | INTEGER | | Max recursion depth |
| `include_hidden` | BOOLEAN | false | Include hidden files |
| `min_size` | BIGINT | | Minimum file size in bytes |
| `max_size` | BIGINT | | Maximum file size in bytes |
| `min_date` | BIGINT | | Minimum modified time (Unix timestamp) |
| `max_date` | BIGINT | | Maximum modified time (Unix timestamp) |
| `content_search` | VARCHAR | | Search inside file contents |

```sql
SELECT * FROM stps_scan('C:/data/');

-- Find large CSV files
SELECT name, path, size
FROM stps_scan('.', recursive := true, file_type := 'csv', min_size := 1000000);

-- Search file contents
SELECT name, path
FROM stps_scan('.', content_search := 'TODO', file_type := 'cpp');
```

#### `stps_read_folders(path VARCHAR) → TABLE`
List directories only (non-recursive).
```sql
SELECT * FROM stps_read_folders('C:/data/');
-- Returns: folder_name, full_path
```

#### `stps_copy_io(source VARCHAR, destination VARCHAR) → VARCHAR`
Copy file. Creates parent directories if needed.
```sql
SELECT stps_copy_io('data.csv', 'backup/data.csv') AS result;
-- Result: 'SUCCESS: Copied data.csv to backup/data.csv'
```

#### `stps_move_io(source VARCHAR, destination VARCHAR) → VARCHAR`
Move file. Creates parent directories if needed.
```sql
SELECT stps_move_io('old_path.csv', 'new_path.csv') AS result;
```

#### `stps_rename_io(old_path VARCHAR, new_path VARCHAR) → VARCHAR`
Rename file. Fails if destination already exists.
```sql
SELECT stps_rename_io('old_name.csv', 'new_name.csv') AS result;
```

#### `stps_delete_io(path VARCHAR) → VARCHAR`
Delete a file or folder (recursively deletes all subfolders and contents). Handles read-only files on Windows.
```sql
-- Delete a file
SELECT stps_delete_io('temp_file.csv') AS result;

-- Delete a folder and all its contents
SELECT stps_delete_io('C:/data/old_export') AS result;
-- Result: 'SUCCESS: Deleted folder and all contents: C:/data/old_export'
```

#### `stps_create_folders_io(folder_path VARCHAR) → VARCHAR`
Create a folder (and all parent directories recursively). Returns WARNING if folder already exists.
```sql
SELECT stps_create_folders_io('C:/data/new_folder') AS result;
-- Result: 'SUCCESS: Created folder: C:/data/new_folder'
-- If exists: 'WARNING: Folder already exists: C:/data/new_folder'
```

**Batch folder creation from data:**
```sql
-- Create folders from Excel data
WITH data AS (
    SELECT stps_to_snake_case(concat_ws(' ', kz, gesellschaft)) AS ordner_name
    FROM read_xlsx('companies.xlsx', all_varchar := true)
)
SELECT stps_create_folders_io('C:/output/' || ordner_name) AS result
FROM data;
```

#### Combining scan + IO for batch operations

Use `stps_scan` with IO functions to perform bulk file operations:

```sql
-- Batch rename: remove 'abc' from all filenames
SELECT stps_rename_io(path, parent_directory || '/' || replace(name, 'abc', ''))
FROM stps_scan('.')
WHERE name LIKE '%abc%';

-- Move all CSV files into a subfolder
SELECT stps_move_io(path, 'csv_files/' || name)
FROM stps_scan('.', file_type := 'csv');

-- Copy large files to backup
SELECT stps_copy_io(path, '/backup' || path)
FROM stps_scan('.', recursive := true, min_size := 10000000);
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

#### `stps_read_gobd(file_path VARCHAR, table_name VARCHAR, delimiter := VARCHAR) → TABLE`
Read a single table from a GoBD/GDPDU export. Requires specifying which table to read.
```sql
SELECT * FROM stps_read_gobd('index.xml', 'Buchungsstapel');
-- Returns: all columns of the specified table as VARCHAR
```

#### `stps_read_gobd_all(file_path VARCHAR, delimiter := VARCHAR, overwrite := BOOLEAN) → TABLE`
Import **all tables** from a local GoBD/GDPDU export into the database. Creates one DuckDB table per GoBD source table with:
- **Normalized column names** (snake_case, lowercase)
- **Empty columns removed** (all NULL/empty values)
- **Smart type casting** (auto-detects integers, decimals, dates)

Returns a summary of what was created.
```sql
-- Import all tables from a GoBD export
SELECT * FROM stps_read_gobd_all('C:/exports/datev/index.xml');
-- Returns: table_name, rows_imported, columns_created, error

-- Re-import with overwrite
SELECT * FROM stps_read_gobd_all('C:/exports/datev/index.xml', overwrite := true);

-- Then query the created tables directly
SELECT * FROM buchungsstapel;
SELECT * FROM sachkontenplan;
```

#### `stps_gobd_list_tables(file_path VARCHAR) → TABLE`
List tables defined in GoBD file.
```sql
SELECT * FROM stps_gobd_list_tables('index.xml');
-- Returns: name, url, description, column_count
```

#### `stps_gobd_table_schema(file_path VARCHAR, table_name VARCHAR) → TABLE`
Get schema of specific GoBD table.
```sql
SELECT * FROM stps_gobd_table_schema('index.xml', 'transactions');
-- Returns: column_name, data_type, description
```

#### `stps_read_gobd_cloud(url VARCHAR, table_name VARCHAR, username := VARCHAR, password := VARCHAR, delimiter := VARCHAR) → TABLE`
Read a single table from a GoBD/GDPDU export stored on a WebDAV/Nextcloud server. Automatically discovers `index.xml` in the given folder (tries direct path, PROPFIND listing, and one level of subfolders).
```sql
-- Read Buchungsstapel from a cloud export
SELECT * FROM stps_read_gobd_cloud(
  'https://cloud.example.com/remote.php/dav/files/user/mandant1/export/',
  'Buchungsstapel',
  username := 'myuser',
  password := 'mypassword'
);

-- With custom delimiter
SELECT * FROM stps_read_gobd_cloud(
  'https://cloud.example.com/remote.php/dav/files/user/export/',
  'Kontenbeschriftungen',
  username := 'myuser',
  password := 'mypassword',
  delimiter := ','
);
```

#### `stps_read_gobd_cloud_folder(url VARCHAR, table_name VARCHAR, child_folder := VARCHAR, username := VARCHAR, password := VARCHAR, delimiter := VARCHAR) → TABLE`
Scan multiple mandant subfolders under a parent URL, read the same GoBD table from each, and return a unified table with `parent_folder` and `child_folder` metadata columns. Silently skips mandants where `index.xml` or the requested table is missing.
```sql
-- Read Buchungsstapel from all mandant folders
SELECT * FROM stps_read_gobd_cloud_folder(
  'https://cloud.example.com/remote.php/dav/files/user/mandanten/',
  'Buchungsstapel',
  child_folder := 'export',
  username := 'myuser',
  password := 'mypassword'
);

-- See which mandants were found
SELECT DISTINCT parent_folder
FROM stps_read_gobd_cloud_folder(
  'https://cloud.example.com/remote.php/dav/files/user/mandanten/',
  'Buchungsstapel',
  child_folder := 'export',
  username := 'myuser',
  password := 'mypassword'
);
```

**Returns:** `parent_folder`, `child_folder`, plus all data columns from the GoBD table (all VARCHAR).

#### `stps_stps_gobd_list_tables_cloud(url VARCHAR, username := VARCHAR, password := VARCHAR) → TABLE`
List all tables defined in a cloud-hosted GoBD `index.xml`.
```sql
SELECT * FROM stps_stps_gobd_list_tables_cloud(
  'https://cloud.example.com/remote.php/dav/files/user/export/',
  username := 'myuser',
  password := 'mypassword'
);
-- Returns: table_name, table_url, description, column_count
```

#### `stps_stps_gobd_table_schema_cloud(url VARCHAR, table_name VARCHAR, username := VARCHAR, password := VARCHAR) → TABLE`
Show the column schema for a specific table in a cloud-hosted GoBD `index.xml`.
```sql
SELECT * FROM stps_stps_gobd_table_schema_cloud(
  'https://cloud.example.com/remote.php/dav/files/user/export/',
  'Buchungsstapel',
  username := 'myuser',
  password := 'mypassword'
);
-- Returns: column_name, data_type, accuracy, column_order
```

#### `stps_read_gobd_cloud_all(url VARCHAR, username := VARCHAR, password := VARCHAR, delimiter := VARCHAR, overwrite := BOOLEAN) → TABLE`
Import **all tables** from a cloud-hosted GoBD/GDPDU export into the database. Same pipeline as `stps_read_gobd_all` but reads from WebDAV/Nextcloud. Creates cleaned DuckDB tables with normalized column names, empty columns removed, and smart type casting.
```sql
SELECT * FROM stps_read_gobd_cloud_all(
  'https://cloud.example.com/remote.php/dav/files/user/export/',
  username := 'myuser',
  password := 'mypassword'
);
-- Returns: table_name, rows_imported, columns_created, error

-- Then query the created tables directly
SELECT * FROM buchungsstapel;
```

#### `stps_read_gobd_cloud_zip_all(url VARCHAR, username := VARCHAR, password := VARCHAR, delimiter := VARCHAR, overwrite := BOOLEAN) → TABLE`
Import **all tables** from a ZIP file hosted on cloud (WebDAV/Nextcloud). Downloads the ZIP, extracts `index.xml` and CSV files in-memory, then runs the same import pipeline: creates cleaned DuckDB tables with normalized column names, empty columns removed, and smart type casting.
```sql
SELECT * FROM stps_read_gobd_cloud_zip_all(
  'https://cloud.example.com/remote.php/dav/files/user/export.zip',
  username := 'myuser',
  password := 'mypassword'
);
-- Returns: table_name, rows_imported, columns_created, error
```

**Notes (cloud GoBD functions):**
- Requires `curl` (functions are only available when the extension is built with libcurl).
- `index.xml` discovery: tries `<url>/index.xml` first, then PROPFIND listing, then one subfolder level.
- Single-table functions (`stps_read_gobd_cloud`, `stps_read_gobd_cloud_folder`) return all data as VARCHAR.
- Authentication uses HTTP Basic Auth via `username`/`password` parameters.
- **Encoding:** Automatically detects and converts Windows-1252 (CP1252) encoded CSV files to UTF-8. This is common for GoBD/GDPDU exports from German accounting software (Navision, DATEV, etc.).

**Notes (`*_all` import pipeline functions):**
- `stps_read_gobd_all`, `stps_read_gobd_cloud_all`, and `stps_read_gobd_cloud_zip_all` **create persistent DuckDB tables**.
- Table names are normalized to snake_case (e.g. `Buchungsstapel` → `buchungsstapel`).
- Column names are normalized to snake_case (e.g. `Konto Nr.` → `konto_nr`).
- Columns that are entirely empty (NULL or `''`) are automatically dropped.
- Types are auto-detected via `stps_smart_cast` (integers, decimals, dates).
- Use `overwrite := true` to replace existing tables; default is to error if table exists.

---

### 📁 Folder Import Functions

#### `stps_import_folder(path VARCHAR, ...) → TABLE`
Import **all supported files** from a local folder into DuckDB tables. Scans the folder for CSV, TSV, Parquet, JSON, XLSX, and XLS files and creates one table per file with:
- **Table name** derived from filename (snake_case, e.g. `Sales Report.xlsx` → `sales_report`)
- **Normalized column names** (snake_case)
- **Empty columns removed** (all NULL/empty values)
- **Smart type casting** via `stps_smart_cast`
- **Database cleanup** via `stps_clean_database()` after all imports

**Named Parameters:**

| Parameter | Type | Default | Applies to | Description |
|-----------|------|---------|------------|-------------|
| `overwrite` | BOOLEAN | false | All | Drop existing tables before re-importing |
| `all_varchar` | BOOLEAN | false | CSV, XLSX | Read all columns as VARCHAR. For CSV: passes `all_varchar=true` to `read_csv_auto`. For XLSX/XLS: passes `columns={'*': 'VARCHAR'}` to `read_sheet`. Useful to avoid type detection errors from mixed-type columns or totals rows. |
| `header` | BOOLEAN | true | CSV, XLSX | Whether the first row contains column headers |
| `ignore_errors` | BOOLEAN | false | CSV, JSON | Skip rows that fail to parse instead of erroring |
| `delimiter` | VARCHAR | auto | CSV | Column delimiter (e.g. `';'`, `'\t'`, `','`). Also accepts `sep` as alias. |
| `sep` | VARCHAR | auto | CSV | Alias for `delimiter` |
| `quote` | VARCHAR | `"` | CSV | Quote character for CSV fields |
| `escape` | VARCHAR | `"` | CSV | Escape character within quoted CSV fields |
| `skip` | BIGINT | 0 | CSV | Number of rows to skip at the beginning of the file |
| `null_str` | VARCHAR | | CSV | String that represents NULL values (e.g. `'NA'`, `'\\N'`). Also accepts `nullstr`. |
| `nullstr` | VARCHAR | | CSV | Alias for `null_str` |
| `sheet` | VARCHAR | | XLSX/XLS | Excel sheet name to read (e.g. `'Buchungen'`). If omitted, reads the first sheet. |
| `range` | VARCHAR | | XLSX/XLS | Excel cell range (e.g. `'A1:D100'`) |
| `reader_options` | VARCHAR | | All | Additional DuckDB reader options passed through verbatim as a raw string (e.g. `'dateformat=''%d.%m.%Y'''`). Use this for any parameter not listed above. |

**Returns:** `table_name`, `file_name`, `rows_imported`, `columns_created`, `error`

```sql
-- Basic: Import all files from a folder
SELECT * FROM stps_import_folder('C:/data/import/');

-- Re-import with overwrite
SELECT * FROM stps_import_folder('C:/data/import/', overwrite := true);

-- Import CSV files with semicolon delimiter and all as text
SELECT * FROM stps_import_folder('C:/data/import/',
    all_varchar := true,
    delimiter := ';'
);

-- Import with German CSV settings
SELECT * FROM stps_import_folder('C:/data/german/',
    delimiter := ';',
    skip := 2,
    null_str := 'N/A'
);

-- Import Excel files reading a specific sheet, all as VARCHAR
SELECT * FROM stps_import_folder('C:/data/excel/',
    all_varchar := true,
    sheet := 'Buchungen'
);

-- Import with generic reader_options passthrough
SELECT * FROM stps_import_folder('C:/data/import/',
    reader_options := 'dateformat=''%d.%m.%Y'''
);

-- Then query the created tables directly
SHOW TABLES;
SELECT * FROM sales_report;
```

**Supported file types:** `.csv`, `.tsv`, `.parquet`, `.json`, `.xlsx`, `.xls`

**Notes:**
- Reader parameters are applied to **all files** in the folder. If the folder contains mixed formats (e.g. CSV and XLSX), only the relevant parameters are used per file type — CSV parameters are ignored for XLSX files and vice versa.
- XLSX/XLS files require the `rusty_sheet` extension (installed automatically from community).
- Each file becomes a separate DuckDB table. Duplicate filenames (e.g. `data.csv` and `data.json`) get a numeric suffix (`data`, `data_2`).
- Files that fail to import are reported with an error message; other files continue importing.

#### `stps_import_nextcloud_folder(url VARCHAR, ...) → TABLE`
Import **all supported files** from a Nextcloud/WebDAV folder into DuckDB tables. Same pipeline as `stps_import_folder` but downloads files from a cloud server.

**Named Parameters:**

| Parameter | Type | Default | Applies to | Description |
|-----------|------|---------|------------|-------------|
| `username` | VARCHAR | | Connection | WebDAV/Nextcloud username |
| `password` | VARCHAR | | Connection | WebDAV/Nextcloud password |
| `overwrite` | BOOLEAN | false | All | Drop existing tables before re-importing |
| `all_varchar` | BOOLEAN | false | CSV, XLSX | Read all columns as VARCHAR (see `stps_import_folder` above) |
| `header` | BOOLEAN | true | CSV, XLSX | Whether the first row contains column headers |
| `ignore_errors` | BOOLEAN | false | CSV, JSON | Skip rows that fail to parse |
| `delimiter` | VARCHAR | auto | CSV | Column delimiter. Also accepts `sep`. |
| `sep` | VARCHAR | auto | CSV | Alias for `delimiter` |
| `quote` | VARCHAR | `"` | CSV | Quote character |
| `escape` | VARCHAR | `"` | CSV | Escape character within quoted fields |
| `skip` | BIGINT | 0 | CSV | Number of rows to skip at the beginning |
| `null_str` | VARCHAR | | CSV | String representing NULL. Also accepts `nullstr`. |
| `nullstr` | VARCHAR | | CSV | Alias for `null_str` |
| `sheet` | VARCHAR | | XLSX/XLS | Excel sheet name |
| `range` | VARCHAR | | XLSX/XLS | Excel cell range (e.g. `'A1:D100'`) |
| `reader_options` | VARCHAR | | All | Additional DuckDB reader options (raw passthrough) |

**Returns:** `table_name`, `file_name`, `rows_imported`, `columns_created`, `error`

```sql
-- Basic: Import all files from a Nextcloud folder
SELECT * FROM stps_import_nextcloud_folder(
    'https://cloud.example.com/remote.php/dav/files/user/data/',
    username := 'myuser',
    password := 'mypassword'
);

-- Import with all columns as VARCHAR (avoids type errors in Excel files)
SELECT * FROM stps_import_nextcloud_folder(
    'https://cloud.example.com/remote.php/dav/files/user/data/',
    username := 'myuser',
    password := 'mypassword',
    all_varchar := true
);

-- Import with CSV-specific settings
SELECT * FROM stps_import_nextcloud_folder(
    'https://cloud.example.com/remote.php/dav/files/user/data/',
    username := 'myuser',
    password := 'mypassword',
    delimiter := ';',
    skip := 1,
    ignore_errors := true
);

-- Import Excel files from specific sheet
SELECT * FROM stps_import_nextcloud_folder(
    'https://cloud.example.com/remote.php/dav/files/user/data/',
    username := 'myuser',
    password := 'mypassword',
    sheet := 'Buchungen',
    all_varchar := true
);

-- Re-import with overwrite
SELECT * FROM stps_import_nextcloud_folder(
    'https://cloud.example.com/remote.php/dav/files/user/data/',
    username := 'myuser',
    password := 'mypassword',
    overwrite := true
);
```

**Notes:**
- Requires `curl` (only available when the extension is built with libcurl).
- Uses WebDAV PROPFIND to list files in the folder, then downloads each supported file.
- Authentication uses HTTP Basic Auth via `username`/`password` parameters.
- Files are downloaded to temp files for import, then cleaned up automatically.
- All reader parameters work identically to `stps_import_folder` — see the parameter descriptions above.

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

#### `stps_drop_duplicates(table_name VARCHAR, columns? LIST) → TABLE`
Remove duplicate rows from a table. Similar to pandas `drop_duplicates()`.
```sql
-- Remove all duplicate rows (checks all columns)
SELECT * FROM stps_drop_duplicates('customers');

-- Remove duplicates based on specific columns (keeps first occurrence)
SELECT * FROM stps_drop_duplicates('customers', ['email']);

-- Using named parameter
SELECT * FROM stps_drop_duplicates('orders', columns := ['customer_id', 'product_id']);
```

**Parameters:**
- `table_name` - Table to deduplicate
- `columns` (optional) - List of columns to check for duplicates. If omitted, all columns are checked.

**Returns:** Table with unique rows only (first occurrence per group is kept).

#### `stps_show_duplicates(table_name VARCHAR, columns? LIST) → TABLE`
Show only the duplicate rows from a table. Useful for data quality checks.
```sql
-- Find all duplicate rows (checks all columns)
SELECT * FROM stps_show_duplicates('customers');

-- Find rows with duplicate emails
SELECT * FROM stps_show_duplicates('customers', ['email']);

-- Find duplicate orders
SELECT * FROM stps_show_duplicates('orders', columns := ['customer_id', 'order_date']);
```

**Parameters:**
- `table_name` - Table to check for duplicates
- `columns` (optional) - List of columns to check for duplicates. If omitted, all columns are checked.

**Returns:** All rows that have at least one duplicate (all occurrences are returned).

---

### 🔧 Advanced Functions

#### `stps_lambda(input ARRAY, function VARCHAR) → ARRAY`
Apply lambda function to array elements.
```sql
SELECT stps_lambda([1, 2, 3], 'x -> x * 2') AS doubled;
-- Result: [2, 4, 6]
```

#### `stps_clean_database() → TABLE`
Drop all empty tables from the database. Returns list of dropped tables.
```sql
-- Drop all empty tables and see what was removed
SELECT * FROM stps_clean_database();
-- Returns: dropped_table (VARCHAR)

-- Example output:
-- dropped_table
-- -------------
-- temp_import
-- old_backup
-- empty_staging
```

**Use Cases:**
- Clean up after ETL processes that create temporary tables
- Remove empty staging tables
- Database maintenance and housekeeping

**Note:** This operation cannot be undone. Only tables with zero rows are dropped.

---

#### `stps_search_database(pattern VARCHAR) → TABLE`
Search the entire database for a value across **all schemas, all tables, and all columns**. Returns every match with full row context as JSON.

```sql
-- Search all schemas and tables for a value
SELECT * FROM stps_search_database('%97462234%');
-- Returns: schema_name, table_name, column_name, matched_value, row_data (JSON)

-- Filter results to a specific schema
SELECT * FROM stps_search_database('%97462234%') WHERE schema_name = 'staging';

-- Search only in the main schema
SELECT * FROM stps_search_database('%Hoeger%') WHERE schema_name = 'main';

-- Find which tables contain a specific ID across all schemas
SELECT DISTINCT schema_name, table_name
FROM stps_search_database('%INV-2024-001%');

-- Get full row context for matches
SELECT schema_name, table_name, column_name, row_data
FROM stps_search_database('%john.doe@%');
```

**Parameters:**
- `pattern` - SQL LIKE pattern (% = any chars, _ = single char)

**Returns:**
| Column | Type | Description |
|--------|------|-------------|
| `schema_name` | VARCHAR | Schema where the match was found |
| `table_name` | VARCHAR | Table where the match was found |
| `column_name` | VARCHAR | Column that contained the match |
| `matched_value` | VARCHAR | The actual matched value |
| `row_data` | VARCHAR | Full row as JSON for context |

**Behavior:**
- Searches across **all schemas** (excludes only `information_schema` and `pg_catalog`)
- Searches every column in every table (casts all types to VARCHAR)
- Case-insensitive matching
- Pattern uses SQL LIKE syntax

---

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

#### `stps_inso_account(source_table VARCHAR, bank_account := BIGINT) → TABLE`

Map commercial bookings (handelsrechtliche Buchungen) to insolvency chart of accounts (Einnahme-/Ausgaberechnung). Filters transactions touching the specified bank account and adds the mapped EA-Konto.

```sql
-- Map all bank account 180000 transactions to insolvency accounts
SELECT * FROM stps_inso_account('rl.buchungen', bank_account=180000);

-- Show only the mapping summary
SELECT DISTINCT counter_kontoart, ea_konto, ea_kontobezeichnung, mapping_source
FROM stps_inso_account('rl.buchungen', bank_account=180000)
ORDER BY counter_kontoart;

-- Sum by EA-Konto
SELECT ea_konto, ea_kontobezeichnung, SUM(umsatz) as total
FROM stps_inso_account('rl.buchungen', bank_account=180000)
WHERE ea_konto IS NOT NULL
GROUP BY ea_konto, ea_kontobezeichnung
ORDER BY ea_konto;
```

**Parameters:**
- `source_table` - Fully qualified table name containing bookings (e.g. `'rl.buchungen'`)
- `bank_account` (named, required) - The bank account number to filter on (e.g. `180000`)

**Returns:** All original columns from the source table, plus:

| Column | Type | Description |
|--------|------|-------------|
| `counter_konto` | BIGINT | Account number on the non-bank side of the transaction |
| `counter_kontobezeichnung` | VARCHAR | Name of the counter-account |
| `counter_kontoart` | VARCHAR | Type of the counter-account (D, K, Aufwand, Erloes, Sachkonto, Geldkonto) |
| `ea_konto` | VARCHAR | Mapped insolvency EA-Konto number |
| `ea_kontobezeichnung` | VARCHAR | Name from inso_kontenrahmen |
| `mapping_source` | VARCHAR | How the mapping was determined |

**Mapping Rules:**
- **Debitor (D)** → `8200` (Forderungseinzug aus L.u.L.)
- **Erloes** → `8200` (Forderungseinzug aus L.u.L.)
- **Kreditor (K)** → Finds the Aufwand account with highest total for that Kreditor, then maps that Aufwand to Ausgabekonten
- **Aufwand** → Best match in Ausgabekonten by name similarity and account prefix; falls back to `4900` (Sonstige betriebliche Aufwendungen)
- **Sachkonto (14xxxx Vorsteuer)** → `1780` (Umsatzsteuerzahlungen)
- **Sachkonto (other)** → Best-effort name match to inso_kontenrahmen, or NULL
- **Geldkonto** → `1360`

**Requirements:**
- Source table must have columns: `decKontoNr`, `kontobezeichnung`, `kontoart`, `decGegenkontoNr`, `gegenkontobezeichnung`, `gegenkontoart`, `umsatz`
- Tables `konto` and `inso_kontenrahmen` must exist in the same schema as the source table

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
-- Extract CSV from ZIP archive
SELECT customer_id, amount, date
FROM stps_zip('monthly_reports.zip', 'january.csv')
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

- **[Complete Function List](howToUseMD/STPS_FUNCTIONS.md)** - Detailed function reference
- **[How to Update](howToUseMD/HOW_TO_UPDATE_EXTENSION.md)** - Get latest version
- **[Build Process](howToUseMD/BUILD_PROCESS.md)** - GitHub Actions build info

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
SELECT * FROM stps_zip('C:/data/archive.zip', 'file.csv');

-- Good: escaped backslashes
SELECT * FROM stps_zip('C:\\data\\archive.zip', 'file.csv');

-- Bad: unescaped backslashes
SELECT * FROM stps_zip('C:\data\archive.zip', 'file.csv');  -- ERROR
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

## Nextcloud/WebDAV table functions

### `stps_nextcloud(url VARCHAR, ...) → TABLE`

Fetch a file directly over WebDAV and return it as a table. File type is auto-detected from the URL extension. All file types are parsed via DuckDB's built-in readers (`read_csv_auto`, `read_parquet`, `read_sheet`).

**Supported formats:** CSV, TSV, Parquet, Arrow, Feather, XLSX, XLS

**Named Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `username` | VARCHAR | | WebDAV/Nextcloud username |
| `password` | VARCHAR | | WebDAV/Nextcloud password |
| `headers` | VARCHAR | | Extra HTTP headers (newline-separated) |
| `all_varchar` | BOOLEAN | false | Read all columns as VARCHAR (avoids type cast errors) |
| `ignore_errors` | BOOLEAN | false | Skip rows that fail to parse (CSV only) |
| `sheet` | VARCHAR | | Excel sheet name (XLSX/XLS only) |
| `range` | VARCHAR | | Excel cell range, e.g. `'A1:D100'` (XLSX/XLS only) |
| `reader_options` | VARCHAR | | Additional DuckDB reader options passed through verbatim |

```sql
-- CSV with auto-detected types
SELECT * FROM stps_nextcloud('https://your-server/path/file.csv', username:='user', password:='pass');

-- Parquet
SELECT * FROM stps_nextcloud('https://your-server/path/file.parquet', username:='user', password:='pass');

-- Excel with all columns as text (avoids type errors from totals rows)
SELECT * FROM stps_nextcloud(
  'https://your-server/path/file.xlsx',
  username := 'user',
  password := 'pass',
  all_varchar := true
);

-- Excel: specific sheet and range
SELECT * FROM stps_nextcloud(
  'https://your-server/path/file.xlsx',
  username := 'user',
  password := 'pass',
  sheet := 'Buchungen',
  range := 'A1:F500'
);

-- CSV with custom reader options
SELECT * FROM stps_nextcloud(
  'https://your-server/path/file.csv',
  username := 'user',
  password := 'pass',
  reader_options := 'delim='';'', header=true'
);

-- Custom headers (bearer token, etc.)
SELECT * FROM stps_nextcloud(
  'https://daten.example.cloud/path/file.csv',
  headers := 'X-API-Key: abc123'
);
```

Notes:
- Uses HTTP GET via libcurl; `username`/`password` map to Basic Auth.
- `headers` lets you pass bearer tokens, extra headers, etc. (one header per line).
- XLSX/XLS requires the DuckDB `rusty_sheet` community extension (auto-installed).
- If `read_csv_auto` fails for CSV files, falls back to a built-in CSV parser (all VARCHAR).

### `stps_nextcloud_folder(parent_url VARCHAR, ...) → TABLE`

Scan a Nextcloud/WebDAV parent folder, enter each company subfolder's `child_folder`, read all matching files, and return a unified table with metadata columns.

The function uses WebDAV PROPFIND to discover subfolders and files. Column names are normalized to snake_case.

**Returns:** `parent_folder`, `child_folder`, `file_name`, plus all data columns.

**Named Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `child_folder` | VARCHAR | | Subfolder within each company folder to look in |
| `file_type` | VARCHAR | `csv` | File extension to match (`csv`, `xlsx`, `parquet`, etc.) |
| `username` | VARCHAR | | WebDAV/Nextcloud username |
| `password` | VARCHAR | | WebDAV/Nextcloud password |
| `all_varchar` | BOOLEAN | false | Read all columns as VARCHAR (avoids type cast errors) |
| `ignore_errors` | BOOLEAN | false | Skip rows that fail to parse (CSV only) |
| `sheet` | VARCHAR | | Excel sheet name (XLSX/XLS only) |
| `range` | VARCHAR | | Excel cell range, e.g. `'A1:D100'` (XLSX/XLS only) |
| `reader_options` | VARCHAR | | Additional DuckDB reader options passed through verbatim |

```sql
-- Read all CSV files from each company's "bank" subfolder
SELECT *
FROM stps_nextcloud_folder(
  'https://cloud.example.com/remote.php/dav/files/user/mandanten',
  child_folder := 'bank',
  file_type := 'csv',
  username := 'myuser',
  password := 'mypassword'
);

-- Read XLSX files with all_varchar to avoid type errors (e.g. totals rows)
SELECT *
FROM stps_nextcloud_folder(
  'https://cloud.example.com/remote.php/dav/files/user/mandanten',
  child_folder := 'Bank',
  file_type := 'xlsx',
  all_varchar := true,
  username := 'myuser',
  password := 'mypassword'
);

-- XLSX with specific sheet
SELECT *
FROM stps_nextcloud_folder(
  'https://cloud.example.com/remote.php/dav/files/user/mandanten',
  child_folder := 'Bank',
  file_type := 'xlsx',
  all_varchar := true,
  sheet := 'Buchungen',
  username := 'myuser',
  password := 'mypassword'
);

-- Read files directly from company folders (no child_folder)
SELECT *
FROM stps_nextcloud_folder(
  'https://cloud.example.com/remote.php/dav/files/user/mandanten',
  file_type := 'csv',
  username := 'myuser',
  password := 'mypassword'
);
```

Notes:
- Uses WebDAV PROPFIND to list directories; requires Nextcloud or any WebDAV-compatible server.
- `child_folder` is optional. If omitted, files are read directly from each company subfolder.
- `file_type` defaults to `csv`. Supports: `csv`, `tsv`, `xlsx`, `xls`, `parquet`, `arrow`, `feather`.
- The schema is determined by the first file found. Files with a different column count are silently skipped.
- Companies where `child_folder` does not exist are silently skipped.
- Column names are normalized to snake_case (like DuckDB's `normalize_names=true`).
- For XLSX files with totals/summary rows that cause type errors, use `all_varchar := true`.

---

#### `stps_ask_ai_gender(name VARCHAR[, model VARCHAR]) → VARCHAR`
Classify first-name gender using Claude (no web search). Returns `male`, `female`, or `unknown`.
```sql
-- Simple lookup
SELECT stps_ask_ai_gender('Ramon');

-- Override model
SELECT stps_ask_ai_gender('Anna', 'claude-3-7-sonnet-20250219');
```

Notes:
- Uses your configured Anthropic key; set via `stps_set_api_key()`.
- Trims input and uses the first token as the first name.
- Web search is disabled; purely model-based.
