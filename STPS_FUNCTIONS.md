# Stps Functions - Complete Reference

## ✅ All 19 Functions with `stps_` Prefix

Renamed from `pgm_` to match your original `Stps` class from ldf.py!

---

## 🚀 Quick Start

```bash
duckdb -unsigned
```

```sql
LOAD '/Users/ramonljevo/Documents/python_path/duckdb-stps/build/release/extension/stps/stps.duckdb_extension';

-- Test it works
SELECT stps_get_guid('test', 'data');
```

---

## 📋 Complete Function List (19 Functions)

### UUID/GUID Functions (3)

#### `stps_uuid()` - Random UUID v4
```sql
SELECT stps_uuid();
-- Result: 3c8cef34-a1a2-4c6a-adfb-ba40ba8c660e (random each time)
```

#### `stps_uuid_from_string(text)` - Deterministic UUID v5
```sql
SELECT stps_uuid_from_string('hello');
-- Result: a430d846-80aa-5d0b-b695-136cbd22ce9b (always same for 'hello')
```

#### `stps_get_guid(col1, col2, ...)` - Multi-column GUID (from ldf.py!)
```sql
-- Your original get_guid function!
SELECT stps_get_guid('col1', 'col2', 'col3');
-- Result: deterministic GUID from combined values

-- Real-world example
SELECT
    name,
    email,
    stps_get_guid(name, email) as unique_id
FROM users;
```

---

### Case Transformation Functions (7)

#### `stps_to_snake_case(text)` - Convert to snake_case
```sql
SELECT stps_to_snake_case('HelloWorld');
-- Result: hello_world
```

#### `stps_to_camel_case(text)` - Convert to camelCase
```sql
SELECT stps_to_camel_case('hello_world');
-- Result: helloWorld
```

#### `stps_to_pascal_case(text)` - Convert to PascalCase
```sql
SELECT stps_to_pascal_case('hello_world');
-- Result: HelloWorld
```

#### `stps_to_kebab_case(text)` - Convert to kebab-case
```sql
SELECT stps_to_kebab_case('HelloWorld');
-- Result: hello-world
```

#### `stps_to_const_case(text)` - Convert to CONST_CASE
```sql
SELECT stps_to_const_case('helloWorld');
-- Result: HELLO_WORLD
```

#### `stps_to_sentence_case(text)` - Convert to Sentence case
```sql
SELECT stps_to_sentence_case('hello world');
-- Result: Hello world
```

#### `stps_to_title_case(text)` - Convert to Title Case
```sql
SELECT stps_to_title_case('hello world');
-- Result: Hello World
```

---

### Text Normalization Functions (4)

#### `stps_remove_accents(text, keep_umlauts)` - Remove accents
```sql
SELECT stps_remove_accents('café', false);
-- Result: cafe

SELECT stps_remove_accents('München', true);
-- Result: München (keeps German umlauts)
```

#### `stps_restore_umlauts(text)` - Restore German umlauts
```sql
SELECT stps_restore_umlauts('gruessen');
-- Result: grüßen
```

#### `stps_clean_string(text)` - Clean whitespace
```sql
SELECT stps_clean_string('  Hello   World  ');
-- Result: Hello World
```

#### `stps_normalize(text)` - Clean and uppercase
```sql
SELECT stps_normalize('  test  ');
-- Result: TEST
```

---

### Null Handling Functions (2)

#### `stps_map_empty_to_null(text)` - Convert empty strings to NULL
```sql
SELECT stps_map_empty_to_null('');
-- Result: NULL

SELECT stps_map_empty_to_null('test');
-- Result: test
```

#### `stps_map_null_to_empty(text)` - Convert NULL to empty string
```sql
SELECT stps_map_null_to_empty(NULL);
-- Result: ''

SELECT stps_map_null_to_empty('test');
-- Result: test
```

---

### PLZ Validation Functions (3)

#### `stps_is_valid_plz(plz)` - Validate German PLZ format
```sql
SELECT stps_is_valid_plz('01067');
-- Result: true (valid 5-digit format)

SELECT stps_is_valid_plz('1067');
-- Result: false (only 4 digits)

SELECT stps_is_valid_plz('00999');
-- Result: false (below valid range 01000-99999)
```

#### `stps_is_valid_plz(plz, strict)` - Validate PLZ with optional strict mode
```sql
-- Strict mode downloads and checks against real German PLZ database
SELECT stps_is_valid_plz('01067', true);
-- Result: true (Dresden exists)

SELECT stps_is_valid_plz('01000', true);
-- Result: false (format ok, but this PLZ doesn't exist)
```

#### `stps_get_plz_gist()` - Get current PLZ data source URL
```sql
SELECT stps_get_plz_gist();
-- Result: https://gist.githubusercontent.com/jbspeakr/4565964/raw/ (default)
```

#### `stps_set_plz_gist(url)` - Set custom PLZ data source URL
```sql
SELECT stps_set_plz_gist('https://example.com/my-plz-data.csv');
-- Result: PLZ gist URL set to: https://example.com/my-plz-data.csv

-- After setting, stps_is_valid_plz with strict=true will download from the new URL
SELECT stps_is_valid_plz('01067', true);
```

---

## 💡 Real-World Examples

### Example 1: Data Deduplication with stps_get_guid
```sql
LOAD 'build/release/extension/stps/stps.duckdb_extension';

-- Create deterministic IDs for deduplication
SELECT DISTINCT
    stps_get_guid(first_name, last_name, birth_date) as person_id,
    first_name,
    last_name,
    birth_date
FROM raw_people
ORDER BY person_id;
```

### Example 2: API Field Mapping
```sql
-- Convert database columns to different naming conventions
SELECT
    column_name,
    stps_to_camel_case(column_name) as javascript_field,
    stps_to_pascal_case(column_name) as csharp_property,
    stps_to_kebab_case(column_name) as css_class
FROM information_schema.columns
WHERE table_name = 'users';
```

### Example 3: Data Cleaning Pipeline
```sql
-- Clean and normalize messy customer data
SELECT
    stps_normalize(stps_clean_string(name)) as clean_name,
    stps_remove_accents(city, false) as normalized_city,
    stps_map_empty_to_null(phone) as phone,
    stps_get_guid(name, email) as customer_guid
FROM raw_customers;
```

### Example 4: Generate Test UUIDs
```sql
-- Generate unique IDs for test data
SELECT
    id,
    name,
    stps_uuid() as random_id,
    stps_uuid_from_string(CAST(id AS VARCHAR)) as deterministic_id
FROM test_users;
```

---

## 🐍 Python/Jupyter Usage

```python
import duckdb

# Connect with unsigned extensions allowed
conn = duckdb.connect(config={'allow_unsigned_extensions': 'true'})

# Load extension
conn.execute("""
    LOAD '/Users/ramonljevo/Documents/python_path/duckdb-stps/build/release/extension/stps/stps.duckdb_extension'
""")

# Use stps_ functions
result = conn.execute("""
    SELECT
        stps_get_guid('user', 'data') as guid,
        stps_to_snake_case('HelloWorld') as snake,
        stps_uuid() as random_uuid
""").df()

print(result)
```

---

## 🔧 CLI Usage

### Option 1: Direct Load
```bash
duckdb -unsigned -c "LOAD 'build/release/extension/stps/stps.duckdb_extension'; SELECT stps_get_guid('test');"
```

### Option 2: Interactive Session
```bash
cd /Users/ramonljevo/Documents/python_path/duckdb-stps
duckdb -unsigned
```
```sql
LOAD 'build/release/extension/stps/stps.duckdb_extension';
SELECT stps_uuid();
```

### Option 3: Persistent Config
```bash
echo "SET allow_unsigned_extensions = true;" > ~/.duckdbrc
```

Now you can use `duckdb` without the `-unsigned` flag!

---

## 📊 Function Summary Table

| Category | Function Count | Prefix |
|----------|---------------|--------|
| UUID/GUID | 3 | `stps_uuid`, `stps_uuid_from_string`, `stps_get_guid` |
| Case Transformations | 7 | `stps_to_*` (snake, camel, pascal, kebab, const, sentence, title) |
| Text Normalization | 4 | `stps_remove_accents`, `stps_restore_umlauts`, `stps_clean_string`, `stps_normalize` |
| Null Handling | 2 | `stps_map_empty_to_null`, `stps_map_null_to_empty` |
| PLZ Validation | 3 | `stps_is_valid_plz` (2 overloads), `stps_get_plz_gist`, `stps_set_plz_gist` |
| ZIP Archives | 2 | `stps_zip`, `stps_view_zip` |
| 7-Zip Archives | 2 | `stps_7zip`, `stps_view_7zip` |
| **Total** | **23** | All use `stps_` prefix |

---

## 📦 Archive Functions

### ZIP Archive Functions

#### `stps_view_zip(zip_path)` - List files in a ZIP archive
```sql
SELECT * FROM stps_view_zip('/path/to/archive.zip');
-- Returns: filename, uncompressed_size, compressed_size, is_directory, index
```

#### `stps_zip(zip_path)` - Read CSV data from a ZIP archive
```sql
-- Read first/only file in archive
SELECT * FROM stps_zip('/path/to/data.zip');

-- Read specific file from archive
SELECT * FROM stps_zip('/path/to/archive.zip', 'data/file.csv');
```

### 7-Zip Archive Functions

#### `stps_view_7zip(archive_path)` - List files in a 7z archive
```sql
SELECT * FROM stps_view_7zip('/path/to/archive.7z');
-- Returns: filename, uncompressed_size, is_directory, index
```

#### `stps_7zip(archive_path)` - Read info from a 7z archive
```sql
SELECT * FROM stps_7zip('/path/to/archive.7z');
-- Note: Full content extraction requires additional implementation.
-- Use stps_view_7zip() to list files in the archive.
```

---

## ✅ Why `stps_` prefix?

- **Matches your original class**: `Stps` class from `stps/ldf.py`
- **Consistent naming**: All 23 functions use the same prefix
- **Avoids conflicts**: Unique prefix prevents collisions with other extensions
- **Recognizable**: Easy to find with tab completion: `stps_<TAB>`

---

**Built:** January 12, 2026
**Extension:** stps.duckdb_extension
**Platform:** macOS ARM64, Linux, Windows
