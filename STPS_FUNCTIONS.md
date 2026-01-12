# Stps Functions - Complete Reference

## ✅ All Functions with `stps_` Prefix

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

## 📋 Complete Function List

### 🔥 Lambda Functions (NEW!)

#### `stps_lambda(table_name, lambda_expr, [varchar_only], [column_pattern])` - Apply transformations to all columns

Apply lambda-like transformations to all columns in a table at once. Perfect for bulk data cleaning and transformation.

**Basic Syntax:**
```sql
SELECT * FROM stps_lambda('table_name', 'c -> transformation(c)');
```

**Parameters:**
- `table_name`: Name of the table to transform
- `lambda_expr`: Lambda expression or SQL expression to apply
  - Format: `'c -> function(c)'` where `c` represents each column
  - Or just: `'function(c)'` without the arrow notation
- `varchar_only` (optional, default=true): Only apply to VARCHAR columns
- `column_pattern` (optional): Only apply to columns containing this pattern

**Examples:**

```sql
-- Trim all VARCHAR columns
SELECT * FROM stps_lambda('my_table', 'c -> trim(c)');

-- Convert all text to uppercase
SELECT * FROM stps_lambda('my_table', 'c -> upper(c)');

-- Apply STPS functions
SELECT * FROM stps_lambda('my_table', 'c -> stps_to_snake_case(c)');
SELECT * FROM stps_lambda('my_table', 'c -> stps_normalize(c)');

-- Chain multiple transformations
SELECT * FROM stps_lambda('my_table', 'c -> upper(stps_to_snake_case(c))');

-- Apply to all columns (not just VARCHAR)
SELECT * FROM stps_lambda('my_table', 'c -> trim(c)', false);

-- Apply only to columns with 'name' in their name
SELECT * FROM stps_lambda('my_table', 'c -> upper(c)', true, 'name');

-- Custom SQL expressions
SELECT * FROM stps_lambda('my_table', 'CASE WHEN c IS NULL THEN ''empty'' ELSE c END');

-- Named parameters
SELECT * FROM stps_lambda('my_table', 'c -> trim(c)', varchar_only := false);
SELECT * FROM stps_lambda('my_table', 'c -> upper(c)', column_pattern := 'name');
```

**Real-World Use Cases:**

```sql
-- Clean all text columns in one go
CREATE TABLE clean_data AS 
SELECT * FROM stps_lambda('raw_data', 'c -> trim(c)');

-- Standardize naming conventions
CREATE TABLE snake_case_data AS
SELECT * FROM stps_lambda('camel_case_data', 'c -> stps_to_snake_case(c)');

-- Remove extra whitespace from all columns
SELECT * FROM stps_lambda('messy_data', 'c -> stps_clean_string(c)');

-- Convert empty strings to NULL across all columns
SELECT * FROM stps_lambda('data_table', 'c -> stps_map_empty_to_null(c)');
```

---

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
| **Total** | **17** | All use `stps_` prefix |

---

## ✅ Why `stps_` prefix?

- **Matches your original class**: `Stps` class from `stps/ldf.py`
- **Consistent naming**: All 17 functions use the same prefix
- **Avoids conflicts**: Unique prefix prevents collisions with other extensions
- **Recognizable**: Easy to find with tab completion: `stps_<TAB>`

---

**Built:** December 27, 2025
**Extension:** stps.duckdb_extension (5.5 MB)
**Platform:** macOS ARM64
