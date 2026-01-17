# Search Data Values Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Modify `stps_search_columns` to search for pattern matches in actual data values across all columns, returning matching rows with a list of which columns matched.

**Architecture:** Replace column name search with dynamic SQL generation that uses LIKE pattern matching on CAST-to-VARCHAR values across all columns. Execute the generated SQL using DuckDB's Connection API and stream results.

**Tech Stack:** C++ (DuckDB Table Function API), DuckDB Connection API for query execution, Dynamic SQL generation

---

## Task 1: Update Bind Data Structure

**Files:**
- Modify: `src/search_columns_function.cpp:13-20`

**Step 1: Update SearchColumnsBindData structure**

Replace the existing structure with:

```cpp
struct SearchColumnsBindData : public TableFunctionData {
    string table_name;
    string search_pattern;
    string generated_sql;  // The dynamic SQL query to execute
    vector<LogicalType> original_column_types;  // Types from source table
    vector<string> original_column_names;  // Names from source table
};
```

Remove:
- `bool case_sensitive` (no longer needed - always case-insensitive)
- `vector<string> matching_columns` (no longer used in bind data)

**Step 2: Remove unused helper functions**

Delete these functions (lines 26-92):
- `to_lower_str`
- `like_match`
- `column_matches_pattern`

These are no longer needed since SQL LIKE handles pattern matching.

**Step 3: Commit**

```bash
git add src/search_columns_function.cpp
git commit -m "refactor: update SearchColumnsBindData for data value search

Remove column name matching logic and case_sensitive parameter.
Add fields for dynamic SQL generation and original table schema.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Update Global State for Query Results

**Files:**
- Modify: `src/search_columns_function.cpp:22-24`

**Step 1: Update SearchColumnsGlobalState structure**

Replace the existing structure with:

```cpp
struct SearchColumnsGlobalState : public GlobalTableFunctionState {
    unique_ptr<QueryResult> result;  // Result from executing dynamic SQL
    unique_ptr<DataChunk> current_chunk;  // Current chunk being processed
    idx_t chunk_offset = 0;  // Offset within current chunk
    bool query_executed = false;  // Track if query has been run
};
```

Remove:
- `idx_t current_idx` (no longer iterating over a vector)

**Step 2: Commit**

```bash
git add src/search_columns_function.cpp
git commit -m "refactor: update SearchColumnsGlobalState for query execution

Add QueryResult and DataChunk tracking for dynamic SQL execution.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Rewrite Bind Function - Get Table Schema

**Files:**
- Modify: `src/search_columns_function.cpp:94-139`

**Step 1: Rewrite SearchColumnsBind function start**

Replace lines 94-105 with:

```cpp
static unique_ptr<FunctionData> SearchColumnsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<SearchColumnsBindData>();

    // Get parameters - only 2 parameters now (removed case_sensitive)
    if (input.inputs.size() < 2) {
        throw BinderException("stps_search_columns requires 2 arguments: table_name, pattern");
    }

    result->table_name = input.inputs[0].GetValue<string>();
    result->search_pattern = input.inputs[1].GetValue<string>();

    // Create connection to query table schema
    Connection conn(context.db->GetDatabase(context));
```

**Step 2: Get table schema using LIMIT 0 query**

Replace lines 106-129 with:

```cpp
    // Get table schema (column names and types) using LIMIT 0
    string schema_query = "SELECT * FROM " + result->table_name + " LIMIT 0";
    auto schema_result = conn.Query(schema_query);

    if (schema_result->HasError()) {
        throw BinderException("Table '%s' not found or inaccessible: %s",
                            result->table_name.c_str(), schema_result->GetError().c_str());
    }

    // Store original column names and types
    result->original_column_names = schema_result->names;
    result->original_column_types = schema_result->types;

    if (result->original_column_names.empty()) {
        throw BinderException("Table '%s' has no columns", result->table_name.c_str());
    }
```

**Step 3: Commit**

```bash
git add src/search_columns_function.cpp
git commit -m "feat: get table schema in bind phase

Query table schema to get column names and types for SQL generation.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 4: Generate Dynamic SQL Query

**Files:**
- Modify: `src/search_columns_function.cpp` (continuation of SearchColumnsBind)

**Step 1: Build the dynamic SQL query**

After the schema retrieval code, add:

```cpp
    // Generate dynamic SQL to search all columns
    // Pattern: SELECT *, [col1_match, col2_match, ...] FROM table WHERE (col1 LIKE pattern OR col2 LIKE pattern...)

    string sql = "SELECT *, LIST_VALUE(";

    // Build list of CASE statements for matched columns
    for (idx_t i = 0; i < result->original_column_names.size(); i++) {
        if (i > 0) sql += ", ";
        string col_name = result->original_column_names[i];
        // Escape single quotes in column name
        string escaped_col = col_name;
        size_t pos = 0;
        while ((pos = escaped_col.find("'", pos)) != string::npos) {
            escaped_col.replace(pos, 1, "''");
            pos += 2;
        }
        sql += "CASE WHEN LOWER(CAST(\"" + col_name + "\" AS VARCHAR)) LIKE LOWER(?) THEN '" + escaped_col + "' END";
    }

    sql += ") FILTER (WHERE value IS NOT NULL) AS matched_columns FROM " + result->table_name + " WHERE ";

    // Build WHERE clause - at least one column must match
    for (idx_t i = 0; i < result->original_column_names.size(); i++) {
        if (i > 0) sql += " OR ";
        sql += "LOWER(CAST(\"" + result->original_column_names[i] + "\" AS VARCHAR)) LIKE LOWER(?)";
    }

    result->generated_sql = sql;
```

**Step 2: Commit**

```bash
git add src/search_columns_function.cpp
git commit -m "feat: generate dynamic SQL for data value search

Build SQL query with LIKE pattern matching across all columns.
Use CAST to VARCHAR to handle all data types.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Set Output Schema in Bind Function

**Files:**
- Modify: `src/search_columns_function.cpp` (continuation of SearchColumnsBind)

**Step 1: Define output schema**

Replace lines 131-137 with:

```cpp
    // Define output schema: original columns + matched_columns array
    for (idx_t i = 0; i < result->original_column_names.size(); i++) {
        return_types.push_back(result->original_column_types[i]);
        names.push_back(result->original_column_names[i]);
    }

    // Add matched_columns as VARCHAR[] (list of strings)
    return_types.push_back(LogicalType::LIST(LogicalType::VARCHAR));
    names.push_back("matched_columns");

    return std::move(result);
}
```

**Step 2: Commit**

```bash
git add src/search_columns_function.cpp
git commit -m "feat: set output schema with matched_columns array

Output includes all original columns plus matched_columns list.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 6: Update Init Function

**Files:**
- Modify: `src/search_columns_function.cpp:141-143`

**Step 1: Update SearchColumnsInit**

The init function just needs to initialize the state - minimal change:

```cpp
static unique_ptr<GlobalTableFunctionState> SearchColumnsInit(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<SearchColumnsGlobalState>();
    state->query_executed = false;
    return std::move(state);
}
```

**Step 2: Commit**

```bash
git add src/search_columns_function.cpp
git commit -m "refactor: update init function for new global state

Initialize query_executed flag.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 7: Rewrite Main Function - Execute Query

**Files:**
- Modify: `src/search_columns_function.cpp:145-168`

**Step 1: Execute the dynamic SQL query once**

Replace the entire SearchColumnsFunction with:

```cpp
static void SearchColumnsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<SearchColumnsBindData>();
    auto &state = data_p.global_state->Cast<SearchColumnsGlobalState>();

    // Execute query on first call
    if (!state.query_executed) {
        Connection conn(context.db->GetDatabase(context));

        // Create prepared statement with parameters
        auto prepared = conn.Prepare(bind_data.generated_sql);
        if (prepared->HasError()) {
            throw InvalidInputException("Failed to prepare search query: %s", prepared->GetError().c_str());
        }

        // Bind pattern parameters
        vector<Value> params;
        // Add pattern for each CASE statement in SELECT
        for (idx_t i = 0; i < bind_data.original_column_names.size(); i++) {
            params.push_back(Value(bind_data.search_pattern));
        }
        // Add pattern for each OR clause in WHERE
        for (idx_t i = 0; i < bind_data.original_column_names.size(); i++) {
            params.push_back(Value(bind_data.search_pattern));
        }

        state.result = prepared->Execute(params);

        if (state.result->HasError()) {
            throw InvalidInputException("Search query failed: %s", state.result->GetError().c_str());
        }

        state.query_executed = true;
    }
```

**Step 2: Commit**

```bash
git add src/search_columns_function.cpp
git commit -m "feat: execute dynamic SQL query in function

Use prepared statement with pattern parameters.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 8: Stream Results to Output

**Files:**
- Modify: `src/search_columns_function.cpp` (continuation of SearchColumnsFunction)

**Step 1: Add result streaming logic**

After the query execution code, add:

```cpp
    // Fetch next chunk if needed
    if (!state.current_chunk || state.chunk_offset >= state.current_chunk->size()) {
        state.current_chunk = state.result->Fetch();
        state.chunk_offset = 0;

        // No more data
        if (!state.current_chunk || state.current_chunk->size() == 0) {
            output.SetCardinality(0);
            return;
        }
    }

    // Copy data from current chunk to output
    idx_t count = 0;
    idx_t max_count = std::min(STANDARD_VECTOR_SIZE, state.current_chunk->size() - state.chunk_offset);

    for (idx_t i = 0; i < max_count; i++) {
        idx_t source_row = state.chunk_offset + i;

        // Copy all columns (including matched_columns from result)
        for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
            auto &source_vector = state.current_chunk->data[col_idx];
            auto &dest_vector = output.data[col_idx];

            // Copy value from source to destination
            VectorOperations::Copy(source_vector, dest_vector, source_row + 1, source_row, i);
        }

        count++;
    }

    state.chunk_offset += count;
    output.SetCardinality(count);
}
```

**Step 2: Commit**

```bash
git add src/search_columns_function.cpp
git commit -m "feat: stream query results to output chunks

Copy data from query result chunks to output.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 9: Update Function Registration

**Files:**
- Modify: `src/search_columns_function.cpp:170-187`

**Step 1: Remove 3-parameter version**

Replace the RegisterSearchColumnsFunction with:

```cpp
void RegisterSearchColumnsFunction(ExtensionLoader& loader) {
    // Only support 2 parameters now: table_name, pattern
    // Removed case_sensitive parameter - always case-insensitive
    TableFunction search_columns_func(
        "stps_search_columns",
        {LogicalType::VARCHAR, LogicalType::VARCHAR},
        SearchColumnsFunction,
        SearchColumnsBind,
        SearchColumnsInit
    );

    loader.RegisterFunction(search_columns_func);
}
```

**Step 2: Commit**

```bash
git add src/search_columns_function.cpp
git commit -m "refactor: simplify function registration to 2 parameters

Remove case_sensitive parameter version.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 10: Add Connection Header

**Files:**
- Modify: `src/search_columns_function.cpp:1-8`

**Step 1: Add missing includes**

Update the includes at the top:

```cpp
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
```

Remove:
```cpp
#include <algorithm>
#include <cctype>
```
(No longer needed)

**Step 2: Commit**

```bash
git add src/search_columns_function.cpp
git commit -m "feat: add Connection API headers

Add client_context.hpp and connection.hpp for query execution.
Remove unused algorithm and cctype headers.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 11: Test Basic Search

**Files:**
- Test manually with DuckDB

**Step 1: Build the extension**

```bash
cd /Users/ramonljevo/Desktop/fuck/stps-extension/.worktrees/search-data-values
make release
```

Expected: Build succeeds

**Step 2: Test with sample data**

```bash
duckdb :memory: << 'EOF'
LOAD 'build/release/extension/stps/stps.duckdb_extension';
CREATE TABLE test (id INTEGER, name VARCHAR, city VARCHAR);
INSERT INTO test VALUES (1, 'Alice Hoeger', 'Berlin');
INSERT INTO test VALUES (2, 'Bob Smith', 'Munich');
INSERT INTO test VALUES (3, 'Charlie Hoeger', 'Hamburg');

SELECT * FROM stps_search_columns('test', '%hoeger%');
EOF
```

Expected output:
```
┌───────┬────────────────┬─────────┬──────────────────┐
│  id   │      name      │  city   │ matched_columns  │
├───────┼────────────────┼─────────┼──────────────────┤
│     1 │ Alice Hoeger   │ Berlin  │ ['name']         │
│     3 │ Charlie Hoeger │ Hamburg │ ['name']         │
└───────┴────────────────┴─────────┴──────────────────┘
```

**Step 3: If test fails, debug and fix**

Check error messages, fix issues, rebuild, retry.

**Step 4: Commit when passing**

```bash
git add src/search_columns_function.cpp
git commit -m "test: verify basic search functionality

Tested with sample data - searches data values across all columns.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 12: Test Edge Cases

**Files:**
- Test manually with DuckDB

**Step 1: Test numeric column search**

```bash
duckdb :memory: << 'EOF'
LOAD 'build/release/extension/stps/stps.duckdb_extension';
CREATE TABLE test2 (id INTEGER, amount FLOAT, code VARCHAR);
INSERT INTO test2 VALUES (123, 45.67, 'ABC');
INSERT INTO test2 VALUES (456, 78.90, 'DEF');

-- Find '123' in integer column
SELECT * FROM stps_search_columns('test2', '%123%');
EOF
```

Expected: Returns row with id=123, matched_columns=['id']

**Step 2: Test multiple column matches**

```bash
duckdb :memory: << 'EOF'
LOAD 'build/release/extension/stps/stps.duckdb_extension';
CREATE TABLE test3 (name VARCHAR, email VARCHAR);
INSERT INTO test3 VALUES ('test@example.com', 'test@backup.com');

-- Pattern 'test' appears in both columns
SELECT * FROM stps_search_columns('test3', '%test%');
EOF
```

Expected: Returns row with matched_columns=['name', 'email']

**Step 3: Test NULL handling**

```bash
duckdb :memory: << 'EOF'
LOAD 'build/release/extension/stps/stps.duckdb_extension';
CREATE TABLE test4 (id INTEGER, name VARCHAR);
INSERT INTO test4 VALUES (1, NULL);
INSERT INTO test4 VALUES (2, 'Alice');

-- NULL values shouldn't match
SELECT * FROM stps_search_columns('test4', '%Alice%');
EOF
```

Expected: Returns only row with id=2

**Step 4: Test empty pattern**

```bash
duckdb :memory: << 'EOF'
LOAD 'build/release/extension/stps/stps.duckdb_extension';
CREATE TABLE test5 (id INTEGER, name VARCHAR);
INSERT INTO test5 VALUES (1, 'Alice');

-- Empty pattern shouldn't match anything
SELECT * FROM stps_search_columns('test5', '');
EOF
```

Expected: Returns 0 rows

**Step 5: Commit when all pass**

```bash
git commit --allow-empty -m "test: verify edge cases (numeric, NULL, multiple matches)

All edge case tests passing.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 13: Update README Documentation

**Files:**
- Modify: `README.md` (search for stps_search_columns section)

**Step 1: Update function description**

Find the `stps_search_columns` section and replace with:

```markdown
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
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: update stps_search_columns documentation

Document new behavior: searches data values, not column names.
Add migration guide for users upgrading from old version.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 14: Final Integration Test

**Files:**
- Test with user's original query

**Step 1: Rebuild and test original query**

```bash
make release
```

```bash
duckdb :memory: << 'EOF'
LOAD 'build/release/extension/stps/stps.duckdb_extension';

CREATE TABLE test (id INTEGER, name VARCHAR, city VARCHAR, email VARCHAR);
INSERT INTO test VALUES (1, 'Alice Hoeger', 'Berlin', 'alice@test.com');
INSERT INTO test VALUES (2, 'Bob Smith', 'Munich', 'bob@hoeger.de');
INSERT INTO test VALUES (3, 'Charlie Brown', 'Hamburg', 'charlie@test.com');

-- User's original query
SELECT * FROM stps_search_columns('test', '%Hoeger%');
EOF
```

Expected output:
```
┌───────┬──────────────┬─────────┬────────────────┬──────────────────┐
│  id   │     name     │  city   │     email      │ matched_columns  │
├───────┼──────────────┼─────────┼────────────────┼──────────────────┤
│     1 │ Alice Hoeger │ Berlin  │ alice@test.com │ ['name']         │
│     2 │ Bob Smith    │ Munich  │ bob@hoeger.de  │ ['email']        │
└───────┴──────────────┴─────────┴────────────────┴──────────────────┘
```

Should return 2 rows (not 0 rows as before).

**Step 2: Commit**

```bash
git commit --allow-empty -m "test: verify user's original query now works

Query returns matching rows with matched_columns array.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Implementation Notes

### Key Design Decisions

1. **Dynamic SQL over manual scanning**: Leverages DuckDB's optimized query engine
2. **Always case-insensitive**: Simplified API, matches most common use case
3. **CAST all columns**: Handle any data type automatically
4. **Prepared statements**: Prevent SQL injection via pattern parameter
5. **LIST_VALUE for matched columns**: Native DuckDB array type

### Potential Issues and Solutions

**Issue**: VectorOperations::Copy might not be the right API
**Solution**: If compilation fails, use alternative: `output.data[col_idx].SetValue(i, state.current_chunk->GetValue(col_idx, source_row))`

**Issue**: SQL generation might have syntax errors
**Solution**: Test generated SQL manually first, add debug logging

**Issue**: Performance on wide tables (100+ columns)
**Solution**: Document performance characteristics, consider column filtering in future

### Testing Strategy

1. Build and load extension
2. Test basic text search
3. Test numeric column search (CAST validation)
4. Test multiple column matches
5. Test NULL handling
6. Test edge patterns (%, empty string)
7. Verify original query works

---

## Success Criteria

- [ ] User's query `SELECT * FROM stps_search_columns('test', '%Hoeger%')` returns rows
- [ ] matched_columns array shows which columns matched
- [ ] Works with all column types (INTEGER, VARCHAR, FLOAT, etc.)
- [ ] NULL values are skipped
- [ ] Case-insensitive matching works
- [ ] Documentation updated
- [ ] No compilation errors
- [ ] Extension loads successfully
