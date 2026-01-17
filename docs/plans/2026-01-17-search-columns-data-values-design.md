# Search Columns Data Values Implementation Design

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Modify `stps_search_columns` to search for pattern matches in actual data values across all columns, rather than searching column names.

**Architecture:** Dynamic SQL generation approach that leverages DuckDB's query engine to search across all columns with CAST to VARCHAR and LIKE pattern matching.

**Tech Stack:** C++ (DuckDB Table Function API), Dynamic SQL generation

---

## Core Functionality

### Input Parameters
- `table_name` (VARCHAR): Table to search
- `pattern` (VARCHAR): SQL LIKE pattern (e.g., '%Hoeger%')

### Output Schema
- All original columns from the source table (passthrough)
- New column: `matched_columns` (VARCHAR[]): Array of column names where pattern was found
- Only returns rows where at least one column matches the pattern

### Behavior
- **Case-insensitive** search (always - no case_sensitive parameter)
- **All column types** are cast to VARCHAR for searching (not just text columns)
- Uses SQL LIKE pattern matching (% = any chars, _ = single char)
- NULL values are skipped (don't match any pattern)

### Example Usage

```sql
-- Find all rows containing 'Hoeger' in any column
SELECT * FROM stps_search_columns('test', '%Hoeger%');

-- Sample output:
┌───────┬───────────────┬─────────┬────────────────┬──────────────────┐
│  id   │     name      │  city   │     email      │ matched_columns  │
├───────┼───────────────┼─────────┼────────────────┼──────────────────┤
│     1 │ Alice Hoeger  │ Berlin  │ alice@test.com │ ['name']         │
│     2 │ Bob Smith     │ Munich  │ bob@hoeger.de  │ ['email']        │
└───────┴───────────────┴─────────┴────────────────┴──────────────────┘
```

## Implementation Approach

### Strategy: Dynamic SQL Generation

The function will generate and execute a SQL query dynamically based on the table schema.

### Generated SQL Structure

For a table with columns `col1`, `col2`, `col3`:

```sql
WITH match_checks AS (
  SELECT *,
         CASE WHEN LOWER(CAST(col1 AS VARCHAR)) LIKE LOWER('%pattern%') THEN 'col1' END AS match_col1,
         CASE WHEN LOWER(CAST(col2 AS VARCHAR)) LIKE LOWER('%pattern%') THEN 'col2' END AS match_col2,
         CASE WHEN LOWER(CAST(col3 AS VARCHAR)) LIKE LOWER('%pattern%') THEN 'col3' END AS match_col3
  FROM table_name
)
SELECT col1, col2, col3,
       LIST(matched_col) FILTER (WHERE matched_col IS NOT NULL) as matched_columns
FROM match_checks
CROSS JOIN UNNEST([match_col1, match_col2, match_col3]) AS t(matched_col)
WHERE match_col1 IS NOT NULL OR match_col2 IS NOT NULL OR match_col3 IS NOT NULL
GROUP BY col1, col2, col3
```

Alternative simpler approach using CASE aggregation:

```sql
SELECT *,
       LIST_VALUE(
         CASE WHEN LOWER(CAST(col1 AS VARCHAR)) LIKE LOWER('%pattern%') THEN 'col1' END,
         CASE WHEN LOWER(CAST(col2 AS VARCHAR)) LIKE LOWER('%pattern%') THEN 'col2' END,
         CASE WHEN LOWER(CAST(col3 AS VARCHAR)) LIKE LOWER('%pattern%') THEN 'col3' END
       ) FILTER (WHERE value IS NOT NULL) as matched_columns
FROM table_name
WHERE LOWER(CAST(col1 AS VARCHAR)) LIKE LOWER('%pattern%')
   OR LOWER(CAST(col2 AS VARCHAR)) LIKE LOWER('%pattern%')
   OR LOWER(CAST(col3 AS VARCHAR)) LIKE LOWER('%pattern%')
```

### Implementation Phases

**Phase 1: Bind Phase** (TableFunctionBindInput)
- Get table schema from catalog
- Extract all column names and types
- Generate dynamic SQL query string
- Set up output schema (original columns + matched_columns array)

**Phase 2: Execution Phase** (TableFunction)
- Execute generated SQL using DuckDB's Connection::Query()
- Stream results to output DataChunk
- Handle result materialization

### Why This Approach

**Pros:**
- Leverages DuckDB's optimized query engine
- Handles all data types automatically via CAST
- Supports LIKE pattern matching natively
- Minimal C++ code - mostly SQL generation
- Maintainable and easy to debug

**Cons:**
- Slightly slower for tables with many columns (CAST overhead)
- Full table scan required (no index usage possible)

**Alternative Considered:**
- Manual row-by-row scanning in C++ - rejected due to complexity and performance

## Error Handling

### Error Cases

1. **Table not found**:
   - Throw `BinderException("Table '%s' not found", table_name)`

2. **Empty table**:
   - Return 0 rows (valid result)

3. **No matches**:
   - Return 0 rows (valid result)

4. **NULL values**:
   - Skip during search (CAST(NULL AS VARCHAR) won't match pattern)

5. **Very large columns**:
   - Let DuckDB handle CAST limits naturally (may truncate or error)

### Edge Cases

1. **Empty pattern** (`''`):
   - Match nothing (LIKE '' only matches empty strings)

2. **Pattern is just `%`**:
   - Match all rows with non-NULL values

3. **Tables with no columns**:
   - Throw error during bind phase

4. **Column name conflicts**:
   - If table already has `matched_columns`, alias it to `matched_columns_original`

5. **Special characters in pattern**:
   - User responsible for escaping (%, _) - standard SQL LIKE behavior

6. **Pattern contains wildcards**:
   - `%` matches any characters
   - `_` matches single character
   - Use `\%` and `\_` to match literal % and _

## Performance Considerations

### Expected Performance

- **Small tables** (< 1000 rows, < 50 columns): < 100ms
- **Medium tables** (< 100k rows, < 100 columns): < 1s
- **Large tables** (1M+ rows, 100+ columns): May be slow (10s+)

### Optimization Notes

- Full table scan required (cannot use indexes)
- CAST overhead for every column in every row
- Pattern matching overhead (LIKE is not indexed)

### Future Enhancements (Not Implemented Now)

- Column filtering: Search only specific columns
- Parallel execution: Split table into chunks
- Type-specific search: Avoid CAST for VARCHAR columns

## Testing Strategy

### Unit Tests

1. **Basic text search**: Find 'Hoeger' in VARCHAR columns
2. **Numeric search**: Find '123' in INTEGER column (cast to VARCHAR)
3. **Multiple column matches**: Row matches in name AND email
4. **No matches**: Pattern not found in any row
5. **Empty table**: No rows to search
6. **NULL handling**: NULLs don't match any pattern
7. **Edge patterns**:
   - `%` (match all non-NULL)
   - `%%` (same as %)
   - `_test_` (exact length match)
   - Empty pattern `''`

### Integration Tests

1. **Real table search**: Use actual test data
2. **All data types**: INT, FLOAT, DATE, TIMESTAMP, VARCHAR, BOOLEAN
3. **Large result set**: Many matching rows
4. **Performance test**: 10k rows, 20 columns

## API Changes (Breaking)

### Old Behavior (Searching Column Names)

```sql
-- Found columns named 'customer_%'
SELECT * FROM stps_search_columns('orders', 'customer_%');
-- Returns: column_name, column_index
```

### New Behavior (Searching Data Values)

```sql
-- Finds rows where any column contains 'customer'
SELECT * FROM stps_search_columns('orders', '%customer%');
-- Returns: all columns + matched_columns array
```

### Migration Guide

**Before:**
```sql
-- Find columns with 'date' in name
SELECT * FROM stps_search_columns('my_table', '%date%');
```

**After:**
```sql
-- Use DuckDB's DESCRIBE or SHOW instead
DESCRIBE my_table;
SHOW COLUMNS FROM my_table;

-- Or use information_schema
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'my_table'
  AND column_name LIKE '%date%';
```

## Documentation Updates

### README.md

Update function description from:
```
Search for columns in a table by name pattern
```

To:
```
Search for pattern matches in data values across all columns
```

### Example Updates

Replace all examples showing column name search with data value search examples.

## Implementation Checklist

- [ ] Modify SearchColumnsBindData structure
- [ ] Update bind phase to generate dynamic SQL
- [ ] Change execution to run dynamic SQL query
- [ ] Handle matched_columns array output
- [ ] Add error handling for edge cases
- [ ] Update README.md documentation
- [ ] Add migration guide to docs
- [ ] Write unit tests
- [ ] Test with various data types
- [ ] Test performance on large tables

---

## Open Questions

None - design approved by user.
