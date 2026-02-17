# Time Travel for DuckDB (Without DuckLake)

Native time travel support for DuckDB tables via the STPS extension. Enables version-based and timestamp-based historical queries on opted-in tables using transparent DML interception.

## Requirements

- Automatic change tracking on opted-in tables only
- Normal INSERT/UPDATE/DELETE syntax (no wrapper functions)
- Full row snapshots per change (not column-level diffs)
- Version-based and timestamp-based reconstruction
- All history kept forever (no compaction)
- Single-column primary key required per tracked table

## Data Model

### Metadata Table: `_stps_tt_tables`

| Column | Type | Description |
|--------|------|-------------|
| `table_name` | VARCHAR | The tracked table name |
| `pk_column` | VARCHAR | Primary key column name |
| `current_version` | BIGINT | Auto-incrementing version counter |
| `created_at` | TIMESTAMP | When tracking was enabled |

### History Table: `_stps_history_{table_name}`

One per tracked table. Mirrors ALL columns of the original table, plus:

| Extra Column | Type | Description |
|--------|------|-------------|
| `_tt_version` | BIGINT | Version number of this change |
| `_tt_operation` | VARCHAR | `'INSERT'`, `'UPDATE'`, or `'DELETE'` |
| `_tt_timestamp` | TIMESTAMP | When the change happened |
| `_tt_pk_value` | VARCHAR | The primary key value (cast to string for uniformity) |

- INSERT and UPDATE store the AFTER image (new state of the row)
- DELETE stores the BEFORE image (the row as it was before deletion)

## Architecture

### Transparent DML Interception via OptimizerExtension

The extension registers a DuckDB `OptimizerExtension` that fires for every query plan before execution. When it detects `LogicalInsert`, `LogicalUpdate`, or `LogicalDelete` on a tracked table, it rewrites the plan into an atomic CTE using DuckDB's `RETURNING` clause.

A thread-local `g_capturing_history` flag prevents the optimizer from recursively intercepting its own history writes.

### Rewritten SQL Patterns

**INSERT:**

```sql
WITH new_version AS (
    UPDATE _stps_tt_tables SET current_version = current_version + 1
    WHERE table_name = 'my_table' RETURNING current_version
),
new_rows AS (INSERT INTO my_table VALUES (...) RETURNING *)
INSERT INTO _stps_history_my_table
SELECT new_rows.*, 'INSERT', now(), new_version.current_version
FROM new_rows, new_version
```

**UPDATE:**

```sql
WITH new_version AS (
    UPDATE _stps_tt_tables SET current_version = current_version + 1
    WHERE table_name = 'my_table' RETURNING current_version
),
changed_rows AS (UPDATE my_table SET ... WHERE ... RETURNING *)
INSERT INTO _stps_history_my_table
SELECT changed_rows.*, 'UPDATE', now(), new_version.current_version
FROM changed_rows, new_version
```

**DELETE:**

```sql
WITH new_version AS (
    UPDATE _stps_tt_tables SET current_version = current_version + 1
    WHERE table_name = 'my_table' RETURNING current_version
),
deleted_rows AS (DELETE FROM my_table WHERE ... RETURNING *)
INSERT INTO _stps_history_my_table
SELECT deleted_rows.*, 'DELETE', now(), new_version.current_version
FROM deleted_rows, new_version
```

All three are fully atomic -- the DML and history capture happen in a single transaction.

### Initial Snapshot

When `stps_tt_enable` is called, all existing rows are captured as version 0 with operation `INSERT`, so time travel works from the very beginning.

## API Surface

### Management Functions (Scalar)

| Function | Description |
|----------|-------------|
| `stps_tt_enable('table', 'pk_column')` | Enable tracking. Creates history table and metadata entry. Snapshots existing rows as version 0. |
| `stps_tt_disable('table')` | Disable tracking. Drops history table and removes metadata. |
| `stps_tt_status()` | Returns a table of all tracked tables with their current version and row counts. |

### Query Functions (Table Functions)

| Function | Description |
|----------|-------------|
| `stps_time_travel('table', version := N)` | Reconstruct full table state at version N |
| `stps_time_travel('table', as_of := TIMESTAMP)` | Reconstruct table state at a point in time |
| `stps_tt_log('table')` | View full change history (all operations, versions, timestamps) |
| `stps_tt_diff('table', from_version := N, to_version := M)` | Show rows that changed between two versions, with operation type |

### Usage Example

```sql
-- 1. Enable tracking
SELECT stps_tt_enable('customers', 'id');

-- 2. Normal DML -- history captured transparently
INSERT INTO customers VALUES (1, 'Alice', 'alice@example.com');
UPDATE customers SET email = 'alice@new.com' WHERE id = 1;
DELETE FROM customers WHERE id = 1;

-- 3. Time travel
SELECT * FROM stps_time_travel('customers', version := 1);
SELECT * FROM stps_time_travel('customers', as_of := '2026-02-01'::TIMESTAMP);

-- 4. Change log
SELECT * FROM stps_tt_log('customers');

-- 5. Diff between versions
SELECT * FROM stps_tt_diff('customers', from_version := 0, to_version := 2);
```

## Reconstruction Algorithm

For `stps_time_travel('table', version := N)`:

```sql
WITH latest_per_pk AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY _tt_pk_value ORDER BY _tt_version DESC) as rn
    FROM _stps_history_my_table
    WHERE _tt_version <= N
)
SELECT [all original columns]
FROM latest_per_pk
WHERE rn = 1
  AND _tt_operation != 'DELETE'
```

For each PK, find the most recent change at or before version N. If it was INSERT or UPDATE, the stored row is the state. If DELETE, exclude it.

For timestamp-based queries, filter on `_tt_timestamp <= T` instead.

The history table is indexed on `(_tt_pk_value, _tt_version DESC)` for fast reconstruction.

## Limitations

1. **ALTER TABLE** -- Adding/dropping columns on a tracked table breaks the history table schema. Must disable and re-enable tracking (loses history).
2. **COPY INTO** -- May bypass the optimizer hook. Use `INSERT INTO ... SELECT * FROM read_csv(...)` instead.
3. **CREATE OR REPLACE TABLE** -- Replacing a tracked table breaks tracking. Must re-enable.
4. **Composite primary keys** -- First version supports single-column PKs only.
5. **Large tables** -- Initial snapshot on `stps_tt_enable` doubles storage temporarily.
6. **DuckDB version coupling** -- `OptimizerExtension` is an internal API that may change between DuckDB versions.
