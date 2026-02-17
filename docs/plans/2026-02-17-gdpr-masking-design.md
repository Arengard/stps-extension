# Design: stps_mask_table — GDPR-Compliant Deterministic Masking

## Purpose

Add a `stps_mask_table` table function that produces anonymized exports of any table using deterministic hash-based pseudonymization. The same input value with the same seed always produces the same masked output, preserving referential integrity across tables.

Primary use case: generating masked data exports for third parties (auditors, consultants) under GDPR requirements.

## SQL Interface

```sql
-- Mask all columns
SELECT * FROM stps_mask_table('my_table');

-- Exclude specific columns from masking
SELECT * FROM stps_mask_table('my_table', exclude := ['id', 'created_at']);

-- Custom seed for deterministic output
SELECT * FROM stps_mask_table('my_table', seed := 'audit-2026');

-- Combined
SELECT * FROM stps_mask_table('my_table', exclude := ['id'], seed := 'audit-2026');
```

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `table_name` | `VARCHAR` | Yes | — | Name of the table to mask |
| `exclude` | `VARCHAR[]` | No | `[]` | Column names to pass through unmasked |
| `seed` | `VARCHAR` | No | `'stps_default_seed'` | Secret key for deterministic hashing |

## Masking Rules by Type

| DuckDB Type | Strategy | Example |
|-------------|----------|---------|
| `VARCHAR` | Keyed hash to hex string, truncated to original length (min 4 chars) | `"Max Muller"` to `"a7f3e9b2c1"` |
| `INTEGER/BIGINT` | Keyed hash to integer in same order of magnitude | `42000` to `78312` |
| `DOUBLE/FLOAT/DECIMAL` | Keyed hash to value with same sign and magnitude | `1234.56` to `8721.34` |
| `DATE` | Keyed hash to shift by deterministic offset (0-365 days) | `2024-03-15` to `2024-07-02` |
| `TIMESTAMP` | Same as DATE for date part, zero out time component | `2024-03-15 14:30:00` to `2024-07-02 00:00:00` |
| `BOOLEAN` | Keyed hash to deterministic flip | `true` to `false` |
| `NULL` | Preserved as-is | `NULL` to `NULL` |
| Other (BLOB, LIST, STRUCT, MAP) | Cast to VARCHAR, mask as string | — |

## Hashing Approach

Keyed hash combining seed, column name, and cell value:

```
hash_input = seed + ":" + column_name + ":" + original_value
masked_value = hash(hash_input)
```

Including the column name ensures the same value in different columns produces different masked outputs (e.g., "Berlin" as a name vs. "Berlin" as a city).

Uses a simple hash function (FNV-1a or similar) — no cryptographic requirements since this is one-way pseudonymization, not encryption.

## Architecture

### Files

| File | Purpose |
|------|---------|
| `src/include/mask_functions.hpp` | Header: declares `RegisterMaskFunctions(ExtensionLoader &)` |
| `src/mask_functions.cpp` | Implementation: bind/init/scan + hash logic |

### Table Function Lifecycle

1. **Bind** — Query `PRAGMA table_info('table_name')` to discover columns and types. Store column metadata, excluded columns list, and seed in bind data. Set output columns to match input schema.
2. **Init** — Execute `SELECT * FROM table_name` and store the MaterializedQueryResult. Initialize row cursor.
3. **Scan** — For each row, iterate columns: if excluded, copy value directly; otherwise, apply type-specific masking via keyed hash. Emit rows in standard DuckDB chunk sizes.

### Registration

In `stps_unified_extension.cpp`:
```cpp
#include "include/mask_functions.hpp"
// In Load():
stps::RegisterMaskFunctions(loader);
```

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Table does not exist | `BinderException: Table 'xxx' not found` |
| Excluded column not in table | `BinderException: Column 'xxx' not found in table 'yyy'` |
| Empty table | Empty result set with correct schema |
| Unsupported types | Cast to VARCHAR, mask as string |

## Scope Boundaries (YAGNI)

Not included in this design:
- No per-column masking strategies
- No `stps_mask_database()` (call per table instead)
- No reversibility / decrypt
- No logging or audit trail
- No scalar `stps_mask()` function
- No schema-only mode

## Dependencies

None beyond what the extension already uses. Pure C++ with DuckDB built-in types and hashing.
