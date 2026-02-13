# GoBD Import Pipeline Design

**Date:** 2026-02-13
**Status:** Approved

## Summary

Refactor GoBD data ingestion from read-only table functions into a table-creating import pipeline. Add three functions that discover GoBD tables, create them as persistent DuckDB tables, normalize column names, drop empty columns, and smart-cast types.

## New Functions

### `stps_read_gobd_all(file_path VARCHAR, delimiter := VARCHAR, overwrite := BOOLEAN) -> TABLE`

Reads all tables from a **local** GoBD/GDPDU export and creates them as persistent DuckDB tables.

### `stps_read_gobd_cloud_all(url VARCHAR, username := VARCHAR, password := VARCHAR, delimiter := VARCHAR, overwrite := BOOLEAN) -> TABLE`

Same pipeline but reads from a **WebDAV/Nextcloud** hosted GoBD export.

### `stps_read_gobd_cloud_zip_all(url VARCHAR, username := VARCHAR, password := VARCHAR, delimiter := VARCHAR, overwrite := BOOLEAN) -> TABLE`

Same pipeline but downloads and extracts a **ZIP file** from cloud containing a GoBD export (index.xml + CSV files).

### Return Value (all three)

Summary table with one row per created table:

| Column | Type | Description |
|--------|------|-------------|
| table_name | VARCHAR | Snake_case name of the created table |
| rows_imported | BIGINT | Number of rows inserted |
| columns_created | INTEGER | Number of columns after cleanup |

### Parameters

- `delimiter` — CSV delimiter, default `';'`
- `overwrite` — If `true`, DROP existing table before creating. If `false` (default), error if table exists.
- `username` / `password` — Cloud authentication (cloud variants only)

## Pipeline (per table)

1. **Parse** — Read index.xml, discover all tables and their schemas
2. **Read CSV** — Load CSV content (from file, HTTP, or ZIP extraction)
3. **Normalize columns** — Convert column names to snake_case (lowercase, replace spaces/special chars with underscores, collapse duplicates with `_2`, `_3` suffix)
4. **Create table** — `CREATE TABLE <snake_case_name> (col1 VARCHAR, ...)` with all VARCHAR initially
5. **Insert data** — Parse CSV rows and INSERT into the table
6. **Drop empty columns** — `ALTER TABLE DROP COLUMN` for columns where all values are NULL or empty
7. **Smart cast** — Detect and convert column types (integers, decimals, dates) using existing smart_cast logic

## Data Source Abstraction

Shared struct populated by each source:

```cpp
struct GobdImportData {
    vector<GobdTable> tables;           // from index.xml
    map<string, string> csv_contents;   // table URL -> CSV content string
};
```

- **Local:** Read files from disk via ifstream
- **Cloud:** Download via curl (existing DiscoverAndDownloadIndexXml + DownloadFile)
- **Cloud ZIP:** Download ZIP via curl, extract with miniz in-memory

All three feed into `ExecuteGobdImportPipeline()` which handles steps 3-7.

## File Changes

| File | Change |
|------|--------|
| `src/include/gobd_reader.hpp` | Add `GobdImportData` struct and `ExecuteGobdImportPipeline` declaration |
| `src/gobd_reader.cpp` | Replace current `stps_read_gobd_all` with table-creating pipeline version |
| `src/gobd_cloud_reader.cpp` | Add `stps_read_gobd_cloud_all` and `stps_read_gobd_cloud_zip_all` |
| `src/stps_unified_extension.cpp` | No changes (registration functions already called) |
| `README.md` | Update documentation |

## What Stays Unchanged

- `stps_read_gobd` — Single-table read-only function (still useful for SELECT queries)
- `stps_gobd_list_tables` / `stps_gobd_table_schema` — Discovery functions
- All cloud single-table functions
- All existing utility functions (used internally by pipeline)

## Encoding

All three sources apply Windows-1252 to UTF-8 auto-detection (common for German accounting exports).

## Overwrite Behavior

- `overwrite := false` (default): If table exists, skip it and include error message in summary
- `overwrite := true`: DROP TABLE IF EXISTS before CREATE
