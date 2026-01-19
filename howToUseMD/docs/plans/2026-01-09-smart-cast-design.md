# stps_smart_cast Design Document

## Overview

A smart type casting system for DuckDB that automatically detects and converts VARCHAR columns to their appropriate types (BOOLEAN, INTEGER, DOUBLE, DATE, TIMESTAMP, UUID) with context-aware German/US locale handling.

## Functions

| Function | Type | Purpose |
|----------|------|---------|
| `stps_smart_cast(table)` | Table | Cast all VARCHAR columns to detected types |
| `stps_smart_cast_analyze(table)` | Table | Return type detection metadata |
| `stps_smart_cast(value)` | Scalar | Auto-detect and cast single value |
| `stps_smart_cast(value, type)` | Scalar | Cast single value to explicit type |

## Usage Examples

```sql
-- Table function: cast all columns
SELECT * FROM stps_smart_cast('my_table')

-- Table function: analyze without casting
SELECT * FROM stps_smart_cast_analyze('my_table')

-- Scalar function: auto-detect
SELECT stps_smart_cast(my_column) FROM table

-- Scalar function: explicit target type
SELECT stps_smart_cast(my_column, 'DOUBLE') FROM table

-- With optional parameters
SELECT * FROM stps_smart_cast('my_table', min_success_rate := 0.5, locale := 'de')
```

## Type Detection Priority

Types are detected in this order (most specific first):

1. **BOOLEAN** - `true`, `false`, `yes`, `no`, `ja`, `nein`, `1`, `0`
2. **INTEGER** - Whole numbers with optional thousands separator
3. **DOUBLE** - Decimal numbers with context-aware locale detection
4. **DATE** - Comprehensive multi-format support (see below)
5. **TIMESTAMP** - Date formats with time component
6. **UUID** - Standard UUID format
7. **VARCHAR** - Fallback when nothing else matches

## Date Format Support

### Standard Formats

| Category | Formats |
|----------|---------|
| ISO | `2024-01-15` |
| Compact | `20240115` |
| German dot | `15.01.2024`, `15.1.2024`, `15.01.24`, `15.1.24` |
| US slash | `01/15/2024`, `1/15/2024`, `01/15/24` |
| EU slash | `15/01/2024`, `15/1/2024`, `15/01/24` |
| Year-first slash | `2024/01/15` |
| Dash EU | `15-01-2024`, `15-1-2024`, `15-01-24` |
| Dash US | `01-15-2024`, `1-15-2024`, `01-15-24` |

### Written Month Formats

| Category | Formats |
|----------|---------|
| English month | `15 Jan 2024`, `Jan 15, 2024`, `January 15, 2024` |
| German month | `15. Januar 2024`, `15. Jan. 2024` |

### Special Formats

| Category | Formats |
|----------|---------|
| Month-only | `Jan 2024`, `Januar 2024`, `2024-01`, `01/2024` |
| Quarter | `Q1 2024`, `2024-Q1`, `1Q2024` |
| Week | `2024-W03`, `W03-2024`, `W03 2024` |
| Relative | `today`, `yesterday`, `tomorrow`, `heute`, `gestern`, `morgen` |

### Timestamp Extensions

All date formats above can include time components:
- `10:30:00`
- `10:30`
- `T10:30:00Z`
- `T10:30:00+01:00`

### Ambiguity Resolution

When date format is ambiguous (e.g., `01/02/2024`):
1. Scan column for unambiguous dates (day > 12)
2. Use detected format for entire column
3. Default to German/EU (day-first) if no context

## German Number Format Detection

### Context-Aware Locale Detection

The function scans all values in a column to detect number format:

**Unambiguous patterns:**

| Pattern | Example | Interpretation |
|---------|---------|----------------|
| Comma as decimal | `1234,56` | German -> `1234.56` |
| Dot-thousands + comma-decimal | `1.234,56` | German -> `1234.56` |
| Comma-thousands + dot-decimal | `1,234.56` | US -> `1234.56` |

**Ambiguous patterns (resolved by column context):**

| Pattern | German | US |
|---------|--------|-----|
| `1.234` | `1234` | `1.234` |
| `1,234` | `1.234` | `1234` |

**Detection algorithm:**

1. Scan all non-null values in column
2. If any value has unambiguous German pattern -> column is German
3. If any value has unambiguous US pattern -> column is US
4. If mixed signals -> keep as VARCHAR (conflict)
5. If all ambiguous -> default to German

### Edge Cases

| Case | Behavior |
|------|----------|
| Scientific notation `1.23e10` | Always parsed as US/scientific |
| Currency symbols `€123,45` | Strip symbol, parse as DOUBLE |
| Percentage `45%` | Strip %, convert to decimal (`0.45`) |
| Leading zeros `007` | Keep as VARCHAR (likely ID/code) |
| Negative numbers `-123` | Parse correctly as INTEGER/DOUBLE |

## Cast Failure Handling

- Values that can't cast to detected type -> NULL
- Original NULL values -> remain NULL
- Empty strings / whitespace -> NULL (pre-processing)

### Column-Level Fallback

- If < `min_success_rate` of values cast successfully -> keep column as VARCHAR
- Default `min_success_rate`: 0.1 (10%)

## Optional Parameters

### Table Function Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `min_success_rate` | DOUBLE | `0.1` | Minimum cast success rate (0.0-1.0) to commit to type |
| `locale` | VARCHAR | `'auto'` | Force locale: `'auto'`, `'de'`, `'en'` |
| `date_format` | VARCHAR | `'auto'` | Force date format: `'auto'`, `'dmy'`, `'mdy'`, `'ymd'` |

### Scalar Function Parameters

| Overload | Parameters |
|----------|------------|
| Auto-detect | `stps_smart_cast(value)` |
| Explicit type | `stps_smart_cast(value, target_type)` |
| With locale | `stps_smart_cast(value, target_type, locale := 'de')` |

## Analyze Function Output Schema

`stps_smart_cast_analyze('table_name')` returns:

| Column | Type | Description |
|--------|------|-------------|
| column_name | VARCHAR | Original column name |
| original_type | VARCHAR | Source type (usually VARCHAR) |
| detected_type | VARCHAR | Inferred target type |
| total_rows | BIGINT | Total row count |
| null_count | BIGINT | Rows that are NULL or empty |
| cast_success_count | BIGINT | Values successfully castable |
| cast_failure_count | BIGINT | Values that would become NULL |

## Implementation Architecture

### File Structure

```
src/
├── smart_cast_function.cpp      # Table functions
├── smart_cast_scalar.cpp        # Scalar function
├── include/
│   ├── smart_cast_function.hpp
│   ├── smart_cast_scalar.hpp
│   └── smart_cast_utils.hpp     # Shared detection/parsing logic
```

### Core Components

**SmartCastUtils** - Shared parsing utilities:
- `DetectLocale(vector<string>)` - column-wide locale detection
- `DetectType(string, locale)` - single value type detection
- `ParseBoolean(string)` - bool parsing
- `ParseInteger(string, locale)` - integer parsing with thousands
- `ParseDouble(string, locale)` - double parsing with locale
- `ParseDate(string)` - multi-format date parsing
- `ParseTimestamp(string)` - multi-format timestamp parsing
- `ParseUUID(string)` - UUID validation and parsing

### Table Function Flow

1. **Bind**: Validate table exists, discover schema
2. **Init**: Full scan to detect types per column, build cast query
3. **Scan**: Execute transformed query, stream results

### Scalar Function Flow

1. Single value -> detect type -> cast and return

## Pre-processing Rules

Before type detection:
1. Trim leading/trailing whitespace
2. Empty strings -> NULL
3. Whitespace-only strings -> NULL
4. NULL values skipped during type detection
