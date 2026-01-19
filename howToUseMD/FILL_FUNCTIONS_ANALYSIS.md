# Fill Functions Analysis and Recommendation

## Current Status

The `fill_functions.cpp` and `fill_functions.hpp` files in this extension are **incomplete and disabled** for good reason.

### Files Analyzed
- `src/fill_functions.cpp` - Empty registration stub
- `src/include/fill_functions.hpp` - Header with `WindowFillDownExecutor` and `WindowFillUpExecutor` classes
- Status in `CMakeLists.txt`: **Commented out** with note "Temporarily disabled - DuckDB API changed"
- Status in `stps_unified_extension.cpp`: **Commented out** registration

## Key Finding: DuckDB Already Has This Built-In

**DuckDB v1.4+ includes a native `fill()` window function** that handles NULL value interpolation.

### DuckDB's Built-In fill() Function

```sql
SELECT
    value,
    fill(value) OVER (ORDER BY timestamp) as filled_value
FROM my_table;
```

**Capabilities:**
- Replaces NULL values with linear interpolation
- Based on closest non-NULL values
- Uses sort/order values for weighting
- Implemented in `duckdb/src/function/window/window_value_function.cpp`
- Uses `WindowFillExecutor` class (part of DuckDB core)

## Why Custom Implementation Was Abandoned

1. **API Changed**: DuckDB's window function API evolved significantly
2. **Already Built-In**: The `fill()` function was added to DuckDB core (v1.4.0)
3. **Redundant**: No need to duplicate functionality that exists in core DuckDB

## Original Intent vs Reality

### Original Plan (Incomplete)
The extension attempted to implement:
- `WindowFillDownExecutor` - Forward fill (propagate last non-NULL forward)
- `WindowFillUpExecutor` - Backward fill (propagate next non-NULL backward)

### Current Reality
DuckDB's `fill()` function provides **linear interpolation**, which is more sophisticated than simple forward/backward fill.

## Recommendations

### Option 1: Remove the Files (Recommended)

**Action:** Delete the incomplete implementation entirely.

**Rationale:**
- Code is incomplete and non-functional
- DuckDB's built-in `fill()` is superior
- Reduces maintenance burden
- Eliminates confusion

```bash
git rm src/fill_functions.cpp
git rm src/include/fill_functions.hpp
# Also remove commented lines from CMakeLists.txt and stps_unified_extension.cpp
```

### Option 2: Document DuckDB's fill() Function

**Action:** Keep files but convert to documentation/examples.

**Rationale:**
- Helps users understand how to use DuckDB's fill()
- Provides migration guide if users expected fill_down/fill_up
- Shows equivalent functionality

### Option 3: Implement True Fill Down/Up (Not Recommended)

**Action:** Complete the implementation for simple forward/backward fill without interpolation.

**Why Not:**
- High effort (complex window function API)
- Limited use case (most users want interpolation)
- DuckDB's fill() is usually better
- Can achieve with SQL workarounds if really needed

## Migration Guide for Users

If someone wants fill-down or fill-up behavior (without interpolation):

### Forward Fill (fill_down equivalent)
```sql
SELECT
    value,
    last_value(value IGNORE NULLS) OVER (
        ORDER BY timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as filled_down
FROM my_table;
```

### Backward Fill (fill_up equivalent)
```sql
SELECT
    value,
    first_value(value IGNORE NULLS) OVER (
        ORDER BY timestamp
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) as filled_up
FROM my_table;
```

### Linear Interpolation (DuckDB built-in)
```sql
SELECT
    value,
    fill(value) OVER (ORDER BY timestamp) as interpolated
FROM my_table;
```

## Conclusion

**Recommendation: Delete the incomplete fill_functions implementation.**

DuckDB's native `fill()` function is:
- Already implemented and tested
- More sophisticated (linear interpolation)
- Part of the core database
- Well-documented
- Performant

The incomplete custom implementation:
- Adds no value
- Creates maintenance burden
- May confuse users
- Is already superseded by better built-in functionality

## Action Plan

1. Delete `src/fill_functions.cpp`
2. Delete `src/include/fill_functions.hpp`
3. Remove commented lines from `CMakeLists.txt`
4. Remove commented lines from `stps_unified_extension.cpp`
5. Update documentation to reference DuckDB's `fill()` function
6. Add examples showing how to use `fill()`, `last_value`, and `first_value` for various fill patterns

## References

- [DuckDB Window Functions Documentation](https://duckdb.org/docs/stable/sql/functions/window_functions)
- [DuckDB 1.4.0 Release](https://duckdb.org/2025/09/16/announcing-duckdb-140) - Introduced fill() function
- [DuckDB fill() Discussion](https://github.com/duckdb/duckdb/discussions/17040)
- DuckDB Source: `duckdb/src/function/window/window_value_function.cpp` (WindowFillExecutor)
