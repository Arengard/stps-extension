# stps_mask_table Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a `stps_mask_table` table function that deterministically masks all columns of a table for GDPR-compliant anonymized exports.

**Architecture:** A single table function using DuckDB's bind/init/scan lifecycle. Bind discovers the source table schema, Init executes `SELECT *` to materialize the data, and Scan applies type-specific keyed hashing (FNV-1a) to each cell. Named parameters `exclude` and `seed` control which columns are passed through and the hash key.

**Tech Stack:** C++, DuckDB extension API (TableFunction, named_parameters), FNV-1a hash (inline implementation, no dependencies).

---

### Task 1: Create the header file

**Files:**
- Create: `src/include/mask_functions.hpp`

**Step 1: Write the header**

```cpp
#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

void RegisterMaskFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
```

**Step 2: Commit**

```bash
git add src/include/mask_functions.hpp
git commit -m "Add mask_functions header for stps_mask_table"
```

---

### Task 2: Create the implementation file with hash utility and BindData

**Files:**
- Create: `src/mask_functions.cpp`

**Step 1: Write the hash utility and BindData struct**

Write `src/mask_functions.cpp` with:

```cpp
#include "include/mask_functions.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <cstdint>
#include <cmath>
#include <string>
#include <vector>
#include <sstream>
#include <iomanip>

namespace duckdb {
namespace stps {

// FNV-1a 64-bit hash
static uint64_t FNV1aHash(const std::string &data) {
    uint64_t hash = 14695981039346656037ULL;
    for (unsigned char c : data) {
        hash ^= c;
        hash *= 1099511628211ULL;
    }
    return hash;
}

// Keyed hash: combines seed + column_name + value
static uint64_t KeyedHash(const std::string &seed, const std::string &column_name, const std::string &value) {
    std::string input = seed + ":" + column_name + ":" + value;
    return FNV1aHash(input);
}

// Convert hash to hex string of given length (min 4)
static std::string HashToHexString(uint64_t hash, idx_t target_length) {
    if (target_length < 4) target_length = 4;
    std::ostringstream oss;
    // Use multiple hashes if needed for long strings
    uint64_t h = hash;
    while (oss.str().size() < target_length) {
        oss << std::hex << std::setfill('0') << std::setw(16) << h;
        h = FNV1aHash(std::to_string(h));
    }
    return oss.str().substr(0, target_length);
}

struct MaskTableBindData : public TableFunctionData {
    std::string table_name;
    std::string seed;
    std::vector<std::string> exclude_columns;
    // Discovered schema
    std::vector<std::string> column_names;
    std::vector<LogicalType> column_types;
    std::vector<bool> column_masked; // true = mask, false = pass through
};

struct MaskTableGlobalState : public GlobalTableFunctionState {
    unique_ptr<QueryResult> query_result;
    unique_ptr<DataChunk> current_chunk;
    idx_t chunk_offset = 0;
    bool finished = false;
};

} // namespace stps
} // namespace duckdb
```

**Step 2: Commit**

```bash
git add src/mask_functions.cpp
git commit -m "Add mask_functions.cpp with hash utility and bind/state structs"
```

---

### Task 3: Implement the Bind function

**Files:**
- Modify: `src/mask_functions.cpp` (append before closing namespace braces)

**Step 1: Add the Bind function**

Append the following before the closing `} // namespace stps` / `} // namespace duckdb`:

```cpp
static unique_ptr<FunctionData> MaskTableBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names
) {
    auto result = make_uniq<MaskTableBindData>();

    // Get table name (required positional arg)
    result->table_name = input.inputs[0].GetValue<string>();
    result->seed = "stps_default_seed";

    // Parse named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "seed") {
            result->seed = kv.second.ToString();
        } else if (kv.first == "exclude") {
            if (kv.second.type().id() == LogicalTypeId::LIST) {
                auto &list_children = ListValue::GetChildren(kv.second);
                for (const auto &child : list_children) {
                    result->exclude_columns.push_back(child.ToString());
                }
            }
        }
    }

    // Discover table schema using information_schema
    Connection conn(context.db->GetDatabase(context));
    auto schema_result = conn.Query(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = '" + result->table_name + "' "
        "AND table_schema NOT IN ('information_schema', 'pg_catalog') "
        "ORDER BY ordinal_position"
    );

    if (schema_result->HasError()) {
        throw BinderException("stps_mask_table: Failed to query schema for table '%s': %s",
                              result->table_name.c_str(), schema_result->GetError().c_str());
    }

    // Collect columns
    while (true) {
        auto chunk = schema_result->Fetch();
        if (!chunk || chunk->size() == 0) break;
        for (idx_t row = 0; row < chunk->size(); row++) {
            result->column_names.push_back(chunk->data[0].GetValue(row).ToString());
        }
    }

    if (result->column_names.empty()) {
        throw BinderException("stps_mask_table: Table '%s' not found or has no columns", result->table_name.c_str());
    }

    // Validate excluded columns exist
    for (const auto &excl : result->exclude_columns) {
        bool found = false;
        for (const auto &col : result->column_names) {
            if (col == excl) { found = true; break; }
        }
        if (!found) {
            throw BinderException("stps_mask_table: Column '%s' not found in table '%s'",
                                  excl.c_str(), result->table_name.c_str());
        }
    }

    // Now get actual types by querying the table with LIMIT 0
    auto type_result = conn.Query("SELECT * FROM \"" + result->table_name + "\" LIMIT 0");
    if (type_result->HasError()) {
        throw BinderException("stps_mask_table: Failed to query table '%s': %s",
                              result->table_name.c_str(), type_result->GetError().c_str());
    }

    // Use the result types and names from the actual query
    result->column_names.clear();
    for (idx_t i = 0; i < type_result->names.size(); i++) {
        result->column_names.push_back(type_result->names[i]);
        result->column_types.push_back(type_result->types[i]);

        // Check if this column is excluded
        bool excluded = false;
        for (const auto &excl : result->exclude_columns) {
            if (excl == type_result->names[i]) { excluded = true; break; }
        }
        result->column_masked.push_back(!excluded);

        // Output schema matches input schema
        return_types.push_back(type_result->types[i]);
        names.push_back(type_result->names[i]);
    }

    return std::move(result);
}
```

**Step 2: Commit**

```bash
git add src/mask_functions.cpp
git commit -m "Add MaskTableBind: schema discovery and parameter parsing"
```

---

### Task 4: Implement Init and the masking logic

**Files:**
- Modify: `src/mask_functions.cpp` (append before closing namespace braces)

**Step 1: Add Init function**

```cpp
static unique_ptr<GlobalTableFunctionState> MaskTableInit(
    ClientContext &context,
    TableFunctionInitInput &input
) {
    auto state = make_uniq<MaskTableGlobalState>();
    state->finished = false;
    return std::move(state);
}
```

**Step 2: Add the type-specific masking function**

```cpp
static Value MaskValue(const Value &original, const LogicalType &type,
                       const std::string &seed, const std::string &column_name) {
    if (original.IsNull()) {
        return Value(nullptr);
    }

    std::string str_val = original.ToString();
    uint64_t hash = KeyedHash(seed, column_name, str_val);

    switch (type.id()) {
        case LogicalTypeId::VARCHAR: {
            idx_t len = str_val.size();
            return Value(HashToHexString(hash, len));
        }
        case LogicalTypeId::INTEGER: {
            int32_t orig = original.GetValue<int32_t>();
            // Preserve order of magnitude
            int32_t magnitude = orig == 0 ? 1 : static_cast<int32_t>(std::pow(10, static_cast<int>(std::log10(std::abs(orig)))));
            int32_t masked = static_cast<int32_t>(hash % (magnitude * 9) + magnitude);
            if (orig < 0) masked = -masked;
            return Value::INTEGER(masked);
        }
        case LogicalTypeId::BIGINT: {
            int64_t orig = original.GetValue<int64_t>();
            int64_t magnitude = orig == 0 ? 1 : static_cast<int64_t>(std::pow(10, static_cast<int>(std::log10(std::abs(static_cast<double>(orig))))));
            int64_t masked = static_cast<int64_t>(hash % (magnitude * 9) + magnitude);
            if (orig < 0) masked = -masked;
            return Value::BIGINT(masked);
        }
        case LogicalTypeId::DOUBLE: {
            double orig = original.GetValue<double>();
            if (orig == 0.0) return Value::DOUBLE(0.0);
            double magnitude = std::pow(10, std::floor(std::log10(std::abs(orig))));
            double fraction = static_cast<double>(hash % 10000) / 10000.0;
            double masked = magnitude * (1.0 + fraction * 9.0);
            if (orig < 0) masked = -masked;
            return Value::DOUBLE(masked);
        }
        case LogicalTypeId::FLOAT: {
            float orig = original.GetValue<float>();
            if (orig == 0.0f) return Value::FLOAT(0.0f);
            double magnitude = std::pow(10, std::floor(std::log10(std::abs(orig))));
            double fraction = static_cast<double>(hash % 10000) / 10000.0;
            float masked = static_cast<float>(magnitude * (1.0 + fraction * 9.0));
            if (orig < 0) masked = -masked;
            return Value::FLOAT(masked);
        }
        case LogicalTypeId::DATE: {
            auto date_val = original.GetValue<date_t>();
            int32_t days_offset = static_cast<int32_t>(hash % 365) + 1;
            date_t masked_date = date_t(date_val.days + days_offset);
            return Value::DATE(masked_date);
        }
        case LogicalTypeId::TIMESTAMP:
        case LogicalTypeId::TIMESTAMP_TZ: {
            auto ts_val = original.GetValue<timestamp_t>();
            auto date_part = Timestamp::GetDate(ts_val);
            int32_t days_offset = static_cast<int32_t>(hash % 365) + 1;
            date_t masked_date = date_t(date_part.days + days_offset);
            // Zero out time component
            auto masked_ts = Timestamp::FromDatetime(masked_date, dtime_t(0));
            if (type.id() == LogicalTypeId::TIMESTAMP_TZ) {
                return Value::TIMESTAMPTZ(masked_ts);
            }
            return Value::TIMESTAMP(masked_ts);
        }
        case LogicalTypeId::BOOLEAN: {
            bool masked = (hash % 2) == 0;
            return Value::BOOLEAN(masked);
        }
        case LogicalTypeId::DECIMAL: {
            // Treat as double for masking, then cast back
            double orig = original.GetValue<double>();
            if (orig == 0.0) return Value(0).DefaultCastAs(type);
            double magnitude = std::pow(10, std::floor(std::log10(std::abs(orig))));
            double fraction = static_cast<double>(hash % 10000) / 10000.0;
            double masked = magnitude * (1.0 + fraction * 9.0);
            if (orig < 0) masked = -masked;
            return Value(masked).DefaultCastAs(type);
        }
        default: {
            // For unsupported types: cast to string, mask as string
            idx_t len = str_val.size();
            return Value(HashToHexString(hash, len));
        }
    }
}
```

**Step 3: Commit**

```bash
git add src/mask_functions.cpp
git commit -m "Add MaskTableInit and MaskValue type-specific masking logic"
```

---

### Task 5: Implement the Scan function

**Files:**
- Modify: `src/mask_functions.cpp` (append before closing namespace braces)

**Step 1: Add the Scan function**

```cpp
static void MaskTableScan(
    ClientContext &context,
    TableFunctionInput &data_p,
    DataChunk &output
) {
    auto &bind_data = data_p.bind_data->Cast<MaskTableBindData>();
    auto &state = data_p.global_state->Cast<MaskTableGlobalState>();

    if (state.finished) {
        output.SetCardinality(0);
        return;
    }

    // On first call, execute the query
    if (!state.query_result) {
        Connection conn(context.db->GetDatabase(context));
        state.query_result = conn.Query("SELECT * FROM \"" + bind_data.table_name + "\"");
        if (state.query_result->HasError()) {
            throw InternalException("stps_mask_table: Failed to query table '%s': %s",
                                    bind_data.table_name.c_str(), state.query_result->GetError().c_str());
        }
    }

    idx_t output_idx = 0;

    while (output_idx < STANDARD_VECTOR_SIZE && !state.finished) {
        // Fetch a new chunk if needed
        if (!state.current_chunk || state.chunk_offset >= state.current_chunk->size()) {
            state.current_chunk = state.query_result->Fetch();
            state.chunk_offset = 0;

            if (!state.current_chunk || state.current_chunk->size() == 0) {
                state.finished = true;
                break;
            }
        }

        // Process rows from current chunk
        while (state.chunk_offset < state.current_chunk->size() && output_idx < STANDARD_VECTOR_SIZE) {
            for (idx_t col = 0; col < bind_data.column_names.size(); col++) {
                Value val = state.current_chunk->data[col].GetValue(state.chunk_offset);

                if (bind_data.column_masked[col]) {
                    val = MaskValue(val, bind_data.column_types[col],
                                    bind_data.seed, bind_data.column_names[col]);
                }

                output.data[col].SetValue(output_idx, val);
            }
            output_idx++;
            state.chunk_offset++;
        }
    }

    output.SetCardinality(output_idx);
}
```

**Step 2: Commit**

```bash
git add src/mask_functions.cpp
git commit -m "Add MaskTableScan: row iteration with type-aware masking"
```

---

### Task 6: Implement Registration and wire into extension

**Files:**
- Modify: `src/mask_functions.cpp` (append before closing namespace braces)
- Modify: `src/stps_unified_extension.cpp` (add include + registration call)
- Modify: `CMakeLists.txt` (add to EXTENSION_SOURCES)

**Step 1: Add the RegisterMaskFunctions function**

Append to `src/mask_functions.cpp` before closing namespace:

```cpp
void RegisterMaskFunctions(ExtensionLoader &loader) {
    TableFunction mask_func("stps_mask_table", {LogicalType::VARCHAR},
                            MaskTableScan, MaskTableBind, MaskTableInit);
    mask_func.named_parameters["exclude"] = LogicalType::LIST(LogicalType::VARCHAR);
    mask_func.named_parameters["seed"] = LogicalType::VARCHAR;
    loader.RegisterFunction(mask_func);
}
```

**Step 2: Add include to `src/stps_unified_extension.cpp`**

Add with the other includes near the top:
```cpp
#include "include/mask_functions.hpp"
```

Add in the `Load()` method alongside other registrations:
```cpp
stps::RegisterMaskFunctions(loader);
```

**Step 3: Add to CMakeLists.txt**

Add `src/mask_functions.cpp` to the `EXTENSION_SOURCES` list (after the last non-curl source, before the closing paren).

**Step 4: Commit**

```bash
git add src/mask_functions.cpp src/stps_unified_extension.cpp CMakeLists.txt
git commit -m "Register stps_mask_table function in extension and build system"
```

---

### Task 7: Build and test

**Files:**
- No new files

**Step 1: Build the extension**

```bash
make -j$(nproc) 2>&1
```

If build fails, fix compilation errors and rebuild.

**Step 2: Test manually with DuckDB**

```sql
-- Create test table
CREATE TABLE test_persons (
    id INTEGER,
    name VARCHAR,
    email VARCHAR,
    salary DOUBLE,
    birth_date DATE,
    active BOOLEAN,
    created_at TIMESTAMP
);

INSERT INTO test_persons VALUES
    (1, 'Max Mueller', 'max@example.com', 75000.50, '1990-03-15', true, '2024-01-15 10:30:00'),
    (2, 'Anna Schmidt', 'anna@example.com', 82000.00, '1985-07-22', true, '2024-02-20 14:45:00'),
    (3, 'Peter Wagner', NULL, 65000.75, '1992-11-08', false, '2024-03-10 09:15:00');

-- Test 1: Mask all columns
SELECT * FROM stps_mask_table('test_persons');

-- Test 2: Exclude id and created_at
SELECT * FROM stps_mask_table('test_persons', exclude := ['id', 'created_at']);

-- Test 3: Custom seed
SELECT * FROM stps_mask_table('test_persons', seed := 'audit-key');

-- Test 4: Verify determinism (run twice, results should be identical)
SELECT * FROM stps_mask_table('test_persons', seed := 'test');
SELECT * FROM stps_mask_table('test_persons', seed := 'test');

-- Test 5: Verify NULLs are preserved
SELECT name FROM stps_mask_table('test_persons') WHERE name IS NULL;
-- Should return 0 rows (only email is NULL for Peter)

-- Test 6: Error case - nonexistent table
SELECT * FROM stps_mask_table('nonexistent_table');
-- Should throw error

-- Test 7: Error case - invalid excluded column
SELECT * FROM stps_mask_table('test_persons', exclude := ['nonexistent_col']);
-- Should throw error
```

**Step 3: Commit any fixes**

```bash
git add -A
git commit -m "Fix build/test issues for stps_mask_table"
```

---

### Task 8: Update README.md

**Files:**
- Modify: `README.md`

**Step 1: Add documentation to README.md**

Add a new section in the appropriate place (near the database utility functions). Follow the existing documentation style:

```markdown
### stps_mask_table(table_name, [exclude], [seed])

GDPR-compliant deterministic masking. Produces an anonymized copy of a table where every value is replaced with a deterministic hash-based pseudonym. The same input value with the same seed always produces the same masked output, preserving referential integrity across tables.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `table_name` | `VARCHAR` | Yes | — | Name of the table to mask |
| `exclude` | `VARCHAR[]` | No | `[]` | Column names to pass through unmasked |
| `seed` | `VARCHAR` | No | `'stps_default_seed'` | Secret key for deterministic hashing |

**Masking by type:**
- **VARCHAR**: Hash to hex string (preserves length, min 4 chars)
- **INTEGER/BIGINT**: Hash to integer in same order of magnitude
- **DOUBLE/FLOAT/DECIMAL**: Hash to value with same sign and magnitude
- **DATE/TIMESTAMP**: Shift by deterministic offset (1-365 days)
- **BOOLEAN**: Deterministic flip
- **NULL**: Preserved as-is

**Examples:**

```sql
-- Mask all columns of a table
SELECT * FROM stps_mask_table('customers');

-- Exclude ID and timestamps from masking
SELECT * FROM stps_mask_table('customers', exclude := ['id', 'created_at']);

-- Use a custom seed (same seed = same output, for cross-table integrity)
SELECT * FROM stps_mask_table('customers', seed := 'audit-2026');
SELECT * FROM stps_mask_table('orders', seed := 'audit-2026');
-- customer_id values will match across both masked outputs

-- Export masked data to file
COPY (SELECT * FROM stps_mask_table('customers', seed := 'audit-2026')) TO 'customers_masked.csv';
```
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "Add stps_mask_table documentation to README"
```

---

### Task 9: Push and verify CI

**Step 1: Push to trigger CI**

```bash
git push
```

**Step 2: Monitor CI builds**

Check GitHub Actions for build status on Windows, Linux, and macOS.
