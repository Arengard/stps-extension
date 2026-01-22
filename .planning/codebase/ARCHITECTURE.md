# Architecture Overview

## Extension Structure

```
┌─────────────────────────────────────────────────────────────┐
│                    DuckDB Engine                             │
├─────────────────────────────────────────────────────────────┤
│                  STPS Extension                              │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐│
│  │   Scalar    │ │   Table     │ │      Aggregate          ││
│  │  Functions  │ │  Functions  │ │      Functions          ││
│  └──────┬──────┘ └──────┬──────┘ └───────────┬─────────────┘│
│         │               │                     │              │
│  ┌──────┴───────────────┴─────────────────────┴─────────────┐│
│  │                  Shared Utilities                         ││
│  │  curl_utils, archive_utils, filesystem_utils              ││
│  └──────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

## Function Categories

### Scalar Functions (1 row → 1 value)
- Text processing: `stps_clean_string`, `stps_normalize`, case transforms
- Validation: `stps_is_valid_iban`, `stps_is_valid_plz`
- Parsing: `stps_split_street`, `stps_get_iban_country_code`
- AI: `stps_ask_ai`, `stps_ai_classify`, `stps_ask_ai_gender`

### Table Functions (→ result set)
- File reading: `stps_zip`, `stps_view_zip`, `next_cloud`
- Data manipulation: `stps_drop_null_columns`, `stps_drop_duplicates`, `stps_show_duplicates`
- Search: `stps_search_columns`
- Smart cast: `stps_smart_cast`

## Key Design Patterns

### 1. Registration Pattern
Each function module exposes a `Register*Functions(ExtensionLoader &loader)` function called from `stps_unified_extension.cpp`.

### 2. Bind-Init-Scan Pattern (Table Functions)
```cpp
// Bind: Parse arguments, determine output schema
unique_ptr<FunctionData> Bind(ClientContext &context, ...)

// Init: Prepare state, open resources
unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, ...)

// Scan: Return data chunks
void Scan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output)
```

### 3. Optional Feature Guards
```cpp
#ifdef HAVE_CURL
// curl-dependent code
#endif
```

## Data Flow

```
User SQL Query
     │
     ▼
DuckDB Parser/Planner
     │
     ▼
STPS Function (Bind phase)
     │  - Validate inputs
     │  - Determine output schema
     ▼
STPS Function (Init phase)
     │  - Open files/connections
     │  - Prepare state
     ▼
STPS Function (Scan phase)
     │  - Return DataChunks
     │  - Stream results
     ▼
Query Result
```
