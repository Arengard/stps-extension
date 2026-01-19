# stps_split_street Function Design

## Overview

A DuckDB scalar function that parses German street addresses into street name and street number components, with intelligent handling of abbreviations and special formats.

## Function Signature

```sql
stps_split_street(address VARCHAR) -> STRUCT(street_name VARCHAR, street_number VARCHAR)
```

## Requirements

### Return Type
- Returns a STRUCT with two fields:
  - `street_name`: The parsed street name (with abbreviation applied if applicable)
  - `street_number`: Everything after the street name (UPPERCASED), or NULL if none

### Abbreviation Rules
- Compound words ending in "straße" or "strasse" are abbreviated to "str."
- Separate words like "Lange Straße" remain unchanged (space before Straße)
- Only straße/strasse suffix is abbreviated (no other suffixes like gasse, weg, etc.)

### Mannheim Address Detection
- Pattern-based: `^[A-Za-z]\d+\s+\d+.*$`
- Examples: "M7 24", "Q3 15a"
- Letter+number becomes street_name, rest becomes street_number

### House Number Parsing
- Only at end of address (standard German format)
- Includes all additions: 23a, 23 D, 23/4, 23-25, 23 1/2
- Street number is always UPPERCASED

### Edge Cases
- Missing house number: street_number = NULL
- Empty/NULL input: both fields = NULL
- Already abbreviated input passes through unchanged
- Numbers within street names preserved (e.g., "Straße des 17. Juni")

## Examples

| Input | street_name | street_number |
|-------|-------------|---------------|
| `"Wienerbergstraße 23"` | `"Wienerbergstr."` | `"23"` |
| `"Wienerbergstrasse 23 D"` | `"Wienerbergstr."` | `"23 D"` |
| `"Lange Straße 15a"` | `"Lange Straße"` | `"15A"` |
| `"Hauptstr. 7/4"` | `"Hauptstr."` | `"7/4"` |
| `"M7 24"` | `"M7"` | `"24"` |
| `"Q3 15a"` | `"Q3"` | `"15A"` |
| `"Straße des 17. Juni 123"` | `"Straße des 17. Juni"` | `"123"` |
| `"Wienerbergstraße"` | `"Wienerbergstr."` | `NULL` |
| `"Am Hang 3-5"` | `"Am Hang"` | `"3-5"` |
| `""` or `NULL` | `NULL` | `NULL` |

## Implementation

### Files

1. `src/street_split.cpp` - Core parsing logic and DuckDB function wrapper
2. `src/include/street_split.hpp` - Header file
3. `src/stps_extension.cpp` - Register the new function

### Structure

```cpp
namespace duckdb {
namespace stps {

struct StreetParseResult {
    std::string street_name;
    std::string street_number;
    bool has_number;
};

StreetParseResult parse_street_address(const std::string& input);
void RegisterStreetSplitFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
```

### Parsing Algorithm

1. Trim and validate input (return NULLs if empty)
2. Check for Mannheim pattern (`[A-Za-z]\d+\s+\d+`)
   - If match: split at space after block code
3. For standard addresses:
   - Find boundary where house number starts (first digit that begins the number portion)
   - Handle embedded numbers in street names (e.g., "17. Juni")
4. Apply abbreviation to street_name:
   - Check if ends with compound "straße"/"strasse" (no preceding space)
   - Replace with "str."
5. Uppercase the street_number
6. Return STRUCT with both fields
