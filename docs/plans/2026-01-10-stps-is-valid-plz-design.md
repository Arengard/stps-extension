# stps_is_valid_plz Function Design

## Overview

A DuckDB scalar function that validates German postal codes (PLZ) with optional strict mode that checks against a real PLZ database.

## Function Signature

```sql
-- Format validation only (default)
stps_is_valid_plz(plz VARCHAR) -> BOOLEAN

-- With strict mode for real PLZ lookup
stps_is_valid_plz(plz VARCHAR, strict BOOLEAN) -> BOOLEAN
```

## Requirements

### Default Mode (strict=false)
- Returns `true` if input is exactly 5 digits and in range 01000-99999
- Returns `false` for invalid format, empty, or NULL input
- No external data needed, instant validation

### Strict Mode (strict=true)
- Downloads PLZ list on first use to `~/.stps/plz.csv`
- Returns `true` only if PLZ exists in the actual database
- Returns `false` for valid format but non-existent PLZ

### Data Source
- URL: `https://gist.githubusercontent.com/jbspeakr/4565964/raw/`
- Format: CSV with columns `plz,ort,bundesland`
- Storage: `~/.stps/plz.csv`
- Loading: On first strict mode call, cached in memory

### Input/Output
- Input: VARCHAR only (preserves leading zeros like "01067")
- Output: BOOLEAN

## Examples

```sql
SELECT stps_is_valid_plz('01067');           -- true (valid format)
SELECT stps_is_valid_plz('00001');           -- false (below 01000)
SELECT stps_is_valid_plz('1067');            -- false (only 4 digits)
SELECT stps_is_valid_plz('ABC12');           -- false (contains letters)
SELECT stps_is_valid_plz('01067', true);     -- true (Dresden exists)
SELECT stps_is_valid_plz('01000', true);     -- false (format ok, doesn't exist)
SELECT stps_is_valid_plz(NULL);              -- false
```

## Implementation

### Files

1. `src/include/plz_validation.hpp` - Header with PlzLoader class
2. `src/plz_validation.cpp` - Implementation
3. `src/stps_unified_extension.cpp` - Register function
4. `CMakeLists.txt` - Add source file

### Class Structure

```cpp
namespace duckdb {
namespace stps {

class PlzLoader {
public:
    static PlzLoader& GetInstance();  // Singleton
    bool IsLoaded() const;
    void EnsureLoaded();              // Downloads if needed
    bool PlzExists(const std::string& plz) const;

private:
    PlzLoader() = default;
    std::unordered_set<std::string> valid_plz_codes_;
    bool loaded_ = false;

    std::string GetPlzFilePath();     // ~/.stps/plz.csv
    bool DownloadPlzFile(const std::string& path);
    bool LoadFromFile(const std::string& path);
};

bool is_valid_plz_format(const std::string& plz);
void RegisterPlzValidationFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
```

### Validation Logic

1. **Format check** (both modes):
   - Must be exactly 5 characters
   - All characters must be digits
   - Numeric value must be >= 01000 and <= 99999

2. **Strict check** (strict=true only):
   - Ensure PLZ list is loaded (download if needed)
   - Check if PLZ exists in `valid_plz_codes_` set
