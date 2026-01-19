---
created: 2026-01-15T21:02
completed: 2026-01-15T21:15
title: Review extension functions for best practices
area: general
files:
  - src/ai_functions.cpp
  - src/address_lookup.cpp
  - src/zip_functions.cpp
  - src/sevenzip_functions.cpp
  - src/io_operations.cpp
  - src/iban_validation.cpp
  - src/text_normalize.cpp
  - src/stps_unified_extension.cpp
---

## Problem

The stps-extension codebase needs a comprehensive review to ensure all functions follow best practices.

## Findings

### CRITICAL: Security Issues

#### 1. Command Injection Risk via `system()` Calls
**Files:** `ai_functions.cpp:263-279`, `address_lookup.cpp:211-252`

Both files construct shell commands by concatenating user-controlled strings and pass them to `system()`. While some JSON escaping is done, shell metacharacters could potentially be injected through file paths or API responses.

```cpp
// ai_functions.cpp:263
std::string cmd = "curl -s -X POST ... -H \"Authorization: Bearer " + api_key + "\" ...";
int result = system(cmd.c_str());
```

**Risk:** An API key or user input containing shell metacharacters (`;`, `|`, `$()`, etc.) could execute arbitrary commands.

**Recommendation:** Use libcurl's C API directly instead of shelling out to curl.

#### 2. Unprotected Global State (Thread Safety)
**File:** `ai_functions.cpp:28-29`

```cpp
static std::string openai_api_key;
static std::string openai_model = "gpt-4o-mini";
```

These globals are accessed/modified without mutex protection, unlike the cache in `address_lookup.cpp` which correctly uses `std::mutex`.

**Risk:** Race conditions in multi-threaded DuckDB usage.

**Recommendation:** Add mutex protection or use thread-local storage.

---

### HIGH: Code Duplication

#### 1. Identical Helper Functions in Archive Files
**Files:** `zip_functions.cpp` and `sevenzip_functions.cpp`

Both files contain nearly identical implementations of:
- `GetFileExtension()` (lines 20-28 in both)
- `IsBinaryFormat()` (lines 31-36 in both)
- `LooksLikeBinary()` (lines 39-49 in both)
- `GetTempDirectory()` (lines 52-63 in both)
- `ExtractToTemp()` (lines 66-80 in both)
- `DetectDelimiter()` (lines 198-213 zip / 78-92 7zip)
- `SplitLine()` (lines 216-235 zip / 95-114 7zip)
- `ParseCSVContent()` (lines 238-299 zip / 117-176 7zip)

**Impact:** ~200 lines of duplicated code, maintenance burden, inconsistency risk.

**Recommendation:** Extract to `src/shared/archive_utils.cpp`.

#### 2. Duplicate Utility Functions
**Files:** `address_lookup.cpp:106-118` vs `utils.hpp`

`trim()` and `to_lower()` are reimplemented in `address_lookup.cpp` when they already exist in `utils.hpp`.

---

### MEDIUM: Error Handling Issues

#### 1. Inconsistent Error Reporting
Some functions return error strings in the result (e.g., `io_operations.cpp`):
```cpp
return "ERROR: Source file does not exist: " + source;
```

While others throw exceptions (e.g., `zip_functions.cpp`):
```cpp
throw IOException("Failed to open ZIP file: " + bind_data.zip_path);
```

**Recommendation:** Standardize on exceptions for errors, success messages for valid results.

#### 2. Temp File Cleanup on Error Paths
**File:** `ai_functions.cpp:279-286`

```cpp
int result = system(cmd.c_str());
std::remove(payload_file.c_str());  // Cleaned up

if (result != 0) {
    std::remove(response_file.c_str());  // Cleaned up here
    return "ERROR: ...";
}
```

This is good, but an RAII wrapper would be safer and cleaner.

---

### MEDIUM: Memory Management

#### 1. Unnecessary `std::move()` on Return Statements
**Files:** `zip_functions.cpp:111,130,394,469,505`, `sevenzip_functions.cpp:209,242,435,510,556,585`

```cpp
return std::move(result);  // Prevents RVO
```

Modern C++ compilers perform Return Value Optimization (RVO). Using `std::move()` on local variables in return statements actually *prevents* this optimization.

**Recommendation:** Change to simply `return result;`

---

### LOW: Naming Inconsistencies

#### 1. Function Prefix Inconsistency
**File:** `text_normalize.cpp`

Functions use `Pgm` prefix instead of `Stps`:
- `PgmRemoveAccentsFunction` (line 251)
- `PgmRemoveAccentsSimpleFunction` (line 261)
- `PgmRestoreUmlautsFunction` (line 271)
- `PgmCleanStringFunction` (line 281)
- `PgmNormalizeFunction` (line 291)

While all other files use `Stps` prefix consistently.

---

### Positive Observations

1. **Good cross-platform support** - Proper `#ifdef _WIN32` handling throughout
2. **Proper DuckDB API usage** - Extension registration, vector operations, type handling all correct
3. **Good rate limiting** - `address_lookup.cpp` implements proper rate limiting with mutex protection
4. **Defensive programming** - Null checks, bounds checking, input validation present in most functions
5. **Informative error messages** - Most errors include context about what failed
6. **Memory cleanup** - Archive functions properly free allocated memory (`mz_free`, `free(outBuf)`)
7. **Cache implementation** - LRU-style cache with TTL in `address_lookup.cpp` is well designed

---

## Recommendations Summary

| Priority | Issue | Files | Effort |
|----------|-------|-------|--------|
| Critical | Replace `system()` with libcurl | ai_functions.cpp, address_lookup.cpp | Medium |
| Critical | Add mutex to global AI state | ai_functions.cpp | Low |
| High | Extract shared archive utilities | zip_functions.cpp, sevenzip_functions.cpp | Medium |
| High | Remove duplicate utility functions | address_lookup.cpp | Low |
| Medium | Standardize error handling | All files | Medium |
| Medium | Remove unnecessary `std::move()` | zip/sevenzip_functions.cpp | Low |
| Low | Rename Pgm -> Stps prefix | text_normalize.cpp | Low |

## Next Steps

1. Create `src/shared/archive_utils.cpp` with shared helpers
2. Evaluate adding libcurl as dependency for HTTP requests
3. Add thread safety to AI function globals
4. Create coding standards document for future contributions
