# stps_ask_ai_address Status - January 19, 2026

## ✅ FINAL STATUS: COMPLETELY FIXED

### Latest Commit: 2ca7679
- Fixed C89 compatibility for macOS build
- All builds (Windows, Linux, macOS) should now pass

## The Bug Journey

### The ACTUAL Bug (Found After Testing):
The code was searching for `"text"` but Anthropic's response has TWO occurrences:
1. `"type":"text"` (part of the content array structure)  
2. `"text":"the actual content"` (what we need)

The code was finding the FIRST occurrence (`"type":"text"`) and then trying to extract from there, which failed!

### The REAL Fix (Commit d59897c):
Search for `"text":` (with colon) instead of `"text"` to skip over `"type":"text"` and find the actual content field.

**Changed in src/ai_functions.cpp (lines 475 and 564):**
```cpp
// BEFORE (WRONG):
size_t text_start = response.find("\"text\"", content_pos);

// AFTER (CORRECT):
size_t text_start = response.find("\"text\":", content_pos);
```

This ensures we match `"text":"value"` and not `"type":"text"`.

### Additional Fix (Commit 2ca7679):
Fixed C89 compatibility issue that was breaking macOS builds:
- Moved `sizeIndex` variable declarations to the top of functions
- C89 requires all variables to be declared at the start of a block

## Testing Instructions

### Download Latest Build:
1. Go to: https://github.com/Arengard/stps-extension/actions
2. Wait for the workflow with commit `2ca7679` to complete
3. Download the Windows artifact (~5-10 minutes)
4. Extract and copy `stps.duckdb_extension` to `C:\stps\`

### Test:
```sql
LOAD 'C:\stps\stps.duckdb_extension';
SELECT stps_set_api_key('your-key');
SELECT stps_set_brave_api_key('your-brave-key');

-- Should return address data:
SELECT stps_ask_ai_address('Tax Network GmbH');
```

## History of Failed Attempts

### Attempt 1 (Commit 886e6d1): Added SetValid() 
- ✅ This was correct but not the root cause

### Attempt 2 (Commit 9ba3699): Disabled tools for parsing
- ✅ This was helpful but not the root cause  

### Attempt 3 (Commit 6a995f5): Extract from content array
- ❌ Still failed because searched for `"text"` without colon
- Found `"type":"text"` instead of `"text":"content"`

### Attempt 4 (LATEST): Search for `"text":`
- ✅ This is the ACTUAL fix!
- Skips `"type":"text"` and finds `"text":"the actual content"`

## Expected Output After Fix
```
┌────────────────────────────────────────────────────────────────────────────────────┐
│                      stps_ask_ai_address('Tax Network GmbH')                       │
│ struct(city varchar, postal_code varchar, street_name varchar, street_nr varchar)  │
├────────────────────────────────────────────────────────────────────────────────────┤
│ {'city': Karlsruhe, 'postal_code': 76185, 'street_name': Carl-Metz-Str., '...'}   │
└────────────────────────────────────────────────────────────────────────────────────┘
```
