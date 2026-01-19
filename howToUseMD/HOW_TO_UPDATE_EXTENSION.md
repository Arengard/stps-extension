# How to Update Your STPS Extension

## Problem: Using Old Version

If you see error messages like:
- "Full content extraction is not yet implemented"
- Functions don't work as documented
- Missing features

You're probably using an **old version** of the extension!

## Solution: Get the Latest Build

The extension is automatically built by GitHub Actions on every push. Here's how to get the latest version:

### Step 1: Go to GitHub Actions

Visit: https://github.com/Arengard/stps-extension/actions

### Step 2: Find the Latest Successful Build

1. Look for the most recent workflow run with a **green checkmark** ✅
2. Click on it
3. Make sure it says "This workflow run completed successfully"

### Step 3: Download the Artifact

1. Scroll down to the **Artifacts** section at the bottom
2. Download one of:
   - `stps-windows-amd64-latest-master` (recommended - always latest from master branch)
   - `stps-windows-amd64-{commit-sha}` (specific commit version)
3. Extract the ZIP file
4. You'll get `stps.duckdb_extension`

### Step 4: Load the New Extension

```sql
-- In DuckDB CLI
FORCE INSTALL 'path/to/stps.duckdb_extension';
LOAD stps;

-- Verify version
SELECT * FROM stps_view_7zip('test.7z');  -- Should work now
```

## How to Check Your Current Version

Unfortunately, there's no built-in version command yet. But you can test features:

```sql
-- Test if extraction works (added in January 2025)
SELECT * FROM stps_7zip('archive.7z', 'file.csv');

-- If you get "Full content extraction is not yet implemented"
-- You need to update!
```

## Recent Feature Additions

### January 14, 2026
- ✅ Fixed `stps_smart_cast` false positive date parsing
- ✅ Enhanced `stps_clean_string` to remove non-printable characters
- ✅ Fixed `stps_split_street` abbreviation logic
- ✅ Added `stps_get_address` function (company address lookup)

### January 13, 2026
- ✅ **stps_7zip now extracts files** (not just lists them!)
- ✅ CSV/TXT file parsing from 7z archives
- ✅ Auto-delimiter detection

### Earlier
- stps_view_7zip (list files in 7z archive)
- stps_zip (extract from ZIP archives)
- Various text processing and validation functions

## Troubleshooting

### "I downloaded but it still doesn't work"

1. **Make sure you unloaded the old extension:**
   ```sql
   -- Close and reopen DuckDB
   -- Or use FORCE INSTALL to overwrite
   ```

2. **Check the file path:**
   ```sql
   -- Use full path
   FORCE INSTALL 'C:/path/to/stps.duckdb_extension';
   ```

3. **Verify you downloaded the right file:**
   - File should be named `stps.duckdb_extension`
   - Size should be several MB (not KB)
   - Extract from ZIP first if needed

### "Build failed on GitHub"

Sometimes builds fail. Wait for the next commit or check the error logs:
1. Go to the failed workflow run
2. Click on the job that failed
3. Read the error message
4. Report issues at: https://github.com/Arengard/stps-extension/issues

### "I can't find the Artifacts section"

You need to be logged into GitHub to see artifacts. If you're not logged in:
1. Create a GitHub account (free)
2. Log in
3. Then go to Actions and download

## Alternative: Build Locally (Not Recommended)

If you really want to build locally, see `BUILD_PROCESS.md`. But GitHub Actions is much easier!

## Quick Reference

| What | Where |
|------|-------|
| Latest builds | https://github.com/Arengard/stps-extension/actions |
| Report issues | https://github.com/Arengard/stps-extension/issues |
| Documentation | `README.md`, `STPS_FUNCTIONS.md` |
| Build process | `BUILD_PROCESS.md` |

---

**Still having issues?** Open an issue on GitHub with:
- Your DuckDB version: `SELECT version();`
- Error message (full text)
- What you're trying to do
