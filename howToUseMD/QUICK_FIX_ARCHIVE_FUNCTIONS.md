# Quick Fix: Archive Functions Not Working

## Issue 1: Syntax Error - Missing FROM

❌ **WRONG:**
```sql
select * stps_7zip('C:\path\file.7z', 'inner.txt')
```

✅ **CORRECT:**
```sql
SELECT * FROM stps_7zip('C:\path\file.7z', 'inner.txt')
--         ^^^^  -- DON'T FORGET FROM!
```

**Explanation:** `stps_7zip` is a TABLE function, not a scalar function. Table functions ALWAYS need `FROM`.

---

## Issue 2: Windows Paths - Backslash Escaping

❌ **WRONG:**
```sql
SELECT * FROM stps_zip('C:\Users\Ramon\file.zip', 'data.csv')
--                      ^^      ^^       -- Backslashes not escaped!
```

✅ **CORRECT Option 1** - Forward slashes (RECOMMENDED):
```sql
SELECT * FROM stps_zip('C:/Users/Ramon/file.zip', 'data.csv');
```

✅ **CORRECT Option 2** - Escape backslashes:
```sql
SELECT * FROM stps_zip('C:\\Users\\Ramon\\file.zip', 'data.csv');
```

**Why?** In SQL strings, `\` is an escape character. `\U` becomes something else!

---

## Issue 3: File Not Found

**Error:** `Failed to open ZIP file: ...`

### Checklist:

1. ✅ **File exists?**
   ```sql
   -- Check if file exists first
   SELECT * FROM stps_path('C:/Users/Ramon.Ljevo/Downloads/')
   WHERE full_path LIKE '%Downloads.7z%';
   ```

2. ✅ **Correct extension?**
   - `.zip` → use `stps_zip()` or `stps_view_zip()`
   - `.7z` → use `stps_7zip()` or `stps_view_7zip()`
   - **Don't mix them up!**

3. ✅ **Path is absolute?**
   ```sql
   -- Relative paths don't work well
   SELECT * FROM stps_zip('./file.zip', 'data.csv');  -- BAD

   -- Use absolute paths
   SELECT * FROM stps_zip('C:/full/path/file.zip', 'data.csv');  -- GOOD
   ```

4. ✅ **File permissions?**
   - Can you open the file in 7-Zip/WinRAR?
   - Is it password protected? (Not supported)

---

## Working Examples

### List files in archive first:

```sql
-- For ZIP files
SELECT * FROM stps_view_zip('C:/Users/Ramon.Ljevo/Downloads/archive.zip');

-- For 7z files
SELECT * FROM stps_view_7zip('C:/Users/Ramon.Ljevo/Downloads/archive.7z');
```

### Extract specific file:

```sql
-- For ZIP files
SELECT * FROM stps_zip('C:/Users/Ramon.Ljevo/Downloads/archive.zip', 'data.csv');

-- For 7z files
SELECT * FROM stps_7zip('C:/Users/Ramon.Ljevo/Downloads/archive.7z', 'data.csv');
```

### Auto-detect first file:

```sql
-- ZIP (without filename - extracts first file)
SELECT * FROM stps_zip('C:/Users/Ramon.Ljevo/Downloads/archive.zip');

-- 7z (without filename - extracts first file)
SELECT * FROM stps_7zip('C:/Users/Ramon.Ljevo/Downloads/archive.7z');
```

---

## Complete Working Example

```sql
-- Step 1: List files to see what's inside
SELECT filename, uncompressed_size
FROM stps_view_7zip('C:/Users/Ramon.Ljevo/Downloads/Downloads.7z');
-- Output:
-- filename                    | uncompressed_size
-- PrimaNotaAuswertung.txt    | 1234
-- OtherFile.csv              | 5678

-- Step 2: Extract the specific file (note FROM and forward slashes!)
SELECT * FROM stps_7zip('C:/Users/Ramon.Ljevo/Downloads/Downloads.7z', 'PrimaNotaAuswertung.txt');
-- Returns: parsed content as table
```

---

## Common Mistakes Summary

| Mistake | Fix |
|---------|-----|
| `select * stps_zip(...)` | `SELECT * FROM stps_zip(...)` |
| `'C:\Users\...'` | `'C:/Users/...'` or `'C:\\Users\\...'` |
| Using `stps_zip()` for `.7z` file | Use `stps_7zip()` instead |
| Using `stps_7zip()` for `.zip` file | Use `stps_zip()` instead |
| Relative path `'./file.zip'` | Use absolute path `'C:/full/path/file.zip'` |

---

## Still Not Working?

1. **Check your DuckDB version:**
   ```sql
   SELECT version();
   ```

2. **Check if extension is loaded:**
   ```sql
   SELECT * FROM duckdb_functions() WHERE function_name LIKE 'stps_%zip%';
   ```

3. **Test with a simple file:**
   - Create a test.zip with test.txt inside
   - Try: `SELECT * FROM stps_zip('C:/test.zip', 'test.txt');`

4. **Check file in File Explorer:**
   - Copy the path from address bar
   - Replace backslashes with forward slashes
   - Paste into SQL query

---

## Pro Tips

✅ **Use forward slashes everywhere** - Works on Windows, Linux, and Mac

✅ **List files first** - Use `stps_view_*` to see what's inside before extracting

✅ **Check file type** - Make sure you're using the right function for the archive type

✅ **Use absolute paths** - Avoids confusion about working directory

✅ **Test incrementally** - First list, then extract one file, then do complex queries
