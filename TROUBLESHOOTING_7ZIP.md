# Troubleshooting stps_7zip Function

## Common Issues and Solutions

### Issue 1: Missing FROM keyword

❌ **WRONG:**
```sql
SELECT * stps_7zip('archive.7z', 'file.csv');
```

✅ **CORRECT:**
```sql
SELECT * FROM stps_7zip('archive.7z', 'file.csv');
```

### Issue 2: Windows path backslashes

Windows file paths use backslashes `\`, but in SQL strings, backslashes are escape characters.

❌ **WRONG:**
```sql
SELECT * FROM stps_7zip('C:\Users\Ramon.Ljevo\Downloads\Downloads.7z', 'file.csv');
```

✅ **CORRECT Option 1** - Escape backslashes:
```sql
SELECT * FROM stps_7zip('C:\\Users\\Ramon.Ljevo\\Downloads\\Downloads.7z', 'file.csv');
```

✅ **CORRECT Option 2** - Use forward slashes (works on Windows):
```sql
SELECT * FROM stps_7zip('C:/Users/Ramon.Ljevo/Downloads/Downloads.7z', 'file.csv');
```

### Issue 3: File extension matters

The function tries to auto-detect the file format based on extension:

- `.csv` → Parsed as CSV with auto-detected delimiter
- `.txt` → Parsed as CSV (tries delimiter detection)
- `.parquet` → Currently returns raw content (parquet parsing not yet implemented)

## Correct Usage Examples

### Extract a specific CSV file:
```sql
SELECT *
FROM stps_7zip('C:/Users/Ramon.Ljevo/Downloads/Downloads.7z', 'PrimaNotaAuswertung.csv');
```

### Extract a specific text file (parsed as CSV):
```sql
SELECT *
FROM stps_7zip('C:/Users/Ramon.Ljevo/Downloads/Downloads.7z', 'PrimaNotaAuswertung.txt');
```

### Extract the first file automatically (no filename specified):
```sql
SELECT *
FROM stps_7zip('C:/Users/Ramon.Ljevo/Downloads/Downloads.7z');
```

### List files in archive first:
```sql
SELECT *
FROM stps_view_7zip('C:/Users/Ramon.Ljevo/Downloads/Downloads.7z');
```

## Error Messages

### "Not a valid 7z archive"
- The file is not a valid 7-Zip archive
- File might be corrupted
- Wrong file extension

### "File not found in 7z archive"
- The specified inner filename doesn't exist in the archive
- Check spelling and case sensitivity
- Use `stps_view_7zip()` to list available files

### "Failed to extract file"
- Archive might be encrypted (password-protected archives not supported)
- File might be corrupted
- Insufficient memory

## Supported Features

✅ **Supported:**
- CSV files (auto-detect delimiter: `,`, `;`, `\t`, `|`)
- Text files (parsed as CSV)
- Multiple files in same archive (specify filename)
- Auto-detection of first file

❌ **Not Yet Supported:**
- Parquet file parsing (returns raw bytes)
- Password-protected archives
- Multi-volume archives
- Solid archives (may have issues)

## Performance Tips

1. **Be specific with filenames** - Faster than auto-detection
2. **Use forward slashes** - Simpler than escaping backslashes
3. **Check archive contents first** - Use `stps_view_7zip()` before extraction

## Example Workflow

```sql
-- Step 1: List files in archive
SELECT filename, size, is_directory
FROM stps_view_7zip('C:/data/archive.7z');

-- Step 2: Extract specific file
SELECT *
FROM stps_7zip('C:/data/archive.7z', 'data.csv');

-- Step 3: Filter and analyze
SELECT column1, COUNT(*)
FROM stps_7zip('C:/data/archive.7z', 'data.csv')
GROUP BY column1;
```

## Quick Reference

| Syntax | Description |
|--------|-------------|
| `stps_7zip('path.7z')` | Extract first file from archive |
| `stps_7zip('path.7z', 'file.csv')` | Extract specific file |
| `stps_view_7zip('path.7z')` | List all files in archive |

---

**Need more help?** Check the main documentation: `README.md` or `STPS_FUNCTIONS.md`
