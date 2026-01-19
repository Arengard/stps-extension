# STPS Extension Autoload Setup

**Date:** 2026-01-02
**Status:** Approved
**Platforms:** macOS, Linux, Windows

## Overview

This document describes how to set up automatic loading of the STPS extension for both DuckDB CLI and Python usage. The setup provides global autoloading across your system for personal convenience.

## Design Goals

- **Personal convenience**: Extension loads automatically in all DuckDB sessions
- **Cross-platform**: Works on macOS, Linux, and Windows
- **Version-aware**: Support multiple DuckDB versions simultaneously
- **Both CLI and Python**: Seamless experience in both environments
- **Easy maintenance**: Simple to update when DuckDB or extension upgrades

## Architecture

### Installation Directory Structure

```
~/.duckdb/                                    # User's DuckDB directory
└── extensions/                               # Custom extensions
    └── stps/                                 # STPS extension
        ├── v1.4.3/                           # Version-specific builds
        │   └── stps.duckdb_extension
        ├── v1.5.0/                           # Future versions
        │   └── stps.duckdb_extension
        └── current -> v1.4.3/                # Optional symlink (macOS/Linux only)
```

**Windows equivalent:** `%USERPROFILE%\.duckdb\extensions\stps\`

### Autoloading Mechanisms

1. **CLI (DuckDB REPL)**: Uses `~/.duckdbrc` configuration file
2. **Python**: Uses helper function that wraps `duckdb.connect()`

## Implementation Steps

### Step 1: Determine Your DuckDB Version

```bash
duckdb --version
```

Note the version (e.g., `v1.4.3`) - you'll use this throughout the setup.

### Step 2: Build Extension for Your DuckDB Version

```bash
./scripts/build-for-version.sh v1.4.3
```

This creates the extension binary at:
```
./build/v1.4.3/extension/stps/stps.duckdb_extension
```

### Step 3: Create Installation Directory

**macOS/Linux:**
```bash
mkdir -p ~/.duckdb/extensions/stps/v1.4.3
```

**Windows:**
```batch
mkdir %USERPROFILE%\.duckdb\extensions\stps\v1.4.3
```

### Step 4: Install Extension Binary

**macOS/Linux:**
```bash
cp ./build/v1.4.3/extension/stps/stps.duckdb_extension \
   ~/.duckdb/extensions/stps/v1.4.3/
```

**Windows:**
```batch
copy build\v1.4.3\extension\stps\stps.duckdb_extension ^
     %USERPROFILE%\.duckdb\extensions\stps\v1.4.3\
```

### Step 5: Configure CLI Autoloading

DuckDB automatically reads and executes `~/.duckdbrc` (or `%USERPROFILE%\.duckdbrc` on Windows) when starting the CLI.

**macOS/Linux:**
```bash
cat > ~/.duckdbrc << 'EOF'
-- Autoload stps extension
.echo Loading stps extension...
LOAD '~/.duckdb/extensions/stps/v1.4.3/stps.duckdb_extension';
.echo stps extension loaded successfully!
EOF
```

**Windows:**
```batch
(
echo -- Autoload stps extension
echo .echo Loading stps extension...
echo LOAD '%USERPROFILE%/.duckdb/extensions/stps/v1.4.3/stps.duckdb_extension';
echo .echo stps extension loaded successfully!
) > %USERPROFILE%\.duckdbrc
```

**Note:** The `.echo` commands provide helpful feedback but are optional.

### Step 6: Create Python Helper Function

Create a helper module (e.g., `duckdb_helpers.py` in your project or home directory):

```python
"""DuckDB connection helpers with stps extension pre-loaded"""
import duckdb
from pathlib import Path


def get_duckdb_connection(database=':memory:'):
    """
    Create DuckDB connection with stps extension pre-loaded.

    This function creates a DuckDB connection and automatically loads
    the stps extension, making all stps_* functions immediately available.

    Args:
        database: Database file path or ':memory:' for in-memory DB (default)

    Returns:
        DuckDB connection with stps extension loaded

    Example:
        >>> from duckdb_helpers import get_duckdb_connection
        >>> conn = get_duckdb_connection()
        >>> result = conn.execute("SELECT stps_is_valid_iban('DE89370400440532013000')").fetchone()
        >>> print(result)
    """
    conn = duckdb.connect(database)

    # Determine extension path (works on both macOS and Windows)
    home = Path.home()
    ext_path = home / '.duckdb' / 'extensions' / 'stps' / 'v1.4.3' / 'stps.duckdb_extension'

    # Load extension
    conn.execute(f"LOAD '{ext_path}'")

    return conn
```

**Usage in scripts:**
```python
from duckdb_helpers import get_duckdb_connection

# Create connection with extension pre-loaded
conn = get_duckdb_connection()

# All stps functions are now available
result = conn.execute("SELECT stps_is_valid_iban('DE89370400440532013000')").fetchone()
print(result)
```

### Step 7: Test the Setup

**Test CLI:**
```bash
# Start DuckDB (use -unsigned flag for custom extensions)
duckdb -unsigned
```

You should see:
```
Loading stps extension...
stps extension loaded successfully!
v1.4.3 xxxxxxxxx
D
```

Test a function:
```sql
SELECT stps_is_valid_iban('DE89370400440532013000');
┌──────────────────────────────────────────┐
│ stps_is_valid_iban('DE89370400440532...') │
│                 boolean                   │
├───────────────────────────────────────────┤
│ true                                      │
└───────────────────────────────────────────┘
```

**Test Python:**
```python
from duckdb_helpers import get_duckdb_connection

conn = get_duckdb_connection()

# Test UUID function
result = conn.execute("SELECT stps_uuid()").fetchone()
print(f"UUID: {result[0]}")

# Test IBAN validation
iban_check = conn.execute("SELECT stps_is_valid_iban('DE89370400440532013000')").fetchone()
print(f"Valid IBAN: {iban_check[0]}")
```

## Maintenance

### When DuckDB Updates

1. **Check your new DuckDB version:**
   ```bash
   duckdb --version
   ```

2. **Build extension for the new version:**
   ```bash
   ./scripts/build-for-version.sh v1.5.0
   ```

3. **Install to new version directory:**

   **macOS/Linux:**
   ```bash
   mkdir -p ~/.duckdb/extensions/stps/v1.5.0
   cp ./build/v1.5.0/extension/stps/stps.duckdb_extension \
      ~/.duckdb/extensions/stps/v1.5.0/
   ```

   **Windows:**
   ```batch
   mkdir %USERPROFILE%\.duckdb\extensions\stps\v1.5.0
   copy build\v1.5.0\extension\stps\stps.duckdb_extension ^
        %USERPROFILE%\.duckdb\extensions\stps\v1.5.0\
   ```

4. **Update configuration files:**
   - Edit `~/.duckdbrc` (or `%USERPROFILE%\.duckdbrc`) to point to `v1.5.0`
   - Update the version in `duckdb_helpers.py` to `v1.5.0`

### When You Update Extension Code

Simply rebuild and reinstall to the same version directory:

```bash
./scripts/build-for-version.sh v1.4.3
# Then copy to ~/.duckdb/extensions/stps/v1.4.3/ (overwrites existing)
```

### Optional: Version Symlink (macOS/Linux only)

Create a `current` symlink to avoid hardcoding versions in configs:

```bash
ln -sf ~/.duckdb/extensions/stps/v1.4.3 ~/.duckdb/extensions/stps/current
```

Then use `current` in your `.duckdbrc` and Python helper:
```
LOAD '~/.duckdb/extensions/stps/current/stps.duckdb_extension';
```

**Note:** Windows requires administrator privileges for symlinks, so the versioned path approach is recommended for cross-platform consistency.

## Implementation Scripts

For convenience, installation scripts are provided:

- `scripts/install-extension.sh` - macOS/Linux installation script
- `scripts/install-extension.bat` - Windows installation script

These scripts automate Steps 3-4 above.

## Troubleshooting

### Extension fails to load in CLI

**Error:** `IO Error: Extension "/path/to/extension" could not be loaded`

**Solution:** Make sure you're using the `-unsigned` flag:
```bash
duckdb -unsigned
```

### Version mismatch error

**Error:** `Extension version mismatch`

**Solution:** Rebuild the extension for your exact DuckDB version:
```bash
duckdb --version  # Check version
./scripts/build-for-version.sh vX.Y.Z  # Build for that version
# Then reinstall
```

### Python helper can't find extension

**Error:** `IO Error: Cannot open file`

**Solution:** Verify the path in `duckdb_helpers.py` matches your installation:
```python
# Check this path matches your installation
ext_path = home / '.duckdb' / 'extensions' / 'stps' / 'v1.4.3' / 'stps.duckdb_extension'
```

### .duckdbrc not being loaded

**Verify file location:**
- macOS/Linux: `~/.duckdbrc`
- Windows: `%USERPROFILE%\.duckdbrc` (typically `C:\Users\YourName\.duckdbrc`)

**Verify file permissions:**
```bash
ls -la ~/.duckdbrc  # Should be readable
```

## Trade-offs and Alternatives

### Chosen Approach: Proper Installation
✅ Clean, follows conventions
✅ Stable installation location
✅ Supports multiple versions
❌ Requires reinstall when updating extension

### Alternative: Direct Path to Build Directory
✅ No installation step needed
✅ Easier during development
❌ Breaks if project directory moves
❌ Less portable

The proper installation approach was chosen for better long-term maintainability and portability between machines.

## References

- DuckDB Extensions Documentation: https://duckdb.org/docs/extensions/overview
- Multi-Version Build System: [2025-12-31-multi-version-build-system.md](2025-12-31-multi-version-build-system.md)
