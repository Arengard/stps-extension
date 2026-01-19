# Multi-Version Build System for DuckDB Extension

**Date:** 2025-12-31
**Status:** Approved Design
**Goal:** Enable building and testing the extension against multiple DuckDB versions without manual submodule management

## Problem Statement

The extension is currently built against DuckDB development version `v1.4.3-4341-ge797eded1b`, but the system has stable DuckDB `v1.4.3` installed. This causes version incompatibility - extensions must match the exact DuckDB commit hash.

We need a flexible system to:
- Build the extension for specific DuckDB versions (starting with v1.4.3)
- Test each version automatically
- Preserve the development submodule state
- Easily add new versions in the future

## Architecture

### Directory Structure

```
stps extension/
├── build/
│   ├── v1.4.3/              # Build for stable release
│   │   ├── extension/
│   │   │   └── stps/
│   │   │       └── stps.duckdb_extension
│   │   └── duckdb           # DuckDB binary (v1.4.3)
│   └── dev/                 # Future: development builds
│
├── scripts/
│   ├── build-for-version.sh # Build extension for specific version
│   └── test-version.sh      # Run tests against specific version
│
├── test/
│   └── test_iban.sql        # Test queries to run
│
└── duckdb/                  # Submodule (restored after each build)
```

### Key Principle

**Submodule isolation**: The `duckdb/` submodule always returns to its original state after builds. Each versioned build is self-contained in its own directory.

## Component Design

### 1. Build Script (`scripts/build-for-version.sh`)

**Usage:**
```bash
./scripts/build-for-version.sh v1.4.3
```

**Process:**

1. **Save current state**: Records the current submodule commit
2. **Checkout target version**: Switches submodule to specified tag/commit
3. **Configure build**: Creates version-specific build directory
4. **Build extension**: Compiles extension and DuckDB binary
5. **Restore submodule**: Returns to original development state
6. **Output confirmation**: Shows paths and next steps

**Error Handling:**
- Fails if git tag doesn't exist (shows available tags)
- Checks for dirty submodule state before proceeding
- Cleans up partial builds on failure
- Uses trap to ensure submodule restoration even on errors

**Implementation:**
- Language: Bash
- Dependencies: Git, CMake, Make (already required)
- Exit code: 0 on success, 1 on failure

### 2. Test Script (`scripts/test-version.sh`)

**Usage:**
```bash
./scripts/test-version.sh v1.4.3
```

**Process:**

1. **Verify build exists**: Checks for extension binary
2. **Detect DuckDB binary**: Tries system DuckDB, falls back to built binary
3. **Run test suite**: Executes test SQL files with extension loaded
4. **Report results**: Shows pass/fail status with clear formatting
5. **Exit code**: 0 if all tests pass, 1 if any fail

**Output Format:**
```
✅ Testing v1.4.3 extension
📍 Extension: build/v1.4.3/extension/stps/stps.duckdb_extension
🔧 DuckDB: v1.4.3 (system)

Running tests:
✓ test_iban.sql passed

All tests passed!
```

**Error Handling:**
- Suggests running build script if extension not found
- Warns on version mismatch between built extension and DuckDB binary
- Provides helpful error messages for common issues

## Workflows

### Initial Setup

```bash
# 1. Build extension for v1.4.3
./scripts/build-for-version.sh v1.4.3

# 2. Run tests to verify it works
./scripts/test-version.sh v1.4.3

# 3. Use with system DuckDB
duckdb -unsigned
> LOAD './build/v1.4.3/extension/stps/stps.duckdb_extension';
> SELECT stps_is_valid_iban('DE89370400440532013000');
```

### Adding New Versions

```bash
# When DuckDB v1.5.0 releases:
./scripts/build-for-version.sh v1.5.0
./scripts/test-version.sh v1.5.0

# Or test against development version:
./scripts/build-for-version.sh main
./scripts/test-version.sh main
```

### Development Cycle

1. Make code changes to source files
2. Rebuild for target version: `./scripts/build-for-version.sh v1.4.3`
3. Run tests: `./scripts/test-version.sh v1.4.3`
4. Iterate until tests pass

### Cleanup

```bash
# Remove specific version
rm -rf build/v1.4.3/

# Remove all versioned builds
rm -rf build/v*/
```

## Edge Cases

### 1. Submodule in Dirty State

**Detection:**
```bash
if [[ -n $(cd duckdb && git status --porcelain) ]]; then
    echo "Error: duckdb submodule has uncommitted changes"
    exit 1
fi
```

### 2. Invalid Version Tag

**Validation:**
```bash
if ! git -C duckdb rev-parse "v1.4.3" >/dev/null 2>&1; then
    echo "Error: Tag v1.4.3 not found"
    echo "Available tags: $(git -C duckdb tag | grep '^v1' | tail -5)"
    exit 1
fi
```

### 3. Build Fails Mid-Process

**Recovery:**
```bash
# Trap to ensure submodule restore:
trap 'git -C duckdb checkout "$ORIGINAL_COMMIT"' EXIT
```

### 4. Disk Space Concerns

- Each build ~200-500MB (extension + DuckDB binary)
- Script warns if building 3+ versions
- Provides cleanup instructions in output

### 5. Version Mismatch Detection

**Verification in test script:**
```bash
BUILT_VERSION=$(strings build/v1.4.3/duckdb | grep -o 'v[0-9.]\+' | head -1)
SYSTEM_VERSION=$(duckdb --version | grep -o 'v[0-9.]\+')
if [[ "$BUILT_VERSION" != "v1.4.3" ]]; then
    echo "Warning: Version mismatch detected"
fi
```

## What Gets Preserved

- ✅ Development submodule state (always restored)
- ✅ All version builds (until manually deleted)
- ✅ Current `build/release` and `build/debug` (untouched)
- ✅ Git working directory remains clean

## Future Enhancements

### Possible Additions

1. **List available builds**: `./scripts/list-versions.sh`
2. **Compare versions**: Build and test against multiple versions simultaneously
3. **CI/CD integration**: Automated testing against supported versions
4. **Version compatibility matrix**: Document which extension features work with which DuckDB versions

### Not Included (YAGNI)

- ❌ Automatic version detection from system DuckDB
- ❌ Extension version numbering separate from DuckDB
- ❌ Binary distribution/packaging (out of scope)
- ❌ Docker-based builds (unnecessary complexity for now)

## Success Criteria

1. ✅ Can build extension for v1.4.3 matching system DuckDB
2. ✅ Extension loads and functions work in system DuckDB
3. ✅ Development submodule state never gets lost
4. ✅ Easy to add new versions using same pattern
5. ✅ Automated testing verifies each build works
6. ✅ Clear error messages guide users when things fail

## Implementation Notes

- Start with v1.4.3 only - prove the pattern works
- Scripts should be well-commented for future reference
- Use consistent output formatting (colors, emojis optional but helpful)
- Consider adding `set -euo pipefail` for strict error handling in bash scripts
