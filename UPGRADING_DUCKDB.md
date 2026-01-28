# Upgrading DuckDB Version for STPS Extension

This document explains how to update the DuckDB version used by the STPS extension build.

## Current Version

As of this writing, the extension is built against **DuckDB v1.4.4**.

## How the Version is Controlled

The DuckDB version is controlled through **git submodules**:

1. `duckdb/` - The main DuckDB source code
2. `extension-ci-tools/` - Build tools and scripts for DuckDB extensions

Both submodules point to specific commits/tags that must be compatible with each other.

## Upgrade Process

### Step 1: Check Available Versions

```bash
# Fetch latest tags/branches for both submodules
cd duckdb && git fetch --tags && cd ..
cd extension-ci-tools && git fetch --all && cd ..

# List available DuckDB versions
cd duckdb && git tag -l "v*" | sort -V | tail -10

# List available extension-ci-tools branches
cd extension-ci-tools && git branch -r | grep -E "v[0-9]"
```

### Step 2: Update DuckDB Submodule

```bash
# Checkout the desired version tag
cd duckdb
git checkout v1.4.4  # Replace with target version
cd ..
```

### Step 3: Update extension-ci-tools Submodule

The extension-ci-tools uses version branches (not tags). For DuckDB 1.4.x versions, use the `v1.4-andium` branch:

```bash
cd extension-ci-tools
git checkout origin/v1.4-andium  # For any 1.4.x version
cd ..
```

**Branch naming convention:**
- `v1.4-andium` - for DuckDB 1.4.x (1.4.0, 1.4.1, 1.4.2, 1.4.3, 1.4.4, etc.)
- `v1.5-variegata` - for DuckDB 1.5.x (when available)
- Future versions will have similar pattern: `vX.Y-<codename>`

### Step 4: Verify the Changes

```bash
# Check submodule status - should show the new versions with a + prefix
git submodule status

# Expected output example:
# +6ddac802ffa9bcfbcc3f5f0d71de5dff9b0bc250 duckdb (v1.4.4)
# +eba10b6d2b7cbd38ae01f56be41e6309a1084f22 extension-ci-tools (remotes/origin/v1.4-andium)
```

### Step 5: Build and Test Locally (Optional but Recommended)

```bash
# Clean any previous build
make clean

# Build the extension
make release

# Or just test compilation
make debug
```

### Step 6: Commit the Submodule Updates

```bash
# Stage the submodule pointer changes
git add duckdb extension-ci-tools

# Commit with descriptive message
git commit -m "Upgrade to DuckDB v1.4.4"

# Push to trigger CI builds
git push
```

## Breaking Changes

When upgrading DuckDB versions, watch out for:

1. **API changes** - DuckDB internal APIs can change between minor versions. Check the [DuckDB changelog](https://github.com/duckdb/duckdb/releases) for breaking changes.

2. **Build system changes** - The extension build system may change. Check extension-ci-tools changelog.

3. **Common issues:**
   - `LogicalType` API changes (e.g., `LogicalType::JSON` vs `LogicalType::JSON()`)
   - `Vector` API changes (e.g., `SetCardinality` methods)
   - `CatalogTransaction` API changes
   - YYJSON macro changes

## Version History

| Date       | DuckDB Version | extension-ci-tools Branch | Notes                    |
|------------|----------------|---------------------------|--------------------------|
| 2025-01    | v1.4.4         | v1.4-andium               | Current version          |
| Previous   | v1.4.3         | v1.4-andium               | Previous version         |

## Troubleshooting

### Build fails after upgrade

1. **Clean build directory:**
   ```bash
   rm -rf build/
   make clean
   ```

2. **Check for API changes** in your extension source files

3. **Compare with DuckDB extension template** for reference:
   https://github.com/duckdb/extension-template

### Submodule issues

If submodules get into a bad state:

```bash
# Reset submodules to recorded state
git submodule update --init --recursive

# Or completely re-clone submodules
git submodule deinit -f .
git submodule update --init --recursive
```

### CI builds fail but local works

- Ensure you pushed the submodule pointer changes (both `duckdb` and `extension-ci-tools` should be staged)
- Check that `submodules: recursive` is set in the checkout action

## Quick Reference Commands

```bash
# Check current versions
git submodule status

# Update to specific DuckDB version (example: v1.4.4)
cd duckdb && git fetch --tags && git checkout v1.4.4 && cd ..
cd extension-ci-tools && git fetch --all && git checkout origin/v1.4-andium && cd ..

# Stage and commit
git add duckdb extension-ci-tools
git commit -m "Upgrade to DuckDB vX.Y.Z"
git push
```
