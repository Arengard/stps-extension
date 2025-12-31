# Pre-built Binaries

This directory contains pre-built binaries for the STPS Extension, automatically built by GitHub Actions.

## Quick Start with Helper Script

The easiest way to get started is to use the platform detection script:

```bash
./check-binary.sh
```

This will automatically detect your platform and show you the correct installation command.

## Available Platforms

Pre-built binaries will be available for the following platforms after the first build:

- **linux_amd64**: Linux x86_64 systems
- **osx_arm64**: macOS Apple Silicon (M1/M2/M3)
- **osx_amd64**: macOS Intel systems
- **windows_amd64**: Windows x86_64 systems

## Quick Start

After cloning or pulling the repository, you can immediately use the pre-built extension without building:

### Linux/macOS Example:
```bash
# Start DuckDB
duckdb

# In DuckDB:
INSTALL './binaries/linux_amd64/polarsgodmode.duckdb_extension';
LOAD polarsgodmode;
SELECT stps_uuid();
```

### Windows Example:
```bash
# Start DuckDB
duckdb.exe

# In DuckDB:
INSTALL './binaries/windows_amd64/polarsgodmode.duckdb_extension';
LOAD polarsgodmode;
SELECT stps_uuid();
```

## Build Information

These binaries are automatically rebuilt whenever source code changes are pushed to the main branches (master, main, develop, dev).

You no longer need to run `make debug` or `make release` after pulling - just use the pre-built binaries!

## Manual Building

If you prefer to build the extension yourself or need a different platform, see the main [README.md](../README.md) for build instructions.
