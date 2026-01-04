# STPS DuckDB Extension

A DuckDB extension providing STPS-specific functions for data transformation, validation, and file operations.

## Features

- **Text Transformations** – Case conversion, normalization, and text processing
- **Data Validation** – IBAN validation and other data quality checks
- **UUID Functions** – UUID generation and manipulation
- **Null Handling** – Enhanced null value processing
- **XML Parsing** – XML data parsing and extraction
- **File Operations** – Filesystem scanning and path manipulation
- **GOBD Reader** – German GOBD standard compliance tools

## Quick Start (Prebuilt Binaries)

**No build tools required!** Download prebuilt binaries from GitHub Actions:

### Step 1: Download the Extension

1. Go to [GitHub Actions](https://github.com/Arengard/stps-extension/actions)
2. Click on the latest successful "Windows Build on Push" workflow (green checkmark ✅)
3. Scroll to the **Artifacts** section at the bottom
4. Download `stps-windows-amd64-latest-master` (or your preferred version)
5. Extract the ZIP file to get `stps.duckdb_extension`

### Step 2: Load in DuckDB

```sql
-- Start DuckDB (unsigned mode required for loading extensions)
duckdb -unsigned

-- Install the extension
INSTALL './stps.duckdb_extension';

-- Load it
LOAD stps;

-- Test it works
SELECT stps_is_valid_iban('DE89370400440532013000') as is_valid;
```

### Step 3: Use the Functions

```sql
-- Text transformations
SELECT stps_upper('hello world') as upper_text;
SELECT stps_lower('HELLO WORLD') as lower_text;

-- IBAN validation
SELECT stps_is_valid_iban('DE89370400440532013000') as valid_iban;

-- UUID generation
SELECT stps_generate_uuid() as new_uuid;

-- File operations
SELECT * FROM stps_scan_directory('C:/path/to/folder');
```

## Available Functions

### Text Functions
- `stps_upper(text)` - Convert to uppercase
- `stps_lower(text)` - Convert to lowercase
- `stps_normalize_text(text)` - Normalize text

### Validation Functions
- `stps_is_valid_iban(iban)` - Validate IBAN

### UUID Functions
- `stps_generate_uuid()` - Generate new UUID

### File Operations
- `stps_scan_directory(path)` - Scan directory contents
- `stps_path_join(parts...)` - Join path components

## Building from Source

If you want to build the extension yourself:

### Prerequisites
- CMake 3.15+
- C++17 compatible compiler
- Git

### Build Steps

```bash
# Clone the repository
git clone https://github.com/Arengard/stps-extension.git
cd stps-extension

# Initialize submodules
git submodule update --init --recursive

# Build the extension
make release
```

### Windows Build
```batch
# Windows Command Prompt or PowerShell
git submodule update --init --recursive
make release
```

The built extension will be in `build/release/extension/stps/stps.duckdb_extension`

## Automated Builds

Every push to any branch automatically builds Windows binaries via GitHub Actions. Artifacts are retained for:
- **90 days** for commit-specific builds
- **30 days** for latest branch builds

Tagged releases (e.g., `v1.0.0`) create permanent GitHub releases with attached binaries.

## Development

### Running Tests

```bash
# Run extension tests
make test
```

### Project Structure

```
stps-extension/
├── src/                          # Source files
│   ├── stps_unified_extension.cpp  # Main extension entry
│   ├── case_transform.cpp        # Text transformations
│   ├── iban_validation.cpp       # IBAN validation
│   ├── uuid_functions.cpp        # UUID operations
│   └── filesystem_functions.cpp  # File operations
├── test/sql/                     # SQL test files
├── CMakeLists.txt               # Build configuration
└── .github/workflows/           # CI/CD workflows
```

## Compatibility

- **DuckDB Version**: v1.4.3
- **Platform**: Windows x64 (binaries provided)
- **Build System**: Linux, macOS, Windows (source builds)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `make test`
5. Submit a pull request

## License

See LICENSE file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/Arengard/stps-extension/issues)
- **Documentation**: See test files in `test/sql/` for usage examples

## Version History

- **Latest**: Automated builds on every commit
- Check [GitHub Releases](https://github.com/Arengard/stps-extension/releases) for tagged versions
