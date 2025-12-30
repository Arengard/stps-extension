# STPS Extension for DuckDB

A comprehensive DuckDB extension providing various utility functions for data processing and analysis.

## Features

This extension includes the following function categories:

### Text Processing
- **Case Transformation**: Functions for converting text case
- **Text Normalization**: Functions for normalizing text data
- **XML Parsing**: XML document parsing capabilities

### Data Operations  
- **GOBD Reader**: Specialized data reading functionality
- **Null Handling**: Advanced null value processing
- **I/O Operations**: File and data input/output utilities

### Validation & Generation
- **IBAN Validation**: International Bank Account Number validation
- **UUID Functions**: UUID generation and manipulation utilities

## Building the Extension

### Prerequisites
- CMake 3.5 or higher
- C++ compiler with C++11 support
- Git (for submodules)

### Quick Build
```bash
# Clone with submodules
git clone --recursive <your-repo-url>
cd stps-extension

# Or if already cloned, initialize submodules
git submodule update --init --recursive

# Build the extension
make debug
```

### Windows Build
```batch
# Windows Command Prompt or PowerShell
build-windows.bat

# Or manually:
git submodule update --init --recursive
make debug
```

### Manual Build
```bash
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j$(nproc)
```

## Usage

After building, load the extension in DuckDB:

```sql
-- Install the extension
INSTALL './build/debug/extension/polarsgodmode/polarsgodmode.duckdb_extension';

-- Load the extension
LOAD polarsgodmode;

-- Use extension functions
SELECT uuid_generate_v4();
```

## Development

### Project Structure
```
├── src/                    # Source files
│   ├── include/           # Header files
│   ├── polarsgodmode_extension.cpp  # Main extension file
│   ├── case_transform.cpp # Case transformation functions
│   ├── text_normalize.cpp # Text normalization functions
│   ├── xml_parser.cpp     # XML parsing functions
│   ├── gobd_reader.cpp    # GOBD reader implementation
│   ├── null_handling.cpp  # Null handling utilities
│   ├── io_operations.cpp  # I/O operation functions
│   ├── iban_validation.cpp # IBAN validation functions
│   ├── uuid_functions.cpp # UUID generation functions
│   └── utils.cpp          # Utility functions
├── test/                  # Test files
├── docs/                  # Documentation
├── duckdb/               # DuckDB submodule
└── extension-ci-tools/   # CI/CD tools submodule
```

### Contributing
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## CI/CD

This repository includes GitHub Actions workflows that automatically:
- Build the extension on Linux and macOS
- Run tests
- Validate the build output

The workflows trigger on pushes to `main`, `master`, `develop`, and `dev` branches.

## License

[Add your license information here]

## Functions Reference

For detailed function documentation, see:
- [STPS Functions Documentation](STPS_FUNCTIONS.md)
- [GOBD Usage Guide](GOBD_USAGE.md)
- [General Usage Guide](USAGE.md)
