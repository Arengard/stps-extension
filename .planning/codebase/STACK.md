# Technology Stack

## Core Technologies

| Category | Technology | Version | Purpose |
|----------|------------|---------|---------|
| Language | C++ | C++17 | Extension implementation |
| Database | DuckDB | v1.4.3 | Host database engine |
| Build | CMake | 3.15+ | Build system |
| Build | MSVC/GCC/Clang | Latest | Compilers |

## Dependencies

### Required
| Dependency | Source | Purpose |
|------------|--------|---------|
| zlib | vcpkg | BLZ LUT decompression, ZIP handling |
| DuckDB SDK | submodule | Extension API |

### Optional
| Dependency | Source | Purpose | Compile Flag |
|------------|--------|---------|--------------|
| libcurl | vcpkg | AI functions, Nextcloud, web APIs | `HAVE_CURL` |

## Build System

- **Primary**: CMake with DuckDB extension-ci-tools
- **Makefile**: Wraps CMake for `make release` / `make debug`
- **Windows**: NMake or Visual Studio 2022
- **CI/CD**: GitHub Actions (Linux, macOS, Windows)

## Bundled Libraries

| Library | Location | Purpose |
|---------|----------|---------|
| miniz | `src/miniz/` | ZIP archive reading (no external dep) |

## Platform Support

| Platform | Status | Notes |
|----------|--------|-------|
| Windows x64 | ✅ Full | Primary development platform |
| Linux x64 | ✅ Full | CI tested |
| macOS (Intel/ARM) | ✅ Full | CI tested |
