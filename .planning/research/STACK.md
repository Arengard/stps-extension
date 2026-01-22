# Stack Research

**Domain:** DuckDB C++ extension for data quality and enrichment
**Researched:** 2026-01-22
**Confidence:** MEDIUM

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| C++ | C++17 | Extension implementation | DuckDB extension API and vectorized execution are C++-first. |
| DuckDB SDK | v1.4.3 | Extension API and engine linkage | Matches repo stack and avoids ABI drift. |
| CMake | 3.15+ | Build system | Standard for DuckDB extensions and CI tooling. |
| extension-ci-tools | current submodule | Build/test packaging | Official DuckDB tooling for extension CI. |

### Supporting Libraries

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| libcurl | latest via vcpkg | HTTP(S) requests | Required for AI, web lookups, Nextcloud. |
| zlib | latest via vcpkg | Decompression | ZIP/BLZ LUT and archive handling. |
| miniz | bundled | ZIP parsing | Lightweight ZIP parsing without external deps. |
| ICU | latest (optional) | Locale-aware normalization | If adding locale/date/number parsing or transliteration. |
| re2 | latest (optional) | Fast regex | If adding heavy regex matching at scale. |

### Development Tools

| Tool | Purpose | Notes |
|------|---------|-------|
| Visual Studio 2022 | Windows build | Required for local Windows builds. |
| GCC/Clang | Linux/macOS builds | Matches CI toolchains. |
| vcpkg | Dependency management | Used by DuckDB extension toolchain. |

## Installation

```bash
# Submodules and build toolchain (example)
git submodule update --init --recursive
make release

# Tests
make test
```

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|-------------------------|
| C++ DuckDB SDK | DuckDB C API | If exposing minimal table functions for C-only use. |
| extension-ci-tools | Custom CMake | If you need a nonstandard build pipeline. |
| libcurl | DuckDB httpfs only | If all network I/O can be delegated to DuckDB. |

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| Heavy ETL frameworks | Overkill for extension scope | Focus on composable SQL functions. |
| Custom HTTP stacks | Maintenance burden | libcurl via vcpkg. |
| Embedding full ML runtimes | Large binary size | Keep AI via external APIs or optional deps. |

## Stack Patterns by Variant

**If adding locale-heavy parsing:**
- Use ICU
- Because unicode/locale correctness is hard to reimplement

**If adding large-file streaming:**
- Use DuckDB table functions with streaming scans
- Because it avoids materializing full data in memory

## Version Compatibility

| Package A | Compatible With | Notes |
|-----------|-----------------|-------|
| DuckDB SDK v1.4.3 | C++17 toolchain | Matches repo stack and CI. |
| CMake 3.15+ | extension-ci-tools | Required by DuckDB build system. |

## Sources

- https://duckdb.org/docs/stable/extensions/overview.html — extension model and usage
- https://duckdb.org/docs/stable/dev/building/building_extensions.html — build guidance
- https://github.com/duckdb/extension-template — reference project structure
- https://duckdb.org/2024/07/05/community-extensions.html — extension ecosystem

---
*Stack research for: DuckDB extension (data quality and enrichment)*
*Researched: 2026-01-22*
