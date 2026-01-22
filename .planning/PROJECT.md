# Project

STPS is a DuckDB extension that adds 50+ SQL functions for data cleansing,
validation, file/archive access, German business data handling, and optional
AI-powered enrichment via Anthropic Claude.

## Audience

- End users who want to load the extension and use the SQL functions.

## Core Features

- Text normalization and case transforms
- IBAN/PLZ/account validation and address parsing
- Archive access (zip/7z) and filesystem helpers
- Table utilities (drop null columns, drop/show duplicates, search columns)
- Smart type casting utilities
- Optional AI functions for enrichment and classification

## Architecture (High-Level)

- Extension entry point registers all functions from `stps_unified_extension.cpp`.
- Function modules are grouped by concern in `src/*.cpp`.
- Table functions follow Bind -> Init -> Scan pattern.
- Optional features (curl-backed network calls) are guarded by `HAVE_CURL`.

## Build and Release

- Primary build is done in GitHub Actions for Linux, macOS, and Windows.
- CI runs `make release` and uploads `stps.duckdb_extension` artifacts.
- Local Windows build (not recommended): `make release` (requires VS 2022).

See `howToUseMD/BUILD_PROCESS.md` for details.

## Testing

- SQL logic tests live in `test/sql/*.test`.
- Run all tests: `make test`.
- Run one test: `./build/release/test/unittest "test/sql/iban_validation.test"`.

Notes:
- AI/Nextcloud tests require external services and keys.

## Configuration and Optional Features

- AI functions use Anthropic keys set via `stps_set_api_key()` or env/config.
- Web search uses optional Brave API key via `stps_set_brave_api_key()`.
- Network features require libcurl and are compiled when `HAVE_CURL` is set.

## Repository Layout

- `src/` C++ sources and headers
- `src/include/` public headers
- `src/shared/` shared utilities
- `test/` SQL tests and data
- `howToUseMD/` user-facing docs and guides
- `scripts/` build/test helpers
- `extension-ci-tools/` build tooling submodule
- `duckdb/` DuckDB submodule

## Docs and References

- `README.md` for installation and function usage
- `howToUseMD/STPS_FUNCTIONS.md` for full function list
- `howToUseMD/AI_FUNCTIONS_GUIDE.md` for AI usage

## Version and Compatibility

- Built against DuckDB v1.4.3 (see `.planning/codebase/STACK.md`).
- README notes DuckDB 0.10.0+ for loading the extension.

## License

See `LICENSE`.
