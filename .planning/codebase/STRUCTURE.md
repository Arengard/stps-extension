# Project Structure

## Directory Layout

```
stps-extension/
├── .github/workflows/      # CI/CD pipelines
│   ├── linux-build-on-push.yml
│   ├── macos-build-on-push.yml
│   └── windows-build-on-push.yml
├── .planning/              # Project planning docs
│   └── codebase/           # This codebase map
├── src/                    # Source code
│   ├── include/            # Header files (.hpp)
│   ├── shared/             # Shared utilities
│   ├── miniz/              # Bundled ZIP library
│   └── kontocheck/         # Bank validation
├── test/
│   ├── sql/                # SQL test files (.test)
│   └── data/               # Test data files
├── duckdb/                 # DuckDB submodule
├── extension-ci-tools/     # Build tooling submodule
├── howToUseMD/             # User documentation
├── scripts/                # Build/install scripts
├── binaries/               # Prebuilt binaries
└── build/                  # Build output (gitignored)
```

## Source File Organization

### Core Extension
| File | Purpose |
|------|---------|
| `stps_unified_extension.cpp` | Main extension entry, registers all functions |
| `stps_extension.cpp` | Alternative/legacy entry point |

### Function Modules (Scalar)
| File | Functions |
|------|-----------|
| `case_transform.cpp` | `stps_to_snake_case`, `stps_to_camel_case`, etc. |
| `text_normalize.cpp` | `stps_clean_string`, `stps_normalize`, `stps_remove_accents` |
| `null_handling.cpp` | `stps_coalesce`, `stps_is_null_or_empty` |
| `uuid_functions.cpp` | `stps_uuid`, `stps_is_valid_uuid` |
| `iban_validation.cpp` | `stps_is_valid_iban`, `stps_get_iban_*` |
| `plz_validation.cpp` | `stps_is_valid_plz` |
| `street_split.cpp` | `stps_split_street` |
| `account_validation.cpp` | German bank account validation |

### Function Modules (Table)
| File | Functions |
|------|-----------|
| `zip_functions.cpp` | `stps_zip`, `stps_view_zip` |
| `nextcloud_functions.cpp` | `stps_nextcloud` |
| `drop_null_columns_function.cpp` | `stps_drop_null_columns` |
| `drop_duplicates_function.cpp` | `stps_drop_duplicates`, `stps_show_duplicates` |
| `search_columns_function.cpp` | `stps_search_columns` |
| `smart_cast_function.cpp` | `stps_smart_cast` |
| `filesystem_functions.cpp` | `stps_list_files`, `stps_read_file` |

### Utility Modules
| File | Purpose |
|------|---------|
| `curl_utils.cpp` | HTTP client wrapper |
| `utils.cpp` | Common utilities |
| `xml_parser.cpp` | XML parsing |
| `gobd_reader.cpp` | GoBD file format |
| `blz_lut_loader.cpp` | Bank code lookup |

### Shared (src/shared/)
| File | Purpose |
|------|---------|
| `archive_utils.cpp` | Archive/CSV parsing helpers |
| `filesystem_utils.cpp` | Path/file helpers |
| `pattern_matcher.cpp` | Pattern matching |
| `content_searcher.cpp` | Content search |

## Naming Conventions

- **Source files**: `snake_case.cpp`
- **Headers**: `snake_case.hpp` in `src/include/`
- **Functions**: `stps_` prefix for all SQL functions
- **Namespaces**: `duckdb::stps`
