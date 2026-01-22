# Testing Strategy

## Test Framework

- **Framework**: DuckDB SQL Logic Tests
- **Location**: `test/sql/*.test`
- **Format**: `.test` files with SQL queries and expected results

## Test File Structure

```
# name: test/sql/function_name.test
# description: Test description
# group: [stps]

require stps

# Test case description
query T
SELECT stps_function('input');
----
expected_output

# Multiple columns
query TI
SELECT name, count FROM stps_table_function('arg');
----
value1	123
value2	456
```

## Test Categories

| File | Coverage |
|------|----------|
| `account_validation.test` | German bank account validation |
| `case_transform.test` | Case conversion functions |
| `drop_null_columns.test` | Column filtering |
| `filesystem.test` | File operations |
| `iban_validation.test` | IBAN validation |
| `io_operations.test` | I/O functions |
| `lambda_function.test` | Lambda expressions |
| `null_handling.test` | NULL handling |
| `plz_validation.test` | German postal codes |
| `smart_cast.test` | Type casting |
| `stps.test` | General extension tests |
| `street_split.test` | Address parsing |
| `text_normalize.test` | Text normalization |
| `uuid_functions.test` | UUID functions |
| `xml_parser.test` | XML parsing |

## Test Data

- **Location**: `test/data/`
- **Formats**: CSV, XML, ZIP

## Running Tests

```bash
# Run all tests
make test

# Run specific test
./build/release/test/unittest "test/sql/iban_validation.test"
```

## CI Testing

- **Linux**: GitHub Actions on ubuntu-latest
- **macOS**: GitHub Actions on macos-latest
- **Windows**: GitHub Actions on windows-latest

## Test Coverage Gaps

- AI functions (require API keys)
- Nextcloud functions (require server)
- Some edge cases in complex functions

## Adding New Tests

1. Create `test/sql/new_function.test`
2. Follow existing test file format
3. Include edge cases and error conditions
4. Add test data if needed
