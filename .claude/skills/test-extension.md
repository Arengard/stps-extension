# Test Extension

Run tests for the STPS DuckDB extension to verify functionality.

## Steps

1. Check if the extension is built (look in build/ directory)
2. If not built, offer to rebuild first
3. Run the test suite using the Makefile or test scripts
4. Parse test results and report:
   - Number of tests passed/failed
   - Any error messages or failures
   - Suggestions for fixing failures
5. If tests pass, confirm extension is working correctly

## Context

The extension has test data in test/data/ and test SQL scripts. Tests verify functions like address validation, IBAN checking, PLZ validation, street parsing.
