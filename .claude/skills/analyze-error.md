# Analyze Error

Analyze build errors, runtime errors, or test failures in the extension.

## Steps

1. Identify the error type:
   - Compilation error
   - Linker error
   - Runtime crash
   - Test failure
   - DuckDB error

2. Gather error details:
   - Full error message
   - Stack trace if available
   - Context where error occurs
   - Recent changes that might have caused it

3. For compilation errors:
   - Identify the problematic file and line
   - Check for syntax errors, type mismatches, missing includes
   - Look for Windows-specific issues (path separators, headers)
   - Check DuckDB API changes

4. For runtime errors:
   - Analyze the error message
   - Check input validation
   - Look for null pointer dereferences
   - Check array bounds
   - Review memory management

5. For test failures:
   - Compare expected vs actual output
   - Check if test data is correct
   - Verify function logic
   - Look for platform-specific issues

6. Search codebase for similar patterns:
   - Find how similar issues were resolved
   - Check for related functions that work correctly

7. Propose solution:
   - Explain root cause
   - Suggest fix with code
   - Mention if it affects other parts
   - Offer to implement the fix

## Context

This Windows-based extension deals with external libraries, string encodings, and various data formats, which can lead to platform-specific and integration issues.
