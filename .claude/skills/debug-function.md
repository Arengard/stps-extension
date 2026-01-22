# Debug Function

Debug a specific function in the STPS extension that isn't working correctly.

## Steps

1. Ask the user:
   - Which function is problematic?
   - What's the expected behavior?
   - What's the actual behavior?
   - Any error messages?
   - Example input that fails?

2. Locate the function implementation:
   - Search for the function definition in src/
   - Read the function code

3. Analyze the issue:
   - Check input validation
   - Review logic flow
   - Look for type mismatches or pointer issues
   - Check error handling

4. Check related code:
   - Review helper functions called
   - Check for DuckDB API usage issues
   - Look at similar working functions

5. Propose fix:
   - Explain the root cause
   - Suggest code changes
   - Consider edge cases

6. If approved, implement the fix:
   - Make the code changes
   - Rebuild the extension
   - Run tests to verify the fix

## Context

Common issues in DuckDB extensions include type mismatches, pointer handling, string encoding, and incorrect use of DuckDB APIs.
