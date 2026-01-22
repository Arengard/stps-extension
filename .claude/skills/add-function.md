# Add New Function

Add a new scalar or table function to the STPS DuckDB extension.

## Steps

1. Ask the user for:
   - Function name (with stps_ prefix)
   - Function type (scalar or table function)
   - Input parameters and types
   - Return type/columns
   - Description of what it does

2. Review existing functions in src/ to understand the pattern:
   - Look at similar functions as examples
   - Check how they're registered in the extension

3. Create the function implementation:
   - Add function code in appropriate file under src/
   - Follow existing naming conventions and patterns
   - Add proper error handling

4. Register the function:
   - Update the extension registration code
   - Add to the appropriate category

5. Create tests:
   - Add test cases in test/ directory
   - Verify edge cases

6. Update documentation:
   - Add function to README.md or appropriate doc file
   - Include usage examples

7. Rebuild and test the extension

## Context

This extension provides STPS (German tax office) related functions for DuckDB, including address parsing, validation, archive handling, and data utilities.
