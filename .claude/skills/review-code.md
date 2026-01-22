# Review Code

Perform a code review of recent changes or a specific file in the extension.

## Steps

1. Ask what to review:
   - Recent commits (use git log)
   - Specific file or function
   - All changes since last commit (git diff)

2. Analyze the code for:
   - **Correctness**: Does it do what it's supposed to?
   - **Memory safety**: Proper pointer handling, no leaks
   - **Error handling**: Appropriate checks and error messages
   - **DuckDB API usage**: Correct use of DuckDB types and functions
   - **Performance**: Any obvious inefficiencies
   - **Style**: Follows existing code conventions
   - **Security**: Input validation, no SQL injection or buffer overflows

3. Check for common DuckDB extension issues:
   - Type mismatches (std::vector vs duckdb::vector)
   - String handling (UTF-8 encoding)
   - Proper use of LogicalType
   - Correct result binding
   - Thread safety if applicable

4. Provide feedback:
   - List issues found with severity (critical/major/minor)
   - Suggest specific improvements
   - Highlight good practices observed
   - Reference relevant documentation

5. If critical issues found, offer to fix them

## Context

This extension interacts with external libraries (7zip, potentially web APIs) and handles various data types, so careful validation and error handling are essential.
