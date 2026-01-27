# Claude Code Instructions for STPS Extension

## When Adding New Functions

After implementing any new function in this extension:

1. **Update README.md** - Add documentation for the new function including:
   - Function signature with parameter types and return type
   - Description of what it does
   - SQL examples showing usage
   - Any important notes or requirements

2. **Follow the existing documentation pattern** - Look at how similar functions are documented in README.md and match that style.

3. **Location in README.md** - Add new functions to the appropriate section:
   - UUID functions → "🆔 UUID Functions" section
   - Database utilities → "🔧 Advanced Functions" section
   - Validation functions → "✅ Data Validation" section
   - Text processing → "📝 Text Processing & Normalization" section
   - etc.

## Code Structure

When creating new functions:

1. **Header file**: `src/include/<function_name>.hpp`
2. **Source file**: `src/<function_name>.cpp`
3. **Register in**: `src/stps_unified_extension.cpp`
   - Add `#include` at the top
   - Call `RegisterXxxFunction(loader)` in the `Load()` method
4. **Add to CMakeLists.txt**: Add the `.cpp` file to `EXTENSION_SOURCES`

## Testing

Before committing:
- Verify the extension builds locally if possible
- Push to trigger GitHub Actions builds on all platforms (Windows, Linux, macOS)

## Commit Messages

Use descriptive commit messages:
- `Add stps_xxx function for <description>`
- `Fix <function> bug: <description>`
- `Update README with <function> documentation`
