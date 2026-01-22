# Code Conventions

## Naming

### SQL Functions
- **Prefix**: All functions start with `stps_`
- **Style**: `snake_case` (e.g., `stps_is_valid_iban`, `stps_drop_duplicates`)
- **Verbs**: `is_`, `get_`, `to_`, `drop_`, `show_`, `ask_`

### C++ Code
- **Classes**: `PascalCase` (e.g., `StpsExtension`, `CurlHandle`)
- **Functions**: `PascalCase` for public, `camelCase` for private
- **Variables**: `snake_case`
- **Constants**: `UPPER_SNAKE_CASE`
- **Namespaces**: `duckdb::stps`

### Files
- **Source**: `snake_case.cpp`
- **Headers**: `snake_case.hpp`
- **Tests**: `function_name.test`

## Code Structure

### Function Registration Pattern
```cpp
// In header (src/include/foo.hpp)
void RegisterFooFunctions(ExtensionLoader &loader);

// In source (src/foo.cpp)
void RegisterFooFunctions(ExtensionLoader &loader) {
    ScalarFunction func("stps_foo", {LogicalType::VARCHAR}, 
                        LogicalType::VARCHAR, FooFunction);
    loader.RegisterFunction(func);
}
```

### Table Function Pattern
```cpp
struct FooBindData : public TableFunctionData { ... };
struct FooGlobalState : public GlobalTableFunctionState { ... };

static unique_ptr<FunctionData> FooBind(...);
static unique_ptr<GlobalTableFunctionState> FooInit(...);
static void FooScan(...);

TableFunction func("stps_foo", {args}, FooScan, FooBind, FooInit);
```

## Error Handling

- Use `throw BinderException(...)` for binding errors
- Use `throw IOException(...)` for I/O errors
- Use `throw InternalException(...)` for internal errors
- Always include descriptive error messages

## Optional Features

```cpp
#ifdef HAVE_CURL
// Code requiring libcurl
#endif
```

## Comments

- Use `//` for inline comments
- Use `/* */` for block comments
- Document function purpose in header files
- Include usage examples in README.md

## Formatting

- 4-space indentation
- Braces on same line for functions
- Max line length ~120 characters
- Include guards: `#pragma once`
