# Technical Concerns & Debt

## Known Issues

### 1. SSL Certificate Handling (Windows)
- **Status**: Recently fixed
- **Issue**: curl SSL errors on Windows with OpenSSL backend
- **Solution**: Added CA bundle detection and native CA support
- **File**: `src/curl_utils.cpp`

### 2. Disabled Features
- **fill_functions.cpp**: Temporarily disabled due to DuckDB API changes
- **Impact**: `stps_ffill`, `stps_bfill` not available

### 3. Legacy Code
- Multiple `gobd_reader_*.cpp` files (broken, old, v3 versions)
- Should consolidate to single implementation

## Technical Debt

### Code Duplication
- Pattern matching logic duplicated across files
- CSV parsing in multiple locations
- Should consolidate into shared utilities

### Missing Validation
- Some functions don't validate input thoroughly
- Table name escaping added recently, may need audit

### Error Messages
- Some error messages not user-friendly
- Could improve with more context

## Security Considerations

### API Keys
- Stored in memory only (not persisted)
- Set via `stps_set_*_api_key()` functions
- Should consider environment variable support

### SSL Verification
- Currently enabled by default
- CA bundle detection for Windows
- Corporate proxy environments may need CURLSSLOPT_NO_REVOKE

### Input Sanitization
- Table/column names now escaped with `EscapeIdentifier()`
- SQL injection protection in table functions

## Performance Concerns

### Large File Handling
- ZIP extraction loads entire file to memory
- Consider streaming for large archives

### Query Result Materialization
- Some table functions materialize full result
- Could improve memory usage with streaming

## Platform-Specific Issues

### Windows
- Path separator handling (/ vs \)
- SSL certificate store access
- NMake vs MSBuild differences

### macOS
- Universal binary support needed
- Different curl SSL backends

### Linux
- Generally most stable platform
- Docker build support available

## Maintenance Tasks

1. Remove legacy gobd_reader_*.cpp files
2. Enable fill_functions after API update
3. Add environment variable support for API keys
4. Improve test coverage for AI functions
5. Document all 50+ functions comprehensively
