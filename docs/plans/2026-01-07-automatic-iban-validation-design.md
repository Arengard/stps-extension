# Automatic German IBAN Validation Design

**Date:** 2026-01-07
**Status:** Approved
**Author:** Claude Sonnet 4.5

## Overview

Enable `stps_is_valid_iban()` to automatically validate German IBANs by looking up the BLZ (Bankleitzahl) in the downloaded LUT file to determine the correct Prüfziffermethode and validate the account number using kontocheck.

## Problem Statement

Currently, `stps_is_valid_iban()` only performs ISO 13616 MOD-97 validation. For German IBANs, this misses account number validation using the bank-specific check digit algorithms. Different German banks use different Prüfziffermethoden (0x00-0xC6), requiring a lookup table to map BLZ → method ID.

Users must currently use `stps_is_valid_german_iban(iban, method_id)` with explicit method_id, which is inconvenient.

## Goals

1. Parse the binary BLZ LUT file format to extract BLZ → method_id mappings
2. Enable automatic German IBAN validation without explicit method_id
3. Maintain backward compatibility and graceful degradation
4. Keep implementation simple and maintainable

## Architecture

### Component Overview

```
stps_is_valid_iban()
    ├─→ Standard MOD-97 validation (all countries)
    └─→ German IBAN detected?
        ├─→ Extract BLZ from BBAN
        ├─→ BlzLutLoader.LookupCheckMethod(blz) → method_id
        └─→ kontocheck::CheckMethods::ValidateAccount(account, method_id)
```

### Key Components

1. **LUT Parser** (`blz_lut_loader.cpp`)
   - Parse binary LUT format (BLZ Lookup Table/Format 1.1)
   - Build in-memory hash map: `unordered_map<string, uint8_t>`
   - Lazy loading on first IBAN validation call

2. **Integration Point** (`iban_validation.cpp:116-139`)
   - Uncomment existing TODO block
   - Replace placeholder with `BlzLutLoader::GetInstance().LookupCheckMethod()`
   - Automatic validation within `validate_iban()`

3. **Data Structures**
   - `blz_to_method_`: Hash map for O(1) BLZ lookup
   - ~14,000 entries × 16 bytes = ~224 KB memory footprint

## LUT File Format

### Binary Structure

```
[Header]         "BLZ Lookup Table/Format 1.1\n" (28 bytes)
[Info Line]      Human-readable metadata (optional, ends with \n)
[Count]          4 bytes - number of BLZ entries (little-endian)
[Checksum]       4 bytes - Adler32 checksum
[Compressed Data] Variable length - delta-encoded BLZ + method IDs
```

### Delta Encoding Scheme

Each BLZ entry uses 1-5 bytes based on difference from previous value:

| Byte Value | Meaning | Additional Bytes |
|------------|---------|------------------|
| 0-250 | Add to previous BLZ | 0 |
| 251 | Subtract 2-byte difference | 2 (little-endian) |
| 252 | Subtract 1-byte difference | 1 |
| 253 | Absolute 4-byte BLZ | 4 (little-endian) |
| 254 | Add 2-byte difference | 2 (little-endian) |
| 255 | Reserved (error) | - |

### Method ID Storage

After each BLZ value:
- If byte = `0xFF`: Entry is invalid, skip next byte
- Otherwise: Byte value = method ID (0x00-0xC6)

Example:
```
BLZ: 10000000, Method: 09
BLZ: 10000001, Method: 51  (delta = 1)
BLZ: 37040044, Method: 06  (large jump, use 253)
```

## Data Structures

### BlzLutLoader Class

```cpp
class BlzLutLoader {
public:
    static BlzLutLoader& GetInstance();
    bool Initialize(ClientContext &context);
    bool LookupCheckMethod(const std::string& blz, uint8_t& method_id);
    bool IsLoaded() const;

private:
    BlzLutLoader();
    bool ParseLutFile(const std::string& file_path);

    // In-memory representation
    std::unordered_map<std::string, uint8_t> blz_to_method_;
    bool is_loaded_;
    int entry_count_;
};
```

### Parsing Algorithm

```
1. Read entire file into memory buffer (1 MB)
2. Verify header: "BLZ Lookup Table/Format 1.1\n"
3. Skip info line until '\n'
4. Read count (4 bytes) and checksum (4 bytes)
5. Verify Adler32 checksum of remaining data
6. Parse delta-encoded entries:
   prev_blz = 0
   for i in 0..count:
       delta_byte = read_byte()
       blz = decode_delta(delta_byte, prev_blz)

       if read_byte() == 0xFF:
           skip_byte()  // Invalid entry
           continue

       method_id = read_byte()
       blz_to_method_[format_blz(blz)] = method_id
       prev_blz = blz
7. Return entry_count
```

### Decode Delta Function

```cpp
uint32_t decode_delta(uint8_t delta_byte, uint32_t prev_blz, uint8_t*& ptr) {
    switch (delta_byte) {
        case 0...250:
            return prev_blz + delta_byte;
        case 251: {  // 2-byte subtract
            uint16_t diff = ptr[0] | (ptr[1] << 8);
            ptr += 2;
            return prev_blz - diff;
        }
        case 252: {  // 1-byte subtract
            uint8_t diff = ptr[0];
            ptr += 1;
            return prev_blz - diff;
        }
        case 253: {  // 4-byte absolute
            uint32_t blz = ptr[0] | (ptr[1] << 8) | (ptr[2] << 16) | (ptr[3] << 24);
            ptr += 4;
            return blz;
        }
        case 254: {  // 2-byte add
            uint16_t diff = ptr[0] | (ptr[1] << 8);
            ptr += 2;
            return prev_blz + diff;
        }
        default:  // 255 or invalid
            throw std::runtime_error("Invalid delta byte");
    }
}
```

## Integration with IBAN Validation

### Modified validate_iban() Function

```cpp
bool validate_iban(const std::string& iban) {
    // Remove spaces and convert to uppercase
    std::string cleaned = clean_iban(iban);

    // Standard MOD-97 validation (existing code)
    if (!validate_mod97(cleaned)) {
        return false;
    }

    // Extract country code
    std::string country_code = cleaned.substr(0, 2);

    // NEW: Automatic German IBAN validation
    if (country_code == "DE" && cleaned.length() == 22) {
        std::string bban = cleaned.substr(4);  // Skip "DE" + 2 check digits
        if (bban.length() == 18) {
            std::string blz = bban.substr(0, 8);
            std::string account = bban.substr(8, 10);

            // Look up check method from BLZ LUT
            uint8_t method_id;
            if (BlzLutLoader::GetInstance().LookupCheckMethod(blz, method_id)) {
                // Validate account number using kontocheck
                auto check_result = kontocheck::CheckMethods::ValidateAccount(
                    account, method_id, blz);

                if (check_result != kontocheck::CheckResult::OK) {
                    return false;  // Invalid account number
                }
            }
            // If BLZ not found, continue (MOD-97 already passed)
        }
    }

    return true;
}
```

### Initialization Flow

```
Extension Load
    ├─→ Check ~/.stps/blz.lut exists
    ├─→ If missing: Download from https://www.michael-plugge.de/blz.lut
    └─→ File ready (1 MB)

First stps_is_valid_iban() call
    ├─→ BlzLutLoader::GetInstance().LookupCheckMethod()
    ├─→ Check if is_loaded_ == false
    ├─→ Call ParseLutFile() - loads 14,000 entries (150ms)
    ├─→ Set is_loaded_ = true
    └─→ Perform lookup (O(1))

Subsequent calls
    └─→ Direct hash map lookup (< 1μs)
```

## Validation Behavior

### Decision Matrix

| IBAN Type | MOD-97 | BLZ in LUT? | Account Check | Result |
|-----------|--------|-------------|---------------|--------|
| Non-German | ✓ Pass | N/A | N/A | Valid |
| Non-German | ✗ Fail | N/A | N/A | Invalid |
| German | ✓ Pass | ✓ Yes | ✓ Pass | Valid |
| German | ✓ Pass | ✓ Yes | ✗ Fail | Invalid |
| German | ✓ Pass | ✗ No | Skipped | Valid (degraded) |
| German | ✗ Fail | Any | Skipped | Invalid |

### Graceful Degradation

- **LUT file missing**: Download fails → Skip account validation, use MOD-97 only
- **LUT parsing fails**: Log warning → Skip account validation
- **BLZ not in LUT**: Continue with MOD-97 only (unknown bank)
- **Invalid method_id**: Should not happen (LUT data validated)

## Error Handling

### Parse Errors

```cpp
bool ParseLutFile(const std::string& file_path) {
    try {
        // ... parsing logic ...
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to parse LUT file: " << e.what() << std::endl;
        std::cerr << "German IBAN validation will use MOD-97 only" << std::endl;
        is_loaded_ = false;
        return false;
    }
}
```

### Lookup Errors

```cpp
bool LookupCheckMethod(const std::string& blz, uint8_t& method_id) {
    if (!is_loaded_) {
        // Lazy load on first call
        std::string lut_path = GetLutFilePath();
        if (!ParseLutFile(lut_path)) {
            return false;  // Parsing failed
        }
        is_loaded_ = true;
    }

    auto it = blz_to_method_.find(blz);
    if (it == blz_to_method_.end()) {
        return false;  // BLZ not found - not an error
    }

    method_id = it->second;
    return true;
}
```

## Testing Strategy

### Unit Tests

1. **LUT Parser Tests** (`test/sql/blz_lut_loader.test`)
   - Parse valid LUT file
   - Lookup known BLZ → method_id
   - Handle missing BLZ (not found)
   - Reject corrupted LUT file (bad checksum)
   - Handle truncated file

2. **IBAN Validation Tests** (`test/sql/iban_validation.test`)
   - Valid German IBAN with correct account number
   - Invalid German IBAN with bad account number
   - Valid German IBAN with unknown BLZ (graceful degradation)
   - Non-German IBANs (unchanged behavior)

### Test Cases

```sql
-- Valid German IBAN (MOD-97 + account check)
SELECT stps_is_valid_iban('DE89370400440532013000');
-- Expected: true (if account number passes method check)

-- Invalid German IBAN (bad account number)
SELECT stps_is_valid_iban('DE89370400440532013999');
-- Expected: false

-- Unknown BLZ (graceful degradation)
SELECT stps_is_valid_iban('DE89999999990532013000');
-- Expected: true (MOD-97 passes, unknown BLZ skipped)

-- Non-German IBAN (existing behavior)
SELECT stps_is_valid_iban('GB82WEST12345698765432');
-- Expected: true
```

## Performance Considerations

### Memory Usage

- **LUT file on disk**: 1.0 MB (compressed)
- **In-memory hash map**: ~224 KB (14,000 entries × 16 bytes)
- **Total memory impact**: Acceptable for a database extension

### Parsing Time

- **One-time cost**: ~150ms to parse 1 MB file on first call
- **Lookup time**: O(1) hash map lookup, < 1μs per call
- **Lazy loading**: Only parses when first German IBAN validated

### Optimization Opportunities

1. **Preload at extension startup**: Parse LUT during `Load()` instead of lazy
   - Pro: No first-call latency
   - Con: Increased extension load time

2. **Binary serialization**: Cache parsed data to avoid re-parsing
   - Pro: Faster subsequent loads
   - Con: Additional complexity, version compatibility

**Decision**: Use lazy loading for simplicity. Parse time is acceptable.

## Implementation Plan

### Phase 1: LUT Parser (Core)
1. Implement `ParseLutFile()` with delta decoding
2. Implement Adler32 checksum verification
3. Build `blz_to_method_` hash map
4. Add unit tests for parser

### Phase 2: Integration
1. Modify `validate_iban()` to call `LookupCheckMethod()`
2. Handle graceful degradation (BLZ not found)
3. Add integration tests for IBAN validation

### Phase 3: Testing & Documentation
1. Test with real German IBANs
2. Verify performance (parse time, memory usage)
3. Update README with automatic validation feature
4. Document BLZ LUT file format

## Files to Modify

1. **src/blz_lut_loader.cpp** (~200 lines added)
   - `ParseLutFile()` - parse binary LUT format
   - `DecodeDelta()` - decode delta encoding
   - `VerifyChecksum()` - Adler32 verification

2. **src/include/blz_lut_loader.hpp** (~20 lines)
   - Add `blz_to_method_` member
   - Add `entry_count_` member

3. **src/iban_validation.cpp** (~15 lines modified)
   - Uncomment lines 116-139
   - Replace placeholder with actual lookup call

4. **test/sql/iban_validation.test** (~30 lines added)
   - Add automatic German IBAN validation tests

## Alternatives Considered

### Alternative 1: Parse Bundesbank Text File
- **Pros**: Simpler format (tab-delimited text)
- **Cons**: Larger file (~5 MB), slower parsing
- **Decision**: Binary LUT is more efficient

### Alternative 2: Extend kontocheck C library
- **Pros**: Reuse proven code
- **Cons**: Complex C interop, harder to maintain
- **Decision**: Port relevant logic to C++

### Alternative 3: Always require method_id parameter
- **Pros**: No LUT parsing needed
- **Cons**: Poor user experience
- **Decision**: Automatic lookup is worth the complexity

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| LUT format changes | High | Version check in header, fail gracefully |
| Checksum mismatch | Medium | Verify Adler32, reject corrupted files |
| Parsing errors | Medium | Try-catch, log errors, continue with MOD-97 |
| Memory exhaustion | Low | 224 KB is acceptable, monitor usage |
| BLZ data outdated | Low | Download quarterly updates from source |

## Future Enhancements

1. **Automatic LUT updates**: Check for new versions quarterly
2. **Multi-country support**: Extend to Austrian IBANs (uses same system)
3. **Performance monitoring**: Track parse time and lookup statistics
4. **Cache parsed data**: Serialize to disk for faster loads

## References

- [kontocheck library](https://sourceforge.net/projects/kontocheck/)
- [konto_check.c source](https://metacpan.org/release/MICHEL/Business-KontoCheck-5.1/source/konto_check.c)
- [BLZ LUT Format 2.0](https://kontocheck.sourceforge.net/konto_check.php?fix_nav=0&ausgabe=4&sub=3)
- [Deutsche Bundesbank BLZ data](https://www.bundesbank.de/de/aufgaben/unbarer-zahlungsverkehr/serviceangebot/bankleitzahlen/download-bankleitzahlen-602592)
- [ISO 13616 IBAN Standard](https://www.iso.org/standard/81090.html)

## Approval

This design has been reviewed and approved for implementation.

**Approved by:** User
**Date:** 2026-01-07
