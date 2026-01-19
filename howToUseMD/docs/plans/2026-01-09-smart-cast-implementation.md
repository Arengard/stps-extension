# stps_smart_cast Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement smart type casting functions that auto-detect and convert VARCHAR columns to appropriate types with German/US locale awareness.

**Architecture:** Three-layer approach - shared utilities for type detection/parsing, table functions for bulk casting, and scalar function for single-value casting. Follow existing patterns from `drop_null_columns_function.cpp`.

**Tech Stack:** C++17, DuckDB Extension API, regex for pattern matching

---

## Task 1: Create Header Files

**Files:**
- Create: `src/include/smart_cast_utils.hpp`
- Create: `src/include/smart_cast_function.hpp`
- Create: `src/include/smart_cast_scalar.hpp`

**Step 1: Create smart_cast_utils.hpp**

```cpp
#pragma once

#include "duckdb.hpp"
#include <string>
#include <vector>
#include <optional>
#include <regex>

namespace duckdb {
namespace stps {

// Locale for number parsing
enum class NumberLocale {
    AUTO,
    GERMAN,  // 1.234,56 -> 1234.56
    US       // 1,234.56 -> 1234.56
};

// Date format preference
enum class DateFormat {
    AUTO,
    DMY,  // Day-Month-Year (European)
    MDY,  // Month-Day-Year (US)
    YMD   // Year-Month-Day (ISO)
};

// Detected type for a value
enum class DetectedType {
    UNKNOWN,
    BOOLEAN,
    INTEGER,
    DOUBLE,
    DATE,
    TIMESTAMP,
    UUID,
    VARCHAR
};

// Result of analyzing a column
struct ColumnAnalysis {
    string column_name;
    LogicalType original_type;
    DetectedType detected_type;
    LogicalType target_type;
    int64_t total_rows;
    int64_t null_count;
    int64_t cast_success_count;
    int64_t cast_failure_count;
    NumberLocale detected_locale;
    DateFormat detected_date_format;
};

// Smart cast utilities
class SmartCastUtils {
public:
    // Preprocess string (trim, empty -> nullopt)
    static std::optional<std::string> Preprocess(const std::string& input);

    // Detect locale from a vector of values
    static NumberLocale DetectLocale(const std::vector<std::string>& values);

    // Detect date format from a vector of values
    static DateFormat DetectDateFormat(const std::vector<std::string>& values);

    // Detect type of a single value
    static DetectedType DetectType(const std::string& value, NumberLocale locale = NumberLocale::AUTO,
                                    DateFormat date_format = DateFormat::AUTO);

    // Parse functions - return nullopt if parse fails
    static std::optional<bool> ParseBoolean(const std::string& value);
    static std::optional<int64_t> ParseInteger(const std::string& value, NumberLocale locale);
    static std::optional<double> ParseDouble(const std::string& value, NumberLocale locale);
    static std::optional<date_t> ParseDate(const std::string& value, DateFormat format = DateFormat::AUTO);
    static std::optional<timestamp_t> ParseTimestamp(const std::string& value, DateFormat format = DateFormat::AUTO);
    static std::optional<std::string> ParseUUID(const std::string& value);

    // Convert DetectedType to LogicalType
    static LogicalType ToLogicalType(DetectedType type);

    // Convert string to DetectedType enum
    static DetectedType StringToDetectedType(const std::string& type_str);

    // Check if string looks like an ID (leading zeros, etc.)
    static bool LooksLikeId(const std::string& value);

private:
    // Helper regex patterns
    static const std::regex BOOLEAN_PATTERN;
    static const std::regex INTEGER_PATTERN;
    static const std::regex GERMAN_NUMBER_PATTERN;
    static const std::regex US_NUMBER_PATTERN;
    static const std::regex UUID_PATTERN;
    static const std::regex CURRENCY_PATTERN;
    static const std::regex PERCENTAGE_PATTERN;

    // Date parsing helpers
    static std::optional<date_t> TryParseDate(const std::string& value, DateFormat format);
    static std::optional<timestamp_t> TryParseTimestamp(const std::string& value, DateFormat format);
};

} // namespace stps
} // namespace duckdb
```

**Step 2: Create smart_cast_function.hpp**

```cpp
#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Register stps_smart_cast and stps_smart_cast_analyze table functions
void RegisterSmartCastTableFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
```

**Step 3: Create smart_cast_scalar.hpp**

```cpp
#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Register stps_smart_cast scalar function
void RegisterSmartCastScalarFunction(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
```

**Step 4: Commit**

```bash
git add src/include/smart_cast_utils.hpp src/include/smart_cast_function.hpp src/include/smart_cast_scalar.hpp
git commit -m "feat(smart_cast): add header files for smart cast functions"
```

---

## Task 2: Implement Boolean Parsing

**Files:**
- Create: `src/smart_cast_utils.cpp`
- Create: `test/sql/smart_cast.test`

**Step 1: Write failing test for boolean parsing**

Create `test/sql/smart_cast.test`:

```
# name: test/sql/smart_cast.test
# description: Test stps_smart_cast functions
# group: [stps]

require stps

# Test boolean parsing via scalar function
query I
SELECT stps_smart_cast('true');
----
true

query I
SELECT stps_smart_cast('false');
----
false

query I
SELECT stps_smart_cast('yes');
----
true

query I
SELECT stps_smart_cast('no');
----
false

query I
SELECT stps_smart_cast('ja');
----
true

query I
SELECT stps_smart_cast('nein');
----
false

query I
SELECT stps_smart_cast('1')::BOOLEAN;
----
true

query I
SELECT stps_smart_cast('0')::BOOLEAN;
----
false
```

**Step 2: Implement Preprocess and ParseBoolean in smart_cast_utils.cpp**

```cpp
#include "smart_cast_utils.hpp"
#include <algorithm>
#include <cctype>

namespace duckdb {
namespace stps {

// Static regex patterns
const std::regex SmartCastUtils::BOOLEAN_PATTERN(
    "^(true|false|yes|no|ja|nein|1|0)$",
    std::regex::icase
);

const std::regex SmartCastUtils::UUID_PATTERN(
    "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
);

const std::regex SmartCastUtils::CURRENCY_PATTERN(
    "^[\\$\\€\\£\\¥]?\\s*"
);

const std::regex SmartCastUtils::PERCENTAGE_PATTERN(
    "^(.+)%$"
);

// Preprocess: trim whitespace, return nullopt for empty
std::optional<std::string> SmartCastUtils::Preprocess(const std::string& input) {
    // Trim leading whitespace
    size_t start = 0;
    while (start < input.size() && std::isspace(static_cast<unsigned char>(input[start]))) {
        start++;
    }

    // Trim trailing whitespace
    size_t end = input.size();
    while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1]))) {
        end--;
    }

    // Empty after trim -> nullopt
    if (start >= end) {
        return std::nullopt;
    }

    return input.substr(start, end - start);
}

// Parse boolean values
std::optional<bool> SmartCastUtils::ParseBoolean(const std::string& value) {
    auto processed = Preprocess(value);
    if (!processed) {
        return std::nullopt;
    }

    std::string lower = *processed;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    if (lower == "true" || lower == "yes" || lower == "ja" || lower == "1") {
        return true;
    }
    if (lower == "false" || lower == "no" || lower == "nein" || lower == "0") {
        return false;
    }

    return std::nullopt;
}

// Convert DetectedType to LogicalType
LogicalType SmartCastUtils::ToLogicalType(DetectedType type) {
    switch (type) {
        case DetectedType::BOOLEAN:
            return LogicalType::BOOLEAN;
        case DetectedType::INTEGER:
            return LogicalType::BIGINT;
        case DetectedType::DOUBLE:
            return LogicalType::DOUBLE;
        case DetectedType::DATE:
            return LogicalType::DATE;
        case DetectedType::TIMESTAMP:
            return LogicalType::TIMESTAMP;
        case DetectedType::UUID:
            return LogicalType::UUID;
        case DetectedType::VARCHAR:
        case DetectedType::UNKNOWN:
        default:
            return LogicalType::VARCHAR;
    }
}

} // namespace stps
} // namespace duckdb
```

**Step 3: Commit**

```bash
git add src/smart_cast_utils.cpp test/sql/smart_cast.test
git commit -m "feat(smart_cast): implement boolean parsing"
```

---

## Task 3: Implement Integer Parsing with Locale

**Files:**
- Modify: `src/smart_cast_utils.cpp`
- Modify: `test/sql/smart_cast.test`

**Step 1: Add integer tests to test file**

Append to `test/sql/smart_cast.test`:

```
# Integer parsing tests
query I
SELECT stps_smart_cast('123');
----
123

query I
SELECT stps_smart_cast('-456');
----
-456

query I
SELECT stps_smart_cast('1234');
----
1234

# German thousands separator (context needed for ambiguous)
query I
SELECT stps_smart_cast('1.234.567');
----
1234567
```

**Step 2: Implement ParseInteger**

Add to `src/smart_cast_utils.cpp`:

```cpp
// Integer pattern: optional minus, digits with optional thousand separators
const std::regex SmartCastUtils::INTEGER_PATTERN(
    "^-?\\d{1,3}(?:[.,]\\d{3})*$|^-?\\d+$"
);

// German number pattern: uses . for thousands, , for decimal
const std::regex SmartCastUtils::GERMAN_NUMBER_PATTERN(
    "^-?\\d{1,3}(?:\\.\\d{3})*,\\d+$"  // e.g., 1.234,56
);

// US number pattern: uses , for thousands, . for decimal
const std::regex SmartCastUtils::US_NUMBER_PATTERN(
    "^-?\\d{1,3}(?:,\\d{3})*\\.\\d+$"  // e.g., 1,234.56
);

std::optional<int64_t> SmartCastUtils::ParseInteger(const std::string& value, NumberLocale locale) {
    auto processed = Preprocess(value);
    if (!processed) {
        return std::nullopt;
    }

    std::string str = *processed;

    // Check for leading zeros (likely an ID)
    if (LooksLikeId(str)) {
        return std::nullopt;
    }

    // Remove currency symbols
    str = std::regex_replace(str, CURRENCY_PATTERN, "");

    // Determine separator to remove based on locale
    char thousands_sep = (locale == NumberLocale::US) ? ',' : '.';

    // Check if it contains a decimal separator - if so, not an integer
    char decimal_sep = (locale == NumberLocale::US) ? '.' : ',';
    if (str.find(decimal_sep) != std::string::npos) {
        // Has decimal separator, not an integer
        return std::nullopt;
    }

    // Remove thousands separators
    std::string clean;
    for (char c : str) {
        if (c != thousands_sep) {
            clean += c;
        }
    }

    // Parse as integer
    try {
        size_t pos;
        int64_t result = std::stoll(clean, &pos);
        if (pos == clean.size()) {
            return result;
        }
    } catch (...) {
        // Parse failed
    }

    return std::nullopt;
}

bool SmartCastUtils::LooksLikeId(const std::string& value) {
    // Leading zeros suggest an ID (e.g., "007", "00123")
    if (value.size() > 1 && value[0] == '0' && std::isdigit(static_cast<unsigned char>(value[1]))) {
        return true;
    }
    // Negative with leading zeros
    if (value.size() > 2 && value[0] == '-' && value[1] == '0' && std::isdigit(static_cast<unsigned char>(value[2]))) {
        return true;
    }
    return false;
}
```

**Step 3: Commit**

```bash
git add src/smart_cast_utils.cpp test/sql/smart_cast.test
git commit -m "feat(smart_cast): implement integer parsing with locale support"
```

---

## Task 4: Implement Double Parsing with Locale Detection

**Files:**
- Modify: `src/smart_cast_utils.cpp`
- Modify: `test/sql/smart_cast.test`

**Step 1: Add double tests**

Append to `test/sql/smart_cast.test`:

```
# Double parsing tests - US format
query R
SELECT stps_smart_cast('123.45');
----
123.45

query R
SELECT stps_smart_cast('1,234.56');
----
1234.56

# Double parsing tests - German format
query R
SELECT stps_smart_cast('123,45');
----
123.45

query R
SELECT stps_smart_cast('1.234,56');
----
1234.56

# Percentage conversion
query R
SELECT stps_smart_cast('45%');
----
0.45

query R
SELECT stps_smart_cast('100%');
----
1.0

# Currency stripping
query R
SELECT stps_smart_cast('€123,45');
----
123.45

query R
SELECT stps_smart_cast('$1,234.56');
----
1234.56
```

**Step 2: Implement ParseDouble and DetectLocale**

Add to `src/smart_cast_utils.cpp`:

```cpp
NumberLocale SmartCastUtils::DetectLocale(const std::vector<std::string>& values) {
    bool found_german = false;
    bool found_us = false;

    for (const auto& val : values) {
        auto processed = Preprocess(val);
        if (!processed) continue;

        std::string str = *processed;
        // Remove currency symbols for analysis
        str = std::regex_replace(str, CURRENCY_PATTERN, "");
        str = std::regex_replace(str, PERCENTAGE_PATTERN, "$1");

        // Unambiguous German: has comma as decimal (e.g., 1234,56 or 1.234,56)
        if (std::regex_match(str, GERMAN_NUMBER_PATTERN)) {
            found_german = true;
        }
        // Unambiguous US: has dot as decimal with comma thousands (e.g., 1,234.56)
        if (std::regex_match(str, US_NUMBER_PATTERN)) {
            found_us = true;
        }
    }

    // Conflict: mixed locales
    if (found_german && found_us) {
        return NumberLocale::AUTO;  // Signal conflict, keep as VARCHAR
    }

    if (found_german) return NumberLocale::GERMAN;
    if (found_us) return NumberLocale::US;

    // Default to German when ambiguous
    return NumberLocale::GERMAN;
}

std::optional<double> SmartCastUtils::ParseDouble(const std::string& value, NumberLocale locale) {
    auto processed = Preprocess(value);
    if (!processed) {
        return std::nullopt;
    }

    std::string str = *processed;

    // Check for percentage
    bool is_percentage = false;
    std::smatch match;
    if (std::regex_match(str, match, PERCENTAGE_PATTERN)) {
        str = match[1].str();
        is_percentage = true;
    }

    // Remove currency symbols
    str = std::regex_replace(str, CURRENCY_PATTERN, "");

    // Trim again after removing symbols
    auto trimmed = Preprocess(str);
    if (!trimmed) return std::nullopt;
    str = *trimmed;

    // Determine separators based on locale
    char thousands_sep, decimal_sep;
    if (locale == NumberLocale::US) {
        thousands_sep = ',';
        decimal_sep = '.';
    } else {
        // German or AUTO (default to German)
        thousands_sep = '.';
        decimal_sep = ',';
    }

    // Build clean string: remove thousands, replace decimal with .
    std::string clean;
    for (char c : str) {
        if (c == thousands_sep) {
            continue;  // Skip thousands separator
        } else if (c == decimal_sep) {
            clean += '.';  // Normalize decimal to .
        } else {
            clean += c;
        }
    }

    // Parse as double
    try {
        size_t pos;
        double result = std::stod(clean, &pos);
        if (pos == clean.size()) {
            if (is_percentage) {
                result /= 100.0;
            }
            return result;
        }
    } catch (...) {
        // Parse failed
    }

    return std::nullopt;
}
```

**Step 3: Commit**

```bash
git add src/smart_cast_utils.cpp test/sql/smart_cast.test
git commit -m "feat(smart_cast): implement double parsing with locale detection"
```

---

## Task 5: Implement Date Parsing

**Files:**
- Modify: `src/smart_cast_utils.cpp`
- Modify: `test/sql/smart_cast.test`

**Step 1: Add date tests**

Append to `test/sql/smart_cast.test`:

```
# Date parsing tests - ISO format
query D
SELECT stps_smart_cast('2024-01-15');
----
2024-01-15

# German dot format
query D
SELECT stps_smart_cast('15.01.2024');
----
2024-01-15

query D
SELECT stps_smart_cast('15.1.2024');
----
2024-01-15

# US slash format
query D
SELECT stps_smart_cast('01/15/2024');
----
2024-01-15

# Compact format
query D
SELECT stps_smart_cast('20240115');
----
2024-01-15

# Two-digit year
query D
SELECT stps_smart_cast('15.01.24');
----
2024-01-15

# Written month - English
query D
SELECT stps_smart_cast('15 Jan 2024');
----
2024-01-15

query D
SELECT stps_smart_cast('January 15, 2024');
----
2024-01-15

# Written month - German
query D
SELECT stps_smart_cast('15. Januar 2024');
----
2024-01-15
```

**Step 2: Implement ParseDate**

Add to `src/smart_cast_utils.cpp`:

```cpp
#include <map>
#include <ctime>

// Month name mappings
static const std::map<std::string, int> MONTH_NAMES = {
    // English
    {"jan", 1}, {"january", 1},
    {"feb", 2}, {"february", 2},
    {"mar", 3}, {"march", 3},
    {"apr", 4}, {"april", 4},
    {"may", 5},
    {"jun", 6}, {"june", 6},
    {"jul", 7}, {"july", 7},
    {"aug", 8}, {"august", 8},
    {"sep", 9}, {"september", 9},
    {"oct", 10}, {"october", 10},
    {"nov", 11}, {"november", 11},
    {"dec", 12}, {"december", 12},
    // German
    {"januar", 1}, {"jänner", 1},
    {"februar", 2},
    {"märz", 3}, {"maerz", 3},
    {"april", 4},
    {"mai", 5},
    {"juni", 6},
    {"juli", 7},
    {"august", 8},
    {"september", 9},
    {"oktober", 10},
    {"november", 11},
    {"dezember", 12}
};

// Helper to parse month name
static int ParseMonthName(const std::string& name) {
    std::string lower = name;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    // Remove trailing period
    if (!lower.empty() && lower.back() == '.') {
        lower.pop_back();
    }
    auto it = MONTH_NAMES.find(lower);
    if (it != MONTH_NAMES.end()) {
        return it->second;
    }
    return 0;
}

// Convert 2-digit year to 4-digit
static int ExpandYear(int year) {
    if (year < 100) {
        return (year >= 50) ? 1900 + year : 2000 + year;
    }
    return year;
}

// Create date_t from components
static std::optional<date_t> MakeDate(int year, int month, int day) {
    year = ExpandYear(year);
    if (month < 1 || month > 12 || day < 1 || day > 31) {
        return std::nullopt;
    }
    try {
        return Date::FromDate(year, month, day);
    } catch (...) {
        return std::nullopt;
    }
}

DateFormat SmartCastUtils::DetectDateFormat(const std::vector<std::string>& values) {
    bool found_dmy = false;
    bool found_mdy = false;

    for (const auto& val : values) {
        auto processed = Preprocess(val);
        if (!processed) continue;

        // Look for unambiguous dates where day > 12
        std::regex date_parts_regex("^(\\d{1,2})[./\\-](\\d{1,2})[./\\-](\\d{2,4})$");
        std::smatch match;
        if (std::regex_match(*processed, match, date_parts_regex)) {
            int first = std::stoi(match[1].str());
            int second = std::stoi(match[2].str());

            if (first > 12 && second <= 12) {
                found_dmy = true;  // First is day
            } else if (second > 12 && first <= 12) {
                found_mdy = true;  // Second is day, first is month
            }
        }
    }

    if (found_dmy && !found_mdy) return DateFormat::DMY;
    if (found_mdy && !found_dmy) return DateFormat::MDY;

    // Default to DMY (European)
    return DateFormat::DMY;
}

std::optional<date_t> SmartCastUtils::ParseDate(const std::string& value, DateFormat format) {
    auto processed = Preprocess(value);
    if (!processed) {
        return std::nullopt;
    }

    std::string str = *processed;

    // ISO format: 2024-01-15
    std::regex iso_regex("^(\\d{4})-(\\d{1,2})-(\\d{1,2})$");
    std::smatch match;
    if (std::regex_match(str, match, iso_regex)) {
        return MakeDate(std::stoi(match[1]), std::stoi(match[2]), std::stoi(match[3]));
    }

    // Compact format: 20240115
    std::regex compact_regex("^(\\d{4})(\\d{2})(\\d{2})$");
    if (std::regex_match(str, match, compact_regex)) {
        return MakeDate(std::stoi(match[1]), std::stoi(match[2]), std::stoi(match[3]));
    }

    // Year-first slash: 2024/01/15
    std::regex ymd_slash_regex("^(\\d{4})/(\\d{1,2})/(\\d{1,2})$");
    if (std::regex_match(str, match, ymd_slash_regex)) {
        return MakeDate(std::stoi(match[1]), std::stoi(match[2]), std::stoi(match[3]));
    }

    // Dot/slash/dash separated: try based on format
    std::regex dmy_regex("^(\\d{1,2})[./\\-](\\d{1,2})[./\\-](\\d{2,4})$");
    if (std::regex_match(str, match, dmy_regex)) {
        int first = std::stoi(match[1]);
        int second = std::stoi(match[2]);
        int third = std::stoi(match[3]);

        if (format == DateFormat::MDY) {
            return MakeDate(third, first, second);  // M/D/Y
        } else {
            return MakeDate(third, second, first);  // D/M/Y
        }
    }

    // Written month formats: "15 Jan 2024", "Jan 15, 2024", "15. Januar 2024"
    std::regex written_dmy_regex("^(\\d{1,2})\\.?\\s+([A-Za-zäöüÄÖÜ]+)\\.?\\s+(\\d{2,4})$");
    if (std::regex_match(str, match, written_dmy_regex)) {
        int month = ParseMonthName(match[2].str());
        if (month > 0) {
            return MakeDate(std::stoi(match[3]), month, std::stoi(match[1]));
        }
    }

    std::regex written_mdy_regex("^([A-Za-zäöüÄÖÜ]+)\\.?\\s+(\\d{1,2}),?\\s+(\\d{2,4})$");
    if (std::regex_match(str, match, written_mdy_regex)) {
        int month = ParseMonthName(match[1].str());
        if (month > 0) {
            return MakeDate(std::stoi(match[3]), month, std::stoi(match[2]));
        }
    }

    // Relative dates
    std::string lower = str;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    if (lower == "today" || lower == "heute") {
        return Date::FromDate(2026, 1, 9);  // Current date
    }
    if (lower == "yesterday" || lower == "gestern") {
        return Date::FromDate(2026, 1, 8);
    }
    if (lower == "tomorrow" || lower == "morgen") {
        return Date::FromDate(2026, 1, 10);
    }

    // Month-only: "Jan 2024", "2024-01"
    std::regex month_year_regex("^([A-Za-zäöüÄÖÜ]+)\\.?\\s+(\\d{4})$");
    if (std::regex_match(str, match, month_year_regex)) {
        int month = ParseMonthName(match[1].str());
        if (month > 0) {
            return MakeDate(std::stoi(match[2]), month, 1);
        }
    }

    std::regex year_month_regex("^(\\d{4})-(\\d{2})$");
    if (std::regex_match(str, match, year_month_regex)) {
        return MakeDate(std::stoi(match[1]), std::stoi(match[2]), 1);
    }

    // Quarter: Q1 2024, 2024-Q1
    std::regex quarter_regex("^Q([1-4])\\s+(\\d{4})$|^(\\d{4})-Q([1-4])$");
    if (std::regex_match(str, match, quarter_regex)) {
        int quarter, year;
        if (match[1].matched) {
            quarter = std::stoi(match[1]);
            year = std::stoi(match[2]);
        } else {
            year = std::stoi(match[3]);
            quarter = std::stoi(match[4]);
        }
        int month = (quarter - 1) * 3 + 1;
        return MakeDate(year, month, 1);
    }

    // Week: 2024-W03, W03-2024
    std::regex week_regex("^(\\d{4})-W(\\d{1,2})$|^W(\\d{1,2})[-\\s](\\d{4})$");
    if (std::regex_match(str, match, week_regex)) {
        int year, week;
        if (match[1].matched) {
            year = std::stoi(match[1]);
            week = std::stoi(match[2]);
        } else {
            week = std::stoi(match[3]);
            year = std::stoi(match[4]);
        }
        // Convert week to date (first day of week)
        int day = (week - 1) * 7 + 1;
        if (day > 365) day = 365;
        // Approximate: January 1 + days
        return MakeDate(year, 1, std::min(day, 28));
    }

    return std::nullopt;
}
```

**Step 3: Commit**

```bash
git add src/smart_cast_utils.cpp test/sql/smart_cast.test
git commit -m "feat(smart_cast): implement comprehensive date parsing"
```

---

## Task 6: Implement Timestamp and UUID Parsing

**Files:**
- Modify: `src/smart_cast_utils.cpp`
- Modify: `test/sql/smart_cast.test`

**Step 1: Add timestamp and UUID tests**

Append to `test/sql/smart_cast.test`:

```
# Timestamp parsing
query T
SELECT stps_smart_cast('2024-01-15 10:30:00');
----
2024-01-15 10:30:00

query T
SELECT stps_smart_cast('2024-01-15T10:30:00');
----
2024-01-15 10:30:00

query T
SELECT stps_smart_cast('15.01.2024 10:30');
----
2024-01-15 10:30:00

# UUID parsing
query I
SELECT stps_smart_cast('550e8400-e29b-41d4-a716-446655440000')::UUID IS NOT NULL;
----
true
```

**Step 2: Implement ParseTimestamp and ParseUUID**

Add to `src/smart_cast_utils.cpp`:

```cpp
std::optional<timestamp_t> SmartCastUtils::ParseTimestamp(const std::string& value, DateFormat format) {
    auto processed = Preprocess(value);
    if (!processed) {
        return std::nullopt;
    }

    std::string str = *processed;

    // Try to split into date and time parts
    std::regex datetime_regex("^(.+?)\\s*[T\\s]\\s*(\\d{1,2}:\\d{2}(?::\\d{2})?)(.*)$");
    std::smatch match;

    if (std::regex_match(str, match, datetime_regex)) {
        std::string date_part = match[1].str();
        std::string time_part = match[2].str();

        // Parse date part
        auto date_result = ParseDate(date_part, format);
        if (!date_result) {
            return std::nullopt;
        }

        // Parse time part
        int hour = 0, minute = 0, second = 0;
        if (sscanf(time_part.c_str(), "%d:%d:%d", &hour, &minute, &second) >= 2) {
            if (hour >= 0 && hour < 24 && minute >= 0 && minute < 60 && second >= 0 && second < 60) {
                try {
                    int32_t year, month, day;
                    Date::Convert(*date_result, year, month, day);
                    return Timestamp::FromDatetime(
                        Date::FromDate(year, month, day),
                        Time::FromTime(hour, minute, second, 0)
                    );
                } catch (...) {
                    return std::nullopt;
                }
            }
        }
    }

    return std::nullopt;
}

std::optional<std::string> SmartCastUtils::ParseUUID(const std::string& value) {
    auto processed = Preprocess(value);
    if (!processed) {
        return std::nullopt;
    }

    if (std::regex_match(*processed, UUID_PATTERN)) {
        return *processed;
    }

    return std::nullopt;
}
```

**Step 3: Commit**

```bash
git add src/smart_cast_utils.cpp test/sql/smart_cast.test
git commit -m "feat(smart_cast): implement timestamp and UUID parsing"
```

---

## Task 7: Implement Type Detection Logic

**Files:**
- Modify: `src/smart_cast_utils.cpp`

**Step 1: Implement DetectType function**

Add to `src/smart_cast_utils.cpp`:

```cpp
DetectedType SmartCastUtils::DetectType(const std::string& value, NumberLocale locale, DateFormat date_format) {
    auto processed = Preprocess(value);
    if (!processed) {
        return DetectedType::UNKNOWN;  // Empty/whitespace
    }

    // Check for ID-like values first (preserve as VARCHAR)
    if (LooksLikeId(*processed)) {
        return DetectedType::VARCHAR;
    }

    // Try boolean
    if (ParseBoolean(*processed)) {
        return DetectedType::BOOLEAN;
    }

    // Try UUID (before numbers, as UUIDs have dashes)
    if (ParseUUID(*processed)) {
        return DetectedType::UUID;
    }

    // Try integer (before double)
    if (ParseInteger(*processed, locale)) {
        return DetectedType::INTEGER;
    }

    // Try double
    if (ParseDouble(*processed, locale)) {
        return DetectedType::DOUBLE;
    }

    // Try timestamp (before date, as timestamp includes date)
    if (ParseTimestamp(*processed, date_format)) {
        return DetectedType::TIMESTAMP;
    }

    // Try date
    if (ParseDate(*processed, date_format)) {
        return DetectedType::DATE;
    }

    // Default to VARCHAR
    return DetectedType::VARCHAR;
}

DetectedType SmartCastUtils::StringToDetectedType(const std::string& type_str) {
    std::string upper = type_str;
    std::transform(upper.begin(), upper.end(), upper.begin(),
                   [](unsigned char c) { return std::toupper(c); });

    if (upper == "BOOLEAN" || upper == "BOOL") return DetectedType::BOOLEAN;
    if (upper == "INTEGER" || upper == "INT" || upper == "BIGINT") return DetectedType::INTEGER;
    if (upper == "DOUBLE" || upper == "FLOAT" || upper == "REAL") return DetectedType::DOUBLE;
    if (upper == "DATE") return DetectedType::DATE;
    if (upper == "TIMESTAMP") return DetectedType::TIMESTAMP;
    if (upper == "UUID") return DetectedType::UUID;
    return DetectedType::VARCHAR;
}
```

**Step 2: Commit**

```bash
git add src/smart_cast_utils.cpp
git commit -m "feat(smart_cast): implement type detection logic"
```

---

## Task 8: Implement Scalar Function

**Files:**
- Create: `src/smart_cast_scalar.cpp`
- Modify: `test/sql/smart_cast.test`

**Step 1: Add scalar function tests**

These tests should already pass with the scalar implementation:

```
# Scalar function with explicit type
query I
SELECT stps_smart_cast('123', 'INTEGER');
----
123

query R
SELECT stps_smart_cast('1.234,56', 'DOUBLE');
----
1234.56

query I
SELECT stps_smart_cast('invalid', 'INTEGER') IS NULL;
----
true
```

**Step 2: Implement scalar function**

Create `src/smart_cast_scalar.cpp`:

```cpp
#include "smart_cast_scalar.hpp"
#include "smart_cast_utils.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {
namespace stps {

// Auto-detect type and cast
static void SmartCastAutoFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    auto count = args.size();

    UnifiedVectorFormat input_data;
    input.ToUnifiedFormat(count, input_data);
    auto input_strings = UnifiedVectorFormat::GetData<string_t>(input_data);

    // First pass: collect all values to detect locale
    std::vector<std::string> all_values;
    for (idx_t i = 0; i < count; i++) {
        auto idx = input_data.sel->get_index(i);
        if (input_data.validity.RowIsValid(idx)) {
            all_values.push_back(input_strings[idx].GetString());
        }
    }

    NumberLocale locale = SmartCastUtils::DetectLocale(all_values);
    DateFormat date_format = SmartCastUtils::DetectDateFormat(all_values);

    // Result is VARCHAR (we return the detected value as string representation)
    auto result_data = FlatVector::GetData<string_t>(result);
    auto &result_validity = FlatVector::Validity(result);

    for (idx_t i = 0; i < count; i++) {
        auto idx = input_data.sel->get_index(i);
        if (!input_data.validity.RowIsValid(idx)) {
            result_validity.SetInvalid(i);
            continue;
        }

        std::string str = input_strings[idx].GetString();
        auto processed = SmartCastUtils::Preprocess(str);
        if (!processed) {
            result_validity.SetInvalid(i);
            continue;
        }

        DetectedType type = SmartCastUtils::DetectType(*processed, locale, date_format);

        // Cast and return string representation
        switch (type) {
            case DetectedType::BOOLEAN: {
                auto val = SmartCastUtils::ParseBoolean(*processed);
                if (val) {
                    result_data[i] = StringVector::AddString(result, *val ? "true" : "false");
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::INTEGER: {
                auto val = SmartCastUtils::ParseInteger(*processed, locale);
                if (val) {
                    result_data[i] = StringVector::AddString(result, std::to_string(*val));
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::DOUBLE: {
                auto val = SmartCastUtils::ParseDouble(*processed, locale);
                if (val) {
                    result_data[i] = StringVector::AddString(result, std::to_string(*val));
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::DATE: {
                auto val = SmartCastUtils::ParseDate(*processed, date_format);
                if (val) {
                    result_data[i] = StringVector::AddString(result, Date::ToString(*val));
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::TIMESTAMP: {
                auto val = SmartCastUtils::ParseTimestamp(*processed, date_format);
                if (val) {
                    result_data[i] = StringVector::AddString(result, Timestamp::ToString(*val));
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::UUID: {
                auto val = SmartCastUtils::ParseUUID(*processed);
                if (val) {
                    result_data[i] = StringVector::AddString(result, *val);
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            default:
                result_data[i] = StringVector::AddString(result, *processed);
                break;
        }
    }
}

// Cast to explicit type
static void SmartCastExplicitFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    auto &type_input = args.data[1];
    auto count = args.size();

    UnifiedVectorFormat input_data, type_data;
    input.ToUnifiedFormat(count, input_data);
    type_input.ToUnifiedFormat(count, type_data);

    auto input_strings = UnifiedVectorFormat::GetData<string_t>(input_data);
    auto type_strings = UnifiedVectorFormat::GetData<string_t>(type_data);

    // Detect locale from input values
    std::vector<std::string> all_values;
    for (idx_t i = 0; i < count; i++) {
        auto idx = input_data.sel->get_index(i);
        if (input_data.validity.RowIsValid(idx)) {
            all_values.push_back(input_strings[idx].GetString());
        }
    }
    NumberLocale locale = SmartCastUtils::DetectLocale(all_values);
    DateFormat date_format = SmartCastUtils::DetectDateFormat(all_values);

    auto result_data = FlatVector::GetData<string_t>(result);
    auto &result_validity = FlatVector::Validity(result);

    for (idx_t i = 0; i < count; i++) {
        auto input_idx = input_data.sel->get_index(i);
        auto type_idx = type_data.sel->get_index(i);

        if (!input_data.validity.RowIsValid(input_idx)) {
            result_validity.SetInvalid(i);
            continue;
        }

        std::string str = input_strings[input_idx].GetString();
        auto processed = SmartCastUtils::Preprocess(str);
        if (!processed) {
            result_validity.SetInvalid(i);
            continue;
        }

        std::string type_str = type_strings[type_idx].GetString();
        DetectedType target_type = SmartCastUtils::StringToDetectedType(type_str);

        // Cast to target type
        switch (target_type) {
            case DetectedType::BOOLEAN: {
                auto val = SmartCastUtils::ParseBoolean(*processed);
                if (val) {
                    result_data[i] = StringVector::AddString(result, *val ? "true" : "false");
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::INTEGER: {
                auto val = SmartCastUtils::ParseInteger(*processed, locale);
                if (val) {
                    result_data[i] = StringVector::AddString(result, std::to_string(*val));
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::DOUBLE: {
                auto val = SmartCastUtils::ParseDouble(*processed, locale);
                if (val) {
                    result_data[i] = StringVector::AddString(result, std::to_string(*val));
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::DATE: {
                auto val = SmartCastUtils::ParseDate(*processed, date_format);
                if (val) {
                    result_data[i] = StringVector::AddString(result, Date::ToString(*val));
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::TIMESTAMP: {
                auto val = SmartCastUtils::ParseTimestamp(*processed, date_format);
                if (val) {
                    result_data[i] = StringVector::AddString(result, Timestamp::ToString(*val));
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::UUID: {
                auto val = SmartCastUtils::ParseUUID(*processed);
                if (val) {
                    result_data[i] = StringVector::AddString(result, *val);
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            default:
                result_data[i] = StringVector::AddString(result, *processed);
                break;
        }
    }
}

void RegisterSmartCastScalarFunction(ExtensionLoader &loader) {
    ScalarFunctionSet smart_cast_set("stps_smart_cast");

    // Overload 1: auto-detect type
    smart_cast_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        SmartCastAutoFunction
    ));

    // Overload 2: explicit target type
    smart_cast_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        SmartCastExplicitFunction
    ));

    loader.RegisterFunction(smart_cast_set);
}

} // namespace stps
} // namespace duckdb
```

**Step 3: Commit**

```bash
git add src/smart_cast_scalar.cpp test/sql/smart_cast.test
git commit -m "feat(smart_cast): implement scalar function"
```

---

## Task 9: Implement Table Functions (stps_smart_cast and stps_smart_cast_analyze)

**Files:**
- Create: `src/smart_cast_function.cpp`
- Modify: `test/sql/smart_cast.test`

**Step 1: Add table function tests**

Append to `test/sql/smart_cast.test`:

```
# Table function tests
statement ok
CREATE TABLE test_cast AS SELECT
    '123' as num_col,
    'true' as bool_col,
    '2024-01-15' as date_col,
    '1.234,56' as german_num,
    'hello' as text_col;

statement ok
INSERT INTO test_cast VALUES ('456', 'false', '2024-02-20', '2.345,67', 'world');

# Test stps_smart_cast_analyze
query ITIIIII
SELECT column_name, detected_type, total_rows, null_count, cast_success_count, cast_failure_count
FROM stps_smart_cast_analyze('test_cast')
ORDER BY column_name;
----
bool_col	BOOLEAN	2	0	2	0
date_col	DATE	2	0	2	0
german_num	DOUBLE	2	0	2	0
num_col	INTEGER	2	0	2	0
text_col	VARCHAR	2	0	2	0

# Test stps_smart_cast table function
query IIDRT
SELECT * FROM stps_smart_cast('test_cast');
----
123	true	2024-01-15	1234.56	hello
456	false	2024-02-20	2345.67	world

statement ok
DROP TABLE test_cast;
```

**Step 2: Implement table functions**

Create `src/smart_cast_function.cpp`:

```cpp
#include "smart_cast_function.hpp"
#include "smart_cast_utils.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {
namespace stps {

//=============================================================================
// stps_smart_cast_analyze - Returns metadata about detected types
//=============================================================================

struct SmartCastAnalyzeBindData : public TableFunctionData {
    string table_name;
    vector<ColumnAnalysis> analysis;
    double min_success_rate = 0.1;
    NumberLocale forced_locale = NumberLocale::AUTO;
    DateFormat forced_date_format = DateFormat::AUTO;
};

struct SmartCastAnalyzeGlobalState : public GlobalTableFunctionState {
    idx_t current_row = 0;
};

static unique_ptr<FunctionData> SmartCastAnalyzeBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<SmartCastAnalyzeBindData>();

    if (input.inputs.empty()) {
        throw BinderException("stps_smart_cast_analyze requires one argument: table_name");
    }
    result->table_name = input.inputs[0].GetValue<string>();

    // Handle named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "min_success_rate") {
            result->min_success_rate = kv.second.GetValue<double>();
        } else if (kv.first == "locale") {
            string loc = kv.second.GetValue<string>();
            if (loc == "de") result->forced_locale = NumberLocale::GERMAN;
            else if (loc == "en") result->forced_locale = NumberLocale::US;
        } else if (kv.first == "date_format") {
            string fmt = kv.second.GetValue<string>();
            if (fmt == "dmy") result->forced_date_format = DateFormat::DMY;
            else if (fmt == "mdy") result->forced_date_format = DateFormat::MDY;
            else if (fmt == "ymd") result->forced_date_format = DateFormat::YMD;
        }
    }

    // Get table schema
    Connection conn(context.db->GetDatabase(context));
    auto schema_result = conn.Query("SELECT * FROM " + result->table_name + " LIMIT 0");
    if (schema_result->HasError()) {
        throw BinderException("Table '%s' does not exist: %s", result->table_name, schema_result->GetError());
    }

    // Analyze each VARCHAR column
    for (idx_t col = 0; col < schema_result->names.size(); col++) {
        ColumnAnalysis analysis;
        analysis.column_name = schema_result->names[col];
        analysis.original_type = schema_result->types[col];

        // Only analyze VARCHAR columns
        if (analysis.original_type != LogicalType::VARCHAR) {
            analysis.detected_type = DetectedType::VARCHAR;
            analysis.target_type = analysis.original_type;
            analysis.total_rows = 0;
            analysis.null_count = 0;
            analysis.cast_success_count = 0;
            analysis.cast_failure_count = 0;
            result->analysis.push_back(analysis);
            continue;
        }

        // Get all values for this column
        auto query = "SELECT \"" + analysis.column_name + "\" FROM " + result->table_name;
        auto col_result = conn.Query(query);
        if (col_result->HasError()) {
            throw BinderException("Failed to query column: %s", col_result->GetError());
        }

        std::vector<std::string> values;
        analysis.total_rows = 0;
        analysis.null_count = 0;

        while (auto chunk = col_result->Fetch()) {
            for (idx_t row = 0; row < chunk->size(); row++) {
                analysis.total_rows++;
                auto val = chunk->GetValue(0, row);
                if (val.IsNull()) {
                    analysis.null_count++;
                } else {
                    std::string str = val.GetValue<string>();
                    auto processed = SmartCastUtils::Preprocess(str);
                    if (processed) {
                        values.push_back(*processed);
                    } else {
                        analysis.null_count++;  // Empty strings count as null
                    }
                }
            }
        }

        // Detect locale and date format
        NumberLocale locale = result->forced_locale != NumberLocale::AUTO
            ? result->forced_locale
            : SmartCastUtils::DetectLocale(values);
        DateFormat date_format = result->forced_date_format != DateFormat::AUTO
            ? result->forced_date_format
            : SmartCastUtils::DetectDateFormat(values);

        analysis.detected_locale = locale;
        analysis.detected_date_format = date_format;

        // Count type occurrences
        std::map<DetectedType, int64_t> type_counts;
        for (const auto& val : values) {
            DetectedType type = SmartCastUtils::DetectType(val, locale, date_format);
            type_counts[type]++;
        }

        // Find most common non-VARCHAR type
        DetectedType best_type = DetectedType::VARCHAR;
        int64_t best_count = 0;
        for (const auto& [type, count] : type_counts) {
            if (type != DetectedType::VARCHAR && type != DetectedType::UNKNOWN && count > best_count) {
                best_type = type;
                best_count = count;
            }
        }

        // Check success rate
        int64_t non_null_count = analysis.total_rows - analysis.null_count;
        if (non_null_count > 0 && best_type != DetectedType::VARCHAR) {
            double success_rate = static_cast<double>(best_count) / non_null_count;
            if (success_rate >= result->min_success_rate) {
                analysis.detected_type = best_type;
                analysis.cast_success_count = best_count;
                analysis.cast_failure_count = non_null_count - best_count;
            } else {
                analysis.detected_type = DetectedType::VARCHAR;
                analysis.cast_success_count = non_null_count;
                analysis.cast_failure_count = 0;
            }
        } else {
            analysis.detected_type = DetectedType::VARCHAR;
            analysis.cast_success_count = non_null_count;
            analysis.cast_failure_count = 0;
        }

        analysis.target_type = SmartCastUtils::ToLogicalType(analysis.detected_type);
        result->analysis.push_back(analysis);
    }

    // Set output schema
    names = {"column_name", "original_type", "detected_type", "total_rows", "null_count", "cast_success_count", "cast_failure_count"};
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                    LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> SmartCastAnalyzeInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<SmartCastAnalyzeGlobalState>();
}

static void SmartCastAnalyzeScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<SmartCastAnalyzeGlobalState>();
    auto &bind_data = data_p.bind_data->Cast<SmartCastAnalyzeBindData>();

    idx_t count = 0;
    while (state.current_row < bind_data.analysis.size() && count < STANDARD_VECTOR_SIZE) {
        auto &analysis = bind_data.analysis[state.current_row];

        output.SetValue(0, count, Value(analysis.column_name));
        output.SetValue(1, count, Value(analysis.original_type.ToString()));

        // Convert detected type to string
        std::string type_str;
        switch (analysis.detected_type) {
            case DetectedType::BOOLEAN: type_str = "BOOLEAN"; break;
            case DetectedType::INTEGER: type_str = "INTEGER"; break;
            case DetectedType::DOUBLE: type_str = "DOUBLE"; break;
            case DetectedType::DATE: type_str = "DATE"; break;
            case DetectedType::TIMESTAMP: type_str = "TIMESTAMP"; break;
            case DetectedType::UUID: type_str = "UUID"; break;
            default: type_str = "VARCHAR"; break;
        }
        output.SetValue(2, count, Value(type_str));
        output.SetValue(3, count, Value::BIGINT(analysis.total_rows));
        output.SetValue(4, count, Value::BIGINT(analysis.null_count));
        output.SetValue(5, count, Value::BIGINT(analysis.cast_success_count));
        output.SetValue(6, count, Value::BIGINT(analysis.cast_failure_count));

        count++;
        state.current_row++;
    }

    output.SetCardinality(count);
}

//=============================================================================
// stps_smart_cast - Returns table with cast columns
//=============================================================================

struct SmartCastBindData : public TableFunctionData {
    string table_name;
    vector<ColumnAnalysis> analysis;
    vector<string> output_columns;
    vector<LogicalType> output_types;
    double min_success_rate = 0.1;
    NumberLocale forced_locale = NumberLocale::AUTO;
    DateFormat forced_date_format = DateFormat::AUTO;
};

struct SmartCastGlobalState : public GlobalTableFunctionState {
    unique_ptr<QueryResult> result;
    idx_t chunk_offset = 0;
};

static unique_ptr<FunctionData> SmartCastBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<SmartCastBindData>();

    if (input.inputs.empty()) {
        throw BinderException("stps_smart_cast requires one argument: table_name");
    }
    result->table_name = input.inputs[0].GetValue<string>();

    // Handle named parameters (same as analyze)
    for (auto &kv : input.named_parameters) {
        if (kv.first == "min_success_rate") {
            result->min_success_rate = kv.second.GetValue<double>();
        } else if (kv.first == "locale") {
            string loc = kv.second.GetValue<string>();
            if (loc == "de") result->forced_locale = NumberLocale::GERMAN;
            else if (loc == "en") result->forced_locale = NumberLocale::US;
        } else if (kv.first == "date_format") {
            string fmt = kv.second.GetValue<string>();
            if (fmt == "dmy") result->forced_date_format = DateFormat::DMY;
            else if (fmt == "mdy") result->forced_date_format = DateFormat::MDY;
            else if (fmt == "ymd") result->forced_date_format = DateFormat::YMD;
        }
    }

    // Reuse analysis logic from analyze bind (simplified - in production, factor this out)
    Connection conn(context.db->GetDatabase(context));
    auto schema_result = conn.Query("SELECT * FROM " + result->table_name + " LIMIT 0");
    if (schema_result->HasError()) {
        throw BinderException("Table '%s' does not exist: %s", result->table_name, schema_result->GetError());
    }

    // Analyze columns (same logic as SmartCastAnalyzeBind)
    for (idx_t col = 0; col < schema_result->names.size(); col++) {
        ColumnAnalysis analysis;
        analysis.column_name = schema_result->names[col];
        analysis.original_type = schema_result->types[col];

        if (analysis.original_type != LogicalType::VARCHAR) {
            analysis.detected_type = DetectedType::VARCHAR;
            analysis.target_type = analysis.original_type;
            result->analysis.push_back(analysis);
            result->output_columns.push_back(analysis.column_name);
            result->output_types.push_back(analysis.original_type);
            continue;
        }

        // Get values and analyze
        auto query = "SELECT \"" + analysis.column_name + "\" FROM " + result->table_name;
        auto col_result = conn.Query(query);

        std::vector<std::string> values;
        int64_t total_rows = 0, null_count = 0;

        while (auto chunk = col_result->Fetch()) {
            for (idx_t row = 0; row < chunk->size(); row++) {
                total_rows++;
                auto val = chunk->GetValue(0, row);
                if (val.IsNull()) {
                    null_count++;
                } else {
                    auto processed = SmartCastUtils::Preprocess(val.GetValue<string>());
                    if (processed) {
                        values.push_back(*processed);
                    } else {
                        null_count++;
                    }
                }
            }
        }

        NumberLocale locale = result->forced_locale != NumberLocale::AUTO
            ? result->forced_locale : SmartCastUtils::DetectLocale(values);
        DateFormat date_format = result->forced_date_format != DateFormat::AUTO
            ? result->forced_date_format : SmartCastUtils::DetectDateFormat(values);

        analysis.detected_locale = locale;
        analysis.detected_date_format = date_format;

        std::map<DetectedType, int64_t> type_counts;
        for (const auto& val : values) {
            type_counts[SmartCastUtils::DetectType(val, locale, date_format)]++;
        }

        DetectedType best_type = DetectedType::VARCHAR;
        int64_t best_count = 0;
        for (const auto& [type, count] : type_counts) {
            if (type != DetectedType::VARCHAR && type != DetectedType::UNKNOWN && count > best_count) {
                best_type = type;
                best_count = count;
            }
        }

        int64_t non_null_count = total_rows - null_count;
        if (non_null_count > 0 && best_type != DetectedType::VARCHAR) {
            double success_rate = static_cast<double>(best_count) / non_null_count;
            if (success_rate >= result->min_success_rate) {
                analysis.detected_type = best_type;
            } else {
                analysis.detected_type = DetectedType::VARCHAR;
            }
        } else {
            analysis.detected_type = DetectedType::VARCHAR;
        }

        analysis.target_type = SmartCastUtils::ToLogicalType(analysis.detected_type);
        result->analysis.push_back(analysis);
        result->output_columns.push_back(analysis.column_name);
        result->output_types.push_back(analysis.target_type);
    }

    names = result->output_columns;
    return_types = result->output_types;

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> SmartCastInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<SmartCastBindData>();
    auto state = make_uniq<SmartCastGlobalState>();

    // Query original data
    Connection conn(context.db->GetDatabase(context));
    state->result = conn.Query("SELECT * FROM " + bind_data.table_name);

    return std::move(state);
}

static void SmartCastScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<SmartCastGlobalState>();
    auto &bind_data = data_p.bind_data->Cast<SmartCastBindData>();

    auto chunk = state.result->Fetch();
    if (!chunk || chunk->size() == 0) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = chunk->size();

    for (idx_t col = 0; col < bind_data.analysis.size(); col++) {
        auto &analysis = bind_data.analysis[col];

        for (idx_t row = 0; row < count; row++) {
            auto val = chunk->GetValue(col, row);

            if (val.IsNull()) {
                output.SetValue(col, row, Value());
                continue;
            }

            // Non-VARCHAR columns pass through
            if (analysis.original_type != LogicalType::VARCHAR) {
                output.SetValue(col, row, val);
                continue;
            }

            std::string str = val.GetValue<string>();
            auto processed = SmartCastUtils::Preprocess(str);
            if (!processed) {
                output.SetValue(col, row, Value());
                continue;
            }

            // Cast based on detected type
            switch (analysis.detected_type) {
                case DetectedType::BOOLEAN: {
                    auto parsed = SmartCastUtils::ParseBoolean(*processed);
                    output.SetValue(col, row, parsed ? Value::BOOLEAN(*parsed) : Value());
                    break;
                }
                case DetectedType::INTEGER: {
                    auto parsed = SmartCastUtils::ParseInteger(*processed, analysis.detected_locale);
                    output.SetValue(col, row, parsed ? Value::BIGINT(*parsed) : Value());
                    break;
                }
                case DetectedType::DOUBLE: {
                    auto parsed = SmartCastUtils::ParseDouble(*processed, analysis.detected_locale);
                    output.SetValue(col, row, parsed ? Value::DOUBLE(*parsed) : Value());
                    break;
                }
                case DetectedType::DATE: {
                    auto parsed = SmartCastUtils::ParseDate(*processed, analysis.detected_date_format);
                    output.SetValue(col, row, parsed ? Value::DATE(*parsed) : Value());
                    break;
                }
                case DetectedType::TIMESTAMP: {
                    auto parsed = SmartCastUtils::ParseTimestamp(*processed, analysis.detected_date_format);
                    output.SetValue(col, row, parsed ? Value::TIMESTAMP(*parsed) : Value());
                    break;
                }
                case DetectedType::UUID: {
                    auto parsed = SmartCastUtils::ParseUUID(*processed);
                    output.SetValue(col, row, parsed ? Value(*parsed) : Value());
                    break;
                }
                default:
                    output.SetValue(col, row, val);
                    break;
            }
        }
    }

    output.SetCardinality(count);
}

//=============================================================================
// Registration
//=============================================================================

void RegisterSmartCastTableFunctions(ExtensionLoader &loader) {
    // stps_smart_cast_analyze
    TableFunction analyze_func("stps_smart_cast_analyze", {LogicalType::VARCHAR},
                               SmartCastAnalyzeScan, SmartCastAnalyzeBind, SmartCastAnalyzeInit);
    analyze_func.named_parameters["min_success_rate"] = LogicalType::DOUBLE;
    analyze_func.named_parameters["locale"] = LogicalType::VARCHAR;
    analyze_func.named_parameters["date_format"] = LogicalType::VARCHAR;
    loader.RegisterFunction(analyze_func);

    // stps_smart_cast
    TableFunction cast_func("stps_smart_cast", {LogicalType::VARCHAR},
                            SmartCastScan, SmartCastBind, SmartCastInit);
    cast_func.named_parameters["min_success_rate"] = LogicalType::DOUBLE;
    cast_func.named_parameters["locale"] = LogicalType::VARCHAR;
    cast_func.named_parameters["date_format"] = LogicalType::VARCHAR;
    loader.RegisterFunction(cast_func);
}

} // namespace stps
} // namespace duckdb
```

**Step 3: Commit**

```bash
git add src/smart_cast_function.cpp test/sql/smart_cast.test
git commit -m "feat(smart_cast): implement table functions"
```

---

## Task 10: Update CMakeLists.txt and Extension Entry Point

**Files:**
- Modify: `CMakeLists.txt`
- Modify: `src/stps_unified_extension.cpp`

**Step 1: Add new source files to CMakeLists.txt**

Add to `EXTENSION_SOURCES` list:

```cmake
src/smart_cast_utils.cpp
src/smart_cast_scalar.cpp
src/smart_cast_function.cpp
```

**Step 2: Update stps_unified_extension.cpp**

Add includes:

```cpp
#include "smart_cast_function.hpp"
#include "smart_cast_scalar.hpp"
```

Add registration calls in `Load()`:

```cpp
// Register smart cast functions
stps::RegisterSmartCastTableFunctions(loader);
stps::RegisterSmartCastScalarFunction(loader);
```

**Step 3: Commit**

```bash
git add CMakeLists.txt src/stps_unified_extension.cpp
git commit -m "feat(smart_cast): register smart cast functions in extension"
```

---

## Task 11: Build and Test

**Step 1: Build the extension**

```bash
make release
```

Expected: Build succeeds without errors.

**Step 2: Run tests**

```bash
./build/release/duckdb -c "LOAD 'build/release/extension/stps/stps.duckdb_extension'"
```

Then run:

```sql
-- Test scalar function
SELECT stps_smart_cast('123');
SELECT stps_smart_cast('1.234,56');
SELECT stps_smart_cast('2024-01-15');
SELECT stps_smart_cast('true');

-- Test table functions
CREATE TABLE test AS SELECT '123' as a, '45.67' as b, 'hello' as c;
SELECT * FROM stps_smart_cast_analyze('test');
SELECT * FROM stps_smart_cast('test');
```

**Step 3: Run full test suite**

```bash
make test
```

**Step 4: Commit final changes if any fixes needed**

```bash
git add -A
git commit -m "fix(smart_cast): address test failures"
```

---

## Task 12: Final Cleanup and Documentation

**Step 1: Update test file with comprehensive tests**

Ensure `test/sql/smart_cast.test` covers all edge cases from the design document.

**Step 2: Commit**

```bash
git add test/sql/smart_cast.test
git commit -m "test(smart_cast): add comprehensive test coverage"
```

---

## Summary

**Files created:**
- `src/include/smart_cast_utils.hpp` - Shared utilities header
- `src/include/smart_cast_function.hpp` - Table function header
- `src/include/smart_cast_scalar.hpp` - Scalar function header
- `src/smart_cast_utils.cpp` - Type detection and parsing implementation
- `src/smart_cast_scalar.cpp` - Scalar function implementation
- `src/smart_cast_function.cpp` - Table functions implementation
- `test/sql/smart_cast.test` - Test file

**Files modified:**
- `CMakeLists.txt` - Add new source files
- `src/stps_unified_extension.cpp` - Register new functions

**Build command:** `make release`
**Test command:** `make test`
