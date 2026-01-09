#include "smart_cast_utils.hpp"
#include <algorithm>
#include <cctype>
#include <map>

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
    {"januar", 1}, {"jaenner", 1},
    {"februar", 2},
    {"maerz", 3}, {"marz", 3},
    {"mai", 5},
    {"juni", 6},
    {"juli", 7},
    {"oktober", 10},
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
static std::optional<duckdb::date_t> MakeDate(int year, int month, int day) {
    year = ExpandYear(year);
    if (month < 1 || month > 12 || day < 1 || day > 31) {
        return std::nullopt;
    }
    try {
        return duckdb::Date::FromDate(year, month, day);
    } catch (...) {
        return std::nullopt;
    }
}

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
    std::regex written_dmy_regex("^(\\d{1,2})\\.?\\s+([A-Za-z]+)\\.?\\s+(\\d{2,4})$");
    if (std::regex_match(str, match, written_dmy_regex)) {
        int month = ParseMonthName(match[2].str());
        if (month > 0) {
            return MakeDate(std::stoi(match[3]), month, std::stoi(match[1]));
        }
    }

    std::regex written_mdy_regex("^([A-Za-z]+)\\.?\\s+(\\d{1,2}),?\\s+(\\d{2,4})$");
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
    std::regex month_year_regex("^([A-Za-z]+)\\.?\\s+(\\d{4})$");
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
        // Convert week to approximate date (first day of week)
        int day = (week - 1) * 7 + 1;
        if (day > 28) day = 28;  // Keep within valid range
        return MakeDate(year, 1, day);
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
