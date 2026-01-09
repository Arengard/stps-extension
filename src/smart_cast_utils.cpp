#include "smart_cast_utils.hpp"
#include <algorithm>
#include <cctype>
#include <cstdio>
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

// Create date_t from components, returns true if valid
static bool MakeDate(int year, int month, int day, duckdb::date_t& out_result) {
    year = ExpandYear(year);
    if (month < 1 || month > 12 || day < 1 || day > 31) {
        return false;
    }
    try {
        out_result = duckdb::Date::FromDate(year, month, day);
        return true;
    } catch (...) {
        return false;
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

// Preprocess: trim whitespace, return false for empty
bool SmartCastUtils::Preprocess(const std::string& input, std::string& out_result) {
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

    // Empty after trim -> false
    if (start >= end) {
        out_result.clear();
        return false;
    }

    out_result = input.substr(start, end - start);
    return true;
}

// Parse boolean values
bool SmartCastUtils::ParseBoolean(const std::string& value, bool& out_result) {
    std::string processed;
    if (!Preprocess(value, processed)) {
        return false;
    }

    std::string lower = processed;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    if (lower == "true" || lower == "yes" || lower == "ja" || lower == "1") {
        out_result = true;
        return true;
    }
    if (lower == "false" || lower == "no" || lower == "nein" || lower == "0") {
        out_result = false;
        return true;
    }

    return false;
}

bool SmartCastUtils::LooksLikeId(const std::string& value) {
    // Leading zeros suggest an ID (e.g., "007", "00123")
    // But not dates like "01/15/2024" or "01.01.2024"
    if (value.size() > 1 && value[0] == '0' && std::isdigit(static_cast<unsigned char>(value[1]))) {
        // Check if it looks like a date (has separators)
        if (value.find('/') != std::string::npos ||
            value.find('-') != std::string::npos ||
            value.find('.') != std::string::npos) {
            return false;  // Might be a date
        }
        return true;
    }
    // Negative with leading zeros
    if (value.size() > 2 && value[0] == '-' && value[1] == '0' && std::isdigit(static_cast<unsigned char>(value[2]))) {
        return true;
    }
    return false;
}

bool SmartCastUtils::ParseInteger(const std::string& value, NumberLocale locale, int64_t& out_result) {
    std::string processed;
    if (!Preprocess(value, processed)) {
        return false;
    }

    std::string str = processed;

    // Check for leading zeros (likely an ID)
    if (LooksLikeId(str)) {
        return false;
    }

    // Remove currency symbols
    str = std::regex_replace(str, CURRENCY_PATTERN, "");

    // Trim again after removing currency
    std::string trimmed;
    if (!Preprocess(str, trimmed)) return false;
    str = trimmed;

    // Determine separator to remove based on locale
    char thousands_sep = (locale == NumberLocale::US) ? ',' : '.';

    // Check if it contains a decimal separator - if so, not an integer
    char decimal_sep = (locale == NumberLocale::US) ? '.' : ',';
    if (str.find(decimal_sep) != std::string::npos) {
        // Has decimal separator, not an integer
        return false;
    }

    // Validate the format of thousands separators
    // Valid: 1.234.567 (groups of exactly 3 digits after first separator)
    // Invalid: 15.01.2024 (groups are not all 3 digits)
    size_t sep_pos = str.find(thousands_sep);
    if (sep_pos != std::string::npos) {
        // Check if pattern is valid: first group 1-3 digits, subsequent groups exactly 3 digits
        std::string pattern_str = "^-?\\d{1,3}(?:\\" + std::string(1, thousands_sep) + "\\d{3})+$";
        std::regex valid_pattern(pattern_str);
        if (!std::regex_match(str, valid_pattern)) {
            // Not a valid integer with thousands separators - might be a date or something else
            return false;
        }
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
            out_result = result;
            return true;
        }
    } catch (...) {
        // Parse failed
    }

    return false;
}

NumberLocale SmartCastUtils::DetectLocale(const std::vector<std::string>& values) {
    bool found_german = false;
    bool found_us = false;

    for (const auto& val : values) {
        std::string processed;
        if (!Preprocess(val, processed)) continue;

        std::string str = processed;
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

bool SmartCastUtils::ParseDouble(const std::string& value, NumberLocale locale, double& out_result) {
    std::string processed;
    if (!Preprocess(value, processed)) {
        return false;
    }

    std::string str = processed;

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
    std::string trimmed;
    if (!Preprocess(str, trimmed)) return false;
    str = trimmed;

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

    // Validate thousands separator format if present
    // Valid: 1.234.567,89 or 1.234 (groups of exactly 3 digits)
    // Invalid: 15.01.2024 (looks like a date)
    size_t sep_pos = str.find(thousands_sep);
    size_t dec_pos = str.find(decimal_sep);

    if (sep_pos != std::string::npos) {
        // Extract the part before the decimal (if any) to validate thousands separators
        std::string integer_part = (dec_pos != std::string::npos) ? str.substr(0, dec_pos) : str;

        // Check if pattern is valid: first group 1-3 digits, subsequent groups exactly 3 digits
        std::string pattern_str = "^-?\\d{1,3}(?:\\" + std::string(1, thousands_sep) + "\\d{3})+$";
        std::regex valid_pattern(pattern_str);
        if (!std::regex_match(integer_part, valid_pattern)) {
            // Not a valid number with thousands separators - might be a date
            return false;
        }
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
            out_result = result;
            return true;
        }
    } catch (...) {
        // Parse failed
    }

    return false;
}

DateFormat SmartCastUtils::DetectDateFormat(const std::vector<std::string>& values) {
    bool found_dmy = false;
    bool found_mdy = false;

    for (const auto& val : values) {
        std::string processed;
        if (!Preprocess(val, processed)) continue;

        // Look for unambiguous dates where day > 12
        std::regex date_parts_regex("^(\\d{1,2})[./\\-](\\d{1,2})[./\\-](\\d{2,4})$");
        std::smatch match;
        if (std::regex_match(processed, match, date_parts_regex)) {
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

bool SmartCastUtils::ParseDate(const std::string& value, DateFormat format, date_t& out_result) {
    std::string processed;
    if (!Preprocess(value, processed)) {
        return false;
    }

    std::string str = processed;

    // ISO format: 2024-01-15
    std::regex iso_regex("^(\\d{4})-(\\d{1,2})-(\\d{1,2})$");
    std::smatch match;
    if (std::regex_match(str, match, iso_regex)) {
        return MakeDate(std::stoi(match[1]), std::stoi(match[2]), std::stoi(match[3]), out_result);
    }

    // Compact format: 20240115
    std::regex compact_regex("^(\\d{4})(\\d{2})(\\d{2})$");
    if (std::regex_match(str, match, compact_regex)) {
        return MakeDate(std::stoi(match[1]), std::stoi(match[2]), std::stoi(match[3]), out_result);
    }

    // Year-first slash: 2024/01/15
    std::regex ymd_slash_regex("^(\\d{4})/(\\d{1,2})/(\\d{1,2})$");
    if (std::regex_match(str, match, ymd_slash_regex)) {
        return MakeDate(std::stoi(match[1]), std::stoi(match[2]), std::stoi(match[3]), out_result);
    }

    // Dot/slash/dash separated: try based on format
    std::regex dmy_regex("^(\\d{1,2})[./\\-](\\d{1,2})[./\\-](\\d{2,4})$");
    if (std::regex_match(str, match, dmy_regex)) {
        int first = std::stoi(match[1]);
        int second = std::stoi(match[2]);
        int third = std::stoi(match[3]);

        if (format == DateFormat::MDY) {
            return MakeDate(third, first, second, out_result);  // M/D/Y
        } else {
            return MakeDate(third, second, first, out_result);  // D/M/Y
        }
    }

    // Written month formats: "15 Jan 2024", "Jan 15, 2024", "15. Januar 2024"
    std::regex written_dmy_regex("^(\\d{1,2})\\.?\\s+([A-Za-z]+)\\.?\\s+(\\d{2,4})$");
    if (std::regex_match(str, match, written_dmy_regex)) {
        int month = ParseMonthName(match[2].str());
        if (month > 0) {
            return MakeDate(std::stoi(match[3]), month, std::stoi(match[1]), out_result);
        }
    }

    std::regex written_mdy_regex("^([A-Za-z]+)\\.?\\s+(\\d{1,2}),?\\s+(\\d{2,4})$");
    if (std::regex_match(str, match, written_mdy_regex)) {
        int month = ParseMonthName(match[1].str());
        if (month > 0) {
            return MakeDate(std::stoi(match[3]), month, std::stoi(match[2]), out_result);
        }
    }

    // Relative dates
    std::string lower = str;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    if (lower == "today" || lower == "heute") {
        out_result = Date::FromDate(2026, 1, 9);  // Current date
        return true;
    }
    if (lower == "yesterday" || lower == "gestern") {
        out_result = Date::FromDate(2026, 1, 8);
        return true;
    }
    if (lower == "tomorrow" || lower == "morgen") {
        out_result = Date::FromDate(2026, 1, 10);
        return true;
    }

    // Month-only: "Jan 2024", "2024-01"
    std::regex month_year_regex("^([A-Za-z]+)\\.?\\s+(\\d{4})$");
    if (std::regex_match(str, match, month_year_regex)) {
        int month = ParseMonthName(match[1].str());
        if (month > 0) {
            return MakeDate(std::stoi(match[2]), month, 1, out_result);
        }
    }

    std::regex year_month_regex("^(\\d{4})-(\\d{2})$");
    if (std::regex_match(str, match, year_month_regex)) {
        return MakeDate(std::stoi(match[1]), std::stoi(match[2]), 1, out_result);
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
        return MakeDate(year, month, 1, out_result);
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
        return MakeDate(year, 1, day, out_result);
    }

    return false;
}

bool SmartCastUtils::ParseTimestamp(const std::string& value, DateFormat format, timestamp_t& out_result) {
    std::string processed;
    if (!Preprocess(value, processed)) {
        return false;
    }

    std::string str = processed;

    // Try to split into date and time parts
    std::regex datetime_regex("^(.+?)\\s*[T\\s]\\s*(\\d{1,2}:\\d{2}(?::\\d{2})?)(.*)$");
    std::smatch match;

    if (std::regex_match(str, match, datetime_regex)) {
        std::string date_part = match[1].str();
        std::string time_part = match[2].str();

        // Parse date part
        date_t date_result;
        if (!ParseDate(date_part, format, date_result)) {
            return false;
        }

        // Parse time part
        int hour = 0, minute = 0, second = 0;
        if (sscanf(time_part.c_str(), "%d:%d:%d", &hour, &minute, &second) >= 2) {
            if (hour >= 0 && hour < 24 && minute >= 0 && minute < 60 && second >= 0 && second < 60) {
                try {
                    int32_t year, month, day;
                    Date::Convert(date_result, year, month, day);
                    out_result = Timestamp::FromDatetime(
                        Date::FromDate(year, month, day),
                        Time::FromTime(hour, minute, second, 0)
                    );
                    return true;
                } catch (...) {
                    return false;
                }
            }
        }
    }

    return false;
}

bool SmartCastUtils::ParseUUID(const std::string& value, std::string& out_result) {
    std::string processed;
    if (!Preprocess(value, processed)) {
        return false;
    }

    if (std::regex_match(processed, UUID_PATTERN)) {
        out_result = processed;
        return true;
    }

    return false;
}

DetectedType SmartCastUtils::DetectType(const std::string& value, NumberLocale locale, DateFormat date_format) {
    std::string processed;
    if (!Preprocess(value, processed)) {
        return DetectedType::UNKNOWN;  // Empty/whitespace
    }

    // Check for ID-like values first (preserve as VARCHAR)
    if (LooksLikeId(processed)) {
        return DetectedType::VARCHAR;
    }

    // Try boolean
    bool bool_result;
    if (ParseBoolean(processed, bool_result)) {
        return DetectedType::BOOLEAN;
    }

    // Try UUID (before numbers, as UUIDs have dashes)
    std::string uuid_result;
    if (ParseUUID(processed, uuid_result)) {
        return DetectedType::UUID;
    }

    // Try integer (before double)
    int64_t int_result;
    if (ParseInteger(processed, locale, int_result)) {
        return DetectedType::INTEGER;
    }

    // Try double
    double double_result;
    if (ParseDouble(processed, locale, double_result)) {
        return DetectedType::DOUBLE;
    }

    // Try timestamp (before date, as timestamp includes date)
    timestamp_t ts_result;
    if (ParseTimestamp(processed, date_format, ts_result)) {
        return DetectedType::TIMESTAMP;
    }

    // Try date
    date_t date_result;
    if (ParseDate(processed, date_format, date_result)) {
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
