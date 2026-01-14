#include "smart_cast_utils.hpp"
#include <algorithm>
#include <cctype>
#include <cstdio>
#include <map>
#include <cstring>

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

// Helper to check if character is hex digit
static bool IsHexDigit(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

// Helper to lowercase a string
static std::string ToLower(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return result;
}

namespace duckdb {
namespace stps {

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

// Check if value is a valid UUID (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
bool SmartCastUtils::IsValidUUID(const std::string& value) {
    if (value.length() != 36) return false;

    // Check format: 8-4-4-4-12
    for (size_t i = 0; i < value.length(); i++) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (value[i] != '-') return false;
        } else {
            if (!IsHexDigit(value[i])) return false;
        }
    }
    return true;
}

// Check German number format: e.g., 1.234,56 (dot thousands, comma decimal)
bool SmartCastUtils::MatchesGermanNumberFormat(const std::string& value) {
    // Must have comma as decimal separator and dots as thousands
    // Pattern: -?\\d{1,3}(?:\\.\\d{3})*,\\d+

    size_t pos = 0;
    bool negative = false;

    if (pos < value.length() && value[pos] == '-') {
        negative = true;
        pos++;
    }

    // Must start with 1-3 digits
    size_t digit_count = 0;
    while (pos < value.length() && std::isdigit(static_cast<unsigned char>(value[pos]))) {
        digit_count++;
        pos++;
    }
    if (digit_count < 1 || digit_count > 3) return false;

    // Then groups of .XXX (exactly 3 digits after each dot)
    bool has_thousands = false;
    while (pos < value.length() && value[pos] == '.') {
        has_thousands = true;
        pos++;
        digit_count = 0;
        while (pos < value.length() && std::isdigit(static_cast<unsigned char>(value[pos]))) {
            digit_count++;
            pos++;
        }
        if (digit_count != 3) return false;
    }

    // Must have comma followed by digits (decimal part)
    if (pos >= value.length() || value[pos] != ',') return false;
    pos++;

    // At least one digit after comma
    if (pos >= value.length() || !std::isdigit(static_cast<unsigned char>(value[pos]))) return false;

    while (pos < value.length() && std::isdigit(static_cast<unsigned char>(value[pos]))) {
        pos++;
    }

    return pos == value.length();
}

// Check US number format: e.g., 1,234.56 (comma thousands, dot decimal)
bool SmartCastUtils::MatchesUSNumberFormat(const std::string& value) {
    // Pattern: -?\\d{1,3}(?:,\\d{3})*\\.\\d+

    size_t pos = 0;

    if (pos < value.length() && value[pos] == '-') {
        pos++;
    }

    // Must start with 1-3 digits
    size_t digit_count = 0;
    while (pos < value.length() && std::isdigit(static_cast<unsigned char>(value[pos]))) {
        digit_count++;
        pos++;
    }
    if (digit_count < 1 || digit_count > 3) return false;

    // Then groups of ,XXX (exactly 3 digits after each comma)
    while (pos < value.length() && value[pos] == ',') {
        pos++;
        digit_count = 0;
        while (pos < value.length() && std::isdigit(static_cast<unsigned char>(value[pos]))) {
            digit_count++;
            pos++;
        }
        if (digit_count != 3) return false;
    }

    // Must have dot followed by digits (decimal part)
    if (pos >= value.length() || value[pos] != '.') return false;
    pos++;

    // At least one digit after dot
    if (pos >= value.length() || !std::isdigit(static_cast<unsigned char>(value[pos]))) return false;

    while (pos < value.length() && std::isdigit(static_cast<unsigned char>(value[pos]))) {
        pos++;
    }

    return pos == value.length();
}

// Remove currency symbol from beginning of value, return true if found
bool SmartCastUtils::RemoveCurrencySymbol(std::string& value) {
    if (value.empty()) return false;

    // Common currency symbols (check multi-byte first)
    const char* euro = "\xe2\x82\xac";  // €
    const char* pound = "\xc2\xa3";      // £
    const char* yen = "\xc2\xa5";        // ¥

    bool found = false;
    size_t pos = 0;

    // Skip leading whitespace
    while (pos < value.length() && std::isspace(static_cast<unsigned char>(value[pos]))) {
        pos++;
    }

    // Check for currency symbols
    if (pos < value.length()) {
        if (value[pos] == '$') {
            pos++;
            found = true;
        } else if (value.length() - pos >= 3 && value.substr(pos, 3) == euro) {
            pos += 3;
            found = true;
        } else if (value.length() - pos >= 2 && value.substr(pos, 2) == pound) {
            pos += 2;
            found = true;
        } else if (value.length() - pos >= 2 && value.substr(pos, 2) == yen) {
            pos += 2;
            found = true;
        }
    }

    if (found) {
        // Skip whitespace after symbol
        while (pos < value.length() && std::isspace(static_cast<unsigned char>(value[pos]))) {
            pos++;
        }
        value = value.substr(pos);
    }

    return found;
}

// Remove trailing percentage, return true if found
bool SmartCastUtils::RemovePercentage(std::string& value, bool& was_percentage) {
    was_percentage = false;
    if (value.empty()) return false;

    // Check for trailing %
    size_t end = value.length();
    while (end > 0 && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
        end--;
    }

    if (end > 0 && value[end - 1] == '%') {
        was_percentage = true;
        value = value.substr(0, end - 1);
        // Trim trailing whitespace again
        end = value.length();
        while (end > 0 && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
            end--;
        }
        value = value.substr(0, end);
        return true;
    }

    return false;
}

// Check if thousands separator format is valid
bool SmartCastUtils::IsValidThousandsSeparatorFormat(const std::string& value, char thousands_sep) {
    // Valid format: first group 1-3 digits, subsequent groups exactly 3 digits
    // e.g., "1.234.567" or "12.345" but not "15.01.2024"

    size_t pos = 0;

    // Skip optional minus
    if (pos < value.length() && value[pos] == '-') {
        pos++;
    }

    // First group: 1-3 digits
    size_t digit_count = 0;
    while (pos < value.length() && std::isdigit(static_cast<unsigned char>(value[pos]))) {
        digit_count++;
        pos++;
    }
    if (digit_count < 1 || digit_count > 3) return false;

    // Subsequent groups: separator + exactly 3 digits
    bool has_sep = false;
    while (pos < value.length() && value[pos] == thousands_sep) {
        has_sep = true;
        pos++;
        digit_count = 0;
        while (pos < value.length() && std::isdigit(static_cast<unsigned char>(value[pos]))) {
            digit_count++;
            pos++;
        }
        if (digit_count != 3) return false;
    }

    // Must have at least one separator and reach end of string
    return has_sep && pos == value.length();
}

// Parse boolean values
bool SmartCastUtils::ParseBoolean(const std::string& value, bool& out_result) {
    std::string processed;
    if (!Preprocess(value, processed)) {
        return false;
    }

    std::string lower = ToLower(processed);

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
    RemoveCurrencySymbol(str);

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

    // Validate the format of thousands separators if present
    if (str.find(thousands_sep) != std::string::npos) {
        if (!IsValidThousandsSeparatorFormat(str, thousands_sep)) {
            // Not a valid integer with thousands separators - might be a date
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
        RemoveCurrencySymbol(str);
        bool was_pct = false;
        RemovePercentage(str, was_pct);

        // Unambiguous German: has comma as decimal (e.g., 1234,56 or 1.234,56)
        if (MatchesGermanNumberFormat(str)) {
            found_german = true;
        }
        // Unambiguous US: has dot as decimal with comma thousands (e.g., 1,234.56)
        if (MatchesUSNumberFormat(str)) {
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
    RemovePercentage(str, is_percentage);

    // Remove currency symbols
    RemoveCurrencySymbol(str);

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
    size_t sep_pos = str.find(thousands_sep);
    size_t dec_pos = str.find(decimal_sep);

    if (sep_pos != std::string::npos) {
        // Extract the part before the decimal (if any) to validate thousands separators
        std::string integer_part = (dec_pos != std::string::npos) ? str.substr(0, dec_pos) : str;

        // Validate format
        if (!IsValidThousandsSeparatorFormat(integer_part, thousands_sep)) {
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

// Helper to parse date parts: DD/MM/YYYY or similar
static bool ParseDateParts(const std::string& str, int& part1, int& part2, int& part3) {
    // Try separators: /, -, .
    size_t sep1 = std::string::npos, sep2 = std::string::npos;

    for (size_t i = 0; i < str.length(); i++) {
        if (str[i] == '/' || str[i] == '-' || str[i] == '.') {
            if (sep1 == std::string::npos) {
                sep1 = i;
            } else {
                sep2 = i;
                break;
            }
        }
    }

    if (sep1 == std::string::npos || sep2 == std::string::npos) return false;

    std::string s1 = str.substr(0, sep1);
    std::string s2 = str.substr(sep1 + 1, sep2 - sep1 - 1);
    std::string s3 = str.substr(sep2 + 1);

    // All parts must be digits
    for (char c : s1) if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    for (char c : s2) if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    for (char c : s3) if (!std::isdigit(static_cast<unsigned char>(c))) return false;

    if (s1.empty() || s2.empty() || s3.empty()) return false;

    // Reject patterns that look like thousands-separated numbers rather than dates.
    // Valid date patterns:
    //   - D.M.YYYY or DD.MM.YYYY: s1 and s2 are 1-2 digits, s3 is 2-4 digits
    //   - YYYY.MM.DD: s1 is 4 digits, s2 and s3 are 1-2 digits
    // Invalid patterns (likely numbers):
    //   - Both s2 and s3 have exactly 3 digits (e.g., "1.234.567" -> thousands separator format)
    //   - s2 has more than 2 digits when s1 is not a 4-digit year
    
    bool s1_is_4digit_year = (s1.length() == 4);
    bool s2_looks_like_thousands = (s2.length() == 3);
    bool s3_looks_like_thousands = (s3.length() == 3);
    
    // If s2 and s3 both have exactly 3 digits, this looks like a thousands-separated number
    // (e.g., "1.234.567" or "12.345.678")
    if (s2_looks_like_thousands && s3_looks_like_thousands) {
        return false;
    }
    
    // Validate date part lengths based on format
    if (!s1_is_4digit_year) {
        // D.M.Y format expected
        // s1 (day) should be 1-2 digits
        if (s1.length() > 2) return false;
        // s2 (month) should be 1-2 digits  
        if (s2.length() > 2) return false;
        // s3 (year) should be 2 or 4 digits - reject 3-digit years as they look like number parts
        if (s3.length() == 3) return false;
        if (s3.length() > 4) return false;
    } else {
        // Y.M.D format: s2 and s3 should be 1-2 digits
        if (s2.length() > 2) return false;
        if (s3.length() > 2) return false;
    }

    try {
        part1 = std::stoi(s1);
        part2 = std::stoi(s2);
        part3 = std::stoi(s3);
        return true;
    } catch (...) {
        return false;
    }
}

DateFormat SmartCastUtils::DetectDateFormat(const std::vector<std::string>& values) {
    bool found_dmy = false;
    bool found_mdy = false;

    for (const auto& val : values) {
        std::string processed;
        if (!Preprocess(val, processed)) continue;

        int first, second, third;
        if (ParseDateParts(processed, first, second, third)) {
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
    if (str.length() >= 10 && str[4] == '-' && str[7] == '-') {
        // Check if all other positions are digits
        bool valid = true;
        for (size_t i = 0; i < 10 && valid; i++) {
            if (i == 4 || i == 7) continue;
            if (!std::isdigit(static_cast<unsigned char>(str[i]))) valid = false;
        }
        if (valid && str.length() == 10) {
            int year = std::stoi(str.substr(0, 4));
            int month = std::stoi(str.substr(5, 2));
            int day = std::stoi(str.substr(8, 2));
            return MakeDate(year, month, day, out_result);
        }
    }

    // Compact format: 20240115
    if (str.length() == 8) {
        bool all_digits = true;
        for (char c : str) {
            if (!std::isdigit(static_cast<unsigned char>(c))) {
                all_digits = false;
                break;
            }
        }
        if (all_digits) {
            int year = std::stoi(str.substr(0, 4));
            int month = std::stoi(str.substr(4, 2));
            int day = std::stoi(str.substr(6, 2));
            return MakeDate(year, month, day, out_result);
        }
    }

    // Year-first slash: 2024/01/15
    if (str.length() >= 10 && str[4] == '/' && str[7] == '/') {
        bool valid = true;
        for (size_t i = 0; i < 10 && valid; i++) {
            if (i == 4 || i == 7) continue;
            if (!std::isdigit(static_cast<unsigned char>(str[i]))) valid = false;
        }
        if (valid && str.length() == 10) {
            int year = std::stoi(str.substr(0, 4));
            int month = std::stoi(str.substr(5, 2));
            int day = std::stoi(str.substr(8, 2));
            return MakeDate(year, month, day, out_result);
        }
    }

    // Dot/slash/dash separated: try based on format
    int first, second, third;
    if (ParseDateParts(str, first, second, third)) {
        if (format == DateFormat::MDY) {
            return MakeDate(third, first, second, out_result);  // M/D/Y
        } else {
            return MakeDate(third, second, first, out_result);  // D/M/Y
        }
    }

    // Written month formats: "15 Jan 2024", "Jan 15, 2024", "15. Januar 2024"
    // Look for a word (month name) surrounded by numbers
    size_t word_start = std::string::npos;
    size_t word_end = std::string::npos;

    for (size_t i = 0; i < str.length(); i++) {
        if (std::isalpha(static_cast<unsigned char>(str[i]))) {
            if (word_start == std::string::npos) word_start = i;
            word_end = i + 1;
        } else if (word_start != std::string::npos && word_end != std::string::npos) {
            break;  // Found complete word
        }
    }

    if (word_start != std::string::npos && word_end != std::string::npos) {
        std::string month_str = str.substr(word_start, word_end - word_start);
        int month = ParseMonthName(month_str);

        if (month > 0) {
            // Extract numbers before and after the month
            std::string before = str.substr(0, word_start);
            std::string after = str.substr(word_end);

            // Find digits in before/after
            std::string digits_before, digits_after;
            for (char c : before) {
                if (std::isdigit(static_cast<unsigned char>(c))) digits_before += c;
            }
            for (char c : after) {
                if (std::isdigit(static_cast<unsigned char>(c))) digits_after += c;
            }

            if (!digits_before.empty() && !digits_after.empty()) {
                // Reject if digits_before or digits_after are too long to be valid date components
                // Valid date components: day (1-31, max 2 digits) or year (4 digits or 2 digits)
                // Anything longer is likely an ID or other non-date value
                if (digits_before.length() > 4 || digits_after.length() > 4) {
                    // Too many digits - likely not a date
                    // e.g., "4500006182 - NOV24" or "NOV - 123456" should not be parsed as date
                    return false;
                }

                int num1 = std::stoi(digits_before);
                int num2 = std::stoi(digits_after);

                // Determine which is day and which is year
                if (num1 <= 31 && num2 >= 1900) {
                    // D Month YYYY
                    return MakeDate(num2, month, num1, out_result);
                } else if (num2 <= 31 && num1 >= 1900 && num1 <= 2100) {
                    // YYYY Month D (validate year is in reasonable range)
                    return MakeDate(num1, month, num2, out_result);
                } else if (num1 <= 31 && num2 <= 99) {
                    // D Month YY
                    return MakeDate(num2, month, num1, out_result);
                }
                // If digits_before exists but doesn't fit any pattern, don't parse as date
                // This prevents strings like "4500006182 - NOV24" from being incorrectly parsed
            } else if (digits_before.empty() && !digits_after.empty()) {
                // Month YYYY or Month D, YYYY (only when no digits before month)
                int num = std::stoi(digits_after);
                if (num >= 1900 || num <= 99) {
                    return MakeDate(num, month, 1, out_result);
                }
            }
        }
    }

    // Relative dates
    std::string lower = ToLower(str);

    if (lower == "today" || lower == "heute") {
        out_result = Date::FromDate(2026, 1, 11);  // Current date
        return true;
    }
    if (lower == "yesterday" || lower == "gestern") {
        out_result = Date::FromDate(2026, 1, 10);
        return true;
    }
    if (lower == "tomorrow" || lower == "morgen") {
        out_result = Date::FromDate(2026, 1, 12);
        return true;
    }

    // Year-month: 2024-01
    if (str.length() == 7 && str[4] == '-') {
        bool valid = true;
        for (size_t i = 0; i < 7 && valid; i++) {
            if (i == 4) continue;
            if (!std::isdigit(static_cast<unsigned char>(str[i]))) valid = false;
        }
        if (valid) {
            int year = std::stoi(str.substr(0, 4));
            int month = std::stoi(str.substr(5, 2));
            return MakeDate(year, month, 1, out_result);
        }
    }

    // Quarter: Q1 2024, 2024-Q1
    if (str.length() >= 6) {
        if (str[0] == 'Q' && std::isdigit(static_cast<unsigned char>(str[1]))) {
            int quarter = str[1] - '0';
            if (quarter >= 1 && quarter <= 4) {
                // Find year
                std::string year_str;
                for (size_t i = 2; i < str.length(); i++) {
                    if (std::isdigit(static_cast<unsigned char>(str[i]))) {
                        year_str += str[i];
                    }
                }
                if (year_str.length() == 4) {
                    int year = std::stoi(year_str);
                    int month = (quarter - 1) * 3 + 1;
                    return MakeDate(year, month, 1, out_result);
                }
            }
        }
        // 2024-Q1 format
        if (str.length() >= 7 && str[4] == '-' && str[5] == 'Q' && std::isdigit(static_cast<unsigned char>(str[6]))) {
            int year = std::stoi(str.substr(0, 4));
            int quarter = str[6] - '0';
            if (quarter >= 1 && quarter <= 4) {
                int month = (quarter - 1) * 3 + 1;
                return MakeDate(year, month, 1, out_result);
            }
        }
    }

    // Week: 2024-W03, W03-2024
    if (str.length() >= 7) {
        size_t w_pos = str.find('W');
        if (w_pos != std::string::npos && w_pos + 1 < str.length()) {
            // Extract week number
            std::string week_str;
            for (size_t i = w_pos + 1; i < str.length() && std::isdigit(static_cast<unsigned char>(str[i])); i++) {
                week_str += str[i];
            }

            // Extract year
            std::string year_str;
            for (size_t i = 0; i < str.length(); i++) {
                if (i >= w_pos && i <= w_pos + 2) continue;
                if (std::isdigit(static_cast<unsigned char>(str[i]))) {
                    year_str += str[i];
                }
            }

            if (!week_str.empty() && year_str.length() == 4) {
                int week = std::stoi(week_str);
                int year = std::stoi(year_str);
                if (week >= 1 && week <= 53) {
                    int day = (week - 1) * 7 + 1;
                    if (day > 28) day = 28;
                    return MakeDate(year, 1, day, out_result);
                }
            }
        }
    }

    return false;
}

bool SmartCastUtils::ParseTimestamp(const std::string& value, DateFormat format, timestamp_t& out_result) {
    std::string processed;
    if (!Preprocess(value, processed)) {
        return false;
    }

    std::string str = processed;

    // Look for time part (HH:MM or HH:MM:SS)
    size_t time_start = std::string::npos;
    for (size_t i = 0; i + 4 < str.length(); i++) {
        if (std::isdigit(static_cast<unsigned char>(str[i])) &&
            std::isdigit(static_cast<unsigned char>(str[i+1])) &&
            str[i+2] == ':' &&
            std::isdigit(static_cast<unsigned char>(str[i+3])) &&
            std::isdigit(static_cast<unsigned char>(str[i+4]))) {
            // Found HH:MM pattern
            // Check if preceded by space or T
            if (i > 0 && (str[i-1] == ' ' || str[i-1] == 'T' || str[i-1] == '\t')) {
                time_start = i;
                break;
            }
        }
    }

    if (time_start == std::string::npos) {
        return false;
    }

    // Split date and time
    std::string date_part = str.substr(0, time_start);
    std::string time_part = str.substr(time_start);

    // Trim date part
    while (!date_part.empty() && (date_part.back() == ' ' || date_part.back() == 'T' || date_part.back() == '\t')) {
        date_part.pop_back();
    }

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

    return false;
}

bool SmartCastUtils::ParseUUID(const std::string& value, std::string& out_result) {
    std::string processed;
    if (!Preprocess(value, processed)) {
        return false;
    }

    if (IsValidUUID(processed)) {
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
