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
