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
