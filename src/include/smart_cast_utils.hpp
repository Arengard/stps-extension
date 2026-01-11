#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/time.hpp"
#include <string>
#include <vector>

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

// Smart cast utilities - Windows-safe implementation without static regex objects
class SmartCastUtils {
public:
    // Preprocess string (trim, empty -> returns false with empty out_result)
    // Returns true if value is valid (non-empty after trim), false otherwise
    static bool Preprocess(const std::string& input, std::string& out_result);

    // Detect locale from a vector of values
    static NumberLocale DetectLocale(const std::vector<std::string>& values);

    // Detect date format from a vector of values
    static DateFormat DetectDateFormat(const std::vector<std::string>& values);

    // Detect type of a single value
    static DetectedType DetectType(const std::string& value, NumberLocale locale = NumberLocale::AUTO,
                                    DateFormat date_format = DateFormat::AUTO);

    // Parse functions - return true if parse succeeds, false otherwise
    static bool ParseBoolean(const std::string& value, bool& out_result);
    static bool ParseInteger(const std::string& value, NumberLocale locale, int64_t& out_result);
    static bool ParseDouble(const std::string& value, NumberLocale locale, double& out_result);
    static bool ParseDate(const std::string& value, DateFormat format, date_t& out_result);
    static bool ParseTimestamp(const std::string& value, DateFormat format, timestamp_t& out_result);
    static bool ParseUUID(const std::string& value, std::string& out_result);

    // Convert DetectedType to LogicalType
    static LogicalType ToLogicalType(DetectedType type);

    // Convert string to DetectedType enum
    static DetectedType StringToDetectedType(const std::string& type_str);

    // Check if string looks like an ID (leading zeros, etc.)
    static bool LooksLikeId(const std::string& value);

private:
    // Helper functions for manual parsing (Windows-safe, no static regex)
    static bool IsValidUUID(const std::string& value);
    static bool MatchesGermanNumberFormat(const std::string& value);
    static bool MatchesUSNumberFormat(const std::string& value);
    static bool RemoveCurrencySymbol(std::string& value);
    static bool RemovePercentage(std::string& value, bool& was_percentage);
    static bool IsValidThousandsSeparatorFormat(const std::string& value, char thousands_sep);
};

} // namespace stps
} // namespace duckdb
