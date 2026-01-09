#include "smart_cast_scalar.hpp"
#include "smart_cast_utils.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

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
        std::string processed;
        if (!SmartCastUtils::Preprocess(str, processed)) {
            result_validity.SetInvalid(i);
            continue;
        }

        DetectedType type = SmartCastUtils::DetectType(processed, locale, date_format);

        // Cast and return string representation
        switch (type) {
            case DetectedType::BOOLEAN: {
                bool val;
                if (SmartCastUtils::ParseBoolean(processed, val)) {
                    result_data[i] = StringVector::AddString(result, val ? "true" : "false");
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::INTEGER: {
                int64_t val;
                if (SmartCastUtils::ParseInteger(processed, locale, val)) {
                    result_data[i] = StringVector::AddString(result, std::to_string(val));
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::DOUBLE: {
                double val;
                if (SmartCastUtils::ParseDouble(processed, locale, val)) {
                    result_data[i] = StringVector::AddString(result, std::to_string(val));
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::DATE: {
                date_t val;
                if (SmartCastUtils::ParseDate(processed, date_format, val)) {
                    result_data[i] = StringVector::AddString(result, Date::ToString(val));
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::TIMESTAMP: {
                timestamp_t val;
                if (SmartCastUtils::ParseTimestamp(processed, date_format, val)) {
                    result_data[i] = StringVector::AddString(result, Timestamp::ToString(val));
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::UUID: {
                std::string val;
                if (SmartCastUtils::ParseUUID(processed, val)) {
                    result_data[i] = StringVector::AddString(result, val);
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            default:
                result_data[i] = StringVector::AddString(result, processed);
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
        std::string processed;
        if (!SmartCastUtils::Preprocess(str, processed)) {
            result_validity.SetInvalid(i);
            continue;
        }

        std::string type_str = type_strings[type_idx].GetString();
        DetectedType target_type = SmartCastUtils::StringToDetectedType(type_str);

        // Cast to target type
        switch (target_type) {
            case DetectedType::BOOLEAN: {
                bool val;
                if (SmartCastUtils::ParseBoolean(processed, val)) {
                    result_data[i] = StringVector::AddString(result, val ? "true" : "false");
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::INTEGER: {
                int64_t val;
                if (SmartCastUtils::ParseInteger(processed, locale, val)) {
                    result_data[i] = StringVector::AddString(result, std::to_string(val));
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::DOUBLE: {
                double val;
                if (SmartCastUtils::ParseDouble(processed, locale, val)) {
                    result_data[i] = StringVector::AddString(result, std::to_string(val));
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::DATE: {
                date_t val;
                if (SmartCastUtils::ParseDate(processed, date_format, val)) {
                    result_data[i] = StringVector::AddString(result, Date::ToString(val));
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::TIMESTAMP: {
                timestamp_t val;
                if (SmartCastUtils::ParseTimestamp(processed, date_format, val)) {
                    result_data[i] = StringVector::AddString(result, Timestamp::ToString(val));
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            case DetectedType::UUID: {
                std::string val;
                if (SmartCastUtils::ParseUUID(processed, val)) {
                    result_data[i] = StringVector::AddString(result, val);
                } else {
                    result_validity.SetInvalid(i);
                }
                break;
            }
            default:
                result_data[i] = StringVector::AddString(result, processed);
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
