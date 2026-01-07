#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "kontocheck/check_methods.hpp"

namespace duckdb {
namespace stps {

// stps_validate_account_number(account_number VARCHAR, method_id INTEGER, blz VARCHAR DEFAULT '') -> BOOLEAN
static void ValidateAccountNumberFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &account_vector = args.data[0];
    auto &method_vector = args.data[1];

    // BLZ parameter is optional
    bool has_blz = args.data.size() > 2;

    UnifiedVectorFormat account_data;
    UnifiedVectorFormat method_data;
    UnifiedVectorFormat blz_data;

    account_vector.ToUnifiedFormat(args.size(), account_data);
    method_vector.ToUnifiedFormat(args.size(), method_data);

    if (has_blz) {
        auto &blz_vector = args.data[2];
        blz_vector.ToUnifiedFormat(args.size(), blz_data);
    }

    auto account_ptr = UnifiedVectorFormat::GetData<string_t>(account_data);
    auto method_ptr = UnifiedVectorFormat::GetData<int32_t>(method_data);
    auto blz_ptr = has_blz ? UnifiedVectorFormat::GetData<string_t>(blz_data) : nullptr;
    auto result_data = FlatVector::GetData<bool>(result);
    auto &result_validity = FlatVector::Validity(result);

    for (idx_t i = 0; i < args.size(); i++) {
        auto account_idx = account_data.sel->get_index(i);
        auto method_idx = method_data.sel->get_index(i);

        if (!account_data.validity.RowIsValid(account_idx) ||
            !method_data.validity.RowIsValid(method_idx)) {
            result_validity.SetInvalid(i);
            continue;
        }

        string account_str = account_ptr[account_idx].GetString();
        int32_t method_id = method_ptr[method_idx];

        string blz_str = "";
        if (has_blz) {
            auto blz_idx = blz_data.sel->get_index(i);
            if (blz_data.validity.RowIsValid(blz_idx)) {
                blz_str = blz_ptr[blz_idx].GetString();
            }
        }

        // Validate method_id range (0x00-0xC6)
        if (method_id < 0 || method_id > 0xC6) {
            result_validity.SetInvalid(i);
            continue;
        }

        auto check_result = kontocheck::CheckMethods::ValidateAccount(
            account_str,
            static_cast<uint8_t>(method_id),
            blz_str
        );

        // Return true only if validation succeeded (OK)
        result_data[i] = (check_result == kontocheck::CheckResult::OK);
    }
}

// stps_validate_account_result(account_number VARCHAR, method_id INTEGER, blz VARCHAR DEFAULT '') -> VARCHAR
// Returns: 'OK', 'FALSE', 'INVALID_KTO', 'NOT_IMPLEMENTED'
static void ValidateAccountResultFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &account_vector = args.data[0];
    auto &method_vector = args.data[1];

    // BLZ parameter is optional
    bool has_blz = args.data.size() > 2;

    UnifiedVectorFormat account_data;
    UnifiedVectorFormat method_data;
    UnifiedVectorFormat blz_data;

    account_vector.ToUnifiedFormat(args.size(), account_data);
    method_vector.ToUnifiedFormat(args.size(), method_data);

    if (has_blz) {
        auto &blz_vector = args.data[2];
        blz_vector.ToUnifiedFormat(args.size(), blz_data);
    }

    auto account_ptr = UnifiedVectorFormat::GetData<string_t>(account_data);
    auto method_ptr = UnifiedVectorFormat::GetData<int32_t>(method_data);
    auto blz_ptr = has_blz ? UnifiedVectorFormat::GetData<string_t>(blz_data) : nullptr;
    auto result_data = FlatVector::GetData<string_t>(result);
    auto &result_validity = FlatVector::Validity(result);

    for (idx_t i = 0; i < args.size(); i++) {
        auto account_idx = account_data.sel->get_index(i);
        auto method_idx = method_data.sel->get_index(i);

        if (!account_data.validity.RowIsValid(account_idx) ||
            !method_data.validity.RowIsValid(method_idx)) {
            result_validity.SetInvalid(i);
            continue;
        }

        string account_str = account_ptr[account_idx].GetString();
        int32_t method_id = method_ptr[method_idx];

        string blz_str = "";
        if (has_blz) {
            auto blz_idx = blz_data.sel->get_index(i);
            if (blz_data.validity.RowIsValid(blz_idx)) {
                blz_str = blz_ptr[blz_idx].GetString();
            }
        }

        // Validate method_id range
        if (method_id < 0 || method_id > 0xC6) {
            result_data[i] = StringVector::AddString(result, "INVALID_METHOD");
            continue;
        }

        auto check_result = kontocheck::CheckMethods::ValidateAccount(
            account_str,
            static_cast<uint8_t>(method_id),
            blz_str
        );

        // Convert result to string
        const char* result_str;
        switch (check_result) {
            case kontocheck::CheckResult::OK:
                result_str = "OK";
                break;
            case kontocheck::CheckResult::FALSE:
                result_str = "FALSE";
                break;
            case kontocheck::CheckResult::INVALID_KTO:
                result_str = "INVALID_KTO";
                break;
            case kontocheck::CheckResult::NOT_IMPLEMENTED:
                result_str = "NOT_IMPLEMENTED";
                break;
            default:
                result_str = "UNKNOWN";
                break;
        }

        result_data[i] = StringVector::AddString(result, result_str);
    }
}

void RegisterAccountValidationFunctions(ExtensionLoader &loader) {
    // stps_validate_account_number - returns boolean
    ScalarFunctionSet validate_account_set("stps_validate_account_number");

    // With BLZ parameter
    validate_account_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR},
        LogicalType::BOOLEAN,
        ValidateAccountNumberFunction
    ));

    // Without BLZ parameter (default empty string)
    validate_account_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::INTEGER},
        LogicalType::BOOLEAN,
        ValidateAccountNumberFunction
    ));

    loader.RegisterFunction(validate_account_set);

    // stps_validate_account_result - returns string result
    ScalarFunctionSet validate_result_set("stps_validate_account_result");

    // With BLZ parameter
    validate_result_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        ValidateAccountResultFunction
    ));

    // Without BLZ parameter
    validate_result_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::INTEGER},
        LogicalType::VARCHAR,
        ValidateAccountResultFunction
    ));

    loader.RegisterFunction(validate_result_set);
}

} // namespace stps
} // namespace duckdb
