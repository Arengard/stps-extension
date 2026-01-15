#include "iban_validation.hpp"
#include "kontocheck/check_methods.hpp"
#include "blz_lut_loader.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include <string>
#include <algorithm>
#include <cctype>
#include <unordered_map>

namespace duckdb {
namespace stps {

// IBAN country code lengths (ISO 13616)
static const std::unordered_map<std::string, int> IBAN_LENGTHS = {
    {"AD", 24}, {"AE", 23}, {"AL", 28}, {"AT", 20}, {"AZ", 28},
    {"BA", 20}, {"BE", 16}, {"BG", 22}, {"BH", 22}, {"BR", 29},
    {"BY", 28}, {"CH", 21}, {"CR", 22}, {"CY", 28}, {"CZ", 24},
    {"DE", 22}, {"DK", 18}, {"DO", 28}, {"EE", 20}, {"EG", 29},
    {"ES", 24}, {"FI", 18}, {"FO", 18}, {"FR", 27}, {"GB", 22},
    {"GE", 22}, {"GI", 23}, {"GL", 18}, {"GR", 27}, {"GT", 28},
    {"HR", 21}, {"HU", 28}, {"IE", 22}, {"IL", 23}, {"IS", 26},
    {"IT", 27}, {"JO", 30}, {"KW", 30}, {"KZ", 20}, {"LB", 28},
    {"LC", 32}, {"LI", 21}, {"LT", 20}, {"LU", 20}, {"LV", 21},
    {"MC", 27}, {"MD", 24}, {"ME", 22}, {"MK", 19}, {"MR", 27},
    {"MT", 31}, {"MU", 30}, {"NL", 18}, {"NO", 15}, {"PK", 24},
    {"PL", 28}, {"PS", 29}, {"PT", 25}, {"QA", 29}, {"RO", 24},
    {"RS", 22}, {"SA", 24}, {"SE", 24}, {"SI", 19}, {"SK", 24},
    {"SM", 27}, {"TN", 24}, {"TR", 26}, {"UA", 29}, {"VA", 22},
    {"VG", 24}, {"XK", 20}
};

// Helper function to perform mod97 operation for large numbers
std::string mod97(const std::string& number) {
    std::string remainder = "";

    for (char digit : number) {
        remainder += digit;

        // Process when we have enough digits
        if (remainder.length() >= 9) {
            long long num = std::stoll(remainder);
            remainder = std::to_string(num % 97);
        }
    }

    // Final mod97
    if (!remainder.empty()) {
        long long num = std::stoll(remainder);
        return std::to_string(num % 97);
    }

    return "0";
}

// Validate IBAN using the standard algorithm
bool validate_iban(const std::string& iban) {
    // Remove spaces and convert to uppercase
    std::string cleaned;
    for (char c : iban) {
        if (!std::isspace(c)) {
            cleaned += std::toupper(c);
        }
    }

    // Check minimum length (at least 15 characters: 2 country + 2 check + min 11 account)
    if (cleaned.length() < 15) {
        return false;
    }

    // Check if it starts with 2 letters (country code)
    if (!std::isalpha(cleaned[0]) || !std::isalpha(cleaned[1])) {
        return false;
    }

    // Check if positions 2-3 are digits (check digits)
    if (!std::isdigit(cleaned[2]) || !std::isdigit(cleaned[3])) {
        return false;
    }

    // Extract country code
    std::string country_code = cleaned.substr(0, 2);

    // Check if country code is valid and length matches
    auto it = IBAN_LENGTHS.find(country_code);
    if (it == IBAN_LENGTHS.end()) {
        return false;  // Unknown country code
    }

    if (cleaned.length() != static_cast<size_t>(it->second)) {
        return false;  // Wrong length for this country
    }

    // Rearrange: Move first 4 characters to the end
    std::string rearranged = cleaned.substr(4) + cleaned.substr(0, 4);

    // Convert letters to numbers (A=10, B=11, ..., Z=35)
    std::string numeric;
    for (char c : rearranged) {
        if (std::isdigit(c)) {
            numeric += c;
        } else if (std::isalpha(c)) {
            numeric += std::to_string(c - 'A' + 10);
        } else {
            return false;  // Invalid character
        }
    }

    // Calculate mod97 - should be 1 for valid IBAN
    std::string mod_result = mod97(numeric);
    if (mod_result != "1") {
        return false;
    }

    // Additional validation for German IBANs using kontocheck
    // Automatic lookup of check method from BLZ LUT file
    if (country_code == "DE" && cleaned.length() == 22) {
        std::string bban = cleaned.substr(4);
        if (bban.length() == 18) {
            std::string blz = bban.substr(0, 8);
            std::string account = bban.substr(8, 10);

            // Look up check method from BLZ LUT
            uint8_t method_id;
            if (BlzLutLoader::GetInstance().LookupCheckMethod(blz, method_id)) {
                // Found method - validate account number
                auto check_result = kontocheck::CheckMethods::ValidateAccount(
                    account, method_id, blz);

                if (check_result != kontocheck::CheckResult::OK) {
                    return false;  // Invalid account number
                }
            }
            // If BLZ not found in LUT, continue (MOD-97 already passed)
        }
    }

    return true;
}

// Helper function to format IBAN with spaces (every 4 characters)
std::string format_iban(const std::string& iban) {
    // Remove all spaces first
    std::string cleaned;
    for (char c : iban) {
        if (!std::isspace(c)) {
            cleaned += std::toupper(c);
        }
    }

    // Add space every 4 characters
    std::string formatted;
    for (size_t i = 0; i < cleaned.length(); i++) {
        if (i > 0 && i % 4 == 0) {
            formatted += ' ';
        }
        formatted += cleaned[i];
    }

    return formatted;
}

// Helper function to get country code from IBAN
std::string get_country_code(const std::string& iban) {
    // Remove spaces and convert to uppercase
    std::string cleaned;
    for (char c : iban) {
        if (!std::isspace(c)) {
            cleaned += std::toupper(c);
        }
    }

    // Country code is first 2 characters
    if (cleaned.length() >= 2 && std::isalpha(cleaned[0]) && std::isalpha(cleaned[1])) {
        return cleaned.substr(0, 2);
    }

    return "";
}

// Helper function to get check digits from IBAN
std::string get_check_digits(const std::string& iban) {
    // Remove spaces and convert to uppercase
    std::string cleaned;
    for (char c : iban) {
        if (!std::isspace(c)) {
            cleaned += std::toupper(c);
        }
    }

    // Check digits are characters 3-4 (positions 2-3)
    if (cleaned.length() >= 4 && std::isdigit(cleaned[2]) && std::isdigit(cleaned[3])) {
        return cleaned.substr(2, 2);
    }

    return "";
}

// Helper function to get BBAN (Basic Bank Account Number) from IBAN
std::string get_bban(const std::string& iban) {
    // Remove spaces and convert to uppercase
    std::string cleaned;
    for (char c : iban) {
        if (!std::isspace(c)) {
            cleaned += std::toupper(c);
        }
    }

    // BBAN is everything after the first 4 characters (country code + check digits)
    if (cleaned.length() > 4) {
        return cleaned.substr(4);
    }

    return "";
}

// Simple test function
static void StpsIbanTestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            return StringVector::AddString(result, "TEST_OK");
        });
}

// Validate German IBAN with kontocheck (when method is known)
bool validate_german_iban_with_kontocheck(const std::string& iban, uint8_t method_id) {
    // First do standard IBAN validation
    if (!validate_iban(iban)) {
        return false;
    }

    // Remove spaces and convert to uppercase
    std::string cleaned;
    for (char c : iban) {
        if (!std::isspace(c)) {
            cleaned += std::toupper(c);
        }
    }

    // Check if it's a German IBAN
    if (cleaned.length() != 22 || cleaned.substr(0, 2) != "DE") {
        return false;  // Not a German IBAN
    }

    // Extract BLZ and account number from BBAN
    std::string bban = cleaned.substr(4);  // Skip "DE" + check digits
    if (bban.length() != 18) {
        return false;
    }

    std::string blz = bban.substr(0, 8);
    std::string account = bban.substr(8, 10);

    // Validate account number using kontocheck
    auto check_result = kontocheck::CheckMethods::ValidateAccount(
        account,
        method_id,
        blz
    );

    return (check_result == kontocheck::CheckResult::OK);
}

// DuckDB scalar function wrapper
static void StpsIsValidIbanFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, bool>(
        args.data[0], result, args.size(),
        [&](string_t iban) {
            std::string iban_str = iban.GetString();
            return validate_iban(iban_str);
        });
}

// Validate German IBAN with kontocheck - requires method ID
static void StpsIsValidGermanIbanFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &iban_vector = args.data[0];
    auto &method_vector = args.data[1];

    UnifiedVectorFormat iban_data;
    UnifiedVectorFormat method_data;

    iban_vector.ToUnifiedFormat(args.size(), iban_data);
    method_vector.ToUnifiedFormat(args.size(), method_data);

    auto iban_ptr = UnifiedVectorFormat::GetData<string_t>(iban_data);
    auto method_ptr = UnifiedVectorFormat::GetData<int32_t>(method_data);
    auto result_data = FlatVector::GetData<bool>(result);
    auto &result_validity = FlatVector::Validity(result);

    for (idx_t i = 0; i < args.size(); i++) {
        auto iban_idx = iban_data.sel->get_index(i);
        auto method_idx = method_data.sel->get_index(i);

        if (!iban_data.validity.RowIsValid(iban_idx) ||
            !method_data.validity.RowIsValid(method_idx)) {
            result_validity.SetInvalid(i);
            continue;
        }

        std::string iban_str = iban_ptr[iban_idx].GetString();
        int32_t method_id = method_ptr[method_idx];

        // Validate method_id range
        if (method_id < 0 || method_id > 0xC6) {
            result_data[i] = false;
            continue;
        }

        result_data[i] = validate_german_iban_with_kontocheck(
            iban_str,
            static_cast<uint8_t>(method_id)
        );
    }
}

// DuckDB scalar function for formatting IBAN
static void StpsFormatIbanFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t iban) {
            std::string iban_str = iban.GetString();
            std::string formatted = format_iban(iban_str);
            return StringVector::AddString(result, formatted);
        });
}

// DuckDB scalar function for getting country code
static void StpsGetIbanCountryCodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t iban) {
            std::string iban_str = iban.GetString();
            std::string country_code = get_country_code(iban_str);
            return StringVector::AddString(result, country_code);
        });
}

// DuckDB scalar function for getting check digits
static void StpsGetIbanCheckDigitsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t iban) {
            std::string iban_str = iban.GetString();
            std::string check_digits = get_check_digits(iban_str);
            return StringVector::AddString(result, check_digits);
        });
}

// DuckDB scalar function for getting BBAN
static void StpsGetBbanFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t iban) {
            std::string iban_str = iban.GetString();
            std::string bban = get_bban(iban_str);
            return StringVector::AddString(result, bban);
        });
}

void RegisterIbanValidationFunctions(ExtensionLoader &loader) {
    // stps_is_valid_iban(iban) - Validate IBAN using MOD-97 algorithm
    ScalarFunctionSet is_valid_iban_set("stps_is_valid_iban");
    is_valid_iban_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BOOLEAN, StpsIsValidIbanFunction));
    loader.RegisterFunction(is_valid_iban_set);

    // stps_format_iban(iban) - Format IBAN with spaces every 4 characters
    ScalarFunctionSet format_iban_set("stps_format_iban");
    format_iban_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, StpsFormatIbanFunction));
    loader.RegisterFunction(format_iban_set);

    // stps_get_iban_country_code(iban) - Extract country code from IBAN
    ScalarFunctionSet get_country_code_set("stps_get_iban_country_code");
    get_country_code_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, StpsGetIbanCountryCodeFunction));
    loader.RegisterFunction(get_country_code_set);

    // stps_get_iban_check_digits(iban) - Extract check digits from IBAN
    ScalarFunctionSet get_check_digits_set("stps_get_iban_check_digits");
    get_check_digits_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, StpsGetIbanCheckDigitsFunction));
    loader.RegisterFunction(get_check_digits_set);

    // stps_get_bban(iban) - Extract BBAN (Basic Bank Account Number) from IBAN
    ScalarFunctionSet get_bban_set("stps_get_bban");
    get_bban_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, StpsGetBbanFunction));
    loader.RegisterFunction(get_bban_set);

    // stps_is_valid_german_iban(iban, method_id) - Validate German IBAN with kontocheck
    ScalarFunctionSet is_valid_german_iban_set("stps_is_valid_german_iban");
    is_valid_german_iban_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::INTEGER},
        LogicalType::BOOLEAN,
        StpsIsValidGermanIbanFunction));
    loader.RegisterFunction(is_valid_german_iban_set);
}

} // namespace stps
} // namespace duckdb
