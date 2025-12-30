#include "include/iban_validation.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <string>
#include <algorithm>
#include <cctype>

namespace duckdb {
namespace polarsgodmode {

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
    return (mod_result == "1");
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

void RegisterIbanValidationFunctions(ExtensionLoader &loader) {
    // stps_is_valid_iban(iban) - Returns true if IBAN is valid
    ScalarFunctionSet is_valid_iban_set("stps_is_valid_iban");
    is_valid_iban_set.AddFunction(ScalarFunction({LogicalType::VARCHAR},
                                                  LogicalType::BOOLEAN,
                                                  StpsIsValidIbanFunction));
    loader.RegisterFunction(is_valid_iban_set);
}

} // namespace polarsgodmode
} // namespace duckdb
