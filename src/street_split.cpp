#include "street_split.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <regex>
#include <algorithm>
#include <cctype>

namespace duckdb {
namespace stps {

static std::string trim(const std::string& str) {
    size_t start = str.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) return "";
    size_t end = str.find_last_not_of(" \t\n\r");
    return str.substr(start, end - start + 1);
}

static std::string to_upper(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    return result;
}

static std::string apply_abbreviation(const std::string& street_name) {
    std::string result = street_name;

    // Check for compound "straße" or "strasse" (no space before it)
    // We need to find these at the end of a word that's attached to other characters
    std::regex compound_strasse_regex(R"((\S)(straße|strasse)$)", std::regex::icase);

    if (std::regex_search(result, compound_strasse_regex)) {
        // Replace the suffix with "str."
        result = std::regex_replace(result, std::regex(R"((straße|strasse)$)", std::regex::icase), "str.");
    }

    return result;
}

static bool is_mannheim_address(const std::string& input, std::string& block_code, std::string& house_number) {
    // Pattern: single letter followed by digits, then space, then house number
    // Examples: "M7 24", "Q3 15a", "L15 7"
    std::regex mannheim_regex(R"(^([A-Za-z]\d+)\s+(\d+.*)$)");
    std::smatch match;

    if (std::regex_match(input, match, mannheim_regex)) {
        block_code = match[1].str();
        house_number = match[2].str();
        return true;
    }
    return false;
}

static size_t find_house_number_start(const std::string& input) {
    // Find where the house number starts
    // This is tricky because street names can contain numbers (e.g., "Straße des 17. Juni")
    // We look for a number that:
    // 1. Is preceded by a space
    // 2. Is at the end of the string or followed by non-letter characters (additions like "a", " D", "/4")

    // Strategy: scan from the end to find the last "word" that starts with a digit
    // and is likely a house number (not part of a date like "17.")

    size_t len = input.length();
    if (len == 0) return std::string::npos;

    // Find the last space-separated token that starts with a digit
    size_t last_space = input.rfind(' ');

    while (last_space != std::string::npos) {
        size_t token_start = last_space + 1;
        if (token_start < len && std::isdigit(static_cast<unsigned char>(input[token_start]))) {
            // Check if this looks like a house number (not followed by a period indicating ordinal/date)
            // Find the end of the numeric part
            size_t num_end = token_start;
            while (num_end < len && std::isdigit(static_cast<unsigned char>(input[num_end]))) {
                num_end++;
            }

            // If next char is '.', this might be an ordinal (like "17. Juni") - skip it
            if (num_end < len && input[num_end] == '.') {
                // Check if followed by more text (indicating it's part of street name)
                size_t after_dot = num_end + 1;
                while (after_dot < len && std::isspace(static_cast<unsigned char>(input[after_dot]))) {
                    after_dot++;
                }
                if (after_dot < len && std::isalpha(static_cast<unsigned char>(input[after_dot]))) {
                    // This is an ordinal in the street name, look for earlier number
                    last_space = input.rfind(' ', last_space - 1);
                    continue;
                }
            }

            // This looks like a house number
            return last_space;
        }

        if (last_space == 0) break;
        last_space = input.rfind(' ', last_space - 1);
    }

    return std::string::npos;
}

StreetParseResult parse_street_address(const std::string& input) {
    StreetParseResult result;
    result.has_number = false;

    std::string trimmed = trim(input);
    if (trimmed.empty()) {
        return result;
    }

    // Check for Mannheim-style address first
    std::string block_code, house_number;
    if (is_mannheim_address(trimmed, block_code, house_number)) {
        result.street_name = block_code;
        result.street_number = to_upper(trim(house_number));
        result.has_number = true;
        return result;
    }

    // Standard address parsing
    size_t split_pos = find_house_number_start(trimmed);

    if (split_pos == std::string::npos) {
        // No house number found
        result.street_name = apply_abbreviation(trimmed);
        result.has_number = false;
    } else {
        // Split at the house number
        std::string street_part = trim(trimmed.substr(0, split_pos));
        std::string number_part = trim(trimmed.substr(split_pos + 1));

        result.street_name = apply_abbreviation(street_part);
        result.street_number = to_upper(number_part);
        result.has_number = !result.street_number.empty();
    }

    return result;
}

static void StpsSplitStreetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input_vector = args.data[0];
    auto count = args.size();

    // Result is a STRUCT with two VARCHAR fields
    auto &struct_entries = StructVector::GetEntries(result);
    auto &street_name_vec = *struct_entries[0];
    auto &street_number_vec = *struct_entries[1];

    // Flatten input for uniform access
    UnifiedVectorFormat input_data;
    input_vector.ToUnifiedFormat(count, input_data);
    auto input_strings = UnifiedVectorFormat::GetData<string_t>(input_data);

    // Process each row
    for (idx_t i = 0; i < count; i++) {
        auto idx = input_data.sel->get_index(i);

        if (!input_data.validity.RowIsValid(idx)) {
            FlatVector::SetNull(result, i, true);
            FlatVector::SetNull(street_name_vec, i, true);
            FlatVector::SetNull(street_number_vec, i, true);
            continue;
        }

        auto input_str = input_strings[idx].GetString();
        auto parsed = parse_street_address(input_str);

        if (parsed.street_name.empty() && !parsed.has_number) {
            FlatVector::SetNull(result, i, true);
            FlatVector::SetNull(street_name_vec, i, true);
            FlatVector::SetNull(street_number_vec, i, true);
        } else {
            FlatVector::SetNull(result, i, false);
            FlatVector::SetNull(street_name_vec, i, false);
            FlatVector::GetData<string_t>(street_name_vec)[i] = StringVector::AddString(street_name_vec, parsed.street_name);

            if (parsed.has_number) {
                FlatVector::SetNull(street_number_vec, i, false);
                FlatVector::GetData<string_t>(street_number_vec)[i] = StringVector::AddString(street_number_vec, parsed.street_number);
            } else {
                FlatVector::SetNull(street_number_vec, i, true);
            }
        }
    }
}

void RegisterStreetSplitFunctions(ExtensionLoader &loader) {
    // Define the return type: STRUCT(street_name VARCHAR, street_number VARCHAR)
    child_list_t<LogicalType> struct_children;
    struct_children.push_back(make_pair("street_name", LogicalType::VARCHAR));
    struct_children.push_back(make_pair("street_number", LogicalType::VARCHAR));
    auto return_type = LogicalType::STRUCT(std::move(struct_children));

    ScalarFunctionSet split_street_set("stps_split_street");
    split_street_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR},
        return_type,
        StpsSplitStreetFunction
    ));

    loader.RegisterFunction(split_street_set);
}

} // namespace stps
} // namespace duckdb
