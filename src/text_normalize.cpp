#include "include/text_normalize.hpp"
#include "include/utils.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <unordered_map>
#include <regex>

namespace duckdb {
namespace polarsgodmode {

// Accent to ASCII mapping (comprehensive)
static const std::unordered_map<std::string, std::string> ACCENT_MAP = {
    // Latin accents
    {"à", "a"}, {"á", "a"}, {"â", "a"}, {"ã", "a"}, {"å", "a"}, {"ā", "a"},
    {"è", "e"}, {"é", "e"}, {"ê", "e"}, {"ë", "e"}, {"ē", "e"}, {"ė", "e"},
    {"ì", "i"}, {"í", "i"}, {"î", "i"}, {"ï", "i"}, {"ī", "i"},
    {"ò", "o"}, {"ó", "o"}, {"ô", "o"}, {"õ", "o"}, {"ø", "o"}, {"ō", "o"},
    {"ù", "u"}, {"ú", "u"}, {"û", "u"}, {"ū", "u"},
    {"ý", "y"}, {"ÿ", "y"},
    {"ñ", "n"}, {"ç", "c"},
    {"À", "A"}, {"Á", "A"}, {"Â", "A"}, {"Ã", "A"}, {"Å", "A"}, {"Ā", "A"},
    {"È", "E"}, {"É", "E"}, {"Ê", "E"}, {"Ë", "E"}, {"Ē", "E"}, {"Ė", "E"},
    {"Ì", "I"}, {"Í", "I"}, {"Î", "I"}, {"Ï", "I"}, {"Ī", "I"},
    {"Ò", "O"}, {"Ó", "O"}, {"Ô", "O"}, {"Õ", "O"}, {"Ø", "O"}, {"Ō", "O"},
    {"Ù", "U"}, {"Ú", "U"}, {"Û", "U"}, {"Ū", "U"},
    {"Ý", "Y"}, {"Ÿ", "Y"},
    {"Ñ", "N"}, {"Ç", "C"},
    // Special characters
    {"æ", "ae"}, {"Æ", "AE"}, {"œ", "oe"}, {"Œ", "OE"},
};

// German umlauts mapping
static const std::unordered_map<std::string, std::string> UMLAUT_TO_ASCII = {
    {"ä", "ae"}, {"ö", "oe"}, {"ü", "ue"}, {"ß", "ss"},
    {"Ä", "Ae"}, {"Ö", "Oe"}, {"Ü", "Ue"}
};

static const std::unordered_map<std::string, std::string> ASCII_TO_UMLAUT = {
    {"ae", "ä"}, {"oe", "ö"}, {"ue", "ü"}, {"ss", "ß"},
    {"Ae", "Ä"}, {"Oe", "Ö"}, {"Ue", "Ü"},
    {"AE", "Ä"}, {"OE", "Ö"}, {"UE", "Ü"}
};

std::string convert_umlauts_to_ascii(const std::string& input) {
    std::string result = input;

    for (const auto& pair : UMLAUT_TO_ASCII) {
        size_t pos = 0;
        while ((pos = result.find(pair.first, pos)) != std::string::npos) {
            result.replace(pos, pair.first.length(), pair.second);
            pos += pair.second.length();
        }
    }

    return result;
}

std::string convert_ascii_to_umlauts(const std::string& input) {
    std::string result = input;

    // Process in order of length (longer first to avoid partial replacements)
    std::vector<std::pair<std::string, std::string>> sorted_mappings(
        ASCII_TO_UMLAUT.begin(), ASCII_TO_UMLAUT.end());

    std::sort(sorted_mappings.begin(), sorted_mappings.end(),
              [](const std::pair<std::string, std::string>& a, const std::pair<std::string, std::string>& b) {
                  return a.first.length() > b.first.length();
              });

    for (const auto& pair : sorted_mappings) {
        size_t pos = 0;
        while ((pos = result.find(pair.first, pos)) != std::string::npos) {
            result.replace(pos, pair.first.length(), pair.second);
            pos += pair.second.length();
        }
    }

    return result;
}

std::string remove_accents(const std::string& input, bool keep_umlauts) {
    std::string result = input;

    // First handle umlauts if not keeping them
    if (!keep_umlauts) {
        result = convert_umlauts_to_ascii(result);
    }

    // Remove other accents
    for (const auto& pair : ACCENT_MAP) {
        size_t pos = 0;
        while ((pos = result.find(pair.first, pos)) != std::string::npos) {
            result.replace(pos, pair.first.length(), pair.second);
            pos += pair.second.length();
        }
    }

    return result;
}

std::string restore_umlauts(const std::string& input) {
    return convert_ascii_to_umlauts(input);
}

std::string normalize_text(const std::string& input, bool trim_ws, bool lower_case) {
    std::string result = input;

    // Normalize whitespace (multiple spaces to single space)
    result = std::regex_replace(result, std::regex("\\s+"), " ");

    if (trim_ws) {
        result = trim(result);
    }

    if (lower_case) {
        result = to_lower(result);
    }

    return result;
}

std::string clean_string(const std::string& input) {
    std::string result = trim(input);

    // Normalize whitespace
    result = std::regex_replace(result, std::regex("\\s+"), " ");

    // Remove control characters
    result.erase(std::remove_if(result.begin(), result.end(),
                                 [](unsigned char c) { return std::iscntrl(c) && c != '\n' && c != '\t'; }),
                 result.end());

    return result;
}

// DuckDB scalar function wrappers
static void PgmRemoveAccentsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, bool, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t input, bool keep_umlauts) {
            std::string input_str = input.GetString();
            std::string output = remove_accents(input_str, keep_umlauts);
            return StringVector::AddString(result, output);
        });
}

static void PgmRemoveAccentsSimpleFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            std::string input_str = input.GetString();
            std::string output = remove_accents(input_str, false);
            return StringVector::AddString(result, output);
        });
}

static void PgmRestoreUmlautsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            std::string input_str = input.GetString();
            std::string output = restore_umlauts(input_str);
            return StringVector::AddString(result, output);
        });
}

static void PgmCleanStringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            std::string input_str = input.GetString();
            std::string output = clean_string(input_str);
            return StringVector::AddString(result, output);
        });
}

static void PgmNormalizeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            std::string input_str = input.GetString();
            std::string output = normalize_text(input_str, true, false);
            return StringVector::AddString(result, output);
        });
}

void RegisterTextNormalizeFunctions(ExtensionLoader &loader) {
    // stps_remove_accents with keep_umlauts parameter
    ScalarFunctionSet remove_accents_set("stps_remove_accents");
    remove_accents_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                                   PgmRemoveAccentsSimpleFunction));
    remove_accents_set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN},
                                                   LogicalType::VARCHAR, PgmRemoveAccentsFunction));
    loader.RegisterFunction(remove_accents_set);

    // stps_restore_umlauts
    ScalarFunctionSet restore_umlauts_set("stps_restore_umlauts");
    restore_umlauts_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                                    PgmRestoreUmlautsFunction));
    loader.RegisterFunction(restore_umlauts_set);

    // stps_clean_string
    ScalarFunctionSet clean_string_set("stps_clean_string");
    clean_string_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                                 PgmCleanStringFunction));
    loader.RegisterFunction(clean_string_set);

    // stps_normalize
    ScalarFunctionSet normalize_set("stps_normalize");
    normalize_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                             PgmNormalizeFunction));
    loader.RegisterFunction(normalize_set);
}

} // namespace polarsgodmode
} // namespace duckdb
