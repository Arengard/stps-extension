#include "text_normalize.hpp"
#include "utils.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <unordered_map>
#include <cctype>
#include <algorithm>
#include <cctype>
#include <string>

namespace duckdb {
namespace stps {

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

// Collapse multiple whitespace characters into single space (no regex)
static std::string collapse_whitespace(const std::string& input) {
    std::string result;
    result.reserve(input.size());
    bool prev_was_space = false;

    for (char c : input) {
        bool is_space = std::isspace(static_cast<unsigned char>(c));
        if (is_space) {
            if (!prev_was_space) {
                result += ' ';
            }
            prev_was_space = true;
        } else {
            result += c;
            prev_was_space = false;
        }
    }

    return result;
}

std::string normalize_text(const std::string& input, bool trim_ws, bool lower_case) {
    std::string result = input;

    // Normalize whitespace (multiple spaces to single space)
    result = collapse_whitespace(result);

    if (trim_ws) {
        result = trim(result);
    }

    if (lower_case) {
        result = to_lower(result);
    }

    return result;
}

std::string clean_string(const std::string& input) {
    // Step 1: Trim leading and trailing whitespace
    size_t start = input.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) return "";
    size_t end = input.find_last_not_of(" \t\n\r");
    std::string result = input.substr(start, end - start + 1);

    // Step 2: Remove/replace non-printable characters and special whitespace
    std::string cleaned;
    cleaned.reserve(result.size());

    for (size_t i = 0; i < result.size();) {
        unsigned char c = static_cast<unsigned char>(result[i]);

        // UTF-8 NBSP: 0xC2 0xA0 -> replace with space
        if (i + 1 < result.size() && c == 0xC2 &&
            static_cast<unsigned char>(result[i + 1]) == 0xA0) {
            cleaned += ' ';
            i += 2;
            continue;
        }

        // Latin-1 NBSP: 0xA0 -> replace with space
        if (c == 0xA0) {
            cleaned += ' ';
            ++i;
            continue;
        }

        // Zero-width space: 0xE2 0x80 0x8B -> remove
        if (i + 2 < result.size() && c == 0xE2 &&
            static_cast<unsigned char>(result[i + 1]) == 0x80 &&
            static_cast<unsigned char>(result[i + 2]) == 0x8B) {
            i += 3;
            continue;
        }

        // Zero-width non-joiner: 0xE2 0x80 0x8C -> remove
        if (i + 2 < result.size() && c == 0xE2 &&
            static_cast<unsigned char>(result[i + 1]) == 0x80 &&
            static_cast<unsigned char>(result[i + 2]) == 0x8C) {
            i += 3;
            continue;
        }

        // Zero-width joiner: 0xE2 0x80 0x8D -> remove
        if (i + 2 < result.size() && c == 0xE2 &&
            static_cast<unsigned char>(result[i + 1]) == 0x80 &&
            static_cast<unsigned char>(result[i + 2]) == 0x8D) {
            i += 3;
            continue;
        }

        // Soft hyphen: 0xC2 0xAD -> remove
        if (i + 1 < result.size() && c == 0xC2 &&
            static_cast<unsigned char>(result[i + 1]) == 0xAD) {
            i += 2;
            continue;
        }

        // ASCII control characters (0x00-0x1F and 0x7F)
        // Convert \n, \t, \r to space; remove other control chars
        if (c < 0x20 || c == 0x7F) {
            if (c == '\n' || c == '\t' || c == '\r') {
                cleaned += ' ';
            }
            // Otherwise skip control characters
            ++i;
            continue;
        }

        // Keep printable ASCII (0x20-0x7E) and UTF-8 continuation bytes
        cleaned += result[i];
        ++i;
    }

    // Step 3: Collapse consecutive whitespace into single spaces
    std::string collapsed;
    collapsed.reserve(cleaned.size());
    bool prev_was_space = false;

    for (char c : cleaned) {
        bool is_space = std::isspace(static_cast<unsigned char>(c));
        if (is_space) {
            if (!prev_was_space) {
                collapsed += ' ';
            }
            prev_was_space = true;
        } else {
            collapsed += c;
            prev_was_space = false;
        }
    }

    // Step 4: Final trim to remove any leading/trailing spaces from collapsing
    start = collapsed.find_first_not_of(' ');
    if (start == std::string::npos) return "";
    end = collapsed.find_last_not_of(' ');

    return collapsed.substr(start, end - start + 1);
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

    auto remove_accents_simple = ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                                 PgmRemoveAccentsSimpleFunction);
    remove_accents_simple.description = "Removes accents from text, converting accented characters to their ASCII equivalents (including German umlauts).\n"
                                        "Usage: SELECT stps_remove_accents('Café Müller');\n"
                                        "Returns: VARCHAR (text with accents removed, e.g., 'Cafe Mueller')";
    remove_accents_set.AddFunction(remove_accents_simple);

    auto remove_accents_with_param = ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN},
                                                     LogicalType::VARCHAR, PgmRemoveAccentsFunction);
    remove_accents_with_param.description = "Removes accents from text with optional preservation of German umlauts.\n"
                                            "Usage: SELECT stps_remove_accents('Café Müller', true);\n"
                                            "Returns: VARCHAR (text with accents removed, umlauts preserved if second parameter is true)";
    remove_accents_set.AddFunction(remove_accents_with_param);

    loader.RegisterFunction(remove_accents_set);

    // stps_restore_umlauts
    ScalarFunctionSet restore_umlauts_set("stps_restore_umlauts");
    auto restore_umlauts = ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                          PgmRestoreUmlautsFunction);
    restore_umlauts.description = "Converts ASCII representations of German umlauts back to their proper Unicode forms (ae->ä, oe->ö, ue->ü, ss->ß).\n"
                                  "Usage: SELECT stps_restore_umlauts('Mueller');\n"
                                  "Returns: VARCHAR (text with umlauts restored, e.g., 'Müller')";
    restore_umlauts_set.AddFunction(restore_umlauts);
    loader.RegisterFunction(restore_umlauts_set);

    // stps_clean_string
    ScalarFunctionSet clean_string_set("stps_clean_string");
    auto clean_string = ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                       PgmCleanStringFunction);
    clean_string.description = "Cleans text by removing non-printable characters, zero-width spaces, soft hyphens, and control characters. Converts special whitespace to regular spaces and collapses multiple spaces.\n"
                               "Usage: SELECT stps_clean_string('Text  with\\u00A0\\u200Bextra\\tspaces');\n"
                               "Returns: VARCHAR (cleaned text with normalized whitespace and removed invisible characters)";
    clean_string_set.AddFunction(clean_string);
    loader.RegisterFunction(clean_string_set);

    // stps_normalize
    ScalarFunctionSet normalize_set("stps_normalize");
    auto normalize = ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                    PgmNormalizeFunction);
    normalize.description = "Normalizes text by collapsing multiple whitespace characters into single spaces and trimming leading/trailing whitespace.\n"
                           "Usage: SELECT stps_normalize('  Text   with   spaces  ');\n"
                           "Returns: VARCHAR (normalized text with single spaces and no leading/trailing whitespace)";
    normalize_set.AddFunction(normalize);
    loader.RegisterFunction(normalize_set);
}

} // namespace stps
} // namespace duckdb
