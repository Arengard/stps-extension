#include "case_transform.hpp"
#include "utils.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {
namespace stps {

std::string to_snake_case(const std::string& input) {
    auto words = split_words(input);
    for (auto& word : words) {
        word = to_lower(word);
    }
    return join_strings(words, "_");
}

std::string to_camel_case(const std::string& input) {
    auto words = split_words(input);
    if (words.empty()) return "";

    std::string result;
    for (size_t i = 0; i < words.size(); i++) {
        std::string word = to_lower(words[i]);
        if (i > 0 && !word.empty()) {
            word[0] = std::toupper(static_cast<unsigned char>(word[0]));
        }
        result += word;
    }
    return result;
}

std::string to_pascal_case(const std::string& input) {
    auto words = split_words(input);
    std::string result;

    for (auto& word : words) {
        std::string lower_word = to_lower(word);
        if (!lower_word.empty()) {
            lower_word[0] = std::toupper(static_cast<unsigned char>(lower_word[0]));
        }
        result += lower_word;
    }
    return result;
}

std::string to_kebab_case(const std::string& input) {
    auto words = split_words(input);
    for (auto& word : words) {
        word = to_lower(word);
    }
    return join_strings(words, "-");
}

std::string to_const_case(const std::string& input) {
    auto words = split_words(input);
    for (auto& word : words) {
        word = to_upper(word);
    }
    return join_strings(words, "_");
}

std::string to_sentence_case(const std::string& input) {
    auto words = split_words(input);
    std::string result = join_strings(words, " ");
    result = to_lower(result);

    if (!result.empty()) {
        result[0] = std::toupper(static_cast<unsigned char>(result[0]));
    }
    return result;
}

std::string to_title_case(const std::string& input) {
    auto words = split_words(input);

    for (auto& word : words) {
        word = to_lower(word);
        if (!word.empty()) {
            word[0] = std::toupper(static_cast<unsigned char>(word[0]));
        }
    }

    return join_strings(words, " ");
}

// DuckDB scalar function wrappers
static void PgmToSnakeCaseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            std::string input_str = input.GetString();
            std::string output = to_snake_case(input_str);
            return StringVector::AddString(result, output);
        });
}

static void PgmToCamelCaseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            std::string input_str = input.GetString();
            std::string output = to_camel_case(input_str);
            return StringVector::AddString(result, output);
        });
}

static void PgmToPascalCaseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            std::string input_str = input.GetString();
            std::string output = to_pascal_case(input_str);
            return StringVector::AddString(result, output);
        });
}

static void PgmToKebabCaseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            std::string input_str = input.GetString();
            std::string output = to_kebab_case(input_str);
            return StringVector::AddString(result, output);
        });
}

static void PgmToConstCaseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            std::string input_str = input.GetString();
            std::string output = to_const_case(input_str);
            return StringVector::AddString(result, output);
        });
}

static void PgmToSentenceCaseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            std::string input_str = input.GetString();
            std::string output = to_sentence_case(input_str);
            return StringVector::AddString(result, output);
        });
}

static void PgmToTitleCaseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            std::string input_str = input.GetString();
            std::string output = to_title_case(input_str);
            return StringVector::AddString(result, output);
        });
}

void RegisterCaseTransformFunctions(ExtensionLoader &loader) {
    // Register all case transformation functions
    // Note: Descriptions are documented in STPS_FUNCTIONS.md

    ScalarFunctionSet snake_case_set("stps_to_snake_case");
    snake_case_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, PgmToSnakeCaseFunction));
    loader.RegisterFunction(snake_case_set);

    ScalarFunctionSet camel_case_set("stps_to_camel_case");
    camel_case_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, PgmToCamelCaseFunction));
    loader.RegisterFunction(camel_case_set);

    ScalarFunctionSet pascal_case_set("stps_to_pascal_case");
    pascal_case_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, PgmToPascalCaseFunction));
    loader.RegisterFunction(pascal_case_set);

    ScalarFunctionSet kebab_case_set("stps_to_kebab_case");
    kebab_case_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, PgmToKebabCaseFunction));
    loader.RegisterFunction(kebab_case_set);

    ScalarFunctionSet const_case_set("stps_to_const_case");
    const_case_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, PgmToConstCaseFunction));
    loader.RegisterFunction(const_case_set);

    ScalarFunctionSet sentence_case_set("stps_to_sentence_case");
    sentence_case_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, PgmToSentenceCaseFunction));
    loader.RegisterFunction(sentence_case_set);

    ScalarFunctionSet title_case_set("stps_to_title_case");
    title_case_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, PgmToTitleCaseFunction));
    loader.RegisterFunction(title_case_set);
}

} // namespace stps
} // namespace duckdb
