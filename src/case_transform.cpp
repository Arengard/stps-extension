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
    auto snake_case_func = ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, PgmToSnakeCaseFunction);
    snake_case_func.description = "Converts a string to snake_case format (lowercase words separated by underscores).\n"
                                  "Usage: SELECT stps_to_snake_case('HelloWorld');\n"
                                  "Returns: VARCHAR (e.g., 'hello_world')";
    ScalarFunctionSet snake_case_set("stps_to_snake_case");
    snake_case_set.AddFunction(snake_case_func);
    loader.RegisterFunction(snake_case_set);

    auto camel_case_func = ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, PgmToCamelCaseFunction);
    camel_case_func.description = "Converts a string to camelCase format (first word lowercase, subsequent words capitalized, no separators).\n"
                                  "Usage: SELECT stps_to_camel_case('hello_world');\n"
                                  "Returns: VARCHAR (e.g., 'helloWorld')";
    ScalarFunctionSet camel_case_set("stps_to_camel_case");
    camel_case_set.AddFunction(camel_case_func);
    loader.RegisterFunction(camel_case_set);

    auto pascal_case_func = ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, PgmToPascalCaseFunction);
    pascal_case_func.description = "Converts a string to PascalCase format (all words capitalized, no separators).\n"
                                   "Usage: SELECT stps_to_pascal_case('hello_world');\n"
                                   "Returns: VARCHAR (e.g., 'HelloWorld')";
    ScalarFunctionSet pascal_case_set("stps_to_pascal_case");
    pascal_case_set.AddFunction(pascal_case_func);
    loader.RegisterFunction(pascal_case_set);

    auto kebab_case_func = ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, PgmToKebabCaseFunction);
    kebab_case_func.description = "Converts a string to kebab-case format (lowercase words separated by hyphens).\n"
                                  "Usage: SELECT stps_to_kebab_case('HelloWorld');\n"
                                  "Returns: VARCHAR (e.g., 'hello-world')";
    ScalarFunctionSet kebab_case_set("stps_to_kebab_case");
    kebab_case_set.AddFunction(kebab_case_func);
    loader.RegisterFunction(kebab_case_set);

    auto const_case_func = ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, PgmToConstCaseFunction);
    const_case_func.description = "Converts a string to CONST_CASE format (uppercase words separated by underscores).\n"
                                  "Usage: SELECT stps_to_const_case('helloWorld');\n"
                                  "Returns: VARCHAR (e.g., 'HELLO_WORLD')";
    ScalarFunctionSet const_case_set("stps_to_const_case");
    const_case_set.AddFunction(const_case_func);
    loader.RegisterFunction(const_case_set);

    auto sentence_case_func = ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, PgmToSentenceCaseFunction);
    sentence_case_func.description = "Converts a string to Sentence case format (first letter capitalized, rest lowercase, words separated by spaces).\n"
                                     "Usage: SELECT stps_to_sentence_case('HELLO_WORLD');\n"
                                     "Returns: VARCHAR (e.g., 'Hello world')";
    ScalarFunctionSet sentence_case_set("stps_to_sentence_case");
    sentence_case_set.AddFunction(sentence_case_func);
    loader.RegisterFunction(sentence_case_set);

    auto title_case_func = ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, PgmToTitleCaseFunction);
    title_case_func.description = "Converts a string to Title Case format (all words capitalized, separated by spaces).\n"
                                  "Usage: SELECT stps_to_title_case('hello_world');\n"
                                  "Returns: VARCHAR (e.g., 'Hello World')";
    ScalarFunctionSet title_case_set("stps_to_title_case");
    title_case_set.AddFunction(title_case_func);
    loader.RegisterFunction(title_case_set);
}

} // namespace stps
} // namespace duckdb
