#include "include/utils.hpp"
#include <sstream>
#include <regex>

namespace duckdb {
namespace polarsgodmode {

std::string trim(const std::string& str) {
    if (str.empty()) return str;

    size_t start = 0;
    size_t end = str.length() - 1;

    while (start <= end && std::isspace(static_cast<unsigned char>(str[start]))) {
        start++;
    }

    while (end > start && std::isspace(static_cast<unsigned char>(str[end]))) {
        end--;
    }

    return str.substr(start, end - start + 1);
}

std::string to_lower(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return result;
}

std::string to_upper(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    return result;
}

bool is_empty_or_whitespace(const std::string& str) {
    return str.empty() || std::all_of(str.begin(), str.end(),
                                      [](unsigned char c) { return std::isspace(c); });
}

bool is_word_boundary(char c) {
    return !std::isalnum(static_cast<unsigned char>(c));
}

bool is_digit_char(char c) {
    return std::isdigit(static_cast<unsigned char>(c));
}

bool is_alpha_char(char c) {
    return std::isalpha(static_cast<unsigned char>(c));
}

bool is_upper_char(char c) {
    return std::isupper(static_cast<unsigned char>(c));
}

bool is_lower_char(char c) {
    return std::islower(static_cast<unsigned char>(c));
}

std::vector<std::string> split_words(const std::string& str) {
    std::vector<std::string> words;
    std::string current_word;
    bool last_was_upper = false;
    bool last_was_lower = false;

    for (size_t i = 0; i < str.length(); i++) {
        char c = str[i];

        // Handle delimiters (space, underscore, hyphen, etc.)
        if (is_word_boundary(c) && !is_alpha_char(c) && !is_digit_char(c)) {
            if (!current_word.empty()) {
                words.push_back(current_word);
                current_word.clear();
            }
            last_was_upper = false;
            last_was_lower = false;
            continue;
        }

        // Handle camelCase and PascalCase transitions
        if (is_upper_char(c)) {
            // Check for acronyms (e.g., "XMLParser" -> ["XML", "Parser"])
            if (last_was_upper && i + 1 < str.length() && is_lower_char(str[i + 1])) {
                // End of acronym, start new word with last letter
                if (current_word.length() > 1) {
                    words.push_back(current_word.substr(0, current_word.length() - 1));
                    current_word = current_word.substr(current_word.length() - 1);
                }
            } else if (last_was_lower) {
                // Transition from lower to upper (e.g., "camelCase")
                if (!current_word.empty()) {
                    words.push_back(current_word);
                    current_word.clear();
                }
            }
            current_word += c;
            last_was_upper = true;
            last_was_lower = false;
        } else {
            current_word += c;
            last_was_upper = false;
            last_was_lower = is_alpha_char(c);
        }
    }

    if (!current_word.empty()) {
        words.push_back(current_word);
    }

    return words;
}

std::string join_strings(const std::vector<std::string>& strings, const std::string& separator) {
    if (strings.empty()) return "";

    std::ostringstream result;
    result << strings[0];

    for (size_t i = 1; i < strings.size(); i++) {
        result << separator << strings[i];
    }

    return result.str();
}

} // namespace polarsgodmode
} // namespace duckdb
