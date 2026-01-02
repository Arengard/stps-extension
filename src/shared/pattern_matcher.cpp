#include "shared/pattern_matcher.hpp"
#include <regex>
#include <algorithm>

namespace stps {
namespace shared {

bool PatternMatcher::MatchesGlobPattern(const std::string& filename, const std::string& pattern) {
    if (pattern.empty()) {
        return true;
    }

    // Convert glob pattern to regex
    std::string regex_pattern;
    regex_pattern.reserve(pattern.size() * 2);

    for (char c : pattern) {
        if (c == '*') {
            regex_pattern += ".*";
        } else if (c == '?') {
            regex_pattern += ".";
        } else if (std::string(".^$|()[]{}+\\").find(c) != std::string::npos) {
            // Escape special regex characters
            regex_pattern += "\\";
            regex_pattern += c;
        } else {
            regex_pattern += c;
        }
    }

    try {
        std::regex re(regex_pattern, std::regex::icase);
        return std::regex_match(filename, re);
    } catch (const std::regex_error&) {
        // Invalid pattern, return false
        return false;
    }
}

} // namespace shared
} // namespace stps
