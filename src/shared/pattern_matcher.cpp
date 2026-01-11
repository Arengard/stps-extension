#include "shared/pattern_matcher.hpp"
#include <algorithm>
#include <cctype>

namespace stps {
namespace shared {

// Simple glob pattern matcher (no regex - Windows safe)
// Supports: * (match any), ? (match single char)
// Case-insensitive matching
bool PatternMatcher::MatchesGlobPattern(const std::string& filename, const std::string& pattern) {
    if (pattern.empty()) {
        return true;
    }

    // Convert both to lowercase for case-insensitive matching
    std::string fn = filename;
    std::string pat = pattern;
    std::transform(fn.begin(), fn.end(), fn.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    std::transform(pat.begin(), pat.end(), pat.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    // Recursive glob matching
    size_t fi = 0, pi = 0;
    size_t star_idx = std::string::npos;
    size_t match_idx = 0;

    while (fi < fn.length()) {
        if (pi < pat.length() && (pat[pi] == '?' || pat[pi] == fn[fi])) {
            // Character match or ? wildcard
            fi++;
            pi++;
        } else if (pi < pat.length() && pat[pi] == '*') {
            // * wildcard - remember position
            star_idx = pi;
            match_idx = fi;
            pi++;
        } else if (star_idx != std::string::npos) {
            // Backtrack - try matching one more character with *
            pi = star_idx + 1;
            match_idx++;
            fi = match_idx;
        } else {
            // No match
            return false;
        }
    }

    // Skip remaining * in pattern
    while (pi < pat.length() && pat[pi] == '*') {
        pi++;
    }

    return pi == pat.length();
}

} // namespace shared
} // namespace stps
