#pragma once

#include <string>

namespace stps {
namespace shared {

class PatternMatcher {
public:
    // Match filename against glob pattern (* and ? wildcards)
    // Returns true if filename matches the pattern
    // Pattern examples: "*.txt", "test*.cpp", "file?.log"
    static bool MatchesGlobPattern(const std::string& filename, const std::string& pattern);
};

} // namespace shared
} // namespace stps
