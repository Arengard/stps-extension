#pragma once

#include "duckdb/common/file_system.hpp"
#include <string>

namespace stps {
namespace shared {

class ContentSearcher {
public:
    // Maximum file size for content search (100MB)
    static constexpr int64_t MAX_SEARCH_SIZE = 100 * 1024 * 1024;

    // Check if a file is likely binary (not text)
    static bool IsBinaryFile(duckdb::FileSystem& fs, const std::string& file_path);

    // Search for text within a file
    // Returns true if the text is found
    // Automatically skips binary files and files > MAX_SEARCH_SIZE
    static bool FileContainsText(duckdb::FileSystem& fs, const std::string& file_path, const std::string& search_text);

private:
    // Check if file extension suggests text content
    static bool IsTextExtension(const std::string& extension);

    // Boyer-Moore-Horspool algorithm for fast text search
    static bool BoyerMooreSearch(const char* text, size_t text_len, const std::string& pattern);
};

} // namespace shared
} // namespace stps
