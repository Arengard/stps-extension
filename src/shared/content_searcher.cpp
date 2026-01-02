#include "shared/content_searcher.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_open_flags.hpp"
#include <algorithm>
#include <vector>
#include <cstring>

namespace stps {
namespace shared {

bool ContentSearcher::IsTextExtension(const std::string& extension) {
    static const std::vector<std::string> text_extensions = {
        "txt", "md", "cpp", "h", "hpp", "c", "cc", "cxx",
        "py", "js", "ts", "tsx", "jsx", "java", "go", "rs",
        "json", "xml", "html", "htm", "css", "scss", "sass",
        "yaml", "yml", "toml", "ini", "cfg", "conf",
        "sh", "bash", "zsh", "fish", "ps1", "bat", "cmd",
        "sql", "r", "rb", "pl", "php", "swift", "kt", "scala",
        "log", "csv", "tsv"
    };

    std::string ext_lower = extension;
    std::transform(ext_lower.begin(), ext_lower.end(), ext_lower.begin(), ::tolower);

    return std::find(text_extensions.begin(), text_extensions.end(), ext_lower) != text_extensions.end();
}

bool ContentSearcher::IsBinaryFile(duckdb::FileSystem& fs, const std::string& file_path) {
    // Check extension first (fast)
    std::string ext = fs.ExtractExtension(file_path);

    // If extension suggests text, return false
    if (IsTextExtension(ext)) {
        return false;
    }

    // Check for known binary extensions
    std::string ext_lower = ext;
    std::transform(ext_lower.begin(), ext_lower.end(), ext_lower.begin(), ::tolower);

    static const std::vector<std::string> binary_extensions = {
        "jpg", "jpeg", "png", "gif", "bmp", "ico", "webp",
        "mp4", "avi", "mov", "mkv", "flv", "wmv", "webm",
        "mp3", "wav", "flac", "ogg", "m4a", "wma",
        "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx",
        "zip", "tar", "gz", "bz2", "7z", "rar",
        "exe", "dll", "so", "dylib", "bin", "dat",
        "class", "jar", "war", "ear", "o", "a"
    };

    if (std::find(binary_extensions.begin(), binary_extensions.end(), ext_lower) != binary_extensions.end()) {
        return true;
    }

    // Read first 8KB to check for null bytes (indicates binary)
    try {
        auto handle = fs.OpenFile(file_path, duckdb::FileOpenFlags::FILE_FLAGS_READ);
        char buffer[8192];
        int64_t bytes_read = handle->Read(buffer, sizeof(buffer));

        // Check for null bytes
        for (int64_t i = 0; i < bytes_read; ++i) {
            if (buffer[i] == '\0') {
                return true;
            }
        }
    } catch (...) {
        return true; // Can't read, assume binary
    }

    return false;
}

bool ContentSearcher::BoyerMooreSearch(const char* text, size_t text_len, const std::string& pattern) {
    if (pattern.empty() || text_len < pattern.size()) {
        return false;
    }

    const size_t pattern_len = pattern.size();

    // Build bad character table
    int bad_char[256];
    for (int i = 0; i < 256; ++i) {
        bad_char[i] = -1;
    }
    for (size_t i = 0; i < pattern_len; ++i) {
        bad_char[static_cast<unsigned char>(pattern[i])] = static_cast<int>(i);
    }

    // Search
    size_t shift = 0;
    while (shift <= (text_len - pattern_len)) {
        int j = static_cast<int>(pattern_len) - 1;

        while (j >= 0 && pattern[j] == text[shift + j]) {
            --j;
        }

        if (j < 0) {
            return true; // Pattern found
        }

        shift += std::max(1, j - bad_char[static_cast<unsigned char>(text[shift + j])]);
    }

    return false;
}

bool ContentSearcher::FileContainsText(duckdb::FileSystem& fs, const std::string& file_path, const std::string& search_text) {
    if (search_text.empty()) {
        return true;
    }

    try {
        // Check file size
        auto handle = fs.OpenFile(file_path, duckdb::FileOpenFlags::FILE_FLAGS_READ);
        int64_t file_size = fs.GetFileSize(*handle);

        if (file_size > MAX_SEARCH_SIZE) {
            return false;
        }

        // Skip binary files
        if (IsBinaryFile(fs, file_path)) {
            return false;
        }

        // Read file into memory
        std::vector<char> buffer(file_size);
        handle->Read(buffer.data(), file_size);

        return BoyerMooreSearch(buffer.data(), file_size, search_text);
    } catch (...) {
        return false;
    }
}

} // namespace shared
} // namespace stps
