#include "stps_extension.hpp"
#include <fstream>
#include <algorithm>
#include <chrono>
#include <thread>
#include <mutex>

namespace stps {

FileEntry FileSystemScanner::CreateEntry(const fs::directory_entry& entry) {
    FileEntry result;
    result.name = entry.path().filename().string();
    result.path = entry.path().string();
    result.type = entry.is_directory() ? "directory" : "file";
    result.parent_directory = entry.path().parent_path().string();

    // Get size (0 for directories)
    result.size = entry.is_regular_file() ? fs::file_size(entry.path()) : 0;

    // Get last modified time as Unix timestamp
    auto ftime = fs::last_write_time(entry.path());
    auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
        ftime - fs::file_time_type::clock::now() + std::chrono::system_clock::now()
    );
    result.modified_time = std::chrono::system_clock::to_time_t(sctp);

    // Get extension
    if (entry.is_regular_file() && entry.path().has_extension()) {
        result.extension = entry.path().extension().string();
        // Remove leading dot
        if (!result.extension.empty() && result.extension[0] == '.') {
            result.extension = result.extension.substr(1);
        }
    }

    return result;
}

bool FileSystemScanner::MatchesPattern(const std::string& filename, const std::string& pattern) {
    if (pattern.empty()) return true;

    // Convert glob pattern to regex
    std::string regex_pattern = pattern;

    // Escape special regex characters except * and ?
    std::string escaped;
    for (char c : regex_pattern) {
        if (c == '*') {
            escaped += ".*";
        } else if (c == '?') {
            escaped += ".";
        } else if (std::string(".^$|()[]{}+\\").find(c) != std::string::npos) {
            escaped += "\\";
            escaped += c;
        } else {
            escaped += c;
        }
    }

    std::regex re(escaped, std::regex::icase);
    return std::regex_match(filename, re);
}

bool FileSystemScanner::SearchFileContent(const fs::path& file_path, const std::string& search_text) {
    if (search_text.empty()) return true;

    // Skip large files for content search (> 100MB)
    if (fs::file_size(file_path) > 100 * 1024 * 1024) {
        return false;
    }

    // Skip binary files (basic check)
    std::string ext = file_path.extension().string();
    std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);

    static const std::vector<std::string> text_extensions = {
        ".txt", ".md", ".cpp", ".h", ".hpp", ".c", ".py", ".js", ".ts",
        ".json", ".xml", ".html", ".css", ".java", ".go", ".rs", ".sh"
    };

    bool is_text = std::find(text_extensions.begin(), text_extensions.end(), ext) != text_extensions.end();
    if (!is_text) return false;

    std::ifstream file(file_path);
    if (!file.is_open()) return false;

    std::string line;
    while (std::getline(file, line)) {
        if (line.find(search_text) != std::string::npos) {
            return true;
        }
    }

    return false;
}

bool FileSystemScanner::MatchesFilter(const fs::directory_entry& entry, const ScanOptions& options) {
    // Hidden files filter
    if (!options.include_hidden && entry.path().filename().string()[0] == '.') {
        return false;
    }

    // Only apply filters to files, not directories (for recursive traversal)
    if (!entry.is_regular_file()) {
        return entry.is_directory(); // Allow directories for recursion
    }

    // File type filter
    if (!options.file_type.empty()) {
        std::string ext = entry.path().extension().string();
        if (!ext.empty() && ext[0] == '.') {
            ext = ext.substr(1);
        }
        std::string file_type_lower = options.file_type;
        std::string ext_lower = ext;
        std::transform(file_type_lower.begin(), file_type_lower.end(), file_type_lower.begin(), ::tolower);
        std::transform(ext_lower.begin(), ext_lower.end(), ext_lower.begin(), ::tolower);

        if (ext_lower != file_type_lower) {
            return false;
        }
    }

    // Size filters
    if (options.min_size >= 0 || options.max_size >= 0) {
        int64_t size = fs::file_size(entry.path());
        if (options.min_size >= 0 && size < options.min_size) return false;
        if (options.max_size >= 0 && size > options.max_size) return false;
    }

    // Date filters
    if (options.min_date >= 0 || options.max_date >= 0) {
        auto ftime = fs::last_write_time(entry.path());
        auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
            ftime - fs::file_time_type::clock::now() + std::chrono::system_clock::now()
        );
        int64_t timestamp = std::chrono::system_clock::to_time_t(sctp);

        if (options.min_date >= 0 && timestamp < options.min_date) return false;
        if (options.max_date >= 0 && timestamp > options.max_date) return false;
    }

    // Pattern matching
    if (!options.pattern.empty()) {
        if (!MatchesPattern(entry.path().filename().string(), options.pattern)) {
            return false;
        }
    }

    // Content search (most expensive, do last)
    if (!options.content_search.empty()) {
        if (!SearchFileContent(entry.path(), options.content_search)) {
            return false;
        }
    }

    return true;
}

void FileSystemScanner::ScanRecursive(
    const fs::path& path,
    const ScanOptions& options,
    std::vector<FileEntry>& results,
    int current_depth
) {
    // Check depth limit
    if (options.max_depth >= 0 && current_depth > options.max_depth) {
        return;
    }

    try {
        for (const auto& entry : fs::directory_iterator(path)) {
            if (MatchesFilter(entry, options)) {
                if (entry.is_regular_file()) {
                    results.push_back(CreateEntry(entry));
                } else if (entry.is_directory() && options.recursive) {
                    // Add directory entry
                    results.push_back(CreateEntry(entry));
                    // Recurse into subdirectory
                    ScanRecursive(entry.path(), options, results, current_depth + 1);
                }
            } else if (entry.is_directory() && options.recursive) {
                // Even if directory doesn't match filter, recurse into it
                ScanRecursive(entry.path(), options, results, current_depth + 1);
            }
        }
    } catch (const fs::filesystem_error& e) {
        // Skip directories we can't access
    }
}

std::vector<FileEntry> FileSystemScanner::ScanDirectory(const ScanOptions& options) {
    std::vector<FileEntry> results;

    fs::path base_path(options.base_path);
    if (!fs::exists(base_path)) {
        throw std::runtime_error("Path does not exist: " + options.base_path);
    }

    if (!fs::is_directory(base_path)) {
        throw std::runtime_error("Path is not a directory: " + options.base_path);
    }

    if (options.recursive) {
        ScanRecursive(base_path, options, results, 0);
    } else {
        // Non-recursive scan
        try {
            for (const auto& entry : fs::directory_iterator(base_path)) {
                if (MatchesFilter(entry, options)) {
                    results.push_back(CreateEntry(entry));
                }
            }
        } catch (const fs::filesystem_error& e) {
            throw std::runtime_error("Error scanning directory: " + std::string(e.what()));
        }
    }

    return results;
}

} // namespace stps
