#include "path_function.hpp"
#include "shared/filesystem_utils.hpp"
#include "shared/pattern_matcher.hpp"
#include "duckdb/common/file_system.hpp"
#include <algorithm>

namespace stps {

bool PathScanner::PassesFilters(duckdb::FileSystem& fs, const std::string& path, bool is_directory,
                                const PathOptions& options) {
    const std::string filename = shared::FileSystemUtils::GetName(fs, path);

    // Check hidden files first (cheapest)
    if (!options.include_hidden && shared::FileSystemUtils::IsHidden(filename)) {
        return false;
    }

    // Check pattern second (glob matching on name only)
    if (!options.pattern.empty()) {
        if (!shared::PatternMatcher::MatchesGlobPattern(filename, options.pattern)) {
            return false;
        }
    }

    // For directories, allow recursion but don't include in results unless matching
    if (is_directory) {
        return options.recursive;
    }

    // Check file_type third (extension comparison)
    if (!options.file_type.empty() && !is_directory) {
        std::string ext = shared::FileSystemUtils::GetExtension(fs, path);
        std::string file_type_lower = options.file_type;
        std::string ext_lower = ext;
        std::transform(file_type_lower.begin(), file_type_lower.end(), file_type_lower.begin(), ::tolower);
        std::transform(ext_lower.begin(), ext_lower.end(), ext_lower.begin(), ::tolower);

        if (ext_lower != file_type_lower) {
            return false;
        }
    }

    return true;
}

void PathScanner::ScanRecursive(duckdb::FileSystem& fs, const std::string& path, const PathOptions& options,
                                std::vector<shared::FileInfo>& results, int current_depth) {
    // Check depth limit
    if (options.max_depth >= 0 && current_depth > options.max_depth) {
        return;
    }

    try {
        // Use ListFiles to iterate through directory entries
        fs.ListFiles(path, [&](const std::string& entry_name, bool is_dir) {
            // Build full path by joining current path with entry name
            std::string full_path = fs.JoinPath(path, entry_name);
            if (PassesFilters(fs, full_path, is_dir, options)) {
                if (!is_dir) {
                    // Add file entry
                    results.push_back(shared::FileSystemUtils::GetFileStats(fs, full_path, false));
                } else if (options.recursive) {
                    // Add directory entry
                    results.push_back(shared::FileSystemUtils::GetFileStats(fs, full_path, true));
                    // Recurse into subdirectory
                    ScanRecursive(fs, full_path, options, results, current_depth + 1);
                }
            } else if (is_dir && options.recursive) {
                // Even if directory doesn't match filter, still recurse
                ScanRecursive(fs, full_path, options, results, current_depth + 1);
            }
        });
    } catch (...) {
        // Skip directories we can't access (permission denied, etc.)
    }
}

std::vector<shared::FileInfo> PathScanner::ScanPath(duckdb::FileSystem& fs, const PathOptions& options) {
    std::vector<shared::FileInfo> results;
    results.reserve(256); // Pre-allocate for typical case

    // Validate path
    if (!fs.DirectoryExists(options.base_path)) {
        if (!fs.FileExists(options.base_path)) {
            throw std::runtime_error("Path does not exist: " + options.base_path);
        } else {
            throw std::runtime_error("Path is not a directory: " + options.base_path);
        }
    }

    if (options.recursive) {
        ScanRecursive(fs, options.base_path, options, results, 0);
    } else {
        // Non-recursive scan (optimized path)
        try {
            fs.ListFiles(options.base_path, [&](const std::string& entry_path, bool is_dir) {
                // Build full path by joining base_path with entry
                std::string full_path = fs.JoinPath(options.base_path, entry_path);
                if (PassesFilters(fs, full_path, is_dir, options)) {
                    results.push_back(shared::FileSystemUtils::GetFileStats(fs, full_path, is_dir));
                }
            });
        } catch (const std::exception& e) {
            throw std::runtime_error("Error scanning directory: " + std::string(e.what()));
        }
    }

    return results;
}

} // namespace stps
