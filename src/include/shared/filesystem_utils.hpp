#pragma once

#include "duckdb/common/file_system.hpp"
#include <string>
#include <cstdint>

namespace stps {
namespace shared {

struct FileInfo {
    std::string name;
    std::string path;
    std::string type;  // "file" or "directory"
    int64_t size;
    int64_t modified_time;
    std::string extension;
    std::string parent_directory;
};

class FileSystemUtils {
public:
    // Get file stats from a path
    static FileInfo GetFileStats(duckdb::FileSystem& fs, const std::string& path, bool is_directory);

    // Check if a filename is hidden (starts with .)
    static bool IsHidden(const std::string& filename);

    // Extract file extension (without dot)
    static std::string GetExtension(duckdb::FileSystem& fs, const std::string& path);

    // Get the last modified time as Unix timestamp
    static int64_t GetModifiedTime(duckdb::FileSystem& fs, const std::string& path);

    // Extract the name from a path
    static std::string GetName(duckdb::FileSystem& fs, const std::string& path);

    // Get the parent directory from a path
    static std::string GetParentDirectory(duckdb::FileSystem& fs, const std::string& path);
};

} // namespace shared
} // namespace stps
