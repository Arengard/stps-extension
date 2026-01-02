#include "shared/filesystem_utils.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_open_flags.hpp"

namespace stps {
namespace shared {

FileInfo FileSystemUtils::GetFileStats(duckdb::FileSystem& fs, const std::string& path, bool is_directory) {
    FileInfo info;

    info.name = GetName(fs, path);
    info.path = path;
    info.type = is_directory ? "directory" : "file";
    info.parent_directory = GetParentDirectory(fs, path);

    // Get size (0 for directories)
    if (!is_directory) {
        try {
            auto handle = fs.OpenFile(path, duckdb::FileOpenFlags::FILE_FLAGS_READ);
            info.size = fs.GetFileSize(*handle);
        } catch (...) {
            info.size = 0;
        }
    } else {
        info.size = 0;
    }

    // Get last modified time
    info.modified_time = GetModifiedTime(fs, path);

    // Get extension
    info.extension = GetExtension(fs, path);

    return info;
}

bool FileSystemUtils::IsHidden(const std::string& filename) {
    if (filename.empty()) {
        return false;
    }
    // Hidden files start with . (but not . or ..)
    return filename[0] == '.' && filename != "." && filename != "..";
}

std::string FileSystemUtils::GetExtension(duckdb::FileSystem& fs, const std::string& path) {
    std::string ext = fs.ExtractExtension(path);
    return ext;
}

int64_t FileSystemUtils::GetModifiedTime(duckdb::FileSystem& fs, const std::string& path) {
    try {
        auto handle = fs.OpenFile(path, duckdb::FileOpenFlags::FILE_FLAGS_READ);
        auto timestamp = fs.GetLastModifiedTime(*handle);
        // Convert DuckDB timestamp_t to Unix timestamp (seconds since epoch)
        // DuckDB timestamp is in microseconds since epoch
        return timestamp.value / 1000000;
    } catch (...) {
        return 0;
    }
}

std::string FileSystemUtils::GetName(duckdb::FileSystem& fs, const std::string& path) {
    return fs.ExtractName(path);
}

std::string FileSystemUtils::GetParentDirectory(duckdb::FileSystem& fs, const std::string& path) {
    // Find the last path separator
    std::string separator = fs.PathSeparator(path);
    size_t pos = path.find_last_of(separator);
    if (pos == std::string::npos) {
        return "";
    }
    return path.substr(0, pos);
}

} // namespace shared
} // namespace stps
