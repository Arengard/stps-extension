#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "shared/filesystem_utils.hpp"
#include <string>
#include <vector>

namespace stps {

struct PathOptions {
    std::string base_path;
    bool recursive = false;
    std::string file_type;
    std::string pattern;
    int max_depth = -1;
    bool include_hidden = false;
};

class PathScanner {
public:
    static std::vector<stps::shared::FileInfo> ScanPath(duckdb::FileSystem& fs, const PathOptions& options);

private:
    static void ScanRecursive(duckdb::FileSystem& fs, const std::string& path, const PathOptions& options,
                              std::vector<stps::shared::FileInfo>& results, int current_depth);
    static bool PassesFilters(duckdb::FileSystem& fs, const std::string& path, bool is_directory,
                              const PathOptions& options);
};

} // namespace stps
