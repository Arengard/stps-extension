#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "shared/filesystem_utils.hpp"
#include <string>
#include <vector>

namespace stps {

struct ScanFunctionOptions {
    // PATH() parameters
    std::string base_path;
    bool recursive = false;
    std::string file_type;
    std::string pattern;
    int max_depth = -1;
    bool include_hidden = false;

    // Advanced SCAN() parameters
    int64_t min_size = -1;
    int64_t max_size = -1;
    int64_t min_date = -1;
    int64_t max_date = -1;
    std::string content_search;
};

class ScanScanner {
public:
    static std::vector<stps::shared::FileInfo> ScanPath(duckdb::FileSystem& fs, const ScanFunctionOptions& options);

private:
    static void ScanRecursive(duckdb::FileSystem& fs, const std::string& path, const ScanFunctionOptions& options,
                              std::vector<stps::shared::FileInfo>& results, int current_depth);
    static bool PassesBasicFilters(duckdb::FileSystem& fs, const std::string& path, bool is_directory,
                                    const ScanFunctionOptions& options);
    static bool PassesAdvancedFilters(duckdb::FileSystem& fs, const std::string& path,
                                       const ScanFunctionOptions& options);
};

} // namespace stps
