#pragma once

#include "duckdb/main/extension/extension_loader.hpp"
#include <string>
#include <unordered_set>

namespace duckdb {
namespace stps {

class PlzLoader {
public:
    static PlzLoader& GetInstance();

    bool IsLoaded() const { return loaded_; }
    void EnsureLoaded();
    bool PlzExists(const std::string& plz) const;
    std::string GetPlzFilePath();

    // Configurable PLZ gist URL
    void SetPlzGistUrl(const std::string& url);
    std::string GetPlzGistUrl() const;

    // Reset the loader to reload data with new URL
    void Reset();

private:
    PlzLoader();
    PlzLoader(const PlzLoader&) = delete;
    PlzLoader& operator=(const PlzLoader&) = delete;

    std::unordered_set<std::string> valid_plz_codes_;
    bool loaded_ = false;
    std::string plz_file_path_;
    std::string plz_download_url_;

    static constexpr const char* DEFAULT_PLZ_DOWNLOAD_URL =
        "https://gist.githubusercontent.com/jbspeakr/4565964/raw/";
    static constexpr const char* PLZ_FILENAME = "plz.csv";

    bool EnsurePlzDirectory();
    bool FileExists(const std::string& path);
    bool DownloadPlzFile(const std::string& path);
    bool LoadFromFile(const std::string& path);
};

bool is_valid_plz_format(const std::string& plz);
void RegisterPlzValidationFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
