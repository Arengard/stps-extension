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
    std::string GetPlzFilePath() const;

    // Reset the loader to reload data
    void Reset();

private:
    PlzLoader();
    PlzLoader(const PlzLoader&) = delete;
    PlzLoader& operator=(const PlzLoader&) = delete;

    std::unordered_set<std::string> valid_plz_codes_;
    bool loaded_ = false;

    // Path to the local PLZ file (C:\stps\Postleitzahlen.txt on Windows, /stps/Postleitzahlen.txt on Unix)
#ifdef _WIN32
    static constexpr const char* PLZ_FILE_PATH = "C:\\stps\\Postleitzahlen.txt";
#else
    static constexpr const char* PLZ_FILE_PATH = "/stps/Postleitzahlen.txt";
#endif

    bool FileExists(const std::string& path) const;
    bool LoadFromFile(const std::string& path);
};

bool is_valid_plz_format(const std::string& plz);
void RegisterPlzValidationFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
