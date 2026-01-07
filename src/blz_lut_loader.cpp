#include "blz_lut_loader.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include <fstream>
#include <iostream>
#include <sys/stat.h>

#ifdef _WIN32
#include <direct.h>
#define mkdir(path, mode) _mkdir(path)
#endif

namespace duckdb {
namespace stps {

BlzLutLoader::BlzLutLoader() : is_loaded_(false) {
}

BlzLutLoader& BlzLutLoader::GetInstance() {
    static BlzLutLoader instance;
    return instance;
}

std::string BlzLutLoader::GetLutFilePath() {
    if (!lut_file_path_.empty()) {
        return lut_file_path_;
    }

    // Get home directory
    const char* home = getenv("HOME");
    if (!home) {
        home = getenv("USERPROFILE"); // Windows fallback
    }

    if (!home) {
        throw std::runtime_error("Cannot determine home directory for LUT file storage");
    }

    // Store in ~/.stps/blz.lut
    std::string stps_dir = std::string(home) + "/.stps";
    lut_file_path_ = stps_dir + "/" + LUT_FILENAME;

    return lut_file_path_;
}

bool BlzLutLoader::EnsureLutDirectory() {
    const char* home = getenv("HOME");
    if (!home) {
        home = getenv("USERPROFILE"); // Windows fallback
    }

    if (!home) {
        return false;
    }

    std::string stps_dir = std::string(home) + "/.stps";

    // Try to create directory if it doesn't exist
    // Use platform-specific mkdir
#ifdef _WIN32
    _mkdir(stps_dir.c_str());
#else
    mkdir(stps_dir.c_str(), 0755);
#endif

    return true;
}

bool BlzLutLoader::FileExists(const std::string& path) {
    std::ifstream f(path);
    return f.good();
}

bool BlzLutLoader::DownloadLutFile(const std::string& dest_path) {
    try {
        // Ensure directory exists
        if (!EnsureLutDirectory()) {
            std::cerr << "Failed to create LUT directory" << std::endl;
            return false;
        }

        std::cout << "Downloading BLZ LUT file from " << LUT_DOWNLOAD_URL << "..." << std::endl;

        // Open output file
        std::ofstream output_file(dest_path, std::ios::binary);
        if (!output_file) {
            std::cerr << "Failed to open output file: " << dest_path << std::endl;
            return false;
        }

        // Use curl or wget as fallback for now
        // TODO: Use DuckDB's HTTP client once we have proper context
        std::string download_cmd = "curl -L -o \"" + dest_path + "\" \"" + LUT_DOWNLOAD_URL + "\"";

        int result = system(download_cmd.c_str());
        if (result != 0) {
            std::cerr << "Failed to download LUT file using curl, trying wget..." << std::endl;
            download_cmd = "wget -O \"" + dest_path + "\" \"" + LUT_DOWNLOAD_URL + "\"";
            result = system(download_cmd.c_str());

            if (result != 0) {
                std::cerr << "Failed to download LUT file with both curl and wget" << std::endl;
                return false;
            }
        }

        std::cout << "Successfully downloaded BLZ LUT file to " << dest_path << std::endl;
        return true;

    } catch (const std::exception& e) {
        std::cerr << "Error downloading LUT file: " << e.what() << std::endl;
        return false;
    }
}

bool BlzLutLoader::ParseLutFile(const std::string& file_path) {
    // TODO: Implement LUT file parsing
    // The LUT file format is a compressed binary format from kontocheck
    // For now, we'll return false to indicate parsing is not yet implemented

    std::cerr << "LUT file parsing not yet implemented" << std::endl;
    std::cerr << "The LUT file is a binary format that requires specific parsing logic" << std::endl;
    std::cerr << "For now, users must specify method_id explicitly when validating German IBANs" << std::endl;

    return false;
}

bool BlzLutLoader::LoadLutFile(const std::string& file_path) {
    if (!FileExists(file_path)) {
        std::cerr << "LUT file does not exist: " << file_path << std::endl;
        return false;
    }

    return ParseLutFile(file_path);
}

bool BlzLutLoader::Initialize(ClientContext &context) {
    if (is_loaded_) {
        return true; // Already loaded
    }

    std::string lut_path = GetLutFilePath();

    // Check if LUT file exists
    if (!FileExists(lut_path)) {
        std::cout << "BLZ LUT file not found at " << lut_path << std::endl;
        std::cout << "Attempting to download..." << std::endl;

        if (!DownloadLutFile(lut_path)) {
            std::cerr << "Failed to download BLZ LUT file" << std::endl;
            std::cerr << "German IBAN validation will require explicit method_id parameter" << std::endl;
            return false;
        }
    }

    // Load and parse the LUT file
    if (LoadLutFile(lut_path)) {
        is_loaded_ = true;
        std::cout << "BLZ LUT file loaded successfully" << std::endl;
        return true;
    }

    std::cerr << "Failed to load BLZ LUT file" << std::endl;
    std::cerr << "German IBAN validation will require explicit method_id parameter" << std::endl;
    return false;
}

bool BlzLutLoader::LookupCheckMethod(const std::string& blz, uint8_t& method_id) {
    if (!is_loaded_) {
        return false;
    }

    auto it = banks_.find(blz);
    if (it == banks_.end()) {
        return false;
    }

    method_id = it->second.check_method;
    return true;
}

bool BlzLutLoader::LookupBank(const std::string& blz, BankEntry& entry) {
    if (!is_loaded_) {
        return false;
    }

    auto it = banks_.find(blz);
    if (it == banks_.end()) {
        return false;
    }

    entry = it->second;
    return true;
}

} // namespace stps
} // namespace duckdb
