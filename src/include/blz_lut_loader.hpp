#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <cstdint>
#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Structure representing a bank entry from the BLZ LUT
struct BankEntry {
    std::string blz;              // 8-digit bank routing number
    uint8_t check_method;         // Prüfziffermethode (00-C6)
    std::string bank_name;        // Bank name
    std::string city;             // City
    std::string bic;              // BIC/SWIFT code (optional)

    BankEntry() : check_method(0) {}
};

// BLZ LUT Loader class
class BlzLutLoader {
public:
    // Get singleton instance
    static BlzLutLoader& GetInstance();

    // Initialize the LUT loader (download if needed, load file)
    bool Initialize(ClientContext &context);

    // Look up check method for a given BLZ
    // Returns true if found, false otherwise
    bool LookupCheckMethod(const std::string& blz, uint8_t& method_id);

    // Look up full bank entry for a given BLZ
    bool LookupBank(const std::string& blz, BankEntry& entry);

    // Check if LUT is loaded
    bool IsLoaded() const { return is_loaded_; }

    // Get path to LUT file
    std::string GetLutFilePath();

    // Download LUT file from source
    bool DownloadLutFile(const std::string& dest_path);

    // Load LUT file into memory
    bool LoadLutFile(const std::string& file_path);

private:
    BlzLutLoader();
    ~BlzLutLoader() = default;

    // Disable copy/move
    BlzLutLoader(const BlzLutLoader&) = delete;
    BlzLutLoader& operator=(const BlzLutLoader&) = delete;

    // Internal helpers
    bool EnsureLutDirectory();
    bool FileExists(const std::string& path);
    bool ParseLutFile(const std::string& file_path);

    // Format 2.0 parsing
    bool ParseFormat20(const std::vector<uint8_t>& buffer);
    bool DecompressBlock(const std::vector<uint8_t>& file_data,
                         uint32_t offset, uint32_t size,
                         std::vector<uint8_t>& output);

    // LUT parsing helpers
    uint32_t Adler32(const uint8_t* data, size_t len);
    uint32_t DecodeDelta(uint8_t delta_byte, uint32_t prev_blz, const uint8_t*& ptr);
    std::string FormatBlz(uint32_t blz);

    // Data members
    bool is_loaded_;
    std::unordered_map<std::string, uint8_t> blz_to_method_;  // BLZ -> method_id
    std::unordered_map<std::string, BankEntry> banks_;       // Full bank info (future use)
    std::string lut_file_path_;
    int entry_count_;

    // Constants
    static constexpr const char* LUT_DOWNLOAD_URL = "https://www.michael-plugge.de/blz.lut";
    static constexpr const char* LUT_FILENAME = "blz.lut";
};

} // namespace stps
} // namespace duckdb
