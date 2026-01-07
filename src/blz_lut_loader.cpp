#include "blz_lut_loader.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include <fstream>
#include <iostream>
#include <vector>
#include <cstring>
#include <sys/stat.h>

#ifdef _WIN32
#include <direct.h>
#define mkdir(path, mode) _mkdir(path)
#endif

namespace duckdb {
namespace stps {

BlzLutLoader::BlzLutLoader() : is_loaded_(false), entry_count_(0) {
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

// Adler32 checksum implementation (from zlib/RFC 1950)
uint32_t BlzLutLoader::Adler32(const uint8_t* data, size_t len) {
    const uint32_t MOD_ADLER = 65521;
    uint32_t a = 1, b = 0;

    for (size_t i = 0; i < len; ++i) {
        a = (a + data[i]) % MOD_ADLER;
        b = (b + a) % MOD_ADLER;
    }

    return (b << 16) | a;
}

// Decode delta-encoded BLZ value
uint32_t BlzLutLoader::DecodeDelta(uint8_t delta_byte, uint32_t prev_blz, const uint8_t*& ptr) {
    switch (delta_byte) {
        case 0 ... 250:
            // Add small delta to previous
            return prev_blz + delta_byte;

        case 251: {
            // 2-byte difference (subtract)
            uint16_t diff = ptr[0] | (ptr[1] << 8);
            ptr += 2;
            return prev_blz - diff;
        }

        case 252: {
            // 1-byte difference (subtract)
            uint8_t diff = ptr[0];
            ptr += 1;
            return prev_blz - diff;
        }

        case 253: {
            // 4-byte absolute value
            uint32_t blz = ptr[0] | (ptr[1] << 8) | (ptr[2] << 16) | (ptr[3] << 24);
            ptr += 4;
            return blz;
        }

        case 254: {
            // 2-byte difference (add)
            uint16_t diff = ptr[0] | (ptr[1] << 8);
            ptr += 2;
            return prev_blz + diff;
        }

        default:  // 255 or other
            throw std::runtime_error("Invalid delta byte: " + std::to_string(delta_byte));
    }
}

// Format BLZ as 8-digit string with leading zeros
std::string BlzLutLoader::FormatBlz(uint32_t blz) {
    char buf[9];
    snprintf(buf, sizeof(buf), "%08u", blz);
    return std::string(buf);
}

bool BlzLutLoader::ParseLutFile(const std::string& file_path) {
    // NOTE: This parser currently supports Format 1.0/1.1 but Format 2.0 uses
    // a completely different structure (block-based with directory/index).
    // The current file from michael-plugge.de is Format 2.0, so parsing will
    // fail. German IBAN validation will fall back to MOD-97 only until Format
    // 2.0 parser is implemented.
    //
    // TODO: Implement Format 2.0 parser (see kontocheck LUT 2.0 documentation)

    try {
        // Read entire file into memory
        std::ifstream file(file_path, std::ios::binary | std::ios::ate);
        if (!file) {
            std::cerr << "Failed to open LUT file: " << file_path << std::endl;
            return false;
        }

        std::streamsize file_size = file.tellg();
        file.seekg(0, std::ios::beg);

        std::vector<uint8_t> buffer(file_size);
        if (!file.read(reinterpret_cast<char*>(buffer.data()), file_size)) {
            std::cerr << "Failed to read LUT file" << std::endl;
            return false;
        }

        const uint8_t* ptr = buffer.data();
        const uint8_t* end = ptr + file_size;

        // Verify header signature (support formats 1.0, 1.1, and 2.0)
        const char* sig20 = "BLZ Lookup Table/Format 2.0\n";
        const char* sig11 = "BLZ Lookup Table/Format 1.1\n";
        const char* sig10 = "BLZ Lookup Table/Format 1.0\n";

        if (file_size < 28 ||
            (memcmp(ptr, sig20, 28) != 0 &&
             memcmp(ptr, sig11, 28) != 0 &&
             memcmp(ptr, sig10, 28) != 0)) {
            std::cerr << "Invalid LUT file signature" << std::endl;
            return false;
        }

        ptr += 28;  // Skip signature

        // Skip info line (ends with \n)
        while (ptr < end && *ptr != '\n') {
            ptr++;
        }
        if (ptr < end) ptr++;  // Skip the \n

        // Check if there's a second line (for format 1.1)
        if (ptr > buffer.data() + 29 && *(ptr - 2) == '\\') {
            // Multi-line info, skip second line
            while (ptr < end && *ptr != '\n') {
                ptr++;
            }
            if (ptr < end) ptr++;
        }

        // Read count (4 bytes, little-endian)
        if (ptr + 8 > end) {
            std::cerr << "LUT file too short for header" << std::endl;
            return false;
        }

        uint32_t count = ptr[0] | (ptr[1] << 8) | (ptr[2] << 16) | (ptr[3] << 24);
        ptr += 4;

        // Read checksum (4 bytes, little-endian)
        uint32_t stored_checksum = ptr[0] | (ptr[1] << 8) | (ptr[2] << 16) | (ptr[3] << 24);
        ptr += 4;

        // Verify checksum
        size_t data_len = end - ptr;
        uint32_t calc_checksum = Adler32(ptr, data_len) ^ count;

        if (calc_checksum != stored_checksum) {
            std::cerr << "Warning: LUT file checksum mismatch (Format 2.0 may use different algorithm)" << std::endl;
            std::cerr << "  Expected: 0x" << std::hex << stored_checksum << std::endl;
            std::cerr << "  Calculated: 0x" << std::hex << calc_checksum << std::dec << std::endl;
            std::cerr << "  Continuing anyway..." << std::endl;
            // Don't fail - Format 2.0 might use a different checksum
            // return false;
        }

        // Parse delta-encoded entries
        blz_to_method_.clear();
        uint32_t prev_blz = 0;
        int parsed_count = 0;

        while (parsed_count < static_cast<int>(count) && ptr < end) {
            // Read delta byte
            uint8_t delta_byte = *ptr++;

            // Decode BLZ
            uint32_t blz = DecodeDelta(delta_byte, prev_blz, ptr);

            // Check for invalid marker (0xFF)
            if (ptr < end && *ptr == 0xFF) {
                ptr += 2;  // Skip 0xFF and next byte
                prev_blz = blz;
                parsed_count++;
                continue;
            }

            // Read method ID
            if (ptr >= end) {
                std::cerr << "Unexpected end of file while reading method ID" << std::endl;
                return false;
            }

            uint8_t method_id = *ptr++;

            // Store in map
            std::string blz_str = FormatBlz(blz);
            blz_to_method_[blz_str] = method_id;

            prev_blz = blz;
            parsed_count++;
        }

        entry_count_ = blz_to_method_.size();
        std::cout << "Successfully parsed BLZ LUT file: " << entry_count_
                  << " entries loaded" << std::endl;

        return true;

    } catch (const std::exception& e) {
        std::cerr << "Error parsing LUT file: " << e.what() << std::endl;
        return false;
    }
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
    // Lazy load LUT file on first lookup
    if (!is_loaded_) {
        std::string lut_path = GetLutFilePath();
        if (!FileExists(lut_path)) {
            // File doesn't exist, can't load
            return false;
        }

        if (!ParseLutFile(lut_path)) {
            // Parsing failed
            return false;
        }

        is_loaded_ = true;
    }

    // Lookup in the hash map
    auto it = blz_to_method_.find(blz);
    if (it == blz_to_method_.end()) {
        return false;  // BLZ not found
    }

    method_id = it->second;
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
