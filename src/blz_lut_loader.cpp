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
#include <zlib.h>

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
    // Handle common case: 0-250 means add to previous
    if (delta_byte <= 250) {
        return prev_blz + delta_byte;
    }

    switch (delta_byte) {
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

// Parse Format 2.0 LUT file (block-based, zlib compressed)
bool BlzLutLoader::ParseFormat20(const std::vector<uint8_t>& buffer) {
    try {
        const uint8_t* ptr = buffer.data();
        const uint8_t* end = ptr + buffer.size();

        // Skip signature line
        ptr += 28;

        // Skip info lines until we find "DATA\n"
        while (ptr < end - 5) {
            if (memcmp(ptr, "DATA\n", 5) == 0) {
                ptr += 5;
                break;
            }
            // Skip to next line
            while (ptr < end && *ptr != '\n') ptr++;
            if (ptr < end) ptr++;
        }

        if (ptr >= end) {
            std::cerr << "Format 2.0: DATA marker not found" << std::endl;
            return false;
        }

        // Read directory: 2 bytes slot count
        if (ptr + 2 > end) {
            std::cerr << "Format 2.0: File too short for directory" << std::endl;
            return false;
        }

        uint16_t slot_count = ptr[0] | (ptr[1] << 8);
        ptr += 2;

        // Read directory entries (12 bytes each: 4 type, 4 offset, 4 size)
        if (ptr + (slot_count * 12) > end) {
            std::cerr << "Format 2.0: File too short for directory entries" << std::endl;
            return false;
        }

        uint32_t blz_block_offset = 0, blz_block_size = 0;
        uint32_t method_block_offset = 0, method_block_size = 0;

        for (int i = 0; i < slot_count; i++) {
            uint32_t block_type = ptr[0] | (ptr[1] << 8) | (ptr[2] << 16) | (ptr[3] << 24);
            uint32_t offset = ptr[4] | (ptr[5] << 8) | (ptr[6] << 16) | (ptr[7] << 24);
            uint32_t size = ptr[8] | (ptr[9] << 8) | (ptr[10] << 16) | (ptr[11] << 24);
            ptr += 12;

            if (block_type == 1) {  // BLZ block
                blz_block_offset = offset;
                blz_block_size = size;
            } else if (block_type == 2) {  // Method block
                method_block_offset = offset;
                method_block_size = size;
            }
        }

        if (blz_block_offset == 0 || method_block_offset == 0) {
            std::cerr << "Format 2.0: Required blocks not found (BLZ or Method)" << std::endl;
            return false;
        }

        // Decompress BLZ block (block 1)
        std::vector<uint8_t> blz_data;
        if (!DecompressBlock(buffer, blz_block_offset, blz_block_size, blz_data)) {
            std::cerr << "Format 2.0: Failed to decompress BLZ block" << std::endl;
            return false;
        }

        // Decompress Method block (block 2)
        std::vector<uint8_t> method_data;
        if (!DecompressBlock(buffer, method_block_offset, method_block_size, method_data)) {
            std::cerr << "Format 2.0: Failed to decompress Method block" << std::endl;
            return false;
        }

        // Parse decompressed BLZ block
        if (blz_data.size() < 4) {
            std::cerr << "Format 2.0: BLZ block too small" << std::endl;
            return false;
        }

        // Read header: bytes 0-1 = main office count, bytes 2-3 = total count
        uint16_t main_count = blz_data[0] | (blz_data[1] << 8);
        uint16_t total_count = blz_data[2] | (blz_data[3] << 8);

        std::cout << "Format 2.0: " << main_count << " main offices, "
                  << total_count << " total entries" << std::endl;

        // Parse delta-encoded BLZ codes starting at byte 4
        std::vector<uint32_t> blz_list;
        const uint8_t* blz_ptr = blz_data.data() + 4;
        const uint8_t* blz_end = blz_data.data() + blz_data.size();
        uint32_t prev_blz = 0;  // Start from 0 (per kontocheck source)

        while (blz_list.size() < main_count && blz_ptr < blz_end) {
            uint8_t delta_byte = *blz_ptr++;

            if (delta_byte <= 253) {
                // Add to previous BLZ
                prev_blz += delta_byte;
            } else if (delta_byte == 254) {
                // Read 2-byte delta, add to previous
                if (blz_ptr + 2 > blz_end) break;
                uint16_t delta = blz_ptr[0] | (blz_ptr[1] << 8);
                blz_ptr += 2;
                prev_blz += delta;
            } else if (delta_byte == 255) {
                // Read 4-byte absolute BLZ value
                if (blz_ptr + 4 > blz_end) break;
                prev_blz = blz_ptr[0] | (blz_ptr[1] << 8) | (blz_ptr[2] << 16) | (blz_ptr[3] << 24);
                blz_ptr += 4;
            }

            blz_list.push_back(prev_blz);
        }

        // Build BLZ → method map
        blz_to_method_.clear();
        size_t valid_entries = (std::min)(blz_list.size(), method_data.size());

        for (size_t i = 0; i < valid_entries; i++) {
            uint32_t blz = blz_list[i];
            uint8_t method = method_data[i];
            std::string blz_str = FormatBlz(blz);
            blz_to_method_[blz_str] = method;
        }

        entry_count_ = blz_to_method_.size();
        std::cout << "Successfully parsed Format 2.0 LUT file: " << entry_count_
                  << " entries loaded" << std::endl;

        return true;

    } catch (const std::exception& e) {
        std::cerr << "Error parsing Format 2.0 LUT file: " << e.what() << std::endl;
        return false;
    }
}

// Decompress a block using zlib
bool BlzLutLoader::DecompressBlock(const std::vector<uint8_t>& file_data,
                                    uint32_t offset, uint32_t size,
                                    std::vector<uint8_t>& output) {
    // size is the compressed data size; total block is header (16) + compressed data (size)
    if (offset + 16 + size > file_data.size()) {
        std::cerr << "Block offset/size out of range" << std::endl;
        return false;
    }

    const uint8_t* block_start = file_data.data() + offset;

    // Read 16-byte block header
    uint32_t block_type = block_start[0] | (block_start[1] << 8) | (block_start[2] << 16) | (block_start[3] << 24);
    uint32_t comp_size = block_start[4] | (block_start[5] << 8) | (block_start[6] << 16) | (block_start[7] << 24);
    uint32_t decomp_size = block_start[8] | (block_start[9] << 8) | (block_start[10] << 16) | (block_start[11] << 24);
    (void)block_type;  // Read for documentation, not currently used
    (void)comp_size;   // Read for documentation, not currently used

    // Compressed data starts after 16-byte header and is 'size' bytes long
    const uint8_t* compressed_data = block_start + 16;
    size_t compressed_len = size;

    // Allocate output buffer
    output.resize(decomp_size + 100);  // Add some extra space

    // Decompress using zlib
    z_stream strm;
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.avail_in = compressed_len;
    strm.next_in = const_cast<Bytef*>(compressed_data);
    strm.avail_out = output.size();
    strm.next_out = output.data();

    int ret = inflateInit(&strm);
    if (ret != Z_OK) {
        std::cerr << "inflateInit failed: " << ret << std::endl;
        return false;
    }

    ret = inflate(&strm, Z_FINISH);
    if (ret != Z_STREAM_END) {
        std::cerr << "inflate failed: " << ret << std::endl;
        inflateEnd(&strm);
        return false;
    }

    size_t actual_size = strm.total_out;
    inflateEnd(&strm);

    output.resize(actual_size);
    return true;
}

bool BlzLutLoader::ParseLutFile(const std::string& file_path) {
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

        // Verify header signature
        const char* sig20 = "BLZ Lookup Table/Format 2.0\n";
        const char* sig11 = "BLZ Lookup Table/Format 1.1\n";
        const char* sig10 = "BLZ Lookup Table/Format 1.0\n";

        if (file_size < 28) {
            std::cerr << "Invalid LUT file: too short" << std::endl;
            return false;
        }

        bool is_format_20 = (memcmp(ptr, sig20, 28) == 0);
        bool is_format_11 = (memcmp(ptr, sig11, 28) == 0);
        bool is_format_10 = (memcmp(ptr, sig10, 28) == 0);

        if (!is_format_20 && !is_format_11 && !is_format_10) {
            std::cerr << "Invalid LUT file signature" << std::endl;
            return false;
        }

        // Format 2.0 uses completely different structure
        if (is_format_20) {
            return ParseFormat20(buffer);
        }

        // Format 1.0/1.1 parsing (legacy)
        ptr += 28;  // Skip signature

        // Skip info line (ends with \n)
        while (ptr < end && *ptr != '\n') {
            ptr++;
        }
        if (ptr < end) ptr++;

        // Check if there's a second line (for format 1.1)
        if (ptr > buffer.data() + 29 && *(ptr - 2) == '\\') {
            while (ptr < end && *ptr != '\n') {
                ptr++;
            }
            if (ptr < end) ptr++;
        }

        // Read count and checksum
        if (ptr + 8 > end) {
            std::cerr << "LUT file too short for header" << std::endl;
            return false;
        }

        uint32_t count = ptr[0] | (ptr[1] << 8) | (ptr[2] << 16) | (ptr[3] << 24);
        ptr += 4;
        uint32_t stored_checksum = ptr[0] | (ptr[1] << 8) | (ptr[2] << 16) | (ptr[3] << 24);
        ptr += 4;

        // Verify checksum
        size_t data_len = end - ptr;
        uint32_t calc_checksum = Adler32(ptr, data_len) ^ count;

        if (calc_checksum != stored_checksum) {
            std::cerr << "LUT file checksum mismatch" << std::endl;
            return false;
        }

        // Parse delta-encoded entries
        blz_to_method_.clear();
        uint32_t prev_blz = 0;
        int parsed_count = 0;

        while (parsed_count < static_cast<int>(count) && ptr < end) {
            uint8_t delta_byte = *ptr++;
            uint32_t blz = DecodeDelta(delta_byte, prev_blz, ptr);

            // Check for invalid marker
            if (ptr < end && *ptr == 0xFF) {
                ptr += 2;
                prev_blz = blz;
                parsed_count++;
                continue;
            }

            if (ptr >= end) {
                std::cerr << "Unexpected end of file" << std::endl;
                return false;
            }

            uint8_t method_id = *ptr++;
            blz_to_method_[FormatBlz(blz)] = method_id;
            prev_blz = blz;
            parsed_count++;
        }

        entry_count_ = blz_to_method_.size();
        std::cout << "Successfully parsed Format 1.x LUT file: " << entry_count_
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
