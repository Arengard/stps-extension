#include "plz_validation.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <fstream>
#include <sstream>
#include <iostream>
#include <cctype>
#include <algorithm>
#include <cstdio>
#include <sys/stat.h>

#ifdef _WIN32
#include <direct.h>
#define mkdir(path, mode) _mkdir(path)
#endif

namespace duckdb {
namespace stps {

PlzLoader::PlzLoader() : loaded_(false), plz_download_url_(DEFAULT_PLZ_DOWNLOAD_URL) {
}

PlzLoader& PlzLoader::GetInstance() {
    static PlzLoader instance;
    return instance;
}

void PlzLoader::SetPlzGistUrl(const std::string& url) {
    if (url != plz_download_url_) {
        plz_download_url_ = url;
        // Reset the loader so it reloads with the new URL
        Reset();
    }
}

std::string PlzLoader::GetPlzGistUrl() const {
    return plz_download_url_;
}

void PlzLoader::Reset() {
    // Delete the cached PLZ file to force re-download with new URL
    if (!plz_file_path_.empty() && FileExists(plz_file_path_)) {
        std::remove(plz_file_path_.c_str());
    }
    loaded_ = false;
    valid_plz_codes_.clear();
    plz_file_path_.clear();
}

std::string PlzLoader::GetPlzFilePath() {
    if (!plz_file_path_.empty()) {
        return plz_file_path_;
    }

    const char* home = getenv("HOME");
    if (!home) {
        home = getenv("USERPROFILE"); // Windows fallback
    }

    if (!home) {
        throw std::runtime_error("Cannot determine home directory for PLZ file storage");
    }

    std::string stps_dir = std::string(home) + "/.stps";
    plz_file_path_ = stps_dir + "/" + PLZ_FILENAME;

    return plz_file_path_;
}

bool PlzLoader::EnsurePlzDirectory() {
    const char* home = getenv("HOME");
    if (!home) {
        home = getenv("USERPROFILE");
    }

    if (!home) {
        return false;
    }

    std::string stps_dir = std::string(home) + "/.stps";

#ifdef _WIN32
    _mkdir(stps_dir.c_str());
#else
    mkdir(stps_dir.c_str(), 0755);
#endif

    return true;
}

bool PlzLoader::FileExists(const std::string& path) {
    std::ifstream f(path);
    return f.good();
}

bool PlzLoader::DownloadPlzFile(const std::string& dest_path) {
    try {
        if (!EnsurePlzDirectory()) {
            std::cerr << "Failed to create PLZ directory" << std::endl;
            return false;
        }

        std::cout << "Downloading PLZ file from " << plz_download_url_ << "..." << std::endl;

        // Use curl or wget as fallback
        std::string download_cmd = "curl -L -s -o \"" + dest_path + "\" \"" + plz_download_url_ + "\"";

        int result = system(download_cmd.c_str());
        if (result != 0) {
            std::cerr << "Failed to download PLZ file using curl, trying wget..." << std::endl;
            download_cmd = "wget -q -O \"" + dest_path + "\" \"" + plz_download_url_ + "\"";
            result = system(download_cmd.c_str());

            if (result != 0) {
                std::cerr << "Failed to download PLZ file with both curl and wget" << std::endl;
                return false;
            }
        }

        std::cout << "Successfully downloaded PLZ file to " << dest_path << std::endl;
        return true;

    } catch (const std::exception& e) {
        std::cerr << "Error downloading PLZ file: " << e.what() << std::endl;
        return false;
    }
}

bool PlzLoader::LoadFromFile(const std::string& path) {
    try {
        std::ifstream file(path);
        if (!file) {
            std::cerr << "Failed to open PLZ file: " << path << std::endl;
            return false;
        }

        valid_plz_codes_.clear();
        std::string line;

        // Detect format from header line
        int plz_column = 0;  // Default: PLZ in first column
        char separator = ';';

        if (std::getline(file, line)) {
            // Detect separator
            if (line.find(';') != std::string::npos) {
                separator = ';';
            } else if (line.find(',') != std::string::npos) {
                separator = ',';
            } else if (line.find('\t') != std::string::npos) {
                separator = '\t';
            }

            // Find PLZ column in header
            std::istringstream header_stream(line);
            std::string col;
            int col_idx = 0;
            while (std::getline(header_stream, col, separator)) {
                // Trim and lowercase for comparison
                size_t start = col.find_first_not_of(" \t\r\n");
                size_t end = col.find_last_not_of(" \t\r\n");
                if (start != std::string::npos) {
                    col = col.substr(start, end - start + 1);
                }
                // Check if this column is the PLZ column
                if (col == "Plz" || col == "plz" || col == "PLZ" || col == "postal_code" || col == "zipcode") {
                    plz_column = col_idx;
                    break;
                }
                col_idx++;
            }
        }

        // Parse data lines
        while (std::getline(file, line)) {
            if (line.empty()) continue;

            // Extract the PLZ column
            std::istringstream line_stream(line);
            std::string field;
            int col_idx = 0;
            std::string plz;

            while (std::getline(line_stream, field, separator)) {
                if (col_idx == plz_column) {
                    plz = field;
                    break;
                }
                col_idx++;
            }

            // Trim whitespace
            size_t start = plz.find_first_not_of(" \t\r\n");
            size_t end = plz.find_last_not_of(" \t\r\n");
            if (start != std::string::npos) {
                plz = plz.substr(start, end - start + 1);
                // Valid PLZ is exactly 5 digits
                if (plz.length() == 5 && std::all_of(plz.begin(), plz.end(), ::isdigit)) {
                    valid_plz_codes_.insert(plz);
                }
            }
        }

        std::cout << "Loaded " << valid_plz_codes_.size() << " unique PLZ codes" << std::endl;
        return !valid_plz_codes_.empty();

    } catch (const std::exception& e) {
        std::cerr << "Error loading PLZ file: " << e.what() << std::endl;
        return false;
    }
}

void PlzLoader::EnsureLoaded() {
    if (loaded_) {
        return;
    }

    std::string plz_path = GetPlzFilePath();

    if (!FileExists(plz_path)) {
        std::cout << "PLZ file not found at " << plz_path << ", downloading..." << std::endl;
        if (!DownloadPlzFile(plz_path)) {
            throw std::runtime_error("Failed to download PLZ file. Check your internet connection.");
        }
    }

    if (!LoadFromFile(plz_path)) {
        throw std::runtime_error("Failed to load PLZ file from " + plz_path);
    }

    loaded_ = true;
}

bool PlzLoader::PlzExists(const std::string& plz) const {
    return valid_plz_codes_.find(plz) != valid_plz_codes_.end();
}

bool is_valid_plz_format(const std::string& plz) {
    // Must be exactly 5 characters
    if (plz.length() != 5) {
        return false;
    }

    // All characters must be digits
    for (char c : plz) {
        if (!std::isdigit(static_cast<unsigned char>(c))) {
            return false;
        }
    }

    // Convert to number and check range (01000 - 99999)
    int plz_num = std::stoi(plz);
    return plz_num >= 1000 && plz_num <= 99999;
}

// DuckDB scalar function - simple version (format check only)
static void StpsIsValidPlzSimpleFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, bool>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            std::string plz = input.GetString();
            return is_valid_plz_format(plz);
        });
}

// DuckDB scalar function - with strict parameter
static void StpsIsValidPlzStrictFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &plz_vector = args.data[0];
    auto &strict_vector = args.data[1];
    auto count = args.size();

    UnifiedVectorFormat plz_data, strict_data;
    plz_vector.ToUnifiedFormat(count, plz_data);
    strict_vector.ToUnifiedFormat(count, strict_data);

    auto plz_strings = UnifiedVectorFormat::GetData<string_t>(plz_data);
    auto strict_values = UnifiedVectorFormat::GetData<bool>(strict_data);

    auto result_data = FlatVector::GetData<bool>(result);
    auto &result_validity = FlatVector::Validity(result);

    for (idx_t i = 0; i < count; i++) {
        auto plz_idx = plz_data.sel->get_index(i);
        auto strict_idx = strict_data.sel->get_index(i);

        if (!plz_data.validity.RowIsValid(plz_idx)) {
            result_data[i] = false;
            continue;
        }

        std::string plz = plz_strings[plz_idx].GetString();

        // First check format
        if (!is_valid_plz_format(plz)) {
            result_data[i] = false;
            continue;
        }

        // If strict mode, check against real PLZ list
        bool strict = strict_data.validity.RowIsValid(strict_idx) && strict_values[strict_idx];
        if (strict) {
            auto& loader = PlzLoader::GetInstance();
            loader.EnsureLoaded();
            result_data[i] = loader.PlzExists(plz);
        } else {
            result_data[i] = true;
        }
    }
}

// DuckDB scalar function to set PLZ gist URL
static void StpsSetPlzGistFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            std::string url = input.GetString();
            auto& loader = PlzLoader::GetInstance();
            loader.SetPlzGistUrl(url);
            return StringVector::AddString(result, "PLZ gist URL set to: " + url);
        });
}

// DuckDB scalar function to get current PLZ gist URL
static void StpsGetPlzGistFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto& loader = PlzLoader::GetInstance();
    std::string url = loader.GetPlzGistUrl();
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
    auto result_data = ConstantVector::GetData<string_t>(result);
    result_data[0] = StringVector::AddString(result, url);
}

void RegisterPlzValidationFunctions(ExtensionLoader &loader) {
    ScalarFunctionSet plz_set("stps_is_valid_plz");

    // Simple version: stps_is_valid_plz(plz VARCHAR) -> BOOLEAN
    plz_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR},
        LogicalType::BOOLEAN,
        StpsIsValidPlzSimpleFunction
    ));

    // With strict parameter: stps_is_valid_plz(plz VARCHAR, strict BOOLEAN) -> BOOLEAN
    plz_set.AddFunction(ScalarFunction(
        {LogicalType::VARCHAR, LogicalType::BOOLEAN},
        LogicalType::BOOLEAN,
        StpsIsValidPlzStrictFunction
    ));

    loader.RegisterFunction(plz_set);

    // Register stps_set_plz_gist function: stps_set_plz_gist(url VARCHAR) -> VARCHAR
    auto set_plz_gist_function = ScalarFunction(
        "stps_set_plz_gist",
        {LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        StpsSetPlzGistFunction
    );
    loader.RegisterFunction(set_plz_gist_function);

    // Register stps_get_plz_gist function: stps_get_plz_gist() -> VARCHAR
    auto get_plz_gist_function = ScalarFunction(
        "stps_get_plz_gist",
        {},
        LogicalType::VARCHAR,
        StpsGetPlzGistFunction
    );
    loader.RegisterFunction(get_plz_gist_function);
}

} // namespace stps
} // namespace duckdb
