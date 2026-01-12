#include "plz_validation.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <fstream>
#include <sstream>
#include <iostream>
#include <cctype>
#include <algorithm>

namespace duckdb {
namespace stps {

PlzLoader::PlzLoader() : loaded_(false) {
}

PlzLoader& PlzLoader::GetInstance() {
    static PlzLoader instance;
    return instance;
}

void PlzLoader::Reset() {
    loaded_ = false;
    valid_plz_codes_.clear();
}

std::string PlzLoader::GetPlzFilePath() const {
    return PLZ_FILE_PATH;
}

bool PlzLoader::FileExists(const std::string& path) const {
    std::ifstream f(path);
    return f.good();
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
        bool first_line = true;

        while (std::getline(file, line)) {
            // Skip the first line (version header like #Version:01.2026)
            if (first_line) {
                first_line = false;
                continue;
            }

            if (line.empty()) continue;

            // Trim whitespace
            size_t start = line.find_first_not_of(" \t\r\n");
            size_t end = line.find_last_not_of(" \t\r\n");
            if (start == std::string::npos) continue;

            std::string plz = line.substr(start, end - start + 1);

            // Valid PLZ is exactly 5 digits
            if (plz.length() == 5 && std::all_of(plz.begin(), plz.end(), ::isdigit)) {
                valid_plz_codes_.insert(plz);
            }
        }

        std::cout << "Loaded " << valid_plz_codes_.size() << " unique PLZ codes from " << path << std::endl;
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
        throw std::runtime_error("PLZ file not found at " + plz_path + 
            ". Please create the file with format: first line is version header (e.g., #Version:01.2026), followed by one PLZ per line.");
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
}

} // namespace stps
} // namespace duckdb
