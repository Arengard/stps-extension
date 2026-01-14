#pragma once

#include "duckdb/main/extension/extension_loader.hpp"
#include <string>

namespace duckdb {
namespace stps {

struct AddressResult {
    std::string city;
    std::string plz;
    std::string full_address;
    std::string street_name;
    std::string street_number;
    bool found = false;
};

// Main lookup function - searches Google for company Impressum and parses address
AddressResult lookup_company_address(const std::string& company_name);

// Register the DuckDB function
void RegisterAddressLookupFunctions(ExtensionLoader& loader);

} // namespace stps
} // namespace duckdb
