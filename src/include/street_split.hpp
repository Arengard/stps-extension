#pragma once

#include "duckdb/main/extension/extension_loader.hpp"
#include <string>

namespace duckdb {
namespace stps {

struct StreetParseResult {
    std::string street_name;
    std::string street_number;
    bool has_number;
};

StreetParseResult parse_street_address(const std::string& input);
void RegisterStreetSplitFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
