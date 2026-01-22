#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace stps {

void RegisterDropDuplicatesFunction(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
