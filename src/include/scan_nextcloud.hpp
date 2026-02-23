#pragma once
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace stps {
void RegisterScanNextcloudFunction(ExtensionLoader &loader);
}
}
