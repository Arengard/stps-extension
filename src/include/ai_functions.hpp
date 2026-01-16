#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Register AI-related functions (Anthropic Claude integration)
void RegisterAIFunctions(ExtensionLoader& loader);

// API key management
void SetAnthropicApiKey(const std::string& key);
std::string GetAnthropicApiKey();

// Model management
void SetAnthropicModel(const std::string& model);
std::string GetAnthropicModel();

} // namespace stps
} // namespace duckdb
