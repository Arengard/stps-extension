#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace stps {

// Register AI-related functions (OpenAI ChatGPT integration)
void RegisterAIFunctions(ExtensionLoader& loader);

// API key management
void SetOpenAIApiKey(const std::string& key);
std::string GetOpenAIApiKey();

} // namespace stps
} // namespace duckdb
