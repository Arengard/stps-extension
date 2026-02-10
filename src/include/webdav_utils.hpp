#pragma once

#include "curl_utils.hpp"
#include <string>
#include <vector>

namespace duckdb {
namespace stps {

// WebDAV PROPFIND entry
struct PropfindEntry {
    std::string href;
    bool is_collection;
};

// Base64 encode for Basic auth
std::string Base64Encode(const std::string &input);

// Get detailed HTTP error message
std::string GetHttpErrorMessage(long http_code, const std::string &url);

// Get detailed curl error message
std::string GetCurlErrorDetails(const std::string &error);

// Percent-encode URL path (preserves already-encoded sequences)
std::string PercentEncodePath(const std::string &path);

// Normalize a full URL by percent-encoding its path component
std::string NormalizeRequestUrl(const std::string &url);

// Decode percent-encoded URL path segments
std::string PercentDecodePath(const std::string &encoded);

// Parse WebDAV PROPFIND XML response into entries
std::vector<PropfindEntry> ParsePropfindResponse(const std::string &xml);

// Extract the last path segment from a URL path (filename or folder name)
std::string GetLastPathSegment(const std::string &path);

// Extract base URL (scheme + host) from full URL
std::string GetBaseUrl(const std::string &url);

// Build auth headers for PROPFIND/GET requests
void BuildAuthHeaders(CurlHeaders &headers, const std::string &username, const std::string &password);

// PROPFIND request body for depth-1 directory listing
extern const char* PROPFIND_BODY;

} // namespace stps
} // namespace duckdb
