#pragma once

#include <string>
#include <curl/curl.h>

namespace duckdb {
namespace stps {

// RAII wrapper for CURL handle
class CurlHandle {
public:
    CurlHandle();
    ~CurlHandle();

    // No copy
    CurlHandle(const CurlHandle&) = delete;
    CurlHandle& operator=(const CurlHandle&) = delete;

    CURL* handle() const { return curl; }

private:
    CURL* curl;
};

// RAII wrapper for curl_slist (headers)
class CurlHeaders {
public:
    CurlHeaders() : headers(nullptr) {}
    ~CurlHeaders();

    // No copy
    CurlHeaders(const CurlHeaders&) = delete;
    CurlHeaders& operator=(const CurlHeaders&) = delete;

    void append(const std::string& header);
    curl_slist* list() const { return headers; }

private:
    curl_slist* headers;
};

// Write callback for curl response data
size_t curl_write_callback(void* contents, size_t size, size_t nmemb, std::string* userp);

// Make HTTP POST request with JSON payload
// Returns: response body as string, or "ERROR: ..." on failure
std::string curl_post_json(const std::string& url,
                           const std::string& json_payload,
                           const CurlHeaders& headers,
                           long* http_code_out = nullptr);

} // namespace stps
} // namespace duckdb
