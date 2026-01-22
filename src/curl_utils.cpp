#include "curl_utils.hpp"
#include <sstream>
#include <fstream>
#include <cstdlib>

#ifdef _WIN32
#include <windows.h>
#endif

namespace duckdb {
namespace stps {

// Check if a file exists
static bool FileExists(const std::string& path) {
    std::ifstream f(path);
    return f.good();
}

// Configure SSL options for curl handle
static void ConfigureSSL(CURL* curl) {
#ifdef _WIN32
    // With Schannel backend (Windows native TLS), certificates are handled by Windows
    // automatically using the system certificate store. No CA bundle file needed.

    // Disable certificate revocation check (often fails in corporate environments
    // due to firewall/proxy issues with OCSP/CRL endpoints)
    curl_easy_setopt(curl, CURLOPT_SSL_OPTIONS, CURLSSLOPT_NO_REVOKE);

    // Enable SSL verification - Schannel uses Windows certificate store
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);

    // If user specifies a custom CA bundle via environment, use it
    // This is a fallback for special cases (e.g., corporate proxy with custom CA)
    const char* env_ca = std::getenv("CURL_CA_BUNDLE");
    if (!env_ca) {
        env_ca = std::getenv("SSL_CERT_FILE");
    }
    if (env_ca && FileExists(env_ca)) {
        curl_easy_setopt(curl, CURLOPT_CAINFO, env_ca);
    }

#else
    // Non-Windows: system CA store is usually configured correctly
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
#endif
}

// ============================================================================
// CurlHandle
// ============================================================================

CurlHandle::CurlHandle() {
    curl = curl_easy_init();
}

CurlHandle::~CurlHandle() {
    if (curl) {
        curl_easy_cleanup(curl);
    }
}

// ============================================================================
// CurlHeaders
// ============================================================================

CurlHeaders::~CurlHeaders() {
    if (headers) {
        curl_slist_free_all(headers);
    }
}

void CurlHeaders::append(const std::string& header) {
    headers = curl_slist_append(headers, header.c_str());
}

// ============================================================================
// Callback and POST function
// ============================================================================

size_t curl_write_callback(void* contents, size_t size, size_t nmemb, std::string* userp) {
    size_t total_size = size * nmemb;
    userp->append((char*)contents, total_size);
    return total_size;
}

std::string curl_post_json(const std::string& url,
                           const std::string& json_payload,
                           const CurlHeaders& headers,
                           long* http_code_out) {
    CurlHandle handle;
    if (!handle.handle()) {
        return "ERROR: Failed to initialize curl";
    }

    std::string response;

    // Set options
    curl_easy_setopt(handle.handle(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(handle.handle(), CURLOPT_POSTFIELDS, json_payload.c_str());
    curl_easy_setopt(handle.handle(), CURLOPT_HTTPHEADER, headers.list());
    curl_easy_setopt(handle.handle(), CURLOPT_WRITEFUNCTION, curl_write_callback);
    curl_easy_setopt(handle.handle(), CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(handle.handle(), CURLOPT_TIMEOUT, 90L);
    curl_easy_setopt(handle.handle(), CURLOPT_FOLLOWLOCATION, 1L);

    // Configure SSL
    ConfigureSSL(handle.handle());

    // Perform request
    CURLcode res = curl_easy_perform(handle.handle());

    if (res != CURLE_OK) {
        std::ostringstream err;
        err << "ERROR: curl request failed: " << curl_easy_strerror(res);
        return err.str();
    }

    // Get HTTP status code
    long http_code = 0;
    curl_easy_getinfo(handle.handle(), CURLINFO_RESPONSE_CODE, &http_code);

    if (http_code_out) {
        *http_code_out = http_code;
    }

    if (http_code < 200 || http_code >= 300) {
        std::ostringstream err;
        err << "ERROR: HTTP " << http_code << " - " << response;
        return err.str();
    }

    return response;
}

std::string curl_get(const std::string& url,
                     const CurlHeaders& headers,
                     long* http_code_out) {
    CurlHandle handle;
    if (!handle.handle()) {
        return "ERROR: Failed to initialize curl";
    }

    std::string response;

    // Set options
    curl_easy_setopt(handle.handle(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(handle.handle(), CURLOPT_HTTPHEADER, headers.list());
    curl_easy_setopt(handle.handle(), CURLOPT_WRITEFUNCTION, curl_write_callback);
    curl_easy_setopt(handle.handle(), CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(handle.handle(), CURLOPT_TIMEOUT, 30L);  // 30s timeout for searches
    curl_easy_setopt(handle.handle(), CURLOPT_FOLLOWLOCATION, 1L);

    // Configure SSL
    ConfigureSSL(handle.handle());

    // Perform request
    CURLcode res = curl_easy_perform(handle.handle());

    if (res != CURLE_OK) {
        std::ostringstream err;
        err << "ERROR: curl request failed: " << curl_easy_strerror(res);
        return err.str();
    }

    // Get HTTP status code
    long http_code = 0;
    curl_easy_getinfo(handle.handle(), CURLINFO_RESPONSE_CODE, &http_code);

    if (http_code_out) {
        *http_code_out = http_code;
    }

    if (http_code < 200 || http_code >= 300) {
        std::ostringstream err;
        err << "ERROR: HTTP " << http_code << " - " << response;
        return err.str();
    }

    return response;
}

} // namespace stps
} // namespace duckdb
