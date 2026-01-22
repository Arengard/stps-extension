#include "curl_utils.hpp"
#include <sstream>
#include <fstream>

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
    // On Windows with vcpkg curl (OpenSSL backend), we need to handle SSL carefully
    // Try multiple approaches for maximum compatibility

    // Option 1: Try to use Windows native CA store (works if curl built with Schannel)
    curl_easy_setopt(curl, CURLOPT_SSL_OPTIONS, CURLSSLOPT_NATIVE_CA | CURLSSLOPT_NO_REVOKE);

    // Option 2: Try common CA bundle locations for OpenSSL-based curl
    static const char* ca_paths[] = {
        "C:\\vcpkg\\installed\\x64-windows\\share\\curl\\curl-ca-bundle.crt",
        "C:\\Program Files\\Git\\mingw64\\ssl\\certs\\ca-bundle.crt",
        "C:\\Program Files\\Git\\usr\\ssl\\certs\\ca-bundle.crt",
        "C:\\msys64\\mingw64\\ssl\\certs\\ca-bundle.crt",
        nullptr
    };

    for (int i = 0; ca_paths[i] != nullptr; i++) {
        if (FileExists(ca_paths[i])) {
            curl_easy_setopt(curl, CURLOPT_CAINFO, ca_paths[i]);
            break;
        }
    }
    // Note: CURLSSLOPT_NATIVE_CA | CURLSSLOPT_NO_REVOKE already set above
#endif

    // Enable SSL verification
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
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
