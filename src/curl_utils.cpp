#include "curl_utils.hpp"
#include <sstream>

namespace duckdb {
namespace stps {

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

std::string curl_propfind(const std::string& url,
                          const std::string& request_body,
                          const CurlHeaders& headers,
                          long* http_code_out) {
    CurlHandle handle;
    if (!handle.handle()) {
        return "ERROR: Failed to initialize curl";
    }

    std::string response;

    curl_easy_setopt(handle.handle(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(handle.handle(), CURLOPT_CUSTOMREQUEST, "PROPFIND");
    curl_easy_setopt(handle.handle(), CURLOPT_POSTFIELDS, request_body.c_str());
    curl_easy_setopt(handle.handle(), CURLOPT_HTTPHEADER, headers.list());
    curl_easy_setopt(handle.handle(), CURLOPT_WRITEFUNCTION, curl_write_callback);
    curl_easy_setopt(handle.handle(), CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(handle.handle(), CURLOPT_TIMEOUT, 60L);
    curl_easy_setopt(handle.handle(), CURLOPT_FOLLOWLOCATION, 1L);

    CURLcode res = curl_easy_perform(handle.handle());

    if (res != CURLE_OK) {
        std::ostringstream err;
        err << "ERROR: curl request failed: " << curl_easy_strerror(res);
        return err.str();
    }

    long http_code = 0;
    curl_easy_getinfo(handle.handle(), CURLINFO_RESPONSE_CODE, &http_code);

    if (http_code_out) {
        *http_code_out = http_code;
    }

    // Don't error on 207 Multi-Status (standard WebDAV success response)
    if (http_code != 207 && (http_code < 200 || http_code >= 300)) {
        std::ostringstream err;
        err << "ERROR: HTTP " << http_code << " - " << response;
        return err.str();
    }

    return response;
}

} // namespace stps
} // namespace duckdb
