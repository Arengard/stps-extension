#include "webdav_utils.hpp"
#include <cstring>

namespace duckdb {
namespace stps {

// Simple base64 for basic auth (handles UTF-8 input correctly)
std::string Base64Encode(const std::string &input) {
    static const char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((input.size() + 2) / 3) * 4);

    int val = 0, valb = -6;
    for (unsigned char c : input) {
        val = (val << 8) + c;
        valb += 8;
        while (valb >= 0) {
            out.push_back(table[(val >> valb) & 0x3F]);
            valb -= 6;
        }
    }
    if (valb > -6) {
        out.push_back(table[((val << 8) >> (valb + 8)) & 0x3F]);
    }
    while (out.size() % 4) {
        out.push_back('=');
    }
    return out;
}

// Get detailed HTTP error message
std::string GetHttpErrorMessage(long http_code, const std::string &url) {
    std::string msg = "HTTP error " + std::to_string(http_code);
    switch (http_code) {
        case 400: msg += " (Bad Request)"; break;
        case 401: msg += " (Unauthorized - check username/password)"; break;
        case 403: msg += " (Forbidden - access denied)"; break;
        case 404: msg += " (Not Found - file does not exist)"; break;
        case 405: msg += " (Method Not Allowed)"; break;
        case 408: msg += " (Request Timeout)"; break;
        case 429: msg += " (Too Many Requests)"; break;
        case 500: msg += " (Internal Server Error)"; break;
        case 502: msg += " (Bad Gateway)"; break;
        case 503: msg += " (Service Unavailable)"; break;
        case 504: msg += " (Gateway Timeout)"; break;
        default: break;
    }
    msg += " fetching: " + url;
    return msg;
}

// Get detailed curl error message
std::string GetCurlErrorDetails(const std::string &error) {
    if (error.find("SSL") != std::string::npos || error.find("certificate") != std::string::npos) {
        return error + " (SSL/TLS certificate issue - check server certificate or use http://)";
    }
    if (error.find("timeout") != std::string::npos || error.find("Timeout") != std::string::npos) {
        return error + " (Connection timed out - server may be slow or unreachable)";
    }
    if (error.find("resolve") != std::string::npos || error.find("host") != std::string::npos) {
        return error + " (DNS resolution failed - check URL hostname)";
    }
    if (error.find("connect") != std::string::npos || error.find("refused") != std::string::npos) {
        return error + " (Connection refused - server may be down or port blocked)";
    }
    return error;
}

static bool IsHexChar(char c) {
    return (c >= '0' && c <= '9') ||
           (c >= 'a' && c <= 'f') ||
           (c >= 'A' && c <= 'F');
}

std::string PercentEncodePath(const std::string &path) {
    static const char hex[] = "0123456789ABCDEF";
    std::string encoded;
    encoded.reserve(path.size());

    for (size_t i = 0; i < path.size(); i++) {
        unsigned char c = static_cast<unsigned char>(path[i]);
        if (c == '%' && i + 2 < path.size() && IsHexChar(path[i + 1]) && IsHexChar(path[i + 2])) {
            encoded.append(path, i, 3);
            i += 2;
            continue;
        }
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
            (c >= '0' && c <= '9') || c == '-' || c == '_' ||
            c == '.' || c == '~' || c == '/') {
            encoded.push_back(static_cast<char>(c));
        } else {
            encoded.push_back('%');
            encoded.push_back(hex[(c >> 4) & 0x0F]);
            encoded.push_back(hex[c & 0x0F]);
        }
    }
    return encoded;
}

std::string NormalizeRequestUrl(const std::string &url) {
    size_t scheme_pos = url.find("://");
    if (scheme_pos == std::string::npos) {
        return PercentEncodePath(url);
    }
    size_t host_start = scheme_pos + 3;
    size_t path_start = url.find('/', host_start);
    if (path_start == std::string::npos) {
        return url;
    }
    std::string prefix = url.substr(0, path_start);
    std::string path = url.substr(path_start);
    return prefix + PercentEncodePath(path);
}

std::string PercentDecodePath(const std::string &encoded) {
    std::string decoded;
    decoded.reserve(encoded.size());

    for (size_t i = 0; i < encoded.size(); i++) {
        if (encoded[i] == '%' && i + 2 < encoded.size() &&
            IsHexChar(encoded[i + 1]) && IsHexChar(encoded[i + 2])) {
            char hi = encoded[i + 1];
            char lo = encoded[i + 2];
            auto hex_val = [](char c) -> int {
                if (c >= '0' && c <= '9') return c - '0';
                if (c >= 'a' && c <= 'f') return 10 + c - 'a';
                if (c >= 'A' && c <= 'F') return 10 + c - 'A';
                return 0;
            };
            decoded.push_back(static_cast<char>((hex_val(hi) << 4) | hex_val(lo)));
            i += 2;
        } else {
            decoded.push_back(encoded[i]);
        }
    }
    return decoded;
}

std::vector<PropfindEntry> ParsePropfindResponse(const std::string &xml) {
    std::vector<PropfindEntry> entries;

    // Find all <d:response> or <D:response> blocks
    size_t pos = 0;
    while (pos < xml.size()) {
        // Find response opening tag (case-insensitive prefix)
        size_t resp_start = std::string::npos;
        for (const char *tag : {"<d:response>", "<D:response>", "<d:response ", "<D:response "}) {
            size_t found = xml.find(tag, pos);
            if (found != std::string::npos && (resp_start == std::string::npos || found < resp_start)) {
                resp_start = found;
            }
        }
        if (resp_start == std::string::npos) break;

        // Find response closing tag
        size_t resp_end = std::string::npos;
        for (const char *tag : {"</d:response>", "</D:response>"}) {
            size_t found = xml.find(tag, resp_start);
            if (found != std::string::npos && (resp_end == std::string::npos || found < resp_end)) {
                resp_end = found;
            }
        }
        if (resp_end == std::string::npos) break;

        std::string block = xml.substr(resp_start, resp_end - resp_start);

        PropfindEntry entry;
        entry.is_collection = false;

        // Extract href
        for (const char *open : {"<d:href>", "<D:href>"}) {
            size_t href_start = block.find(open);
            if (href_start != std::string::npos) {
                href_start += strlen(open);
                size_t href_end = block.find("</", href_start);
                if (href_end != std::string::npos) {
                    entry.href = block.substr(href_start, href_end - href_start);
                }
                break;
            }
        }

        // Check if it's a collection (directory)
        if (block.find("<d:collection") != std::string::npos ||
            block.find("<D:collection") != std::string::npos) {
            entry.is_collection = true;
        }

        if (!entry.href.empty()) {
            entries.push_back(entry);
        }

        pos = resp_end + 1;
    }

    return entries;
}

std::string GetLastPathSegment(const std::string &path) {
    std::string clean = path;
    // Remove trailing slash
    while (!clean.empty() && clean.back() == '/') {
        clean.pop_back();
    }
    size_t last_slash = clean.rfind('/');
    if (last_slash != std::string::npos) {
        return clean.substr(last_slash + 1);
    }
    return clean;
}

std::string GetBaseUrl(const std::string &url) {
    size_t scheme_end = url.find("://");
    if (scheme_end == std::string::npos) return "";
    size_t host_end = url.find('/', scheme_end + 3);
    if (host_end == std::string::npos) return url;
    return url.substr(0, host_end);
}

void BuildAuthHeaders(CurlHeaders &headers, const std::string &username, const std::string &password) {
    if (!username.empty() || !password.empty()) {
        std::string credentials = username + ":" + password;
        headers.append("Authorization: Basic " + Base64Encode(credentials));
    }
}

const char* PROPFIND_BODY =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    "<d:propfind xmlns:d=\"DAV:\">"
    "<d:prop><d:resourcetype/></d:prop>"
    "</d:propfind>";

const char* PROPFIND_BODY_EXTENDED =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    "<d:propfind xmlns:d=\"DAV:\">"
    "<d:prop>"
    "<d:resourcetype/>"
    "<d:getcontentlength/>"
    "<d:getlastmodified/>"
    "</d:prop>"
    "</d:propfind>";

// Helper to extract text between XML tags (case-insensitive prefix d:/D:)
static std::string ExtractXmlTagValue(const std::string &block, const char *tag_name) {
    // Try both d: and D: prefixes
    for (const char *prefix : {"d:", "D:"}) {
        std::string open_tag = std::string("<") + prefix + tag_name + ">";
        size_t start = block.find(open_tag);
        if (start != std::string::npos) {
            start += open_tag.size();
            size_t end = block.find("</", start);
            if (end != std::string::npos) {
                return block.substr(start, end - start);
            }
        }
    }
    return "";
}

std::vector<PropfindEntry> ParsePropfindResponseExtended(const std::string &xml) {
    std::vector<PropfindEntry> entries;

    size_t pos = 0;
    while (pos < xml.size()) {
        // Find response opening tag
        size_t resp_start = std::string::npos;
        for (const char *tag : {"<d:response>", "<D:response>", "<d:response ", "<D:response "}) {
            size_t found = xml.find(tag, pos);
            if (found != std::string::npos && (resp_start == std::string::npos || found < resp_start)) {
                resp_start = found;
            }
        }
        if (resp_start == std::string::npos) break;

        // Find response closing tag
        size_t resp_end = std::string::npos;
        for (const char *tag : {"</d:response>", "</D:response>"}) {
            size_t found = xml.find(tag, resp_start);
            if (found != std::string::npos && (resp_end == std::string::npos || found < resp_end)) {
                resp_end = found;
            }
        }
        if (resp_end == std::string::npos) break;

        std::string block = xml.substr(resp_start, resp_end - resp_start);

        PropfindEntry entry;
        entry.is_collection = false;
        entry.content_length = -1;

        // Extract href
        entry.href = ExtractXmlTagValue(block, "href");

        // Check if it's a collection
        if (block.find("<d:collection") != std::string::npos ||
            block.find("<D:collection") != std::string::npos) {
            entry.is_collection = true;
        }

        // Extract content length
        std::string length_str = ExtractXmlTagValue(block, "getcontentlength");
        if (!length_str.empty()) {
            try {
                entry.content_length = std::stoll(length_str);
            } catch (...) {
                entry.content_length = -1;
            }
        }

        // Extract last modified
        entry.last_modified = ExtractXmlTagValue(block, "getlastmodified");

        if (!entry.href.empty()) {
            entries.push_back(entry);
        }

        pos = resp_end + 1;
    }

    return entries;
}

} // namespace stps
} // namespace duckdb
