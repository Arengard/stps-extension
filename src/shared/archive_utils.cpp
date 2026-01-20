#include "archive_utils.hpp"
#include "duckdb/common/exception.hpp"
#include <algorithm>
#include <fstream>
#include <cstdlib>

#ifdef _WIN32
#define NOMINMAX
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace duckdb {
namespace stps {

std::string GetFileExtension(const std::string &filename) {
    size_t dot_pos = filename.rfind('.');
    if (dot_pos == std::string::npos || dot_pos == filename.length() - 1) {
        return "";
    }
    std::string ext = filename.substr(dot_pos + 1);
    std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);
    return ext;
}

bool IsBinaryFormat(const std::string &filename) {
    std::string ext = GetFileExtension(filename);
    return ext == "parquet" || ext == "arrow" || ext == "feather" ||
           ext == "orc" || ext == "avro" || ext == "xlsx" || ext == "xls" ||
           ext == "db" || ext == "sqlite" || ext == "duckdb";
}

bool LooksLikeBinary(const std::string &content) {
    if (content.empty()) return false;
    // Use parentheses around std::min to prevent Windows macro conflict
    size_t check_size = (std::min)(content.size(), static_cast<size_t>(1000));
    int non_printable = 0;
    for (size_t i = 0; i < check_size; i++) {
        unsigned char c = static_cast<unsigned char>(content[i]);
        if (c == 0) return true;
        if (c < 32 && c != '\n' && c != '\r' && c != '\t') non_printable++;
    }
    return (non_printable > (int)(check_size / 10));
}

std::string GetTempDirectory() {
#ifdef _WIN32
    char temp_path[MAX_PATH];
    DWORD len = GetTempPathA(MAX_PATH, temp_path);
    if (len > 0 && len < MAX_PATH) return std::string(temp_path);
    return "C:\\Temp\\";
#else
    const char* tmp = std::getenv("TMPDIR");
    if (tmp) return std::string(tmp) + "/";
    return "/tmp/";
#endif
}

std::string ExtractToTemp(const std::string &content, const std::string &original_filename, const std::string &prefix) {
    std::string temp_dir = GetTempDirectory();
    std::string base_name = original_filename;
    size_t slash_pos = base_name.rfind('/');
    if (slash_pos != std::string::npos) base_name = base_name.substr(slash_pos + 1);
    slash_pos = base_name.rfind('\\');
    if (slash_pos != std::string::npos) base_name = base_name.substr(slash_pos + 1);

    std::string temp_path = temp_dir + prefix + base_name;
    std::ofstream out(temp_path, std::ios::binary);
    if (!out) throw IOException("Failed to create temp file: " + temp_path);
    out.write(content.data(), content.size());
    out.close();
    return temp_path;
}

char DetectDelimiter(const std::string &content) {
    size_t semicolon_count = std::count(content.begin(), content.end(), ';');
    size_t comma_count = std::count(content.begin(), content.end(), ',');
    size_t tab_count = std::count(content.begin(), content.end(), '\t');
    size_t pipe_count = std::count(content.begin(), content.end(), '|');

    if (semicolon_count >= comma_count && semicolon_count >= tab_count && semicolon_count >= pipe_count) {
        return ';';
    } else if (tab_count >= comma_count && tab_count >= pipe_count) {
        return '\t';
    } else if (pipe_count >= comma_count) {
        return '|';
    }
    return ',';
}

std::vector<std::string> SplitLine(const std::string &line, char delimiter) {
    std::vector<std::string> result;
    std::string current;
    bool in_quotes = false;

    for (size_t i = 0; i < line.size(); i++) {
        char c = line[i];
        if (c == '"') {
            in_quotes = !in_quotes;
        } else if (c == delimiter && !in_quotes) {
            result.push_back(current);
            current.clear();
        } else {
            current += c;
        }
    }
    result.push_back(current);

    return result;
}

void ParseCSVContent(const std::string &content,
                     std::vector<std::string> &column_names,
                     std::vector<LogicalType> &column_types,
                     std::vector<std::vector<Value>> &rows) {
    if (content.empty()) {
        return;
    }

    char delimiter = DetectDelimiter(content);

    // Split into lines
    std::vector<std::string> lines;
    size_t start = 0;
    for (size_t i = 0; i < content.size(); i++) {
        if (content[i] == '\n') {
            std::string line = content.substr(start, i - start);
            // Remove carriage return if present
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            if (!line.empty()) {
                lines.push_back(line);
            }
            start = i + 1;
        }
    }
    // Add last line if no trailing newline
    if (start < content.size()) {
        std::string line = content.substr(start);
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (!line.empty()) {
            lines.push_back(line);
        }
    }

    if (lines.empty()) {
        return;
    }

    // Parse header
    column_names = SplitLine(lines[0], delimiter);

    // Initialize all columns as VARCHAR for simplicity
    for (size_t i = 0; i < column_names.size(); i++) {
        column_types.push_back(LogicalType::VARCHAR);
    }

    // Parse data rows
    for (size_t i = 1; i < lines.size(); i++) {
        std::vector<std::string> values = SplitLine(lines[i], delimiter);
        std::vector<Value> row;

        for (size_t j = 0; j < column_names.size(); j++) {
            if (j < values.size()) {
                row.push_back(Value(values[j]));
            } else {
                row.push_back(Value());
            }
        }
        rows.push_back(row);
    }
}

} // namespace stps
} // namespace duckdb
