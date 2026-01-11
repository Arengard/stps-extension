#include "xml_parser.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <fstream>
#include <sstream>
#include <map>
#include <vector>
#include <cctype>

namespace duckdb {
namespace stps {

// Simple XML node structure
struct XmlNode {
    std::string name;
    std::string text;
    std::map<std::string, std::string> attributes;
    std::vector<XmlNode> children;
};

// Trim whitespace
std::string xml_trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\n\r");
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(" \t\n\r");
    return str.substr(first, (last - first + 1));
}

// Parse XML attributes from tag (no regex - manual parsing)
// Pattern: name="value" or name='value'
std::map<std::string, std::string> parse_attributes(const std::string& tag) {
    std::map<std::string, std::string> attrs;
    size_t pos = 0;

    while (pos < tag.length()) {
        // Skip whitespace
        while (pos < tag.length() && std::isspace(static_cast<unsigned char>(tag[pos]))) {
            pos++;
        }
        if (pos >= tag.length()) break;

        // Find attribute name (word characters)
        size_t name_start = pos;
        while (pos < tag.length() &&
               (std::isalnum(static_cast<unsigned char>(tag[pos])) ||
                tag[pos] == '_' || tag[pos] == '-' || tag[pos] == ':')) {
            pos++;
        }
        if (pos == name_start) {
            pos++;  // Skip unknown character
            continue;
        }

        std::string name = tag.substr(name_start, pos - name_start);

        // Skip whitespace before =
        while (pos < tag.length() && std::isspace(static_cast<unsigned char>(tag[pos]))) {
            pos++;
        }

        // Expect =
        if (pos >= tag.length() || tag[pos] != '=') {
            continue;  // Malformed, skip
        }
        pos++;  // Skip =

        // Skip whitespace after =
        while (pos < tag.length() && std::isspace(static_cast<unsigned char>(tag[pos]))) {
            pos++;
        }

        // Expect quote
        if (pos >= tag.length()) break;
        char quote = tag[pos];
        if (quote != '"' && quote != '\'') {
            continue;  // Malformed
        }
        pos++;  // Skip opening quote

        // Find closing quote
        size_t value_start = pos;
        while (pos < tag.length() && tag[pos] != quote) {
            pos++;
        }

        std::string value = tag.substr(value_start, pos - value_start);

        if (pos < tag.length()) {
            pos++;  // Skip closing quote
        }

        attrs[name] = value;
    }

    return attrs;
}

// Parse XML recursively
XmlNode parse_xml_node(const std::string& xml, size_t& pos);

std::vector<XmlNode> parse_xml_children(const std::string& xml, size_t& pos, const std::string& parent_tag) {
    std::vector<XmlNode> children;
    std::string close_tag = "</" + parent_tag + ">";

    while (pos < xml.length()) {
        // Skip whitespace
        while (pos < xml.length() && std::isspace(xml[pos])) {
            pos++;
        }

        if (pos >= xml.length()) break;

        // Check for closing tag
        if (xml.substr(pos, close_tag.length()) == close_tag) {
            pos += close_tag.length();
            break;
        }

        // Check for opening tag
        if (xml[pos] == '<' && pos + 1 < xml.length() && xml[pos + 1] != '/') {
            XmlNode child = parse_xml_node(xml, pos);
            if (!child.name.empty()) {
                children.push_back(child);
            }
        } else {
            // Text content
            size_t next_tag = xml.find('<', pos);
            if (next_tag == std::string::npos) break;

            std::string text = xml_trim(xml.substr(pos, next_tag - pos));
            if (!text.empty() && !children.empty()) {
                children.back().text = text;
            }
            pos = next_tag;
        }
    }

    return children;
}

XmlNode parse_xml_node(const std::string& xml, size_t& pos) {
    XmlNode node;

    // Find opening tag
    size_t tag_start = xml.find('<', pos);
    if (tag_start == std::string::npos) return node;

    size_t tag_end = xml.find('>', tag_start);
    if (tag_end == std::string::npos) return node;

    std::string tag = xml.substr(tag_start + 1, tag_end - tag_start - 1);
    pos = tag_end + 1;

    // Check for self-closing tag
    bool self_closing = (tag.back() == '/');
    if (self_closing) {
        tag = tag.substr(0, tag.length() - 1);
    }

    // Parse tag name and attributes
    size_t space_pos = tag.find(' ');
    if (space_pos != std::string::npos) {
        node.name = xml_trim(tag.substr(0, space_pos));
        node.attributes = parse_attributes(tag.substr(space_pos + 1));
    } else {
        node.name = xml_trim(tag);
    }

    if (self_closing) {
        return node;
    }

    // Parse children and text
    size_t text_start = pos;
    size_t next_tag = xml.find('<', pos);

    if (next_tag != std::string::npos) {
        std::string potential_text = xml_trim(xml.substr(text_start, next_tag - text_start));

        // Check if next tag is closing tag
        std::string close_tag = "</" + node.name + ">";
        if (xml.substr(next_tag, close_tag.length()) == close_tag) {
            node.text = potential_text;
            pos = next_tag + close_tag.length();
        } else {
            // Has children
            node.children = parse_xml_children(xml, pos, node.name);
            if (!potential_text.empty() && node.children.empty()) {
                node.text = potential_text;
            }
        }
    }

    return node;
}

// Convert XML node to JSON
std::string node_to_json(const XmlNode& node, int indent = 0) {
    std::string json;
    std::string indent_str(indent * 2, ' ');

    json += "{\n";

    // Add tag name
    json += indent_str + "  \"_tag\": \"" + node.name + "\"";

    // Add attributes
    if (!node.attributes.empty()) {
        json += ",\n" + indent_str + "  \"_attributes\": {\n";
        bool first_attr = true;
        for (const auto& attr : node.attributes) {
            if (!first_attr) json += ",\n";
            json += indent_str + "    \"" + attr.first + "\": \"" + attr.second + "\"";
            first_attr = false;
        }
        json += "\n" + indent_str + "  }";
    }

    // Add text
    if (!node.text.empty()) {
        json += ",\n" + indent_str + "  \"_text\": \"" + node.text + "\"";
    }

    // Add children
    if (!node.children.empty()) {
        json += ",\n" + indent_str + "  \"_children\": [\n";
        for (size_t i = 0; i < node.children.size(); i++) {
            if (i > 0) json += ",\n";
            json += indent_str + "    " + node_to_json(node.children[i], indent + 2);
        }
        json += "\n" + indent_str + "  ]";
    }

    json += "\n" + indent_str + "}";
    return json;
}

// Read and parse XML file
std::string read_xml_file(const std::string& filepath) {
    try {
        // Read file
        std::ifstream file(filepath);
        if (!file.is_open()) {
            return "{\"error\": \"Cannot open file: " + filepath + "\"}";
        }

        std::stringstream buffer;
        buffer << file.rdbuf();
        std::string xml_content = buffer.str();
        file.close();

        if (xml_content.empty()) {
            return "{\"error\": \"File is empty: " + filepath + "\"}";
        }

        // Remove XML declaration if present
        size_t decl_start = xml_content.find("<?xml");
        if (decl_start != std::string::npos) {
            size_t decl_end = xml_content.find("?>", decl_start);
            if (decl_end != std::string::npos) {
                xml_content = xml_content.substr(decl_end + 2);
            }
        }

        // Parse XML
        size_t pos = 0;
        XmlNode root = parse_xml_node(xml_content, pos);

        if (root.name.empty()) {
            return "{\"error\": \"Failed to parse XML\"}";
        }

        // Convert to JSON
        return node_to_json(root);

    } catch (const std::exception& e) {
        return "{\"error\": \"" + std::string(e.what()) + "\"}";
    }
}

// DuckDB scalar function wrapper - returns JSON string
static void StpsReadXmlFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t filepath) {
            std::string path = filepath.GetString();
            std::string json_result = read_xml_file(path);
            return StringVector::AddString(result, json_result);
        });
}

void RegisterXmlParserFunctions(ExtensionLoader &loader) {
    // stps_read_xml(filepath) - Read XML file and return as JSON string
    ScalarFunctionSet read_xml_set("stps_read_xml");
    read_xml_set.AddFunction(ScalarFunction({LogicalType::VARCHAR},
                                            LogicalType::VARCHAR,
                                            StpsReadXmlFunction));
    loader.RegisterFunction(read_xml_set);

    // stps_read_xml_json(filepath) - Read XML file and return as DuckDB JSON type
    ScalarFunctionSet read_xml_json_set("stps_read_xml_json");
    read_xml_json_set.AddFunction(ScalarFunction({LogicalType::VARCHAR},
                                                  LogicalType::JSON(),
                                                  StpsReadXmlFunction));
    loader.RegisterFunction(read_xml_json_set);
}

} // namespace stps
} // namespace duckdb
