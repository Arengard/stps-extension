#include "uuid_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "yyjson.hpp"
#include <random>
#include <sstream>
#include <iomanip>
#include <cstring>
#include <vector>
#include <algorithm>

namespace duckdb {
namespace stps {

// Simple UUID v4 generation (random)
std::string generate_uuid_v4() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;

    uint64_t data1 = dis(gen);
    uint64_t data2 = dis(gen);

    // Set version to 4
    data1 = (data1 & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000004000ULL;
    // Set variant to RFC4122
    data2 = (data2 & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL;

    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    oss << std::setw(8) << ((data1 >> 32) & 0xFFFFFFFF) << '-';
    oss << std::setw(4) << ((data1 >> 16) & 0xFFFF) << '-';
    oss << std::setw(4) << (data1 & 0xFFFF) << '-';
    oss << std::setw(4) << ((data2 >> 48) & 0xFFFF) << '-';
    oss << std::setw(12) << (data2 & 0xFFFFFFFFFFFFULL);

    return oss.str();
}

// Simple hash function for UUID v5 (deterministic from string)
std::string generate_uuid_v5(const std::string& name) {
    // Simple hash-based UUID generation
    // Using FNV-1a hash for deterministic output
    uint64_t hash1 = 0xcbf29ce484222325ULL;
    uint64_t hash2 = 0x9e3779b97f4a7c15ULL;

    const uint64_t fnv_prime = 0x100000001b3ULL;

    for (char c : name) {
        hash1 ^= static_cast<uint8_t>(c);
        hash1 *= fnv_prime;
        hash2 ^= static_cast<uint8_t>(c);
        hash2 *= fnv_prime;
    }

    // Set version to 5
    hash1 = (hash1 & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000005000ULL;
    // Set variant to RFC4122
    hash2 = (hash2 & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL;

    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    oss << std::setw(8) << ((hash1 >> 32) & 0xFFFFFFFF) << '-';
    oss << std::setw(4) << ((hash1 >> 16) & 0xFFFF) << '-';
    oss << std::setw(4) << (hash1 & 0xFFFF) << '-';
    oss << std::setw(4) << ((hash2 >> 48) & 0xFFFF) << '-';
    oss << std::setw(12) << (hash2 & 0xFFFFFFFFFFFFULL);

    return oss.str();
}

// Convert GUID to 4-level folder path with decimal folder names (0-255)
std::string stps_guid_to_path_impl(const std::string& guid) {
    // Strip hyphens and collect hex characters
    std::string hex;
    for (char c : guid) {
        if (c != '-') {
            if (!std::isxdigit(static_cast<unsigned char>(c))) {
                return "ERROR: Invalid GUID";
            }
            hex += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
    }

    // Need at least 8 hex characters for 4 levels
    if (hex.length() < 8) {
        return "ERROR: Invalid GUID";
    }

    // Convert 4 pairs of hex to decimal folder names
    std::string path;
    for (int i = 0; i < 4; i++) {
        std::string hex_pair = hex.substr(i * 2, 2);
        int value = std::stoi(hex_pair, nullptr, 16);
        if (i > 0) path += "/";
        path += std::to_string(value);
    }

    return path;
}

// Extract all values from JSON in sorted key order for deterministic GUID generation
static void extract_json_values_sorted(yyjson_val *val, std::vector<std::pair<std::string, std::string>> &values, const std::string &prefix = "") {
    if (!val) return;

    yyjson_type type = yyjson_get_type(val);

    switch (type) {
        case YYJSON_TYPE_OBJ: {
            // Collect all key-value pairs
            std::vector<std::pair<std::string, yyjson_val*>> pairs;
            yyjson_obj_iter iter;
            yyjson_obj_iter_init(val, &iter);
            yyjson_val *key;
            while ((key = yyjson_obj_iter_next(&iter))) {
                yyjson_val *child = yyjson_obj_iter_get_val(key);
                std::string key_str = yyjson_get_str(key);
                pairs.push_back({key_str, child});
            }
            // Sort by key for deterministic order
            std::sort(pairs.begin(), pairs.end(),
                [](const auto &a, const auto &b) { return a.first < b.first; });

            for (const auto &pair : pairs) {
                std::string new_prefix = prefix.empty() ? pair.first : prefix + "." + pair.first;
                extract_json_values_sorted(pair.second, values, new_prefix);
            }
            break;
        }
        case YYJSON_TYPE_ARR: {
            size_t idx = 0;
            yyjson_arr_iter iter;
            yyjson_arr_iter_init(val, &iter);
            yyjson_val *child;
            while ((child = yyjson_arr_iter_next(&iter))) {
                std::string new_prefix = prefix + "[" + std::to_string(idx++) + "]";
                extract_json_values_sorted(child, values, new_prefix);
            }
            break;
        }
        case YYJSON_TYPE_STR:
            values.push_back({prefix, yyjson_get_str(val)});
            break;
        case YYJSON_TYPE_NUM:
            if (yyjson_is_int(val)) {
                values.push_back({prefix, std::to_string(yyjson_get_int(val))});
            } else if (yyjson_is_real(val)) {
                values.push_back({prefix, std::to_string(yyjson_get_real(val))});
            }
            break;
        case YYJSON_TYPE_BOOL:
            values.push_back({prefix, yyjson_get_bool(val) ? "true" : "false"});
            break;
        case YYJSON_TYPE_NULL:
            // Skip null values (like stps_get_guid does)
            break;
        default:
            break;
    }
}

// Generate deterministic GUID from JSON content
static std::string generate_guid_from_json(const std::string &json_str) {
    yyjson_doc *doc = yyjson_read(json_str.c_str(), json_str.length(), 0);
    if (!doc) {
        // If not valid JSON, just hash the string directly
        return generate_uuid_v5(json_str);
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    std::vector<std::pair<std::string, std::string>> values;
    extract_json_values_sorted(root, values);
    yyjson_doc_free(doc);

    // Concatenate all values with separator (matching stps_get_guid style)
    std::ostringstream combined;
    bool first = true;
    for (const auto &pair : values) {
        if (!first) combined << "||";
        combined << pair.second;
        first = false;
    }

    return generate_uuid_v5(combined.str());
}

// DuckDB scalar function wrappers
static void PgmUuidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    // Generate UUID v4 for each row
    auto count = args.size();
    auto result_data = FlatVector::GetData<hugeint_t>(result);

    for (idx_t i = 0; i < count; i++) {
        std::string uuid_str = generate_uuid_v4();
        result_data[i] = UUID::FromString(uuid_str);
    }
}

static void PgmUuidFromStringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, hugeint_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            std::string input_str = input.GetString();
            std::string uuid_str = generate_uuid_v5(input_str);
            return UUID::FromString(uuid_str);
        });
}

static void PgmGetGuidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    // Concatenate all input columns and generate deterministic UUID
    auto count = args.size();
    auto result_data = FlatVector::GetData<hugeint_t>(result);
    auto num_args = args.ColumnCount();

    for (idx_t i = 0; i < count; i++) {
        std::ostringstream combined;

        // Concatenate all column values for this row
        for (idx_t col = 0; col < num_args; col++) {
            UnifiedVectorFormat vdata;
            args.data[col].ToUnifiedFormat(count, vdata);
            auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
            auto idx = vdata.sel->get_index(i);

            if (vdata.validity.RowIsValid(idx)) {
                if (col > 0) combined << "||";
                combined << data[idx].GetString();
            }
        }

        std::string uuid_str = generate_uuid_v5(combined.str());
        result_data[i] = UUID::FromString(uuid_str);
    }
}

// For VARCHAR input
static void StpsGuidToPathFunctionVarchar(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            std::string guid = input.GetString();
            std::string path = stps_guid_to_path_impl(guid);
            return StringVector::AddString(result, path);
        });
}

// For UUID input
static void StpsGuidToPathFunctionUUID(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<hugeint_t, string_t>(
        args.data[0], result, args.size(),
        [&](hugeint_t input) {
            // Convert UUID (hugeint_t) to string representation
            std::string guid = UUID::ToString(input);
            std::string path = stps_guid_to_path_impl(guid);
            return StringVector::AddString(result, path);
        });
}

// dguid(json) - Generate deterministic GUID from JSON (works with row_to_json)
static void DguidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, hugeint_t>(
        args.data[0], result, args.size(),
        [&](string_t input) {
            std::string json_str = input.GetString();
            std::string uuid_str = generate_guid_from_json(json_str);
            return UUID::FromString(uuid_str);
        });
}

// Helper to convert a Value to string for GUID generation
static std::string value_to_string(const Value &val) {
    if (val.IsNull()) {
        return "";
    }
    return val.ToString();
}

// Helper to extract struct values in sorted field order
static std::string extract_struct_values_sorted(const Value &struct_val) {
    if (struct_val.IsNull()) {
        return "";
    }

    auto &struct_children = StructValue::GetChildren(struct_val);
    auto &struct_type = struct_val.type();
    auto &child_types = StructType::GetChildTypes(struct_type);

    // Collect field name -> value pairs
    std::vector<std::pair<std::string, std::string>> pairs;
    for (idx_t i = 0; i < child_types.size(); i++) {
        const auto &field_name = child_types[i].first;
        const auto &child_val = struct_children[i];
        if (!child_val.IsNull()) {
            pairs.push_back({field_name, value_to_string(child_val)});
        }
    }

    // Sort by field name for deterministic order
    std::sort(pairs.begin(), pairs.end(),
        [](const auto &a, const auto &b) { return a.first < b.first; });

    // Concatenate values
    std::ostringstream combined;
    bool first = true;
    for (const auto &pair : pairs) {
        if (!first) combined << "||";
        combined << pair.second;
        first = false;
    }

    return combined.str();
}

// dguid(struct) - Generate deterministic GUID from a row/struct
static void DguidStructFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto count = args.size();
    auto &input_vector = args.data[0];
    auto result_data = FlatVector::GetData<hugeint_t>(result);
    auto &result_validity = FlatVector::Validity(result);

    for (idx_t i = 0; i < count; i++) {
        auto val = input_vector.GetValue(i);
        if (val.IsNull()) {
            result_validity.SetInvalid(i);
        } else {
            std::string combined = extract_struct_values_sorted(val);
            std::string uuid_str = generate_uuid_v5(combined);
            result_data[i] = UUID::FromString(uuid_str);
        }
    }
}

void RegisterUuidFunctions(ExtensionLoader &loader) {
    // stps_uuid() - Generate random UUID v4
    ScalarFunctionSet uuid_set("stps_uuid");
    uuid_set.AddFunction(ScalarFunction({}, LogicalType::UUID, PgmUuidFunction));
    loader.RegisterFunction(uuid_set);

    // stps_uuid_from_string(text) - Generate deterministic UUID v5 from string
    ScalarFunctionSet uuid_from_string_set("stps_uuid_from_string");
    uuid_from_string_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::UUID,
                                                 PgmUuidFromStringFunction));
    loader.RegisterFunction(uuid_from_string_set);

    // stps_get_guid(...columns) - Generate deterministic UUID from multiple columns
    ScalarFunctionSet get_guid_set("stps_get_guid");
    get_guid_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::UUID,
                                         PgmGetGuidFunction));
    get_guid_set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR},
                                         LogicalType::UUID, PgmGetGuidFunction));
    get_guid_set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                                         LogicalType::UUID, PgmGetGuidFunction));
    loader.RegisterFunction(get_guid_set);

    // stps_guid_to_path(guid) - Convert GUID to 4-level folder path
    ScalarFunctionSet guid_to_path_set("stps_guid_to_path");
    guid_to_path_set.AddFunction(ScalarFunction({LogicalType::UUID}, LogicalType::VARCHAR,
                                                  StpsGuidToPathFunctionUUID));
    guid_to_path_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                                     StpsGuidToPathFunctionVarchar));
    loader.RegisterFunction(guid_to_path_set);

    // dguid(row/struct) - Generate deterministic GUID from a row
    // Usage: SELECT dguid(t), * FROM test t;
    //        SELECT dguid(test), * FROM test;
    ScalarFunctionSet dguid_set("dguid");
    // Struct/row version - accepts any struct type
    auto dguid_struct_func = ScalarFunction({LogicalType::ANY}, LogicalType::UUID, DguidStructFunction);
    dguid_struct_func.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
    dguid_set.AddFunction(dguid_struct_func);
    // JSON/VARCHAR version for explicit JSON input
    dguid_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::UUID, DguidFunction));
    dguid_set.AddFunction(ScalarFunction({LogicalType::JSON}, LogicalType::UUID, DguidFunction));
    loader.RegisterFunction(dguid_set);
}

} // namespace stps
} // namespace duckdb
