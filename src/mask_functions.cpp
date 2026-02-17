#include "include/mask_functions.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <cstdint>
#include <cmath>
#include <string>
#include <vector>
#include <sstream>
#include <iomanip>

namespace duckdb {
namespace stps {

// FNV-1a 64-bit hash
static uint64_t FNV1aHash(const std::string &data) {
    uint64_t hash = 14695981039346656037ULL;
    for (unsigned char c : data) {
        hash ^= c;
        hash *= 1099511628211ULL;
    }
    return hash;
}

// Keyed hash: combines seed + column_name + value
static uint64_t KeyedHash(const std::string &seed, const std::string &column_name, const std::string &value) {
    std::string input = seed + ":" + column_name + ":" + value;
    return FNV1aHash(input);
}

// Convert hash to hex string of given length (min 4)
static std::string HashToHexString(uint64_t hash, idx_t target_length) {
    if (target_length < 4) target_length = 4;
    std::ostringstream oss;
    uint64_t h = hash;
    while (oss.str().size() < target_length) {
        oss << std::hex << std::setfill('0') << std::setw(16) << h;
        h = FNV1aHash(std::to_string(h));
    }
    return oss.str().substr(0, target_length);
}

struct MaskTableBindData : public TableFunctionData {
    std::string table_name;
    std::string seed;
    std::vector<std::string> exclude_columns;
    std::vector<std::string> column_names;
    std::vector<LogicalType> column_types;
    std::vector<bool> column_masked; // true = mask, false = pass through
};

struct MaskTableGlobalState : public GlobalTableFunctionState {
    unique_ptr<QueryResult> query_result;
    unique_ptr<DataChunk> current_chunk;
    idx_t chunk_offset = 0;
    bool finished = false;
};

static unique_ptr<FunctionData> MaskTableBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names
) {
    auto result = make_uniq<MaskTableBindData>();

    result->table_name = input.inputs[0].GetValue<string>();
    result->seed = "stps_default_seed";

    // Parse named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "seed") {
            result->seed = kv.second.ToString();
        } else if (kv.first == "exclude") {
            if (kv.second.type().id() == LogicalTypeId::LIST) {
                auto &list_children = ListValue::GetChildren(kv.second);
                for (const auto &child : list_children) {
                    result->exclude_columns.push_back(child.ToString());
                }
            }
        }
    }

    // Discover table schema
    Connection conn(context.db->GetDatabase(context));
    auto schema_result = conn.Query(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = '" + result->table_name + "' "
        "AND table_schema NOT IN ('information_schema', 'pg_catalog') "
        "ORDER BY ordinal_position"
    );

    if (schema_result->HasError()) {
        throw BinderException("stps_mask_table: Failed to query schema for table '%s': %s",
                              result->table_name.c_str(), schema_result->GetError().c_str());
    }

    // Collect column names for validation
    while (true) {
        auto chunk = schema_result->Fetch();
        if (!chunk || chunk->size() == 0) break;
        for (idx_t row = 0; row < chunk->size(); row++) {
            result->column_names.push_back(chunk->data[0].GetValue(row).ToString());
        }
    }

    if (result->column_names.empty()) {
        throw BinderException("stps_mask_table: Table '%s' not found or has no columns", result->table_name.c_str());
    }

    // Validate excluded columns exist
    for (const auto &excl : result->exclude_columns) {
        bool found = false;
        for (const auto &col : result->column_names) {
            if (col == excl) { found = true; break; }
        }
        if (!found) {
            throw BinderException("stps_mask_table: Column '%s' not found in table '%s'",
                                  excl.c_str(), result->table_name.c_str());
        }
    }

    // Get actual types by querying the table with LIMIT 0
    auto type_result = conn.Query("SELECT * FROM \"" + result->table_name + "\" LIMIT 0");
    if (type_result->HasError()) {
        throw BinderException("stps_mask_table: Failed to query table '%s': %s",
                              result->table_name.c_str(), type_result->GetError().c_str());
    }

    // Use the result types and names from the actual query
    result->column_names.clear();
    for (idx_t i = 0; i < type_result->names.size(); i++) {
        result->column_names.push_back(type_result->names[i]);
        result->column_types.push_back(type_result->types[i]);

        bool excluded = false;
        for (const auto &excl : result->exclude_columns) {
            if (excl == type_result->names[i]) { excluded = true; break; }
        }
        result->column_masked.push_back(!excluded);

        return_types.push_back(type_result->types[i]);
        names.push_back(type_result->names[i]);
    }

    return std::move(result);
}

} // namespace stps
} // namespace duckdb
