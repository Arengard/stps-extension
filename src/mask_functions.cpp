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

static unique_ptr<GlobalTableFunctionState> MaskTableInit(
    ClientContext &context,
    TableFunctionInitInput &input
) {
    auto state = make_uniq<MaskTableGlobalState>();
    state->finished = false;
    return std::move(state);
}

static Value MaskValue(const Value &original, const LogicalType &type,
                       const std::string &seed, const std::string &column_name) {
    if (original.IsNull()) {
        return Value(nullptr);
    }

    std::string str_val = original.ToString();
    uint64_t hash = KeyedHash(seed, column_name, str_val);

    switch (type.id()) {
        case LogicalTypeId::VARCHAR: {
            idx_t len = str_val.size();
            return Value(HashToHexString(hash, len));
        }
        case LogicalTypeId::INTEGER: {
            int32_t orig = original.GetValue<int32_t>();
            int32_t magnitude = orig == 0 ? 1 : static_cast<int32_t>(std::pow(10, static_cast<int>(std::log10(std::abs(orig)))));
            int32_t masked = static_cast<int32_t>(hash % (magnitude * 9) + magnitude);
            if (orig < 0) masked = -masked;
            return Value::INTEGER(masked);
        }
        case LogicalTypeId::BIGINT: {
            int64_t orig = original.GetValue<int64_t>();
            int64_t magnitude = orig == 0 ? 1 : static_cast<int64_t>(std::pow(10, static_cast<int>(std::log10(std::abs(static_cast<double>(orig))))));
            int64_t masked = static_cast<int64_t>(hash % (magnitude * 9) + magnitude);
            if (orig < 0) masked = -masked;
            return Value::BIGINT(masked);
        }
        case LogicalTypeId::DOUBLE: {
            double orig = original.GetValue<double>();
            if (orig == 0.0) return Value::DOUBLE(0.0);
            double magnitude = std::pow(10, std::floor(std::log10(std::abs(orig))));
            double fraction = static_cast<double>(hash % 10000) / 10000.0;
            double masked = magnitude * (1.0 + fraction * 9.0);
            if (orig < 0) masked = -masked;
            return Value::DOUBLE(masked);
        }
        case LogicalTypeId::FLOAT: {
            float orig = original.GetValue<float>();
            if (orig == 0.0f) return Value::FLOAT(0.0f);
            double magnitude = std::pow(10, std::floor(std::log10(std::abs(orig))));
            double fraction = static_cast<double>(hash % 10000) / 10000.0;
            float masked = static_cast<float>(magnitude * (1.0 + fraction * 9.0));
            if (orig < 0) masked = -masked;
            return Value::FLOAT(masked);
        }
        case LogicalTypeId::DATE: {
            auto date_val = original.GetValue<date_t>();
            int32_t days_offset = static_cast<int32_t>(hash % 365) + 1;
            date_t masked_date = date_t(date_val.days + days_offset);
            return Value::DATE(masked_date);
        }
        case LogicalTypeId::TIMESTAMP:
        case LogicalTypeId::TIMESTAMP_TZ: {
            auto ts_val = original.GetValue<timestamp_t>();
            auto date_part = Timestamp::GetDate(ts_val);
            int32_t days_offset = static_cast<int32_t>(hash % 365) + 1;
            date_t masked_date = date_t(date_part.days + days_offset);
            auto masked_ts = Timestamp::FromDatetime(masked_date, dtime_t(0));
            if (type.id() == LogicalTypeId::TIMESTAMP_TZ) {
                return Value::TIMESTAMPTZ(masked_ts);
            }
            return Value::TIMESTAMP(masked_ts);
        }
        case LogicalTypeId::BOOLEAN: {
            bool masked = (hash % 2) == 0;
            return Value::BOOLEAN(masked);
        }
        case LogicalTypeId::DECIMAL: {
            double orig = original.GetValue<double>();
            if (orig == 0.0) return Value(0).DefaultCastAs(type);
            double magnitude = std::pow(10, std::floor(std::log10(std::abs(orig))));
            double fraction = static_cast<double>(hash % 10000) / 10000.0;
            double masked = magnitude * (1.0 + fraction * 9.0);
            if (orig < 0) masked = -masked;
            return Value(masked).DefaultCastAs(type);
        }
        default: {
            idx_t len = str_val.size();
            return Value(HashToHexString(hash, len));
        }
    }
}

static void MaskTableScan(
    ClientContext &context,
    TableFunctionInput &data_p,
    DataChunk &output
) {
    auto &bind_data = data_p.bind_data->Cast<MaskTableBindData>();
    auto &state = data_p.global_state->Cast<MaskTableGlobalState>();

    if (state.finished) {
        output.SetCardinality(0);
        return;
    }

    // On first call, execute the query
    if (!state.query_result) {
        Connection conn(context.db->GetDatabase(context));
        state.query_result = conn.Query("SELECT * FROM \"" + bind_data.table_name + "\"");
        if (state.query_result->HasError()) {
            throw InternalException("stps_mask_table: Failed to query table '%s': %s",
                                    bind_data.table_name.c_str(), state.query_result->GetError().c_str());
        }
    }

    idx_t output_idx = 0;

    while (output_idx < STANDARD_VECTOR_SIZE && !state.finished) {
        if (!state.current_chunk || state.chunk_offset >= state.current_chunk->size()) {
            state.current_chunk = state.query_result->Fetch();
            state.chunk_offset = 0;

            if (!state.current_chunk || state.current_chunk->size() == 0) {
                state.finished = true;
                break;
            }
        }

        while (state.chunk_offset < state.current_chunk->size() && output_idx < STANDARD_VECTOR_SIZE) {
            for (idx_t col = 0; col < bind_data.column_names.size(); col++) {
                Value val = state.current_chunk->data[col].GetValue(state.chunk_offset);

                if (bind_data.column_masked[col]) {
                    val = MaskValue(val, bind_data.column_types[col],
                                    bind_data.seed, bind_data.column_names[col]);
                }

                output.data[col].SetValue(output_idx, val);
            }
            output_idx++;
            state.chunk_offset++;
        }
    }

    output.SetCardinality(output_idx);
}

} // namespace stps
} // namespace duckdb
