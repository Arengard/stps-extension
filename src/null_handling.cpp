#include "null_handling.hpp"
#include "utils.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {
namespace stps {

std::string map_null_to_empty(const string_t& input, bool is_null) {
    if (is_null) {
        return "";
    }
    return input.GetString();
}

bool should_map_empty_to_null(const std::string& input) {
    return is_empty_or_whitespace(input);
}

// DuckDB scalar function wrappers
static void PgmMapEmptyToNullFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    auto count = args.size();

    UnifiedVectorFormat input_data;
    input.ToUnifiedFormat(count, input_data);

    auto input_strings = UnifiedVectorFormat::GetData<string_t>(input_data);
    auto &result_validity = FlatVector::Validity(result);

    for (idx_t i = 0; i < count; i++) {
        auto idx = input_data.sel->get_index(i);
        if (!input_data.validity.RowIsValid(idx)) {
            result_validity.SetInvalid(i);
            continue;
        }

        std::string input_str = input_strings[idx].GetString();
        if (is_empty_or_whitespace(input_str)) {
            result_validity.SetInvalid(i);
        } else {
            FlatVector::GetData<string_t>(result)[i] = StringVector::AddString(result, input_str);
        }
    }
}

static void PgmMapNullToEmptyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input = args.data[0];
    auto count = args.size();

    UnifiedVectorFormat input_data;
    input.ToUnifiedFormat(count, input_data);

    auto input_strings = UnifiedVectorFormat::GetData<string_t>(input_data);
    auto result_strings = FlatVector::GetData<string_t>(result);

    for (idx_t i = 0; i < count; i++) {
        auto idx = input_data.sel->get_index(i);
        if (!input_data.validity.RowIsValid(idx)) {
            result_strings[i] = StringVector::AddString(result, "");
        } else {
            result_strings[i] = StringVector::AddString(result, input_strings[idx].GetString());
        }
    }
}

void RegisterNullHandlingFunctions(ExtensionLoader &loader) {
    // stps_map_empty_to_null
    ScalarFunctionSet map_empty_to_null_set("stps_map_empty_to_null");
    auto map_empty_to_null_func = ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                                      PgmMapEmptyToNullFunction);
    map_empty_to_null_func.description = "Converts empty strings ('') to NULL values.\n"
                                         "Usage: SELECT stps_map_empty_to_null(column_name);\n"
                                         "Returns: VARCHAR (original value or NULL if empty string)";
    map_empty_to_null_set.AddFunction(map_empty_to_null_func);
    loader.RegisterFunction(map_empty_to_null_set);

    // stps_map_null_to_empty
    ScalarFunctionSet map_null_to_empty_set("stps_map_null_to_empty");
    auto map_null_to_empty_func = ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                                      PgmMapNullToEmptyFunction);
    map_null_to_empty_func.description = "Converts NULL values to empty strings ('').\n"
                                         "Usage: SELECT stps_map_null_to_empty(column_name);\n"
                                         "Returns: VARCHAR (original value or '' if NULL)";
    map_null_to_empty_set.AddFunction(map_null_to_empty_func);
    loader.RegisterFunction(map_null_to_empty_set);
}

} // namespace stps
} // namespace duckdb
