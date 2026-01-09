#include "smart_cast_function.hpp"
#include "smart_cast_utils.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include <map>

namespace duckdb {
namespace stps {

//=============================================================================
// stps_smart_cast_analyze - Returns metadata about detected types
//=============================================================================

struct SmartCastAnalyzeBindData : public TableFunctionData {
    string table_name;
    vector<ColumnAnalysis> analysis;
    double min_success_rate = 0.9;
    NumberLocale forced_locale = NumberLocale::AUTO;
    DateFormat forced_date_format = DateFormat::AUTO;
};

struct SmartCastAnalyzeGlobalState : public GlobalTableFunctionState {
    idx_t current_row = 0;
};

static unique_ptr<FunctionData> SmartCastAnalyzeBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<SmartCastAnalyzeBindData>();

    if (input.inputs.empty()) {
        throw BinderException("stps_smart_cast_analyze requires one argument: table_name");
    }
    result->table_name = input.inputs[0].GetValue<string>();

    // Handle named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "min_success_rate") {
            result->min_success_rate = kv.second.GetValue<double>();
        } else if (kv.first == "locale") {
            string loc = kv.second.GetValue<string>();
            if (loc == "de" || loc == "german") result->forced_locale = NumberLocale::GERMAN;
            else if (loc == "en" || loc == "us") result->forced_locale = NumberLocale::US;
        } else if (kv.first == "date_format") {
            string fmt = kv.second.GetValue<string>();
            if (fmt == "dmy") result->forced_date_format = DateFormat::DMY;
            else if (fmt == "mdy") result->forced_date_format = DateFormat::MDY;
            else if (fmt == "ymd") result->forced_date_format = DateFormat::YMD;
        }
    }

    // Get table schema
    Connection conn(context.db->GetDatabase(context));
    auto schema_result = conn.Query("SELECT * FROM " + result->table_name + " LIMIT 0");
    if (schema_result->HasError()) {
        throw BinderException("Table '%s' does not exist: %s", result->table_name, schema_result->GetError());
    }

    // Analyze each column
    for (idx_t col = 0; col < schema_result->names.size(); col++) {
        ColumnAnalysis analysis;
        analysis.column_name = schema_result->names[col];
        analysis.original_type = schema_result->types[col];

        // Only analyze VARCHAR columns
        if (analysis.original_type != LogicalType::VARCHAR) {
            analysis.detected_type = DetectedType::VARCHAR;
            analysis.target_type = analysis.original_type;
            analysis.total_rows = 0;
            analysis.null_count = 0;
            analysis.cast_success_count = 0;
            analysis.cast_failure_count = 0;
            analysis.detected_locale = NumberLocale::AUTO;
            analysis.detected_date_format = DateFormat::AUTO;
            result->analysis.push_back(analysis);
            continue;
        }

        // Get all values for this column
        auto query = "SELECT \"" + analysis.column_name + "\" FROM " + result->table_name;
        auto col_result = conn.Query(query);
        if (col_result->HasError()) {
            throw BinderException("Failed to query column: %s", col_result->GetError());
        }

        std::vector<std::string> values;
        analysis.total_rows = 0;
        analysis.null_count = 0;

        while (auto chunk = col_result->Fetch()) {
            for (idx_t row = 0; row < chunk->size(); row++) {
                analysis.total_rows++;
                auto val = chunk->GetValue(0, row);
                if (val.IsNull()) {
                    analysis.null_count++;
                } else {
                    std::string str = val.GetValue<string>();
                    std::string processed;
                    if (SmartCastUtils::Preprocess(str, processed)) {
                        values.push_back(processed);
                    } else {
                        analysis.null_count++;  // Empty strings count as null
                    }
                }
            }
        }

        // Detect locale and date format
        NumberLocale locale = result->forced_locale != NumberLocale::AUTO
            ? result->forced_locale
            : SmartCastUtils::DetectLocale(values);
        DateFormat date_format = result->forced_date_format != DateFormat::AUTO
            ? result->forced_date_format
            : SmartCastUtils::DetectDateFormat(values);

        analysis.detected_locale = locale;
        analysis.detected_date_format = date_format;

        // Count type occurrences
        std::map<DetectedType, int64_t> type_counts;
        for (const auto& val : values) {
            DetectedType type = SmartCastUtils::DetectType(val, locale, date_format);
            type_counts[type]++;
        }

        // Find most common non-VARCHAR type
        DetectedType best_type = DetectedType::VARCHAR;
        int64_t best_count = 0;
        for (const auto& kv : type_counts) {
            if (kv.first != DetectedType::VARCHAR && kv.first != DetectedType::UNKNOWN && kv.second > best_count) {
                best_type = kv.first;
                best_count = kv.second;
            }
        }

        // Check success rate
        int64_t non_null_count = analysis.total_rows - analysis.null_count;
        if (non_null_count > 0 && best_type != DetectedType::VARCHAR) {
            double success_rate = static_cast<double>(best_count) / static_cast<double>(non_null_count);
            if (success_rate >= result->min_success_rate) {
                analysis.detected_type = best_type;
                analysis.cast_success_count = best_count;
                analysis.cast_failure_count = non_null_count - best_count;
            } else {
                analysis.detected_type = DetectedType::VARCHAR;
                analysis.cast_success_count = non_null_count;
                analysis.cast_failure_count = 0;
            }
        } else {
            analysis.detected_type = DetectedType::VARCHAR;
            analysis.cast_success_count = non_null_count;
            analysis.cast_failure_count = 0;
        }

        analysis.target_type = SmartCastUtils::ToLogicalType(analysis.detected_type);
        result->analysis.push_back(analysis);
    }

    // Set output schema
    names = {"column_name", "original_type", "detected_type", "total_rows", "null_count", "cast_success_count", "cast_failure_count"};
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                    LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> SmartCastAnalyzeInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<SmartCastAnalyzeGlobalState>();
}

static void SmartCastAnalyzeScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<SmartCastAnalyzeGlobalState>();
    auto &bind_data = data_p.bind_data->Cast<SmartCastAnalyzeBindData>();

    idx_t count = 0;
    while (state.current_row < bind_data.analysis.size() && count < STANDARD_VECTOR_SIZE) {
        auto &analysis = bind_data.analysis[state.current_row];

        output.SetValue(0, count, Value(analysis.column_name));
        output.SetValue(1, count, Value(analysis.original_type.ToString()));

        // Convert detected type to string
        std::string type_str;
        switch (analysis.detected_type) {
            case DetectedType::BOOLEAN: type_str = "BOOLEAN"; break;
            case DetectedType::INTEGER: type_str = "INTEGER"; break;
            case DetectedType::DOUBLE: type_str = "DOUBLE"; break;
            case DetectedType::DATE: type_str = "DATE"; break;
            case DetectedType::TIMESTAMP: type_str = "TIMESTAMP"; break;
            case DetectedType::UUID: type_str = "UUID"; break;
            default: type_str = "VARCHAR"; break;
        }
        output.SetValue(2, count, Value(type_str));
        output.SetValue(3, count, Value::BIGINT(analysis.total_rows));
        output.SetValue(4, count, Value::BIGINT(analysis.null_count));
        output.SetValue(5, count, Value::BIGINT(analysis.cast_success_count));
        output.SetValue(6, count, Value::BIGINT(analysis.cast_failure_count));

        count++;
        state.current_row++;
    }

    output.SetCardinality(count);
}

//=============================================================================
// stps_smart_cast - Returns table with cast columns
//=============================================================================

struct SmartCastBindData : public TableFunctionData {
    string table_name;
    vector<ColumnAnalysis> analysis;
    vector<string> output_columns;
    vector<LogicalType> output_types;
    double min_success_rate = 0.9;
    NumberLocale forced_locale = NumberLocale::AUTO;
    DateFormat forced_date_format = DateFormat::AUTO;
};

struct SmartCastGlobalState : public GlobalTableFunctionState {
    unique_ptr<QueryResult> result;
};

static unique_ptr<FunctionData> SmartCastBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<SmartCastBindData>();

    if (input.inputs.empty()) {
        throw BinderException("stps_smart_cast requires one argument: table_name");
    }
    result->table_name = input.inputs[0].GetValue<string>();

    // Handle named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "min_success_rate") {
            result->min_success_rate = kv.second.GetValue<double>();
        } else if (kv.first == "locale") {
            string loc = kv.second.GetValue<string>();
            if (loc == "de" || loc == "german") result->forced_locale = NumberLocale::GERMAN;
            else if (loc == "en" || loc == "us") result->forced_locale = NumberLocale::US;
        } else if (kv.first == "date_format") {
            string fmt = kv.second.GetValue<string>();
            if (fmt == "dmy") result->forced_date_format = DateFormat::DMY;
            else if (fmt == "mdy") result->forced_date_format = DateFormat::MDY;
            else if (fmt == "ymd") result->forced_date_format = DateFormat::YMD;
        }
    }

    // Get table schema and analyze columns
    Connection conn(context.db->GetDatabase(context));
    auto schema_result = conn.Query("SELECT * FROM " + result->table_name + " LIMIT 0");
    if (schema_result->HasError()) {
        throw BinderException("Table '%s' does not exist: %s", result->table_name, schema_result->GetError());
    }

    // Analyze columns
    for (idx_t col = 0; col < schema_result->names.size(); col++) {
        ColumnAnalysis analysis;
        analysis.column_name = schema_result->names[col];
        analysis.original_type = schema_result->types[col];

        if (analysis.original_type != LogicalType::VARCHAR) {
            analysis.detected_type = DetectedType::VARCHAR;
            analysis.target_type = analysis.original_type;
            result->analysis.push_back(analysis);
            result->output_columns.push_back(analysis.column_name);
            result->output_types.push_back(analysis.original_type);
            continue;
        }

        // Get values and analyze
        auto query = "SELECT \"" + analysis.column_name + "\" FROM " + result->table_name;
        auto col_result = conn.Query(query);

        std::vector<std::string> values;
        int64_t total_rows = 0, null_count = 0;

        while (auto chunk = col_result->Fetch()) {
            for (idx_t row = 0; row < chunk->size(); row++) {
                total_rows++;
                auto val = chunk->GetValue(0, row);
                if (val.IsNull()) {
                    null_count++;
                } else {
                    std::string processed;
                    if (SmartCastUtils::Preprocess(val.GetValue<string>(), processed)) {
                        values.push_back(processed);
                    } else {
                        null_count++;
                    }
                }
            }
        }

        NumberLocale locale = result->forced_locale != NumberLocale::AUTO
            ? result->forced_locale : SmartCastUtils::DetectLocale(values);
        DateFormat date_format = result->forced_date_format != DateFormat::AUTO
            ? result->forced_date_format : SmartCastUtils::DetectDateFormat(values);

        analysis.detected_locale = locale;
        analysis.detected_date_format = date_format;

        std::map<DetectedType, int64_t> type_counts;
        for (const auto& val : values) {
            type_counts[SmartCastUtils::DetectType(val, locale, date_format)]++;
        }

        DetectedType best_type = DetectedType::VARCHAR;
        int64_t best_count = 0;
        for (const auto& kv : type_counts) {
            if (kv.first != DetectedType::VARCHAR && kv.first != DetectedType::UNKNOWN && kv.second > best_count) {
                best_type = kv.first;
                best_count = kv.second;
            }
        }

        int64_t non_null_count = total_rows - null_count;
        if (non_null_count > 0 && best_type != DetectedType::VARCHAR) {
            double success_rate = static_cast<double>(best_count) / static_cast<double>(non_null_count);
            if (success_rate >= result->min_success_rate) {
                analysis.detected_type = best_type;
            } else {
                analysis.detected_type = DetectedType::VARCHAR;
            }
        } else {
            analysis.detected_type = DetectedType::VARCHAR;
        }

        analysis.target_type = SmartCastUtils::ToLogicalType(analysis.detected_type);
        result->analysis.push_back(analysis);
        result->output_columns.push_back(analysis.column_name);
        result->output_types.push_back(analysis.target_type);
    }

    names = result->output_columns;
    return_types = result->output_types;

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> SmartCastInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<SmartCastBindData>();
    auto state = make_uniq<SmartCastGlobalState>();

    // Query original data
    Connection conn(context.db->GetDatabase(context));
    state->result = conn.Query("SELECT * FROM " + bind_data.table_name);

    return std::move(state);
}

static void SmartCastScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<SmartCastGlobalState>();
    auto &bind_data = data_p.bind_data->Cast<SmartCastBindData>();

    auto chunk = state.result->Fetch();
    if (!chunk || chunk->size() == 0) {
        output.SetCardinality(0);
        return;
    }

    idx_t count = chunk->size();

    for (idx_t col = 0; col < bind_data.analysis.size(); col++) {
        auto &analysis = bind_data.analysis[col];

        for (idx_t row = 0; row < count; row++) {
            auto val = chunk->GetValue(col, row);

            if (val.IsNull()) {
                output.SetValue(col, row, Value());
                continue;
            }

            // Non-VARCHAR columns pass through
            if (analysis.original_type != LogicalType::VARCHAR) {
                output.SetValue(col, row, val);
                continue;
            }

            std::string str = val.GetValue<string>();
            std::string processed;
            if (!SmartCastUtils::Preprocess(str, processed)) {
                output.SetValue(col, row, Value());
                continue;
            }

            // Cast based on detected type
            switch (analysis.detected_type) {
                case DetectedType::BOOLEAN: {
                    bool parsed;
                    if (SmartCastUtils::ParseBoolean(processed, parsed)) {
                        output.SetValue(col, row, Value::BOOLEAN(parsed));
                    } else {
                        output.SetValue(col, row, Value());
                    }
                    break;
                }
                case DetectedType::INTEGER: {
                    int64_t parsed;
                    if (SmartCastUtils::ParseInteger(processed, analysis.detected_locale, parsed)) {
                        output.SetValue(col, row, Value::BIGINT(parsed));
                    } else {
                        output.SetValue(col, row, Value());
                    }
                    break;
                }
                case DetectedType::DOUBLE: {
                    double parsed;
                    if (SmartCastUtils::ParseDouble(processed, analysis.detected_locale, parsed)) {
                        output.SetValue(col, row, Value::DOUBLE(parsed));
                    } else {
                        output.SetValue(col, row, Value());
                    }
                    break;
                }
                case DetectedType::DATE: {
                    date_t parsed;
                    if (SmartCastUtils::ParseDate(processed, analysis.detected_date_format, parsed)) {
                        output.SetValue(col, row, Value::DATE(parsed));
                    } else {
                        output.SetValue(col, row, Value());
                    }
                    break;
                }
                case DetectedType::TIMESTAMP: {
                    timestamp_t parsed;
                    if (SmartCastUtils::ParseTimestamp(processed, analysis.detected_date_format, parsed)) {
                        output.SetValue(col, row, Value::TIMESTAMP(parsed));
                    } else {
                        output.SetValue(col, row, Value());
                    }
                    break;
                }
                case DetectedType::UUID: {
                    std::string parsed;
                    if (SmartCastUtils::ParseUUID(processed, parsed)) {
                        output.SetValue(col, row, Value(parsed));
                    } else {
                        output.SetValue(col, row, Value());
                    }
                    break;
                }
                default:
                    output.SetValue(col, row, val);
                    break;
            }
        }
    }

    output.SetCardinality(count);
}

//=============================================================================
// Registration
//=============================================================================

void RegisterSmartCastTableFunctions(ExtensionLoader &loader) {
    // stps_smart_cast_analyze
    TableFunction analyze_func("stps_smart_cast_analyze", {LogicalType::VARCHAR},
                               SmartCastAnalyzeScan, SmartCastAnalyzeBind, SmartCastAnalyzeInit);
    analyze_func.named_parameters["min_success_rate"] = LogicalType::DOUBLE;
    analyze_func.named_parameters["locale"] = LogicalType::VARCHAR;
    analyze_func.named_parameters["date_format"] = LogicalType::VARCHAR;
    loader.RegisterFunction(analyze_func);

    // stps_smart_cast
    TableFunction cast_func("stps_smart_cast", {LogicalType::VARCHAR},
                            SmartCastScan, SmartCastBind, SmartCastInit);
    cast_func.named_parameters["min_success_rate"] = LogicalType::DOUBLE;
    cast_func.named_parameters["locale"] = LogicalType::VARCHAR;
    cast_func.named_parameters["date_format"] = LogicalType::VARCHAR;
    loader.RegisterFunction(cast_func);
}

} // namespace stps
} // namespace duckdb
