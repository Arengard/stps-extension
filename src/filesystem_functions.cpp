#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/file_system.hpp"
#include "path_function.hpp"
#include "scan_function.hpp"
#include <unistd.h>
#include <climits>

namespace duckdb {
namespace stps {

// Bind data for stps_path function
struct PathBindData : public TableFunctionData {
    ::stps::PathOptions options;
};

// Global state for stps_path function
struct PathGlobalState : public GlobalTableFunctionState {
    std::vector<::stps::shared::FileInfo> files;
    idx_t position = 0;
};

// Bind function for stps_path
static unique_ptr<FunctionData> PathBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<PathBindData>();

    // Required parameter: base_path
    if (input.inputs.empty()) {
        throw BinderException("stps_path requires at least one argument: base_path");
    }
    result->options.base_path = input.inputs[0].GetValue<string>();

    // Handle named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "recursive") {
            result->options.recursive = kv.second.GetValue<bool>();
        } else if (kv.first == "file_type") {
            result->options.file_type = kv.second.GetValue<string>();
        } else if (kv.first == "pattern") {
            result->options.pattern = kv.second.GetValue<string>();
        } else if (kv.first == "max_depth") {
            result->options.max_depth = kv.second.GetValue<int32_t>();
        } else if (kv.first == "include_hidden") {
            result->options.include_hidden = kv.second.GetValue<bool>();
        }
    }

    // Define output columns
    names = {"name", "path", "type", "size", "modified_time", "extension", "parent_directory"};
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                    LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR};

    return std::move(result);
}

// Helper function to get absolute path using realpath
static string GetAbsolutePath(const string &path) {
    char resolved_path[PATH_MAX];
    if (realpath(path.c_str(), resolved_path) != nullptr) {
        return string(resolved_path);
    }
    // If realpath fails, try using getcwd for relative paths
    if (!path.empty() && path[0] != '/') {
        char cwd[PATH_MAX];
        if (getcwd(cwd, sizeof(cwd)) != nullptr) {
            if (path == ".") {
                return string(cwd);
            } else if (path.substr(0, 2) == "./") {
                return string(cwd) + "/" + path.substr(2);
            } else {
                return string(cwd) + "/" + path;
            }
        }
    }
    return path;
}

// Init function for stps_path
static unique_ptr<GlobalTableFunctionState> PathInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<PathBindData>();
    auto result = make_uniq<PathGlobalState>();

    // Get the file system from context
    auto &fs = FileSystem::GetFileSystem(context);

    // Convert base_path to absolute path
    ::stps::PathOptions options = bind_data.options;
    options.base_path = GetAbsolutePath(options.base_path);

    // Scan the directory
    try {
        result->files = ::stps::PathScanner::ScanPath(fs, options);
    } catch (const std::exception &e) {
        throw IOException("Error scanning path: " + string(e.what()));
    }

    return std::move(result);
}

// Scan function for stps_path
static void PathScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<PathGlobalState>();

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.position < state.files.size() && count < max_count) {
        auto &file = state.files[state.position];

        output.SetValue(0, count, Value(file.name));
        output.SetValue(1, count, Value(file.path));
        output.SetValue(2, count, Value(file.type));
        output.SetValue(3, count, Value::BIGINT(file.size));
        output.SetValue(4, count, Value::BIGINT(file.modified_time));
        output.SetValue(5, count, Value(file.extension));
        output.SetValue(6, count, Value(file.parent_directory));

        state.position++;
        count++;
    }

    output.SetCardinality(count);
}

// Bind data for stps_scan function
struct ScanBindData : public TableFunctionData {
    ::stps::ScanFunctionOptions options;
};

// Global state for stps_scan function
struct ScanGlobalState : public GlobalTableFunctionState {
    std::vector<::stps::shared::FileInfo> files;
    idx_t position = 0;
};

// Bind function for stps_scan
static unique_ptr<FunctionData> ScanBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<ScanBindData>();

    // Required parameter: base_path
    if (input.inputs.empty()) {
        throw BinderException("stps_scan requires at least one argument: base_path");
    }
    result->options.base_path = input.inputs[0].GetValue<string>();

    // Handle named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "recursive") {
            result->options.recursive = kv.second.GetValue<bool>();
        } else if (kv.first == "file_type") {
            result->options.file_type = kv.second.GetValue<string>();
        } else if (kv.first == "pattern") {
            result->options.pattern = kv.second.GetValue<string>();
        } else if (kv.first == "max_depth") {
            result->options.max_depth = kv.second.GetValue<int32_t>();
        } else if (kv.first == "include_hidden") {
            result->options.include_hidden = kv.second.GetValue<bool>();
        } else if (kv.first == "min_size") {
            result->options.min_size = kv.second.GetValue<int64_t>();
        } else if (kv.first == "max_size") {
            result->options.max_size = kv.second.GetValue<int64_t>();
        } else if (kv.first == "min_date") {
            result->options.min_date = kv.second.GetValue<int64_t>();
        } else if (kv.first == "max_date") {
            result->options.max_date = kv.second.GetValue<int64_t>();
        } else if (kv.first == "content_search") {
            result->options.content_search = kv.second.GetValue<string>();
        }
    }

    // Define output columns
    names = {"name", "path", "type", "size", "modified_time", "extension", "parent_directory"};
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                    LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR};

    return std::move(result);
}

// Init function for stps_scan
static unique_ptr<GlobalTableFunctionState> ScanInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<ScanBindData>();
    auto result = make_uniq<ScanGlobalState>();

    // Get the file system from context
    auto &fs = FileSystem::GetFileSystem(context);

    // Convert base_path to absolute path
    ::stps::ScanFunctionOptions options = bind_data.options;
    options.base_path = GetAbsolutePath(options.base_path);

    // Scan the directory
    try {
        result->files = ::stps::ScanScanner::ScanPath(fs, options);
    } catch (const std::exception &e) {
        throw IOException("Error scanning path: " + string(e.what()));
    }

    return std::move(result);
}

// Scan function for stps_scan
static void ScanScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<ScanGlobalState>();

    idx_t count = 0;
    idx_t max_count = STANDARD_VECTOR_SIZE;

    while (state.position < state.files.size() && count < max_count) {
        auto &file = state.files[state.position];

        output.SetValue(0, count, Value(file.name));
        output.SetValue(1, count, Value(file.path));
        output.SetValue(2, count, Value(file.type));
        output.SetValue(3, count, Value::BIGINT(file.size));
        output.SetValue(4, count, Value::BIGINT(file.modified_time));
        output.SetValue(5, count, Value(file.extension));
        output.SetValue(6, count, Value(file.parent_directory));

        state.position++;
        count++;
    }

    output.SetCardinality(count);
}

// Register filesystem functions
void RegisterFilesystemFunctions(ExtensionLoader &loader) {
    // Register stps_path table function
    TableFunction path_func("stps_path", {LogicalType::VARCHAR}, PathScan, PathBind, PathInit);
    path_func.named_parameters["recursive"] = LogicalType::BOOLEAN;
    path_func.named_parameters["file_type"] = LogicalType::VARCHAR;
    path_func.named_parameters["pattern"] = LogicalType::VARCHAR;
    path_func.named_parameters["max_depth"] = LogicalType::INTEGER;
    path_func.named_parameters["include_hidden"] = LogicalType::BOOLEAN;

    loader.RegisterFunction(path_func);

    // Register stps_scan table function
    TableFunction scan_func("stps_scan", {LogicalType::VARCHAR}, ScanScan, ScanBind, ScanInit);
    scan_func.named_parameters["recursive"] = LogicalType::BOOLEAN;
    scan_func.named_parameters["file_type"] = LogicalType::VARCHAR;
    scan_func.named_parameters["pattern"] = LogicalType::VARCHAR;
    scan_func.named_parameters["max_depth"] = LogicalType::INTEGER;
    scan_func.named_parameters["include_hidden"] = LogicalType::BOOLEAN;
    scan_func.named_parameters["min_size"] = LogicalType::BIGINT;
    scan_func.named_parameters["max_size"] = LogicalType::BIGINT;
    scan_func.named_parameters["min_date"] = LogicalType::BIGINT;
    scan_func.named_parameters["max_date"] = LogicalType::BIGINT;
    scan_func.named_parameters["content_search"] = LogicalType::VARCHAR;

    loader.RegisterFunction(scan_func);
}

} // namespace stps
} // namespace duckdb
