#define DUCKDB_EXTENSION_MAIN
#include "stps_extension.hpp"
#include "path_function.hpp"
#include "scan_function.hpp"
#include "case_transform.hpp"
#include "text_normalize.hpp"
#include "null_handling.hpp"
#include "uuid_functions.hpp"
#include "io_operations.hpp"
#include "iban_validation.hpp"
#include "xml_parser.hpp"
#include "gobd_reader.hpp"
#include "drop_null_columns_function.hpp"
#include "smart_cast_function.hpp"
#include "smart_cast_scalar.hpp"
#include "account_validation.hpp"
#include "fill_functions.hpp"
#include "plz_validation.hpp"
#include "street_split.hpp"
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

struct ReadFoldersBindData : public TableFunctionData {
    stps::ScanOptions options;
};

struct ReadFoldersGlobalState : public GlobalTableFunctionState {
    std::vector<stps::FileEntry> entries;
    idx_t offset = 0;

    idx_t MaxThreads() const override {
        return 1;
    }
};

static unique_ptr<FunctionData> ReadFoldersBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<ReadFoldersBindData>();

    // First parameter: path (required)
    if (input.inputs.empty()) {
        throw BinderException("stps_read_folders requires at least a path parameter");
    }
    bind_data->options.base_path = input.inputs[0].ToString();

    // Named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "recursive") {
            bind_data->options.recursive = BooleanValue::Get(kv.second);
        } else if (kv.first == "file_type") {
            bind_data->options.file_type = StringValue::Get(kv.second);
        } else if (kv.first == "min_size") {
            bind_data->options.min_size = BigIntValue::Get(kv.second);
        } else if (kv.first == "max_size") {
            bind_data->options.max_size = BigIntValue::Get(kv.second);
        } else if (kv.first == "min_date") {
            bind_data->options.min_date = BigIntValue::Get(kv.second);
        } else if (kv.first == "max_date") {
            bind_data->options.max_date = BigIntValue::Get(kv.second);
        } else if (kv.first == "pattern") {
            bind_data->options.pattern = StringValue::Get(kv.second);
        } else if (kv.first == "content_search") {
            bind_data->options.content_search = StringValue::Get(kv.second);
        } else if (kv.first == "max_depth") {
            bind_data->options.max_depth = IntegerValue::Get(kv.second);
        } else if (kv.first == "include_hidden") {
            bind_data->options.include_hidden = BooleanValue::Get(kv.second);
        }
    }

    // Define return columns
    names.emplace_back("name");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("path");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("type");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("size");
    return_types.emplace_back(LogicalType::BIGINT);

    names.emplace_back("modified");
    return_types.emplace_back(LogicalType::TIMESTAMP);

    names.emplace_back("extension");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("parent_directory");
    return_types.emplace_back(LogicalType::VARCHAR);

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> ReadFoldersInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<ReadFoldersBindData>();
    auto result = make_uniq<ReadFoldersGlobalState>();

    try {
        result->entries = stps::FileSystemScanner::ScanDirectory(bind_data.options);
    } catch (const std::exception &e) {
        throw IOException("Error scanning directory: %s", e.what());
    }

    return std::move(result);
}

static void ReadFoldersFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<ReadFoldersGlobalState>();

    idx_t count = 0;
    idx_t row_idx = 0;

    while (state.offset < state.entries.size() && count < STANDARD_VECTOR_SIZE) {
        auto &entry = state.entries[state.offset];

        // name
        output.SetValue(0, row_idx, Value(entry.name));

        // path
        output.SetValue(1, row_idx, Value(entry.path));

        // type
        output.SetValue(2, row_idx, Value(entry.type));

        // size
        output.SetValue(3, row_idx, Value::BIGINT(entry.size));

        // modified (convert Unix timestamp to DuckDB timestamp)
        output.SetValue(4, row_idx, Value::TIMESTAMP(Timestamp::FromEpochSeconds(entry.modified_time)));

        // extension
        output.SetValue(5, row_idx, Value(entry.extension));

        // parent_directory
        output.SetValue(6, row_idx, Value(entry.parent_directory));

        state.offset++;
        row_idx++;
        count++;
    }

    output.SetCardinality(count);
}

// Simple path-only version for SELECT * FROM 'path' syntax
static unique_ptr<FunctionData> ReadPathBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<ReadFoldersBindData>();

    if (input.inputs.empty()) {
        throw BinderException("Path parameter is required");
    }

    bind_data->options.base_path = input.inputs[0].ToString();
    bind_data->options.recursive = false;  // Default to non-recursive

    // Define return columns (same as read_folders)
    names.emplace_back("name");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("path");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("type");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("size");
    return_types.emplace_back(LogicalType::BIGINT);

    names.emplace_back("modified");
    return_types.emplace_back(LogicalType::TIMESTAMP);

    names.emplace_back("extension");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("parent_directory");
    return_types.emplace_back(LogicalType::VARCHAR);

    return std::move(bind_data);
}

// ========== PATH() Function ==========

struct PathBindData : public TableFunctionData {
    stps::PathOptions options;
};

struct PathGlobalState : public GlobalTableFunctionState {
    std::vector<stps::shared::FileInfo> entries;
    idx_t offset = 0;

    idx_t MaxThreads() const override {
        return 1;
    }
};

static unique_ptr<FunctionData> PathBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<PathBindData>();

    if (input.inputs.empty()) {
        throw BinderException("PATH requires at least a path parameter");
    }
    bind_data->options.base_path = input.inputs[0].ToString();

    // Named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "recursive") {
            bind_data->options.recursive = BooleanValue::Get(kv.second);
        } else if (kv.first == "file_type") {
            bind_data->options.file_type = StringValue::Get(kv.second);
        } else if (kv.first == "pattern") {
            bind_data->options.pattern = StringValue::Get(kv.second);
        } else if (kv.first == "max_depth") {
            bind_data->options.max_depth = IntegerValue::Get(kv.second);
        } else if (kv.first == "include_hidden") {
            bind_data->options.include_hidden = BooleanValue::Get(kv.second);
        }
    }

    // Define return columns
    names.emplace_back("name");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("path");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("type");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("size");
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("modified");
    return_types.emplace_back(LogicalType::TIMESTAMP);
    names.emplace_back("extension");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("parent_directory");
    return_types.emplace_back(LogicalType::VARCHAR);

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> PathInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<PathBindData>();
    auto result = make_uniq<PathGlobalState>();

    try {
        result->entries = stps::PathScanner::ScanPath(bind_data.options);
    } catch (const std::exception &e) {
        throw IOException("Error scanning directory: %s", e.what());
    }

    return std::move(result);
}

static void PathFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<PathGlobalState>();

    idx_t count = 0;
    idx_t row_idx = 0;

    while (state.offset < state.entries.size() && count < STANDARD_VECTOR_SIZE) {
        auto &entry = state.entries[state.offset];

        output.SetValue(0, row_idx, Value(entry.name));
        output.SetValue(1, row_idx, Value(entry.path));
        output.SetValue(2, row_idx, Value(entry.type));
        output.SetValue(3, row_idx, Value::BIGINT(entry.size));
        output.SetValue(4, row_idx, Value::TIMESTAMP(Timestamp::FromEpochSeconds(entry.modified_time)));
        output.SetValue(5, row_idx, Value(entry.extension));
        output.SetValue(6, row_idx, Value(entry.parent_directory));

        state.offset++;
        row_idx++;
        count++;
    }

    output.SetCardinality(count);
}

// ========== SCAN() Function ==========

struct ScanBindData : public TableFunctionData {
    stps::ScanFunctionOptions options;
};

struct ScanGlobalState : public GlobalTableFunctionState {
    std::vector<stps::shared::FileInfo> entries;
    idx_t offset = 0;

    idx_t MaxThreads() const override {
        return 1;
    }
};

static unique_ptr<FunctionData> ScanBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<ScanBindData>();

    if (input.inputs.empty()) {
        throw BinderException("SCAN requires at least a path parameter");
    }
    bind_data->options.base_path = input.inputs[0].ToString();

    // Named parameters (all PATH params + advanced filters)
    for (auto &kv : input.named_parameters) {
        if (kv.first == "recursive") {
            bind_data->options.recursive = BooleanValue::Get(kv.second);
        } else if (kv.first == "file_type") {
            bind_data->options.file_type = StringValue::Get(kv.second);
        } else if (kv.first == "pattern") {
            bind_data->options.pattern = StringValue::Get(kv.second);
        } else if (kv.first == "max_depth") {
            bind_data->options.max_depth = IntegerValue::Get(kv.second);
        } else if (kv.first == "include_hidden") {
            bind_data->options.include_hidden = BooleanValue::Get(kv.second);
        } else if (kv.first == "min_size") {
            bind_data->options.min_size = BigIntValue::Get(kv.second);
        } else if (kv.first == "max_size") {
            bind_data->options.max_size = BigIntValue::Get(kv.second);
        } else if (kv.first == "min_date") {
            bind_data->options.min_date = BigIntValue::Get(kv.second);
        } else if (kv.first == "max_date") {
            bind_data->options.max_date = BigIntValue::Get(kv.second);
        } else if (kv.first == "content_search") {
            bind_data->options.content_search = StringValue::Get(kv.second);
        }
    }

    // Define return columns
    names.emplace_back("name");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("path");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("type");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("size");
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("modified");
    return_types.emplace_back(LogicalType::TIMESTAMP);
    names.emplace_back("extension");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("parent_directory");
    return_types.emplace_back(LogicalType::VARCHAR);

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> ScanInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<ScanBindData>();
    auto result = make_uniq<ScanGlobalState>();

    try {
        result->entries = stps::ScanScanner::ScanPath(bind_data.options);
    } catch (const std::exception &e) {
        throw IOException("Error scanning directory: %s", e.what());
    }

    return std::move(result);
}

static void ScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<ScanGlobalState>();

    idx_t count = 0;
    idx_t row_idx = 0;

    while (state.offset < state.entries.size() && count < STANDARD_VECTOR_SIZE) {
        auto &entry = state.entries[state.offset];

        output.SetValue(0, row_idx, Value(entry.name));
        output.SetValue(1, row_idx, Value(entry.path));
        output.SetValue(2, row_idx, Value(entry.type));
        output.SetValue(3, row_idx, Value::BIGINT(entry.size));
        output.SetValue(4, row_idx, Value::TIMESTAMP(Timestamp::FromEpochSeconds(entry.modified_time)));
        output.SetValue(5, row_idx, Value(entry.extension));
        output.SetValue(6, row_idx, Value(entry.parent_directory));

        state.offset++;
        row_idx++;
        count++;
    }

    output.SetCardinality(count);
}

static void LoadInternal(DatabaseInstance &db) {
    Connection con(db);

    // Register stps_read_folders function
    TableFunction read_folders_func("stps_read_folders", {LogicalType::VARCHAR}, ReadFoldersFunction, ReadFoldersBind, ReadFoldersInit);

    // Add named parameters
    read_folders_func.named_parameters["recursive"] = LogicalType::BOOLEAN;
    read_folders_func.named_parameters["file_type"] = LogicalType::VARCHAR;
    read_folders_func.named_parameters["min_size"] = LogicalType::BIGINT;
    read_folders_func.named_parameters["max_size"] = LogicalType::BIGINT;
    read_folders_func.named_parameters["min_date"] = LogicalType::BIGINT;
    read_folders_func.named_parameters["max_date"] = LogicalType::BIGINT;
    read_folders_func.named_parameters["pattern"] = LogicalType::VARCHAR;
    read_folders_func.named_parameters["content_search"] = LogicalType::VARCHAR;
    read_folders_func.named_parameters["max_depth"] = LogicalType::INTEGER;
    read_folders_func.named_parameters["include_hidden"] = LogicalType::BOOLEAN;

    CreateTableFunctionInfo read_folders_info(read_folders_func);
    auto &catalog = Catalog::GetSystemCatalog(db);
    catalog.CreateTableFunction(*con.context, read_folders_info);

    // Register simpler read_path function for SELECT * FROM 'path' syntax
    TableFunction read_path_func("read_path", {LogicalType::VARCHAR}, ReadFoldersFunction, ReadPathBind, ReadFoldersInit);
    CreateTableFunctionInfo read_path_info(read_path_func);
    catalog.CreateTableFunction(*con.context, read_path_info);

    // Register PATH() function (new optimized common-case function)
    TableFunction path_func("PATH", {LogicalType::VARCHAR}, PathFunction, PathBind, PathInit);
    path_func.named_parameters["recursive"] = LogicalType::BOOLEAN;
    path_func.named_parameters["file_type"] = LogicalType::VARCHAR;
    path_func.named_parameters["pattern"] = LogicalType::VARCHAR;
    path_func.named_parameters["max_depth"] = LogicalType::INTEGER;
    path_func.named_parameters["include_hidden"] = LogicalType::BOOLEAN;
    CreateTableFunctionInfo path_info(path_func);
    catalog.CreateTableFunction(*con.context, path_info);

    // Register SCAN() function (new advanced filtering function)
    TableFunction scan_func("SCAN", {LogicalType::VARCHAR}, ScanFunction, ScanBind, ScanInit);
    // PATH() parameters
    scan_func.named_parameters["recursive"] = LogicalType::BOOLEAN;
    scan_func.named_parameters["file_type"] = LogicalType::VARCHAR;
    scan_func.named_parameters["pattern"] = LogicalType::VARCHAR;
    scan_func.named_parameters["max_depth"] = LogicalType::INTEGER;
    scan_func.named_parameters["include_hidden"] = LogicalType::BOOLEAN;
    // Advanced filtering parameters
    scan_func.named_parameters["min_size"] = LogicalType::BIGINT;
    scan_func.named_parameters["max_size"] = LogicalType::BIGINT;
    scan_func.named_parameters["min_date"] = LogicalType::BIGINT;
    scan_func.named_parameters["max_date"] = LogicalType::BIGINT;
    scan_func.named_parameters["content_search"] = LogicalType::VARCHAR;
    CreateTableFunctionInfo scan_info(scan_func);
    catalog.CreateTableFunction(*con.context, scan_info);
}

static void LoadPolarsgodmodeFunctions(DatabaseInstance &db) {
    Connection con(db);
    ExtensionLoader loader(db, "stps");

    stps::RegisterCaseTransformFunctions(loader);
    stps::RegisterTextNormalizeFunctions(loader);
    stps::RegisterNullHandlingFunctions(loader);
    stps::RegisterUuidFunctions(loader);
    stps::RegisterIoOperationFunctions(loader);
    stps::RegisterIbanValidationFunctions(loader);
    stps::RegisterXmlParserFunctions(loader);
    stps::RegisterGobdReaderFunctions(loader);
    stps::RegisterDropNullColumnsFunction(loader);
    stps::RegisterSmartCastTableFunctions(loader);
    stps::RegisterSmartCastScalarFunction(loader);
    stps::RegisterAccountValidationFunctions(loader);
    stps::RegisterFillFunctions(loader);
    stps::RegisterPlzValidationFunctions(loader);
    stps::RegisterStreetSplitFunctions(loader);
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void stps_init(duckdb::DatabaseInstance &db) {
    // Register filesystem table functions
    duckdb::LoadInternal(db);

    // Register stps scalar functions
    duckdb::LoadPolarsgodmodeFunctions(db);
}

DUCKDB_EXTENSION_API const char *stps_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}
