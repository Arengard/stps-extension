#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include <sstream>

namespace duckdb {
namespace stps {

struct CleanDatabaseBindData : public TableFunctionData {
    vector<string> empty_tables;
};

struct CleanDatabaseGlobalState : public GlobalTableFunctionState {
    idx_t current_idx = 0;
    bool finished = false;
};

// Helper: escape identifier for SQL
static string EscapeIdentifier(const string &name) {
    return "\"" + name + "\"";
}

static unique_ptr<FunctionData> CleanDatabaseBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<CleanDatabaseBindData>();

    Connection conn(context.db->GetDatabase(context));

    // Get all tables from the database
    auto tables_result = conn.Query(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
    );

    if (tables_result->HasError()) {
        throw BinderException("Failed to get tables: %s", tables_result->GetError().c_str());
    }

    vector<string> all_tables;
    while (true) {
        auto chunk = tables_result->Fetch();
        if (!chunk || chunk->size() == 0) break;

        for (idx_t row = 0; row < chunk->size(); row++) {
            string table_name = chunk->data[0].GetValue(row).ToString();
            all_tables.push_back(table_name);
        }
    }

    // Check each table for emptiness and drop if empty
    for (const auto &table_name : all_tables) {
        string count_query = "SELECT COUNT(*) FROM " + EscapeIdentifier(table_name);
        auto count_result = conn.Query(count_query);

        if (!count_result->HasError()) {
            auto chunk = count_result->Fetch();
            if (chunk && chunk->size() > 0) {
                int64_t count = chunk->data[0].GetValue(0).GetValue<int64_t>();
                if (count == 0) {
                    // Table is empty, drop it
                    string drop_query = "DROP TABLE " + EscapeIdentifier(table_name);
                    auto drop_result = conn.Query(drop_query);
                    if (!drop_result->HasError()) {
                        result->empty_tables.push_back(table_name);
                    }
                }
            }
        }
    }

    // Output schema: table_name (dropped tables)
    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("dropped_table");

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> CleanDatabaseInit(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<CleanDatabaseGlobalState>();
    state->finished = false;
    return std::move(state);
}

static void CleanDatabaseFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<CleanDatabaseBindData>();
    auto &state = data_p.global_state->Cast<CleanDatabaseGlobalState>();

    if (state.finished) {
        output.SetCardinality(0);
        return;
    }

    idx_t output_idx = 0;

    while (output_idx < STANDARD_VECTOR_SIZE && state.current_idx < bind_data.empty_tables.size()) {
        output.data[0].SetValue(output_idx, Value(bind_data.empty_tables[state.current_idx]));
        output_idx++;
        state.current_idx++;
    }

    if (state.current_idx >= bind_data.empty_tables.size()) {
        state.finished = true;
    }

    output.SetCardinality(output_idx);
}

void RegisterCleanDatabaseFunction(ExtensionLoader& loader) {
    TableFunction clean_database_func(
        "stps_clean_database",
        {},
        CleanDatabaseFunction,
        CleanDatabaseBind,
        CleanDatabaseInit
    );

    loader.RegisterFunction(clean_database_func);
}

} // namespace stps
} // namespace duckdb
