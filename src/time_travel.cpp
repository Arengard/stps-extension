#include "include/time_travel.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/execution/expression_executor_state.hpp"

#include <sstream>

namespace duckdb {
namespace stps {

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//

static string EscapeIdentifier(const string &name) {
    string escaped;
    escaped.reserve(name.size() + 2);
    escaped += '"';
    for (char c : name) {
        if (c == '"') {
            escaped += "\"\"";
        } else {
            escaped += c;
        }
    }
    escaped += '"';
    return escaped;
}

static string EscapeLiteral(const string &value) {
    string escaped;
    escaped.reserve(value.size() + 2);
    escaped += '\'';
    for (char c : value) {
        if (c == '\'') {
            escaped += "''";
        } else {
            escaped += c;
        }
    }
    escaped += '\'';
    return escaped;
}

//===--------------------------------------------------------------------===//
// stps_tt_enable('table_name', 'pk_column') -> VARCHAR
//===--------------------------------------------------------------------===//

static void TimeTravelEnableFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &context = state.GetContext();
    Connection conn(context.db->GetDatabase(context));

    auto &table_name_vec = args.data[0];
    auto &pk_col_vec = args.data[1];

    idx_t count = args.size();
    for (idx_t i = 0; i < count; i++) {
        string table_name = table_name_vec.GetValue(i).ToString();
        string pk_column = pk_col_vec.GetValue(i).ToString();

        string table_escaped = EscapeIdentifier(table_name);
        string pk_escaped = EscapeIdentifier(pk_column);
        string history_table = "_stps_history_" + table_name;
        string history_escaped = EscapeIdentifier(history_table);

        // 1. Validate the table exists
        {
            auto res = conn.Query("SELECT * FROM " + table_escaped + " LIMIT 0");
            if (res->HasError()) {
                throw InvalidInputException("stps_tt_enable: table '%s' does not exist", table_name);
            }
        }

        // 2. Validate the PK column exists
        {
            auto res = conn.Query(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = " + EscapeLiteral(table_name) +
                " AND column_name = " + EscapeLiteral(pk_column) +
                " AND table_schema NOT IN ('information_schema', 'pg_catalog')"
            );
            if (res->HasError()) {
                throw InvalidInputException("stps_tt_enable: failed to query schema for table '%s'", table_name);
            }
            auto chunk = res->Fetch();
            if (!chunk || chunk->size() == 0) {
                throw InvalidInputException("stps_tt_enable: column '%s' does not exist in table '%s'",
                                            pk_column, table_name);
            }
        }

        // 3. Create metadata table if not exists
        {
            auto res = conn.Query(
                "CREATE TABLE IF NOT EXISTS \"_stps_tt_tables\" ("
                "table_name VARCHAR, "
                "pk_column VARCHAR, "
                "current_version BIGINT, "
                "created_at TIMESTAMP)"
            );
            if (res->HasError()) {
                throw InvalidInputException("stps_tt_enable: failed to create metadata table: %s",
                                            res->GetError());
            }
        }

        // 4. Check if the table is already tracked
        {
            auto res = conn.Query(
                "SELECT table_name FROM \"_stps_tt_tables\" "
                "WHERE table_name = " + EscapeLiteral(table_name)
            );
            if (!res->HasError()) {
                auto chunk = res->Fetch();
                if (chunk && chunk->size() > 0) {
                    throw InvalidInputException("stps_tt_enable: table '%s' is already tracked for time travel",
                                                table_name);
                }
            }
        }

        // 5. Create history table mirroring all columns + _tt columns
        //    Get column definitions from the original table
        vector<string> col_names;
        vector<string> col_types;
        {
            auto res = conn.Query(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = " + EscapeLiteral(table_name) +
                " AND table_schema NOT IN ('information_schema', 'pg_catalog') "
                "ORDER BY ordinal_position"
            );
            if (res->HasError()) {
                throw InvalidInputException("stps_tt_enable: failed to query columns for table '%s': %s",
                                            table_name, res->GetError());
            }
            while (true) {
                auto chunk = res->Fetch();
                if (!chunk || chunk->size() == 0) break;
                for (idx_t row = 0; row < chunk->size(); row++) {
                    col_names.push_back(chunk->data[0].GetValue(row).ToString());
                    col_types.push_back(chunk->data[1].GetValue(row).ToString());
                }
            }
        }

        {
            std::ostringstream ddl;
            ddl << "CREATE TABLE " << history_escaped << " (";
            for (idx_t c = 0; c < col_names.size(); c++) {
                if (c > 0) ddl << ", ";
                ddl << EscapeIdentifier(col_names[c]) << " " << col_types[c];
            }
            ddl << ", \"_tt_version\" BIGINT";
            ddl << ", \"_tt_operation\" VARCHAR";
            ddl << ", \"_tt_timestamp\" TIMESTAMP";
            ddl << ", \"_tt_pk_value\" VARCHAR";
            ddl << ")";

            auto res = conn.Query(ddl.str());
            if (res->HasError()) {
                throw InvalidInputException("stps_tt_enable: failed to create history table: %s",
                                            res->GetError());
            }
        }

        // 6. Create index on (_tt_pk_value, _tt_version) on the history table
        {
            string idx_name = "_stps_tt_idx_" + table_name;
            auto res = conn.Query(
                "CREATE INDEX " + EscapeIdentifier(idx_name) +
                " ON " + history_escaped +
                " (\"_tt_pk_value\", \"_tt_version\")"
            );
            if (res->HasError()) {
                // Index creation failure is non-fatal; log but continue
            }
        }

        // 7. Insert metadata row with current_version = 0
        {
            auto res = conn.Query(
                "INSERT INTO \"_stps_tt_tables\" VALUES (" +
                EscapeLiteral(table_name) + ", " +
                EscapeLiteral(pk_column) + ", " +
                "0, current_timestamp)"
            );
            if (res->HasError()) {
                throw InvalidInputException("stps_tt_enable: failed to insert metadata: %s",
                                            res->GetError());
            }
        }

        // 8. Snapshot all existing rows as version 0 with operation INSERT
        {
            std::ostringstream insert_sql;
            insert_sql << "INSERT INTO " << history_escaped << " SELECT ";
            for (idx_t c = 0; c < col_names.size(); c++) {
                if (c > 0) insert_sql << ", ";
                insert_sql << EscapeIdentifier(col_names[c]);
            }
            insert_sql << ", 0 AS \"_tt_version\"";
            insert_sql << ", 'INSERT' AS \"_tt_operation\"";
            insert_sql << ", current_timestamp AS \"_tt_timestamp\"";
            insert_sql << ", CAST(" << pk_escaped << " AS VARCHAR) AS \"_tt_pk_value\"";
            insert_sql << " FROM " << table_escaped;

            auto res = conn.Query(insert_sql.str());
            if (res->HasError()) {
                throw InvalidInputException("stps_tt_enable: failed to snapshot existing rows: %s",
                                            res->GetError());
            }
        }

        // Return success message
        string msg = "Time travel enabled for table '" + table_name + "' with primary key '" + pk_column + "'";
        result.SetValue(i, Value(msg));
    }
}

//===--------------------------------------------------------------------===//
// stps_tt_disable('table_name') -> VARCHAR
//===--------------------------------------------------------------------===//

static void TimeTravelDisableFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &context = state.GetContext();
    Connection conn(context.db->GetDatabase(context));

    auto &table_name_vec = args.data[0];

    idx_t count = args.size();
    for (idx_t i = 0; i < count; i++) {
        string table_name = table_name_vec.GetValue(i).ToString();
        string history_table = "_stps_history_" + table_name;
        string history_escaped = EscapeIdentifier(history_table);

        // 1. Validate the table is currently tracked
        {
            auto res = conn.Query(
                "SELECT table_name FROM \"_stps_tt_tables\" "
                "WHERE table_name = " + EscapeLiteral(table_name)
            );
            if (res->HasError()) {
                throw InvalidInputException("stps_tt_disable: metadata table does not exist. "
                                            "No tables are tracked for time travel.");
            }
            auto chunk = res->Fetch();
            if (!chunk || chunk->size() == 0) {
                throw InvalidInputException("stps_tt_disable: table '%s' is not tracked for time travel",
                                            table_name);
            }
        }

        // 2. Drop the history table
        {
            auto res = conn.Query("DROP TABLE IF EXISTS " + history_escaped);
            if (res->HasError()) {
                throw InvalidInputException("stps_tt_disable: failed to drop history table: %s",
                                            res->GetError());
            }
        }

        // 3. Delete the row from metadata table
        {
            auto res = conn.Query(
                "DELETE FROM \"_stps_tt_tables\" WHERE table_name = " + EscapeLiteral(table_name)
            );
            if (res->HasError()) {
                throw InvalidInputException("stps_tt_disable: failed to remove metadata: %s",
                                            res->GetError());
            }
        }

        // Return success message
        string msg = "Time travel disabled for table '" + table_name + "'";
        result.SetValue(i, Value(msg));
    }
}

//===--------------------------------------------------------------------===//
// Optimizer stub (to be filled in Task 2)
//===--------------------------------------------------------------------===//

static void TimeTravelOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    // Stub: will be implemented in Task 2 to intercept DML and record history
}

void RegisterTimeTravelOptimizer(DatabaseInstance &db) {
    OptimizerExtension tt_optimizer;
    tt_optimizer.optimize_function = TimeTravelOptimize;

    auto &config = DBConfig::GetConfig(db);
    config.optimizer_extensions.push_back(std::move(tt_optimizer));
}

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//

void RegisterTimeTravelFunctions(ExtensionLoader &loader) {
    // stps_tt_enable(table_name VARCHAR, pk_column VARCHAR) -> VARCHAR
    ScalarFunctionSet tt_enable_set("stps_tt_enable");
    ScalarFunction tt_enable_func({LogicalType::VARCHAR, LogicalType::VARCHAR},
                                  LogicalType::VARCHAR, TimeTravelEnableFunction);
    tt_enable_func.stability = FunctionStability::VOLATILE;
    tt_enable_set.AddFunction(tt_enable_func);
    loader.RegisterFunction(tt_enable_set);

    // stps_tt_disable(table_name VARCHAR) -> VARCHAR
    ScalarFunctionSet tt_disable_set("stps_tt_disable");
    ScalarFunction tt_disable_func({LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                   TimeTravelDisableFunction);
    tt_disable_func.stability = FunctionStability::VOLATILE;
    tt_disable_set.AddFunction(tt_disable_func);
    loader.RegisterFunction(tt_disable_set);

    // Placeholder: table functions for time travel queries will be registered here in later tasks
}

} // namespace stps
} // namespace duckdb
