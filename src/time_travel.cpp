#include "time_travel.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"

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
// Optimizer Extension: Transparent DML Interception (Lazy Reconciliation)
//===--------------------------------------------------------------------===//

// Thread-local state to prevent recursive interception
thread_local bool g_tt_capturing = false;

struct PendingCapture {
    string table_name;
    string pk_column;
    int64_t version;
    bool active = false;
};
thread_local PendingCapture g_pending_capture;

// Check if a table is tracked for time travel
static bool IsTableTracked(ClientContext &context, const string &table_name, string &pk_column) {
    if (g_tt_capturing) return false;
    g_tt_capturing = true;
    Connection conn(context.db->GetDatabase(context));
    auto result = conn.Query(
        "SELECT pk_column FROM \"_stps_tt_tables\" WHERE table_name = " + EscapeLiteral(table_name));
    g_tt_capturing = false;
    if (result->HasError() || result->RowCount() == 0) return false;
    auto chunk = result->Fetch();
    if (!chunk || chunk->size() == 0) return false;
    pk_column = chunk->data[0].GetValue(0).ToString();
    return true;
}

// Get column names for a table (excluding internal columns)
static vector<string> GetTableColumns(Connection &conn, const string &table_name) {
    vector<string> cols;
    auto res = conn.Query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = " + EscapeLiteral(table_name) +
        " AND table_schema NOT IN ('information_schema', 'pg_catalog') "
        "ORDER BY ordinal_position");
    if (res->HasError()) return cols;
    while (true) {
        auto chunk = res->Fetch();
        if (!chunk || chunk->size() == 0) break;
        for (idx_t r = 0; r < chunk->size(); r++) {
            cols.push_back(chunk->data[0].GetValue(r).ToString());
        }
    }
    return cols;
}

// Flush the pending capture: snapshot current table state into history
static void FlushPendingCapture(ClientContext &context) {
    if (!g_pending_capture.active) return;
    g_tt_capturing = true;

    Connection conn(context.db->GetDatabase(context));
    string table_name = g_pending_capture.table_name;
    string pk_column = g_pending_capture.pk_column;
    int64_t version = g_pending_capture.version;

    string esc_table = EscapeIdentifier(table_name);
    string esc_pk = EscapeIdentifier(pk_column);
    string history_table = "_stps_history_" + table_name;
    string esc_history = EscapeIdentifier(history_table);

    auto col_names = GetTableColumns(conn, table_name);
    if (col_names.empty()) {
        g_pending_capture.active = false;
        g_tt_capturing = false;
        return;
    }

    // 1. Snapshot all current rows into history
    std::ostringstream snap;
    snap << "INSERT INTO " << esc_history << " SELECT ";
    for (idx_t c = 0; c < col_names.size(); c++) {
        if (c > 0) snap << ", ";
        snap << "t." << EscapeIdentifier(col_names[c]);
    }
    snap << ", " << version
         << ", 'SNAPSHOT'"
         << ", current_timestamp"
         << ", CAST(t." << esc_pk << " AS VARCHAR)"
         << " FROM " << esc_table << " t";
    conn.Query(snap.str());

    // 2. Insert DELETE markers for rows that existed before but no longer exist
    std::ostringstream del;
    del << "INSERT INTO " << esc_history << " SELECT ";
    for (idx_t c = 0; c < col_names.size(); c++) {
        if (c > 0) del << ", ";
        del << "h." << EscapeIdentifier(col_names[c]);
    }
    del << ", " << version
        << ", 'DELETE'"
        << ", current_timestamp"
        << ", h.\"_tt_pk_value\""
        << " FROM ("
        << "  SELECT *, ROW_NUMBER() OVER (PARTITION BY \"_tt_pk_value\" ORDER BY \"_tt_version\" DESC) as rn"
        << "  FROM " << esc_history
        << "  WHERE \"_tt_version\" < " << version
        << ") h"
        << " WHERE h.rn = 1"
        << " AND h.\"_tt_operation\" != 'DELETE'"
        << " AND h.\"_tt_pk_value\" NOT IN ("
        << "  SELECT CAST(" << esc_pk << " AS VARCHAR) FROM " << esc_table
        << ")";
    conn.Query(del.str());

    g_pending_capture.active = false;
    g_tt_capturing = false;
}

// Extract the target table name from a DML logical operator node
static string GetDMLTableName(LogicalOperator &op) {
    switch (op.type) {
    case LogicalOperatorType::LOGICAL_INSERT:
        return op.Cast<LogicalInsert>().table.name;
    case LogicalOperatorType::LOGICAL_UPDATE:
        return op.Cast<LogicalUpdate>().table.name;
    case LogicalOperatorType::LOGICAL_DELETE:
        return op.Cast<LogicalDelete>().table.name;
    default:
        return "";
    }
}

// Walk the plan tree to find DML nodes
static void FindDMLNodes(LogicalOperator &op, vector<LogicalOperator*> &dml_nodes) {
    if (op.type == LogicalOperatorType::LOGICAL_INSERT ||
        op.type == LogicalOperatorType::LOGICAL_UPDATE ||
        op.type == LogicalOperatorType::LOGICAL_DELETE) {
        dml_nodes.push_back(&op);
    }
    for (auto &child : op.children) {
        FindDMLNodes(*child, dml_nodes);
    }
}

// Pre-optimize: flush previous pending capture, detect new DML on tracked tables
static void TimeTravelPreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    if (g_tt_capturing) return;

    // 1. Flush any pending capture from a previous DML
    FlushPendingCapture(input.context);

    // 2. Detect DML on tracked tables in the current plan
    vector<LogicalOperator*> dml_nodes;
    FindDMLNodes(*plan, dml_nodes);

    for (auto *node : dml_nodes) {
        string table_name = GetDMLTableName(*node);
        if (table_name.empty()) continue;
        // Skip our own internal tables
        if (table_name.find("_stps_history_") == 0 || table_name == "_stps_tt_tables") continue;

        string pk_column;
        if (!IsTableTracked(input.context, table_name, pk_column)) continue;

        // Increment version
        g_tt_capturing = true;
        Connection conn(input.context.db->GetDatabase(input.context));
        conn.Query("UPDATE \"_stps_tt_tables\" SET current_version = current_version + 1 "
                   "WHERE table_name = " + EscapeLiteral(table_name));

        auto ver_result = conn.Query(
            "SELECT current_version FROM \"_stps_tt_tables\" WHERE table_name = " + EscapeLiteral(table_name));
        auto ver_chunk = ver_result->Fetch();
        int64_t new_version = ver_chunk->data[0].GetValue(0).GetValue<int64_t>();
        g_tt_capturing = false;

        // Set pending capture — will be flushed on next query
        g_pending_capture = {table_name, pk_column, new_version, true};

        break; // Only handle one DML per query
    }
}

// Post-optimize: flush pending captures for non-DML queries (e.g., SELECT)
static void TimeTravelPostOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    if (g_tt_capturing) return;
    FlushPendingCapture(input.context);
}

void RegisterTimeTravelOptimizer(DatabaseInstance &db) {
    OptimizerExtension tt_optimizer;
    tt_optimizer.pre_optimize_function = TimeTravelPreOptimize;
    tt_optimizer.optimize_function = TimeTravelPostOptimize;

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
