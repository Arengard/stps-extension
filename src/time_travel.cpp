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
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"

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
    auto snap_result = conn.Query(snap.str());
    if (snap_result->HasError()) {
        g_pending_capture.active = false;
        g_tt_capturing = false;
        return;
    }

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
        g_pending_capture.table_name = table_name;
        g_pending_capture.pk_column = pk_column;
        g_pending_capture.version = new_version;
        g_pending_capture.active = true;

        break; // Only handle one DML per query
    }
}

// Post-optimize: flush pending captures ONLY for non-DML queries.
// If PreOptimize just set a pending capture (DML detected), do NOT flush here —
// the DML hasn't executed yet, so flushing would capture the pre-DML state.
static void TimeTravelPostOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    if (g_tt_capturing) return;

    // Check if the current query is DML on a tracked table
    vector<LogicalOperator*> dml_nodes;
    FindDMLNodes(*plan, dml_nodes);
    for (auto *node : dml_nodes) {
        string table_name = GetDMLTableName(*node);
        if (table_name.empty()) continue;
        if (table_name.find("_stps_history_") == 0 || table_name == "_stps_tt_tables") continue;
        // This is a DML query — pending was just set by PreOptimize.
        // Do NOT flush; the flush must happen on the NEXT query after the DML commits.
        return;
    }

    // Non-DML query (e.g., SELECT) — safe to flush pending captures
    FlushPendingCapture(input.context);
}

//===--------------------------------------------------------------------===//
// Table Function: stps_time_travel(table_name, version := N, as_of := TS)
//===--------------------------------------------------------------------===//

struct TimeTravelBindData : public TableFunctionData {
    string table_name;
    int64_t version = -1;
    timestamp_t as_of;
    bool use_as_of = false;
    vector<string> column_names;
    vector<LogicalType> column_types;
};

struct TimeTravelGlobalState : public GlobalTableFunctionState {
    unique_ptr<QueryResult> result;
    bool finished = false;
};

static unique_ptr<FunctionData> TimeTravelBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<TimeTravelBindData>();
    bind_data->table_name = input.inputs[0].GetValue<string>();

    // Get named parameters
    auto version_it = input.named_parameters.find("version");
    auto as_of_it = input.named_parameters.find("as_of");

    if (version_it != input.named_parameters.end()) {
        bind_data->version = version_it->second.GetValue<int64_t>();
    } else if (as_of_it != input.named_parameters.end()) {
        bind_data->as_of = as_of_it->second.GetValue<timestamp_t>();
        bind_data->use_as_of = true;
    } else {
        throw BinderException("stps_time_travel requires either 'version' or 'as_of' parameter");
    }

    Connection conn(context.db->GetDatabase(context));

    // Validate table is tracked
    {
        auto res = conn.Query(
            "SELECT table_name FROM \"_stps_tt_tables\" WHERE table_name = " +
            EscapeLiteral(bind_data->table_name));
        if (res->HasError()) {
            throw BinderException("stps_time_travel: metadata table does not exist. No tables are tracked.");
        }
        auto chunk = res->Fetch();
        if (!chunk || chunk->size() == 0) {
            throw BinderException("stps_time_travel: table '%s' is not tracked for time travel",
                                  bind_data->table_name);
        }
    }

    // Get original column names and types from the actual table
    {
        auto type_res = conn.Query("SELECT * FROM " + EscapeIdentifier(bind_data->table_name) + " LIMIT 0");
        if (type_res->HasError()) {
            throw BinderException("stps_time_travel: failed to query table '%s': %s",
                                  bind_data->table_name, type_res->GetError());
        }
        bind_data->column_names = type_res->names;
        bind_data->column_types = type_res->types;
    }

    for (idx_t i = 0; i < bind_data->column_names.size(); i++) {
        return_types.push_back(bind_data->column_types[i]);
        names.push_back(bind_data->column_names[i]);
    }

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> TimeTravelInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<TimeTravelBindData>();
    auto gstate = make_uniq<TimeTravelGlobalState>();

    string history_escaped = EscapeIdentifier("_stps_history_" + bind_data.table_name);

    // Build original columns list
    std::ostringstream cols;
    for (idx_t c = 0; c < bind_data.column_names.size(); c++) {
        if (c > 0) cols << ", ";
        cols << EscapeIdentifier(bind_data.column_names[c]);
    }

    // Build the WHERE clause for version/timestamp filtering
    string version_filter;
    if (bind_data.use_as_of) {
        version_filter = "\"_tt_timestamp\" <= '" + Timestamp::ToString(bind_data.as_of) + "'::TIMESTAMP";
    } else {
        version_filter = "\"_tt_version\" <= " + std::to_string(bind_data.version);
    }

    std::ostringstream sql;
    sql << "WITH latest_per_pk AS ("
        << "  SELECT *, ROW_NUMBER() OVER (PARTITION BY \"_tt_pk_value\" ORDER BY \"_tt_version\" DESC) as rn"
        << "  FROM " << history_escaped
        << "  WHERE " << version_filter
        << ")"
        << " SELECT " << cols.str()
        << " FROM latest_per_pk"
        << " WHERE rn = 1 AND \"_tt_operation\" != 'DELETE'";

    Connection conn(context.db->GetDatabase(context));
    gstate->result = conn.Query(sql.str());
    if (gstate->result->HasError()) {
        throw InternalException("stps_time_travel: query failed: %s", gstate->result->GetError());
    }

    return std::move(gstate);
}

static void TimeTravelScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &gstate = data_p.global_state->Cast<TimeTravelGlobalState>();
    if (gstate.finished) {
        output.SetCardinality(0);
        return;
    }
    auto chunk = gstate.result->Fetch();
    if (!chunk || chunk->size() == 0) {
        gstate.finished = true;
        output.SetCardinality(0);
        return;
    }
    for (idx_t col = 0; col < output.ColumnCount() && col < chunk->ColumnCount(); col++) {
        output.data[col].Reference(chunk->data[col]);
    }
    output.SetCardinality(chunk->size());
}

//===--------------------------------------------------------------------===//
// Table Function: stps_tt_log(table_name)
//===--------------------------------------------------------------------===//

struct TTLogBindData : public TableFunctionData {
    string table_name;
    vector<string> column_names;
    vector<LogicalType> column_types;
    vector<string> original_columns;
};

struct TTLogGlobalState : public GlobalTableFunctionState {
    unique_ptr<QueryResult> result;
    bool finished = false;
};

static unique_ptr<FunctionData> TTLogBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<TTLogBindData>();
    bind_data->table_name = input.inputs[0].GetValue<string>();

    Connection conn(context.db->GetDatabase(context));

    // Validate table is tracked
    {
        auto res = conn.Query(
            "SELECT table_name FROM \"_stps_tt_tables\" WHERE table_name = " +
            EscapeLiteral(bind_data->table_name));
        if (res->HasError()) {
            throw BinderException("stps_tt_log: metadata table does not exist.");
        }
        auto chunk = res->Fetch();
        if (!chunk || chunk->size() == 0) {
            throw BinderException("stps_tt_log: table '%s' is not tracked for time travel",
                                  bind_data->table_name);
        }
    }

    // Get ALL columns from history table (including _tt_* columns)
    string history_table = "_stps_history_" + bind_data->table_name;
    {
        auto type_res = conn.Query("SELECT * FROM " + EscapeIdentifier(history_table) + " LIMIT 0");
        if (type_res->HasError()) {
            throw BinderException("stps_tt_log: failed to query history table: %s", type_res->GetError());
        }
        bind_data->column_names = type_res->names;
        bind_data->column_types = type_res->types;
    }

    for (idx_t i = 0; i < bind_data->column_names.size(); i++) {
        return_types.push_back(bind_data->column_types[i]);
        names.push_back(bind_data->column_names[i]);
    }

    // Identify original columns (non-_tt_* metadata) for change tracking
    for (const auto &name : bind_data->column_names) {
        if (name != "_tt_version" && name != "_tt_operation" &&
            name != "_tt_timestamp" && name != "_tt_pk_value") {
            bind_data->original_columns.push_back(name);
        }
    }

    // Add _tt_changes column
    auto changes_type = LogicalType::LIST(LogicalType::STRUCT({
        {"column", LogicalType::VARCHAR},
        {"from_value", LogicalType::VARCHAR},
        {"to_value", LogicalType::VARCHAR}
    }));
    return_types.push_back(changes_type);
    names.push_back("_tt_changes");

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> TTLogInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<TTLogBindData>();
    auto gstate = make_uniq<TTLogGlobalState>();

    string history_escaped = EscapeIdentifier("_stps_history_" + bind_data.table_name);

    // Build LAG expressions for each original column
    std::ostringstream lag_cols;
    lag_cols << ", LAG(\"_tt_version\") OVER (PARTITION BY \"_tt_pk_value\" ORDER BY \"_tt_version\") as \"__p_version\"";
    for (idx_t c = 0; c < bind_data.original_columns.size(); c++) {
        string col_esc = EscapeIdentifier(bind_data.original_columns[c]);
        string prev_col = EscapeIdentifier("__p_" + bind_data.original_columns[c]);
        lag_cols << ", LAG(" << col_esc << ") OVER (PARTITION BY \"_tt_pk_value\" ORDER BY \"_tt_version\") as " << prev_col;
    }

    // Build explicit column list for outer SELECT (all history columns in order)
    std::ostringstream select_cols;
    for (idx_t c = 0; c < bind_data.column_names.size(); c++) {
        if (c > 0) select_cols << ", ";
        select_cols << EscapeIdentifier(bind_data.column_names[c]);
    }

    // Build _tt_changes expression
    std::ostringstream changes_expr;
    changes_expr << "CASE WHEN \"__p_version\" IS NOT NULL"
                 << " AND \"_tt_operation\" != 'DELETE' THEN list_filter(list_value(";
    for (idx_t c = 0; c < bind_data.original_columns.size(); c++) {
        if (c > 0) changes_expr << ", ";
        string col_esc = EscapeIdentifier(bind_data.original_columns[c]);
        string col_lit = EscapeLiteral(bind_data.original_columns[c]);
        string prev_col = EscapeIdentifier("__p_" + bind_data.original_columns[c]);
        changes_expr << "CASE WHEN " << prev_col << " IS DISTINCT FROM " << col_esc
                     << " THEN {column: " << col_lit
                     << ", from_value: CAST(" << prev_col << " AS VARCHAR)"
                     << ", to_value: CAST(" << col_esc << " AS VARCHAR)} END";
    }
    changes_expr << "), x -> x IS NOT NULL) END as \"_tt_changes\"";

    std::ostringstream sql;
    sql << "WITH history AS ("
        << "  SELECT *" << lag_cols.str()
        << "  FROM " << history_escaped
        << ")"
        << " SELECT " << select_cols.str() << ", " << changes_expr.str()
        << " FROM history"
        << " ORDER BY \"_tt_version\", \"_tt_pk_value\"";

    Connection conn(context.db->GetDatabase(context));
    gstate->result = conn.Query(sql.str());
    if (gstate->result->HasError()) {
        throw InternalException("stps_tt_log: query failed: %s", gstate->result->GetError());
    }

    return std::move(gstate);
}

static void TTLogScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &gstate = data_p.global_state->Cast<TTLogGlobalState>();
    if (gstate.finished) {
        output.SetCardinality(0);
        return;
    }
    auto chunk = gstate.result->Fetch();
    if (!chunk || chunk->size() == 0) {
        gstate.finished = true;
        output.SetCardinality(0);
        return;
    }
    for (idx_t col = 0; col < output.ColumnCount() && col < chunk->ColumnCount(); col++) {
        output.data[col].Reference(chunk->data[col]);
    }
    output.SetCardinality(chunk->size());
}

//===--------------------------------------------------------------------===//
// Table Function: stps_tt_diff(table_name, from_version, to_version)
//===--------------------------------------------------------------------===//

struct TTDiffBindData : public TableFunctionData {
    string table_name;
    int64_t from_version;
    int64_t to_version;
    vector<string> column_names;
    vector<LogicalType> column_types;
};

struct TTDiffGlobalState : public GlobalTableFunctionState {
    unique_ptr<QueryResult> result;
    bool finished = false;
};

static unique_ptr<FunctionData> TTDiffBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<TTDiffBindData>();
    bind_data->table_name = input.inputs[0].GetValue<string>();

    auto from_it = input.named_parameters.find("from_version");
    auto to_it = input.named_parameters.find("to_version");

    if (from_it == input.named_parameters.end() || to_it == input.named_parameters.end()) {
        throw BinderException("stps_tt_diff requires both 'from_version' and 'to_version' parameters");
    }

    bind_data->from_version = from_it->second.GetValue<int64_t>();
    bind_data->to_version = to_it->second.GetValue<int64_t>();

    Connection conn(context.db->GetDatabase(context));

    // Validate table is tracked
    {
        auto res = conn.Query(
            "SELECT table_name FROM \"_stps_tt_tables\" WHERE table_name = " +
            EscapeLiteral(bind_data->table_name));
        if (res->HasError()) {
            throw BinderException("stps_tt_diff: metadata table does not exist.");
        }
        auto chunk = res->Fetch();
        if (!chunk || chunk->size() == 0) {
            throw BinderException("stps_tt_diff: table '%s' is not tracked for time travel",
                                  bind_data->table_name);
        }
    }

    // Get original column names and types
    {
        auto type_res = conn.Query("SELECT * FROM " + EscapeIdentifier(bind_data->table_name) + " LIMIT 0");
        if (type_res->HasError()) {
            throw BinderException("stps_tt_diff: failed to query table '%s': %s",
                                  bind_data->table_name, type_res->GetError());
        }
        bind_data->column_names = type_res->names;
        bind_data->column_types = type_res->types;
    }

    // Return original columns + _tt_change_type + _tt_changes
    for (idx_t i = 0; i < bind_data->column_names.size(); i++) {
        return_types.push_back(bind_data->column_types[i]);
        names.push_back(bind_data->column_names[i]);
    }
    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("_tt_change_type");

    auto changes_type = LogicalType::LIST(LogicalType::STRUCT({
        {"column", LogicalType::VARCHAR},
        {"from_value", LogicalType::VARCHAR},
        {"to_value", LogicalType::VARCHAR}
    }));
    return_types.push_back(changes_type);
    names.push_back("_tt_changes");

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> TTDiffInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<TTDiffBindData>();
    auto gstate = make_uniq<TTDiffGlobalState>();

    string history_escaped = EscapeIdentifier("_stps_history_" + bind_data.table_name);

    // Build original columns list
    std::ostringstream cols_f, cols_t;
    for (idx_t c = 0; c < bind_data.column_names.size(); c++) {
        if (c > 0) {
            cols_f << ", ";
            cols_t << ", ";
        }
        cols_f << "f." << EscapeIdentifier(bind_data.column_names[c]);
        cols_t << "t." << EscapeIdentifier(bind_data.column_names[c]);
    }

    // Build _tt_changes expression for UPDATE branch
    std::ostringstream changes_expr;
    changes_expr << "list_filter(list_value(";
    for (idx_t c = 0; c < bind_data.column_names.size(); c++) {
        if (c > 0) changes_expr << ", ";
        string col_esc = EscapeIdentifier(bind_data.column_names[c]);
        string col_lit = EscapeLiteral(bind_data.column_names[c]);
        changes_expr << "CASE WHEN f." << col_esc << " IS DISTINCT FROM t." << col_esc
                     << " THEN {column: " << col_lit
                     << ", from_value: CAST(f." << col_esc << " AS VARCHAR)"
                     << ", to_value: CAST(t." << col_esc << " AS VARCHAR)} END";
    }
    changes_expr << "), x -> x IS NOT NULL) as \"_tt_changes\"";

    std::ostringstream sql;
    sql << "WITH from_state AS ("
        << "  SELECT *, ROW_NUMBER() OVER (PARTITION BY \"_tt_pk_value\" ORDER BY \"_tt_version\" DESC) as rn"
        << "  FROM " << history_escaped
        << "  WHERE \"_tt_version\" <= " << bind_data.from_version
        << "), to_state AS ("
        << "  SELECT *, ROW_NUMBER() OVER (PARTITION BY \"_tt_pk_value\" ORDER BY \"_tt_version\" DESC) as rn"
        << "  FROM " << history_escaped
        << "  WHERE \"_tt_version\" <= " << bind_data.to_version
        << "), f AS (SELECT * FROM from_state WHERE rn = 1 AND \"_tt_operation\" != 'DELETE'),"
        << " t AS (SELECT * FROM to_state WHERE rn = 1 AND \"_tt_operation\" != 'DELETE')"
        // UPDATE first (establishes _tt_changes struct type for UNION ALL)
        << " SELECT " << cols_t.str() << ", 'UPDATE' as \"_tt_change_type\", " << changes_expr.str()
        << " FROM t INNER JOIN f ON t.\"_tt_pk_value\" = f.\"_tt_pk_value\""
        << " WHERE t.\"_tt_version\" != f.\"_tt_version\""
        << " UNION ALL"
        // New rows (in to but not from)
        << " SELECT " << cols_t.str() << ", 'INSERT' as \"_tt_change_type\", NULL as \"_tt_changes\""
        << " FROM t WHERE t.\"_tt_pk_value\" NOT IN (SELECT \"_tt_pk_value\" FROM f)"
        << " UNION ALL"
        // Deleted rows (in from but not to)
        << " SELECT " << cols_f.str() << ", 'DELETE' as \"_tt_change_type\", NULL as \"_tt_changes\""
        << " FROM f WHERE f.\"_tt_pk_value\" NOT IN (SELECT \"_tt_pk_value\" FROM t)";

    Connection conn(context.db->GetDatabase(context));
    gstate->result = conn.Query(sql.str());
    if (gstate->result->HasError()) {
        throw InternalException("stps_tt_diff: query failed: %s", gstate->result->GetError());
    }

    return std::move(gstate);
}

static void TTDiffScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &gstate = data_p.global_state->Cast<TTDiffGlobalState>();
    if (gstate.finished) {
        output.SetCardinality(0);
        return;
    }
    auto chunk = gstate.result->Fetch();
    if (!chunk || chunk->size() == 0) {
        gstate.finished = true;
        output.SetCardinality(0);
        return;
    }
    for (idx_t col = 0; col < output.ColumnCount() && col < chunk->ColumnCount(); col++) {
        output.data[col].Reference(chunk->data[col]);
    }
    output.SetCardinality(chunk->size());
}

//===--------------------------------------------------------------------===//
// Table Function: stps_tt_status()
//===--------------------------------------------------------------------===//

struct TTStatusBindData : public TableFunctionData {
    // No input params needed
};

struct TTStatusGlobalState : public GlobalTableFunctionState {
    unique_ptr<QueryResult> result;
    bool finished = false;
};

static unique_ptr<FunctionData> TTStatusBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<TTStatusBindData>();

    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("table_name");

    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("pk_column");

    return_types.push_back(LogicalType::BIGINT);
    names.push_back("current_version");

    return_types.push_back(LogicalType::TIMESTAMP);
    names.push_back("created_at");

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> TTStatusInit(ClientContext &context, TableFunctionInitInput &input) {
    auto gstate = make_uniq<TTStatusGlobalState>();

    Connection conn(context.db->GetDatabase(context));
    gstate->result = conn.Query("SELECT * FROM \"_stps_tt_tables\" ORDER BY table_name");
    if (gstate->result->HasError()) {
        throw InternalException("stps_tt_status: query failed: %s", gstate->result->GetError());
    }

    return std::move(gstate);
}

static void TTStatusScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &gstate = data_p.global_state->Cast<TTStatusGlobalState>();
    if (gstate.finished) {
        output.SetCardinality(0);
        return;
    }
    auto chunk = gstate.result->Fetch();
    if (!chunk || chunk->size() == 0) {
        gstate.finished = true;
        output.SetCardinality(0);
        return;
    }
    for (idx_t col = 0; col < output.ColumnCount() && col < chunk->ColumnCount(); col++) {
        output.data[col].Reference(chunk->data[col]);
    }
    output.SetCardinality(chunk->size());
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

    // stps_time_travel(table, version := N, as_of := TIMESTAMP)
    TableFunction time_travel_func("stps_time_travel", {LogicalType::VARCHAR},
                                    TimeTravelScan, TimeTravelBind, TimeTravelInit);
    time_travel_func.named_parameters["version"] = LogicalType::BIGINT;
    time_travel_func.named_parameters["as_of"] = LogicalType::TIMESTAMP;
    loader.RegisterFunction(time_travel_func);

    // stps_tt_log(table)
    TableFunction log_func("stps_tt_log", {LogicalType::VARCHAR},
                           TTLogScan, TTLogBind, TTLogInit);
    loader.RegisterFunction(log_func);

    // stps_tt_diff(table, from_version, to_version)
    TableFunction diff_func("stps_tt_diff", {LogicalType::VARCHAR},
                            TTDiffScan, TTDiffBind, TTDiffInit);
    diff_func.named_parameters["from_version"] = LogicalType::BIGINT;
    diff_func.named_parameters["to_version"] = LogicalType::BIGINT;
    loader.RegisterFunction(diff_func);

    // stps_tt_status()
    TableFunction status_func("stps_tt_status", {},
                              TTStatusScan, TTStatusBind, TTStatusInit);
    loader.RegisterFunction(status_func);
}

} // namespace stps
} // namespace duckdb
