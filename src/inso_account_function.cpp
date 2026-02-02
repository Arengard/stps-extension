#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include <unordered_map>
#include <algorithm>
#include <cctype>
#include <cmath>
#include <sstream>

namespace duckdb {
namespace stps {

// Helper: lowercase a string
static string ToLowerInso(const string &s) {
    string result;
    result.reserve(s.size());
    for (char c : s) {
        result += std::tolower(static_cast<unsigned char>(c));
    }
    return result;
}

// Helper: extract leading digits from account number as prefix
static string AccountPrefix(int64_t konto, int len) {
    string s = std::to_string(konto);
    if ((int)s.size() <= len) return s;
    return s.substr(0, len);
}

// Helper: compute simple word overlap score between two strings (case-insensitive)
static double NameSimilarity(const string &a, const string &b) {
    auto tokenize = [](const string &s) -> vector<string> {
        vector<string> tokens;
        string current;
        for (char c : s) {
            if (std::isalnum(static_cast<unsigned char>(c))) {
                current += std::tolower(static_cast<unsigned char>(c));
            } else {
                if (!current.empty()) {
                    tokens.push_back(current);
                    current.clear();
                }
            }
        }
        if (!current.empty()) tokens.push_back(current);
        return tokens;
    };

    auto tokens_a = tokenize(a);
    auto tokens_b = tokenize(b);
    if (tokens_a.empty() || tokens_b.empty()) return 0.0;

    int matches = 0;
    for (const auto &ta : tokens_a) {
        for (const auto &tb : tokens_b) {
            if (ta == tb) {
                matches++;
                break;
            }
        }
    }
    // Jaccard-like: matches / max(size_a, size_b)
    return (double)matches / (double)std::max(tokens_a.size(), tokens_b.size());
}

// An entry from rl.inso_kontenrahmen
struct InsoKontoEntry {
    string kontoart;       // "Einnahmekonten" or "Ausgabekonten"
    string decEAKontoNr;   // e.g. "4970", "8200"
    string kontobezeichnung;
};

// Bind data
struct InsoAccountBindData : public TableFunctionData {
    string source_table;
    int64_t bank_account;
    // Original columns from source table
    vector<string> original_column_names;
    vector<LogicalType> original_column_types;
};

// Global state
struct InsoAccountGlobalState : public GlobalTableFunctionState {
    // Mapping tables built at init time
    // Kreditor konto -> (ea_konto, ea_bezeichnung, mapping_source)
    std::unordered_map<int64_t, std::tuple<string, string, string>> kreditor_map;
    // Aufwand konto -> (ea_konto, ea_bezeichnung)
    std::unordered_map<int64_t, std::pair<string, string>> aufwand_ea_map;
    // Sachkonto -> (ea_konto, ea_bezeichnung)
    std::unordered_map<int64_t, std::pair<string, string>> sachkonto_ea_map;
    // All inso kontenrahmen entries (for matching)
    vector<InsoKontoEntry> inso_entries;
    // All konto entries (kontoart, konto number, bezeichnung)
    struct KontoEntry {
        string kontoart;
        int64_t konto;
        string kontobezeichnung;
    };
    vector<KontoEntry> konto_entries;

    // Query result for bank transactions
    unique_ptr<QueryResult> result;
    unique_ptr<DataChunk> current_chunk;
    idx_t chunk_offset = 0;
    bool finished = false;

    // Column indices in the source result
    idx_t col_decKontoNr = 0;
    idx_t col_kontobezeichnung = 0;
    idx_t col_kontoart = 0;
    idx_t col_decGegenkontoNr = 0;
    idx_t col_gegenkontobezeichnung = 0;
    idx_t col_gegenkontoart = 0;
};

// Helper: find the schema prefix for a table name (e.g. "rl.buchungen" -> "rl")
static string GetSchemaPrefix(const string &table_name) {
    auto dot_pos = table_name.find('.');
    if (dot_pos != string::npos) {
        return table_name.substr(0, dot_pos);
    }
    return "";
}

// Helper: escape an identifier
static string EscapeId(const string &name) {
    return "\"" + name + "\"";
}

// Helper: build a schema-qualified, properly escaped table reference
static string EscapeTableRef(const string &table_name) {
    auto dot_pos = table_name.find('.');
    if (dot_pos != string::npos) {
        string schema = table_name.substr(0, dot_pos);
        string table = table_name.substr(dot_pos + 1);
        return EscapeId(schema) + "." + EscapeId(table);
    }
    return EscapeId(table_name);
}

// Find best matching Ausgabekonten entry for an Aufwand account
static std::pair<string, string> FindBestAusgabeMatch(
    int64_t aufwand_konto, const string &aufwand_bezeichnung,
    const vector<InsoKontoEntry> &inso_entries) {

    string best_ea;
    string best_bezeichnung;
    double best_score = -1.0;

    string aufwand_str = std::to_string(aufwand_konto);

    for (const auto &entry : inso_entries) {
        if (entry.kontoart != "Ausgabekonten") continue;

        double score = 0.0;

        string ea_nr = entry.decEAKontoNr;

        // Remove trailing non-numeric chars (e.g. "4782XX")
        string ea_numeric;
        for (char c : ea_nr) {
            if (std::isdigit(static_cast<unsigned char>(c))) {
                ea_numeric += c;
            } else {
                break;
            }
        }

        // Prefix matching: give a bonus for shared leading digits
        int prefix_match = 0;
        int max_check = std::min({(int)aufwand_str.size(), (int)ea_numeric.size(), 4});
        for (int i = 0; i < max_check; i++) {
            if (aufwand_str[i] == ea_numeric[i]) {
                prefix_match++;
            } else {
                break;
            }
        }
        score += prefix_match * 2.0;

        // Name similarity
        double name_score = NameSimilarity(aufwand_bezeichnung, entry.kontobezeichnung);
        score += name_score * 10.0;

        if (score > best_score) {
            best_score = score;
            best_ea = entry.decEAKontoNr;
            best_bezeichnung = entry.kontobezeichnung;
        }
    }

    // Require a minimum score to avoid garbage matches.
    // A name_score of ~0.2 plus no prefix match gives score ~2.0
    // Require at least one meaningful word match (score > 2.0) or a prefix match
    if (best_score < 2.0) {
        // Fallback to 4900 "Sonstige betriebliche Aufwendungen" for unmatched Aufwand
        return {"4900", "Sonstige betriebliche Aufwendungen"};
    }

    return {best_ea, best_bezeichnung};
}

// Find best matching entry for a Sachkonto
static std::pair<string, string> FindBestSachkontoMatch(
    int64_t sachkonto, const string &sachkonto_bezeichnung,
    const vector<InsoKontoEntry> &inso_entries) {

    // Check if it's a Vorsteuer account (14xxxx range)
    string konto_str = std::to_string(sachkonto);
    if (konto_str.size() >= 2 && konto_str.substr(0, 2) == "14") {
        // Vorsteuer -> 1780 (Umsatzsteuerzahlungen)
        return {"1780", "Umsatzsteuerzahlungen"};
    }

    // For other Sachkonten, try name matching across all inso entries
    string best_ea;
    string best_bezeichnung;
    double best_score = -1.0;

    for (const auto &entry : inso_entries) {
        double name_score = NameSimilarity(sachkonto_bezeichnung, entry.kontobezeichnung);
        if (name_score > best_score && name_score > 0.2) {
            best_score = name_score;
            best_ea = entry.decEAKontoNr;
            best_bezeichnung = entry.kontobezeichnung;
        }
    }

    // Return empty if no match found (will show as NULL)
    return {best_ea, best_bezeichnung};
}

// Bind function
static unique_ptr<FunctionData> InsoAccountBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<InsoAccountBindData>();

    if (input.inputs.size() < 1) {
        throw BinderException("stps_inso_account requires at least 1 argument: source table name");
    }
    result->source_table = input.inputs[0].GetValue<string>();

    // Get bank_account named parameter
    auto it = input.named_parameters.find("bank_account");
    if (it != input.named_parameters.end()) {
        result->bank_account = it->second.GetValue<int64_t>();
    } else {
        throw BinderException("stps_inso_account requires named parameter bank_account");
    }

    // Query source table schema
    Connection conn(context.db->GetDatabase(context));
    string escaped_table = EscapeTableRef(result->source_table);
    auto schema_result = conn.Query("SELECT * FROM " + escaped_table + " LIMIT 0");
    if (schema_result->HasError()) {
        throw BinderException("stps_inso_account: cannot query table '%s': %s",
                              result->source_table, schema_result->GetError());
    }

    result->original_column_names = schema_result->names;
    result->original_column_types = schema_result->types;

    // Set return types: all original columns + extra columns
    for (idx_t i = 0; i < schema_result->names.size(); i++) {
        names.push_back(schema_result->names[i]);
        return_types.push_back(schema_result->types[i]);
    }

    // Additional columns
    names.push_back("counter_konto");
    return_types.push_back(LogicalType::BIGINT);

    names.push_back("counter_kontobezeichnung");
    return_types.push_back(LogicalType::VARCHAR);

    names.push_back("counter_kontoart");
    return_types.push_back(LogicalType::VARCHAR);

    names.push_back("ea_konto");
    return_types.push_back(LogicalType::VARCHAR);

    names.push_back("ea_kontobezeichnung");
    return_types.push_back(LogicalType::VARCHAR);

    names.push_back("mapping_source");
    return_types.push_back(LogicalType::VARCHAR);

    return std::move(result);
}

// Init function: build lookup maps and execute filtered query
static unique_ptr<GlobalTableFunctionState> InsoAccountInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<InsoAccountBindData>();
    auto state = make_uniq<InsoAccountGlobalState>();

    Connection conn(context.db->GetDatabase(context));
    string schema_prefix = GetSchemaPrefix(bind_data.source_table);

    // Determine table names for konto and inso_kontenrahmen
    string konto_table = schema_prefix.empty() ? "konto" : (EscapeId(schema_prefix) + ".\"konto\"");
    string inso_table = schema_prefix.empty() ? "inso_kontenrahmen" : (EscapeId(schema_prefix) + ".\"inso_kontenrahmen\"");

    // Step 1: Load inso_kontenrahmen
    {
        auto res = conn.Query("SELECT kontoart, \"decEAKontoNr\", kontobezeichnung FROM " + inso_table);
        if (res->HasError()) {
            throw InternalException("stps_inso_account: failed to load inso_kontenrahmen: %s", res->GetError());
        }
        while (true) {
            auto chunk = res->Fetch();
            if (!chunk || chunk->size() == 0) break;
            for (idx_t row = 0; row < chunk->size(); row++) {
                InsoKontoEntry entry;
                entry.kontoart = chunk->data[0].GetValue(row).ToString();
                entry.decEAKontoNr = chunk->data[1].GetValue(row).ToString();
                entry.kontobezeichnung = chunk->data[2].GetValue(row).ToString();
                state->inso_entries.push_back(entry);
            }
        }
    }

    // Step 2: Load rl.konto
    {
        auto res = conn.Query("SELECT kontoart, konto, kontobezeichnung FROM " + konto_table);
        if (res->HasError()) {
            throw InternalException("stps_inso_account: failed to load konto table: %s", res->GetError());
        }
        while (true) {
            auto chunk = res->Fetch();
            if (!chunk || chunk->size() == 0) break;
            for (idx_t row = 0; row < chunk->size(); row++) {
                InsoAccountGlobalState::KontoEntry entry;
                entry.kontoart = chunk->data[0].GetValue(row).ToString();
                entry.konto = chunk->data[1].GetValue(row).GetValue<int64_t>();
                entry.kontobezeichnung = chunk->data[2].GetValue(row).ToString();
                state->konto_entries.push_back(entry);
            }
        }
    }

    // Step 3: Build Kreditor -> dominant Aufwand mapping
    // For each Kreditor, find which Aufwand gegenkonto has the highest total amount
    {
        string escaped_table = EscapeTableRef(bind_data.source_table);
        string sql = "SELECT \"decKontoNr\", \"decGegenkontoNr\", \"gegenkontobezeichnung\", SUM(umsatz) as total "
                     "FROM " + escaped_table + " "
                     "WHERE kontoart = 'K' AND gegenkontoart = 'Aufwand' "
                     "GROUP BY \"decKontoNr\", \"decGegenkontoNr\", \"gegenkontobezeichnung\" "
                     "ORDER BY \"decKontoNr\", total DESC";
        auto res = conn.Query(sql);
        if (!res->HasError()) {
            // For each Kreditor, keep only the first (highest total) Aufwand
            std::unordered_map<int64_t, std::pair<int64_t, string>> kreditor_best_aufwand;
            while (true) {
                auto chunk = res->Fetch();
                if (!chunk || chunk->size() == 0) break;
                for (idx_t row = 0; row < chunk->size(); row++) {
                    int64_t kreditor = chunk->data[0].GetValue(row).GetValue<int64_t>();
                    int64_t aufwand_konto = chunk->data[1].GetValue(row).GetValue<int64_t>();
                    string aufwand_bez = chunk->data[2].GetValue(row).ToString();

                    // Only store first occurrence per Kreditor (highest total due to ORDER BY)
                    if (kreditor_best_aufwand.find(kreditor) == kreditor_best_aufwand.end()) {
                        kreditor_best_aufwand[kreditor] = {aufwand_konto, aufwand_bez};
                    }
                }
            }

            // Now map each Kreditor's dominant Aufwand to an EA account
            for (auto &kv : kreditor_best_aufwand) {
                int64_t kreditor = kv.first;
                int64_t aufwand_konto = kv.second.first;
                string aufwand_bez = kv.second.second;

                auto ea_match = FindBestAusgabeMatch(aufwand_konto, aufwand_bez, state->inso_entries);
                if (!ea_match.first.empty()) {
                    string source = "Kreditor via Aufwand " + std::to_string(aufwand_konto) +
                                    " (" + aufwand_bez + ")";
                    state->kreditor_map[kreditor] = std::make_tuple(ea_match.first, ea_match.second, source);
                }
            }
        }
    }

    // Step 4: Build Aufwand -> EA mapping for all Aufwand accounts in konto
    for (const auto &entry : state->konto_entries) {
        if (entry.kontoart == "Aufwand") {
            auto match = FindBestAusgabeMatch(entry.konto, entry.kontobezeichnung, state->inso_entries);
            if (!match.first.empty()) {
                state->aufwand_ea_map[entry.konto] = match;
            }
        }
    }

    // Step 5: Build Sachkonto -> EA mapping
    for (const auto &entry : state->konto_entries) {
        if (entry.kontoart == "Sachkonto") {
            auto match = FindBestSachkontoMatch(entry.konto, entry.kontobezeichnung, state->inso_entries);
            if (!match.first.empty()) {
                state->sachkonto_ea_map[entry.konto] = match;
            }
        }
    }

    // Step 6: Find column indices in source
    for (idx_t i = 0; i < bind_data.original_column_names.size(); i++) {
        const string &name = bind_data.original_column_names[i];
        if (name == "decKontoNr") state->col_decKontoNr = i;
        else if (name == "kontobezeichnung") state->col_kontobezeichnung = i;
        else if (name == "kontoart") state->col_kontoart = i;
        else if (name == "decGegenkontoNr") state->col_decGegenkontoNr = i;
        else if (name == "gegenkontobezeichnung") state->col_gegenkontobezeichnung = i;
        else if (name == "gegenkontoart") state->col_gegenkontoart = i;
    }

    // Step 7: Execute the filtered query for bank transactions
    {
        string escaped_table = EscapeTableRef(bind_data.source_table);
        string sql = "SELECT * FROM " + escaped_table +
                     " WHERE \"decKontoNr\" = " + std::to_string(bind_data.bank_account) +
                     " OR \"decGegenkontoNr\" = " + std::to_string(bind_data.bank_account);
        state->result = conn.Query(sql);
        if (state->result->HasError()) {
            throw InternalException("stps_inso_account: failed to query bank transactions: %s",
                                    state->result->GetError());
        }
    }

    state->finished = false;
    return std::move(state);
}

// Scan function: process rows and add mapping columns
static void InsoAccountScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<InsoAccountBindData>();
    auto &state = data_p.global_state->Cast<InsoAccountGlobalState>();

    if (state.finished) {
        output.SetCardinality(0);
        return;
    }

    idx_t output_idx = 0;
    idx_t num_original = bind_data.original_column_names.size();

    while (output_idx < STANDARD_VECTOR_SIZE && !state.finished) {
        // Fetch new chunk if needed
        if (!state.current_chunk || state.chunk_offset >= state.current_chunk->size()) {
            state.current_chunk = state.result->Fetch();
            state.chunk_offset = 0;
            if (!state.current_chunk || state.current_chunk->size() == 0) {
                state.finished = true;
                break;
            }
        }

        while (state.chunk_offset < state.current_chunk->size() && output_idx < STANDARD_VECTOR_SIZE) {
            idx_t row = state.chunk_offset;

            // Copy all original columns
            for (idx_t col = 0; col < num_original; col++) {
                Value val = state.current_chunk->data[col].GetValue(row);
                output.data[col].SetValue(output_idx, val);
            }

            // Determine counter-account (the non-bank side)
            int64_t konto_nr = state.current_chunk->data[state.col_decKontoNr].GetValue(row).GetValue<int64_t>();
            string konto_bez = state.current_chunk->data[state.col_kontobezeichnung].GetValue(row).ToString();
            string konto_art = state.current_chunk->data[state.col_kontoart].GetValue(row).IsNull() ?
                               "" : state.current_chunk->data[state.col_kontoart].GetValue(row).ToString();
            int64_t gegen_nr = state.current_chunk->data[state.col_decGegenkontoNr].GetValue(row).GetValue<int64_t>();
            string gegen_bez = state.current_chunk->data[state.col_gegenkontobezeichnung].GetValue(row).IsNull() ?
                               "" : state.current_chunk->data[state.col_gegenkontobezeichnung].GetValue(row).ToString();
            string gegen_art = state.current_chunk->data[state.col_gegenkontoart].GetValue(row).IsNull() ?
                               "" : state.current_chunk->data[state.col_gegenkontoart].GetValue(row).ToString();

            // The counter-account is the side that is NOT the bank account
            int64_t counter_konto;
            string counter_bez;
            string counter_art;
            if (konto_nr == bind_data.bank_account) {
                counter_konto = gegen_nr;
                counter_bez = gegen_bez;
                counter_art = gegen_art;
            } else {
                counter_konto = konto_nr;
                counter_bez = konto_bez;
                counter_art = konto_art;
            }

            // Set counter columns
            output.data[num_original + 0].SetValue(output_idx, Value::BIGINT(counter_konto));
            output.data[num_original + 1].SetValue(output_idx, Value(counter_bez));
            output.data[num_original + 2].SetValue(output_idx, Value(counter_art));

            // Apply mapping rules based on counter_art
            string ea_konto;
            string ea_bezeichnung;
            string mapping_source;

            if (counter_art == "D") {
                // Debitor -> 8200
                ea_konto = "8200";
                ea_bezeichnung = "Forderungseinzug aus L.u.L. - 19% Ust";
                mapping_source = "Debitor -> 8200";
            } else if (counter_art == "Erlös" || counter_art == "Erloes" || counter_art == u8"Erlös") {
                // Erloes -> 8200
                ea_konto = "8200";
                ea_bezeichnung = "Forderungseinzug aus L.u.L. - 19% Ust";
                mapping_source = "Erloes -> 8200";
            } else if (counter_art == "K") {
                // Kreditor -> look up dominant Aufwand mapping
                auto it = state.kreditor_map.find(counter_konto);
                if (it != state.kreditor_map.end()) {
                    ea_konto = std::get<0>(it->second);
                    ea_bezeichnung = std::get<1>(it->second);
                    mapping_source = std::get<2>(it->second);
                } else {
                    mapping_source = "Kreditor: no Aufwand mapping found";
                }
            } else if (counter_art == "Aufwand") {
                // Aufwand -> match to Ausgabekonten
                auto it = state.aufwand_ea_map.find(counter_konto);
                if (it != state.aufwand_ea_map.end()) {
                    ea_konto = it->second.first;
                    ea_bezeichnung = it->second.second;
                    mapping_source = "Aufwand " + std::to_string(counter_konto) +
                                     " -> " + ea_konto;
                } else {
                    mapping_source = "Aufwand: no match found";
                }
            } else if (counter_art == "Sachkonto") {
                // Sachkonto -> check Vorsteuer or best match
                auto it = state.sachkonto_ea_map.find(counter_konto);
                if (it != state.sachkonto_ea_map.end()) {
                    ea_konto = it->second.first;
                    ea_bezeichnung = it->second.second;
                    string konto_str = std::to_string(counter_konto);
                    if (konto_str.size() >= 2 && konto_str.substr(0, 2) == "14") {
                        mapping_source = "Vorsteuer -> 1780";
                    } else {
                        mapping_source = "Sachkonto " + std::to_string(counter_konto) +
                                         " -> " + ea_konto + " (name match)";
                    }
                } else {
                    mapping_source = "Sachkonto: no match found";
                }
            } else if (counter_art == "Geldkonto") {
                // Geldkonto -> 1360
                ea_konto = "1360";
                ea_bezeichnung = "";
                mapping_source = "Geldkonto -> 1360";
            } else {
                mapping_source = "Unknown kontoart: " + counter_art;
            }

            // Set EA columns
            if (!ea_konto.empty()) {
                output.data[num_original + 3].SetValue(output_idx, Value(ea_konto));
                output.data[num_original + 4].SetValue(output_idx, Value(ea_bezeichnung));
            } else {
                output.data[num_original + 3].SetValue(output_idx, Value());
                output.data[num_original + 4].SetValue(output_idx, Value());
            }
            output.data[num_original + 5].SetValue(output_idx, Value(mapping_source));

            output_idx++;
            state.chunk_offset++;
        }
    }

    output.SetCardinality(output_idx);
}

void RegisterInsoAccountFunction(ExtensionLoader &loader) {
    TableFunction func(
        "stps_inso_account",
        {LogicalType::VARCHAR},
        InsoAccountScan,
        InsoAccountBind,
        InsoAccountInit
    );

    func.named_parameters["bank_account"] = LogicalType::BIGINT;

    loader.RegisterFunction(func);
}

} // namespace stps
} // namespace duckdb
