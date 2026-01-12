#include "stps_lambda_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include <regex>
#include <algorithm>

namespace duckdb {
namespace stps {

// Bind data for stps_lambda function
struct LambdaBindData : public TableFunctionData {
    string table_name;
    string lambda_expr;
    bool varchar_only = true;
    string column_pattern = "";
    vector<string> selected_columns;
    vector<LogicalType> column_types;
    string transformed_query;
};

// Global state for stps_lambda function
struct LambdaGlobalState : public GlobalTableFunctionState {
    unique_ptr<QueryResult> result;
    unique_ptr<DataChunk> current_chunk;
};

// Parse lambda expression to extract the transformation
static string ParseLambdaExpression(const string& lambda_expr) {
    // Lambda format: "c -> expression" or just "expression"
    size_t arrow_pos = lambda_expr.find("->");
    if (arrow_pos != string::npos) {
        // Extract the part after "->"
        string expr = lambda_expr.substr(arrow_pos + 2);
        // Trim whitespace
        size_t start = expr.find_first_not_of(" \t\n\r");
        if (start != string::npos) {
            expr = expr.substr(start);
        }
        size_t end = expr.find_last_not_of(" \t\n\r");
        if (end != string::npos) {
            expr = expr.substr(0, end + 1);
        }
        return expr;
    }
    return lambda_expr;
}

// Apply transformation to a column name
static string ApplyTransformation(const string& column_name, const string& transformation) {
    // Replace 'c' with the actual column name (quoted)
    string result = transformation;
    string quoted_col = "\"" + column_name + "\"";
    
    // Simple replacement: replace 'c' with column name
    // We need to be careful to only replace 'c' as a standalone identifier
    size_t pos = 0;
    string temp_result;
    
    while (pos < result.length()) {
        if (result[pos] == 'c') {
            // Check if 'c' is standalone (not part of a word)
            bool is_start = (pos == 0 || !isalnum(result[pos-1]) && result[pos-1] != '_');
            bool is_end = (pos + 1 >= result.length() || !isalnum(result[pos+1]) && result[pos+1] != '_');
            
            if (is_start && is_end) {
                temp_result += quoted_col;
                pos++;
                continue;
            }
        }
        temp_result += result[pos];
        pos++;
    }
    
    return temp_result;
}

// Bind function for stps_lambda
static unique_ptr<FunctionData> LambdaBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<LambdaBindData>();

    // Required parameters: table_name and lambda_expression
    if (input.inputs.size() < 2) {
        throw BinderException("stps_lambda requires at least two arguments: table_name and lambda_expression");
    }
    
    result->table_name = input.inputs[0].GetValue<string>();
    result->lambda_expr = input.inputs[1].GetValue<string>();

    // Optional positional parameter: varchar_only (default: true)
    if (input.inputs.size() >= 3) {
        result->varchar_only = input.inputs[2].GetValue<bool>();
    }

    // Optional positional parameter: column_pattern (default: "")
    if (input.inputs.size() >= 4) {
        result->column_pattern = input.inputs[3].GetValue<string>();
    }

    // Handle named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "varchar_only") {
            result->varchar_only = kv.second.GetValue<bool>();
        } else if (kv.first == "column_pattern") {
            result->column_pattern = kv.second.GetValue<string>();
        }
    }

    // Create a connection to query table schema
    Connection conn(context.db->GetDatabase(context));

    // Get table schema
    auto schema_query = "SELECT * FROM " + result->table_name + " LIMIT 0";
    auto schema_result = conn.Query(schema_query);
    if (schema_result->HasError()) {
        throw BinderException("Table '%s' does not exist: %s", result->table_name, schema_result->GetError());
    }

    // Parse lambda expression
    string transformation = ParseLambdaExpression(result->lambda_expr);

    // Build the SELECT query with transformations
    string select_parts;
    for (idx_t i = 0; i < schema_result->names.size(); i++) {
        string col_name = schema_result->names[i];
        LogicalType col_type = schema_result->types[i];
        
        bool should_transform = true;
        
        // Apply varchar_only filter
        if (result->varchar_only && col_type != LogicalType::VARCHAR) {
            should_transform = false;
        }
        
        // Apply column_pattern filter
        if (!result->column_pattern.empty()) {
            if (col_name.find(result->column_pattern) == string::npos) {
                should_transform = false;
            }
        }
        
        if (i > 0) {
            select_parts += ", ";
        }
        
        if (should_transform) {
            // Apply transformation
            string transformed = ApplyTransformation(col_name, transformation);
            select_parts += transformed + " AS \"" + col_name + "\"";
        } else {
            // Keep column as-is
            select_parts += "\"" + col_name + "\"";
        }
        
        result->selected_columns.push_back(col_name);
        result->column_types.push_back(col_type);
    }

    result->transformed_query = "SELECT " + select_parts + " FROM " + result->table_name;

    // Set output schema (same as input table)
    names = result->selected_columns;
    return_types = result->column_types;

    return std::move(result);
}

// Init function for stps_lambda
static unique_ptr<GlobalTableFunctionState> LambdaInit(ClientContext &context, TableFunctionInitInput &input) {
    auto result = make_uniq<LambdaGlobalState>();
    
    auto &bind_data = input.bind_data->Cast<LambdaBindData>();
    
    // Execute the transformed query
    Connection conn(context.db->GetDatabase(context));
    result->result = conn.Query(bind_data.transformed_query);
    
    if (result->result->HasError()) {
        throw InternalException("Error executing lambda transformation: %s", result->result->GetError());
    }
    
    return std::move(result);
}

// Execute function for stps_lambda
static void LambdaExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &gstate = data.global_state->Cast<LambdaGlobalState>();

    if (!gstate.result) {
        output.SetCardinality(0);
        return;
    }

    // Try to fetch current chunk if we don't have one
    if (!gstate.current_chunk) {
        gstate.current_chunk = gstate.result->Fetch();
    }

    // If no chunk available, we're done
    if (!gstate.current_chunk || gstate.current_chunk->size() == 0) {
        output.SetCardinality(0);
        return;
    }

    // Reference the chunk data directly to output
    output.Reference(*gstate.current_chunk);
    
    // Fetch the next chunk for next call
    gstate.current_chunk = gstate.result->Fetch();
}

// Register the stps_lambda table function
void RegisterLambdaFunction(ExtensionLoader &loader) {
    TableFunction lambda_func("stps_lambda", {LogicalType::VARCHAR, LogicalType::VARCHAR}, 
                               LambdaExecute, LambdaBind, LambdaInit);
    
    // Add optional parameters
    lambda_func.varargs = LogicalType::ANY;
    
    // Add named parameters
    lambda_func.named_parameters["varchar_only"] = LogicalType::BOOLEAN;
    lambda_func.named_parameters["column_pattern"] = LogicalType::VARCHAR;

    loader.RegisterFunction(lambda_func);
}

} // namespace stps
} // namespace duckdb
