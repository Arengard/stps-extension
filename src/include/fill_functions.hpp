#pragma once

#include "duckdb.hpp"
#include "duckdb/function/window/window_value_function.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {
namespace stps {

// Forward fill executor - fills NULLs with previous non-NULL value
class WindowFillDownExecutor : public WindowValueExecutor {
public:
	WindowFillDownExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

	// Never ignore nulls - we need to see them to fill them
	bool IgnoreNulls() const override {
		return false;
	}

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

// Backward fill executor - fills NULLs with next non-NULL value
class WindowFillUpExecutor : public WindowValueExecutor {
public:
	WindowFillUpExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared);

	// Never ignore nulls - we need to see them to fill them
	bool IgnoreNulls() const override {
		return false;
	}

protected:
	void EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx,
	                      OperatorSinkInput &sink) const override;
};

// Register fill window functions
void RegisterFillFunctions(ExtensionLoader &loader);

} // namespace stps
} // namespace duckdb
