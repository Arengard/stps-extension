#include "fill_functions.hpp"

namespace duckdb {
namespace stps {

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//

void RegisterFillFunctions(ExtensionLoader &loader) {
	// Registration happens automatically via WindowExecutorFactory in DuckDB core
	// The actual implementations are in duckdb/src/function/window/window_value_function.cpp
	// This function is here for consistency with other registration patterns
}

} // namespace stps
} // namespace duckdb
