#include "include/io_operations.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <fstream>
#include <sys/stat.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace duckdb {
namespace polarsgodmode {

// Check if file exists
bool file_exists(const std::string& path) {
    struct stat buffer;
    return (stat(path.c_str(), &buffer) == 0);
}

// Helper function to copy file with error handling
std::string stps_copy_file_impl(const std::string& source, const std::string& destination) {
    try {
        // Check if source exists
        if (!file_exists(source)) {
            return "ERROR: Source file does not exist: " + source;
        }

        // Open source file
        std::ifstream src(source, std::ios::binary);
        if (!src.is_open()) {
            return "ERROR: Cannot open source file: " + source;
        }

        // Open destination file (truncate if exists)
        std::ofstream dest(destination, std::ios::binary | std::ios::trunc);
        if (!dest.is_open()) {
            src.close();
            return "ERROR: Cannot create destination file: " + destination;
        }

        // Copy the file
        dest << src.rdbuf();

        // Close files
        src.close();
        dest.close();

        // Check if copy was successful
        if (!dest.good()) {
            return "ERROR: Failed to write to destination: " + destination;
        }

        return "SUCCESS: Copied " + source + " to " + destination;
    } catch (const std::exception& e) {
        return "ERROR: " + std::string(e.what());
    }
}

// Helper function to move file with error handling
std::string stps_move_file_impl(const std::string& source, const std::string& destination) {
    try {
        // Check if source exists
        if (!file_exists(source)) {
            return "ERROR: Source file does not exist: " + source;
        }

        // Try to rename (move) the file
        #ifdef _WIN32
        // On Windows, remove destination first if it exists
        if (file_exists(destination)) {
            if (!DeleteFileA(destination.c_str())) {
                return "ERROR: Cannot remove existing destination file: " + destination;
            }
        }
        if (!MoveFileA(source.c_str(), destination.c_str())) {
            return "ERROR: Cannot move file from " + source + " to " + destination;
        }
        #else
        // On Unix-like systems, rename handles overwrite
        if (rename(source.c_str(), destination.c_str()) != 0) {
            return "ERROR: Cannot move file from " + source + " to " + destination;
        }
        #endif

        return "SUCCESS: Moved " + source + " to " + destination;
    } catch (const std::exception& e) {
        return "ERROR: " + std::string(e.what());
    }
}

// DuckDB scalar function wrappers
static void StpsCopyIoFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t source, string_t destination) {
            std::string src_str = source.GetString();
            std::string dest_str = destination.GetString();
            std::string result_msg = stps_copy_file_impl(src_str, dest_str);
            return StringVector::AddString(result, result_msg);
        });
}

static void StpsMoveIoFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t source, string_t destination) {
            std::string src_str = source.GetString();
            std::string dest_str = destination.GetString();
            std::string result_msg = stps_move_file_impl(src_str, dest_str);
            return StringVector::AddString(result, result_msg);
        });
}

void RegisterIoOperationFunctions(ExtensionLoader &loader) {
    // stps_copy_io(source_path, destination_path)
    ScalarFunctionSet copy_io_set("stps_copy_io");
    copy_io_set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR},
                                           LogicalType::VARCHAR, StpsCopyIoFunction));
    loader.RegisterFunction(copy_io_set);

    // stps_move_io(source_path, destination_path)
    ScalarFunctionSet move_io_set("stps_move_io");
    move_io_set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR},
                                           LogicalType::VARCHAR, StpsMoveIoFunction));
    loader.RegisterFunction(move_io_set);
}

} // namespace polarsgodmode
} // namespace duckdb
