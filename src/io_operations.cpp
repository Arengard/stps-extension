#include "io_operations.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <fstream>
#include <sys/stat.h>
#include <cerrno>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace duckdb {
namespace stps {

#ifdef _WIN32
// Convert UTF-8 string to UTF-16 wide string for Windows Unicode APIs
static std::wstring Utf8ToWide(const std::string& str) {
    if (str.empty()) return std::wstring();
    int size_needed = MultiByteToWideChar(CP_UTF8, 0, str.c_str(), (int)str.size(), nullptr, 0);
    std::wstring result(size_needed, 0);
    MultiByteToWideChar(CP_UTF8, 0, str.c_str(), (int)str.size(), &result[0], size_needed);
    return result;
}
#endif

// Check if file exists
bool file_exists(const std::string& path) {
#ifdef _WIN32
    std::wstring wpath = Utf8ToWide(path);
    struct _stat buffer;
    return (_wstat(wpath.c_str(), &buffer) == 0);
#else
    struct stat buffer;
    return (stat(path.c_str(), &buffer) == 0);
#endif
}

// Check if directory exists
bool directory_exists(const std::string& path) {
#ifdef _WIN32
    std::wstring wpath = Utf8ToWide(path);
    struct _stat buffer;
    if (_wstat(wpath.c_str(), &buffer) != 0) {
        return false;
    }
    return (buffer.st_mode & _S_IFDIR) != 0;
#else
    struct stat buffer;
    if (stat(path.c_str(), &buffer) != 0) {
        return false;
    }
    return S_ISDIR(buffer.st_mode);
#endif
}

// Get parent directory from path
std::string get_parent_directory(const std::string& path) {
    size_t pos = path.find_last_of("/\\");
    if (pos == std::string::npos) {
        return "";
    }
    return path.substr(0, pos);
}

// Create directory and all parent directories recursively
bool create_directories(const std::string& path) {
    if (path.empty()) {
        return true;
    }

    if (directory_exists(path)) {
        return true;
    }

    // Create parent directories first
    std::string parent = get_parent_directory(path);
    if (!parent.empty() && !directory_exists(parent)) {
        if (!create_directories(parent)) {
            return false;
        }
    }

    // Create this directory
    #ifdef _WIN32
    std::wstring wpath = Utf8ToWide(path);
    return CreateDirectoryW(wpath.c_str(), nullptr) != 0 || GetLastError() == ERROR_ALREADY_EXISTS;
    #else
    return mkdir(path.c_str(), 0755) == 0 || errno == EEXIST;
    #endif
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

        // Create parent directories if they don't exist
        std::string parent_dir = get_parent_directory(destination);
        if (!parent_dir.empty() && !directory_exists(parent_dir)) {
            if (!create_directories(parent_dir)) {
                src.close();
                return "ERROR: Cannot create destination directory: " + parent_dir;
            }
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

        // Create parent directories if they don't exist
        std::string parent_dir = get_parent_directory(destination);
        if (!parent_dir.empty() && !directory_exists(parent_dir)) {
            if (!create_directories(parent_dir)) {
                return "ERROR: Cannot create destination directory: " + parent_dir;
            }
        }

        // Try to rename (move) the file
        #ifdef _WIN32
        // On Windows, remove destination first if it exists
        if (file_exists(destination)) {
            std::wstring wdest = Utf8ToWide(destination);
            if (!DeleteFileW(wdest.c_str())) {
                return "ERROR: Cannot remove existing destination file: " + destination;
            }
        }
        {
            std::wstring wsrc = Utf8ToWide(source);
            std::wstring wdest = Utf8ToWide(destination);
            if (!MoveFileW(wsrc.c_str(), wdest.c_str())) {
                return "ERROR: Cannot move file from " + source + " to " + destination;
            }
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

// Helper function to rename file with error handling
std::string stps_rename_file_impl(const std::string& old_name, const std::string& new_name) {
    try {
        // Check if source exists
        if (!file_exists(old_name)) {
            return "ERROR: File does not exist: " + old_name;
        }

        // Check if destination already exists
        if (file_exists(new_name)) {
            return "ERROR: Destination file already exists: " + new_name;
        }

        // Try to rename the file
        #ifdef _WIN32
        {
            std::wstring wold = Utf8ToWide(old_name);
            std::wstring wnew = Utf8ToWide(new_name);
            if (!MoveFileW(wold.c_str(), wnew.c_str())) {
                return "ERROR: Cannot rename file from " + old_name + " to " + new_name;
            }
        }
        #else
        if (rename(old_name.c_str(), new_name.c_str()) != 0) {
            return "ERROR: Cannot rename file from " + old_name + " to " + new_name;
        }
        #endif

        return "SUCCESS: Renamed " + old_name + " to " + new_name;
    } catch (const std::exception& e) {
        return "ERROR: " + std::string(e.what());
    }
}

// Helper function to delete file with error handling
std::string stps_delete_file_impl(const std::string& path) {
    try {
        // Check if file exists
        if (!file_exists(path)) {
            return "ERROR: File does not exist: " + path;
        }

        // Try to delete the file
        #ifdef _WIN32
        {
            std::wstring wpath = Utf8ToWide(path);
            if (!DeleteFileW(wpath.c_str())) {
                return "ERROR: Cannot delete file: " + path;
            }
        }
        #else
        if (unlink(path.c_str()) != 0) {
            return "ERROR: Cannot delete file: " + path;
        }
        #endif

        return "SUCCESS: Deleted " + path;
    } catch (const std::exception& e) {
        return "ERROR: " + std::string(e.what());
    }
}

// Helper function to create folder with error handling
std::string stps_create_folders_impl(const std::string& path) {
    try {
        if (path.empty()) {
            return "ERROR: Empty folder path";
        }

        // Check if directory already exists
        if (directory_exists(path)) {
            return "WARNING: Folder already exists: " + path;
        }

        // Create the directory (and all parents)
        if (!create_directories(path)) {
            return "ERROR: Cannot create folder: " + path;
        }

        return "SUCCESS: Created folder: " + path;
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

static void StpsDeleteIoFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t path) {
            std::string path_str = path.GetString();
            std::string result_msg = stps_delete_file_impl(path_str);
            return StringVector::AddString(result, result_msg);
        });
}

static void StpsCreateFoldersIoFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, string_t>(
        args.data[0], result, args.size(),
        [&](string_t path) {
            std::string path_str = path.GetString();
            std::string result_msg = stps_create_folders_impl(path_str);
            return StringVector::AddString(result, result_msg);
        });
}

static void StpsRenameIoFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    BinaryExecutor::Execute<string_t, string_t, string_t>(
        args.data[0], args.data[1], result, args.size(),
        [&](string_t old_name, string_t new_name) {
            std::string old_str = old_name.GetString();
            std::string new_str = new_name.GetString();
            std::string result_msg = stps_rename_file_impl(old_str, new_str);
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

    // stps_delete_io(path)
    ScalarFunctionSet delete_io_set("stps_delete_io");
    delete_io_set.AddFunction(ScalarFunction({LogicalType::VARCHAR},
                                             LogicalType::VARCHAR, StpsDeleteIoFunction));
    loader.RegisterFunction(delete_io_set);

    // stps_create_folders_io(folder_path)
    ScalarFunctionSet create_folders_io_set("stps_create_folders_io");
    create_folders_io_set.AddFunction(ScalarFunction({LogicalType::VARCHAR},
                                                     LogicalType::VARCHAR, StpsCreateFoldersIoFunction));
    loader.RegisterFunction(create_folders_io_set);

    // stps_io_rename(old_name, new_name)
    ScalarFunctionSet rename_io_set("stps_io_rename");
    rename_io_set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR},
                                             LogicalType::VARCHAR, StpsRenameIoFunction));
    loader.RegisterFunction(rename_io_set);
}

} // namespace stps
} // namespace duckdb
