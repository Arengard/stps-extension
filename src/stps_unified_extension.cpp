#include "duckdb.hpp"
#include "duckdb/main/extension.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/client_context.hpp"
#include <fstream>
#include <iostream>
#include <ios>

// Include all function registration headers
#include "case_transform.hpp"
#include "text_normalize.hpp"
#include "null_handling.hpp"
#include "uuid_functions.hpp"
#include "io_operations.hpp"
#include "iban_validation.hpp"
#include "xml_parser.hpp"
#include "gobd_reader.hpp"
#include "drop_null_columns_function.hpp"
// #include "search_columns_function.hpp"  // Uncommitted work-in-progress
#include "account_validation.hpp"
#include "smart_cast_scalar.hpp"
#include "smart_cast_function.hpp"
#include "blz_lut_loader.hpp"
// #include "fill_functions.hpp"  // Temporarily disabled

namespace duckdb {
namespace stps {
    void RegisterFilesystemFunctions(ExtensionLoader &loader);
}

class StpsExtension : public Extension {
public:
    void Load(ExtensionLoader &loader) override {
        // Register all stps functions
        stps::RegisterCaseTransformFunctions(loader);
        stps::RegisterTextNormalizeFunctions(loader);
        stps::RegisterNullHandlingFunctions(loader);
        stps::RegisterUuidFunctions(loader);
        stps::RegisterIoOperationFunctions(loader);
        stps::RegisterIbanValidationFunctions(loader);
        stps::RegisterXmlParserFunctions(loader);
        stps::RegisterGobdReaderFunctions(loader);
        stps::RegisterAccountValidationFunctions(loader);

        // Register filesystem table functions
        stps::RegisterFilesystemFunctions(loader);

        // Register drop null columns function
        stps::RegisterDropNullColumnsFunction(loader);

        // Register smart cast functions
        stps::RegisterSmartCastScalarFunction(loader);
        stps::RegisterSmartCastTableFunctions(loader);

        // Register search columns function
        // stps::RegisterSearchColumnsFunction(loader);  // Uncommitted work-in-progress

        // Register fill window functions
        // stps::RegisterFillFunctions(loader);  // Temporarily disabled

        // Initialize BLZ LUT loader (download if not present)
        // This will check if ~/.stps/blz.lut exists and download if needed
        // Note: Actual loading/parsing happens lazily on first use
        std::string lut_path = stps::BlzLutLoader::GetInstance().GetLutFilePath();
        if (!stps::BlzLutLoader::GetInstance().IsLoaded()) {
            // Check if file exists, download if not
            std::ifstream f(lut_path);
            if (!f.good()) {
                std::cout << "BLZ LUT file not found, downloading..." << std::endl;
                stps::BlzLutLoader::GetInstance().DownloadLutFile(lut_path);
            }
        }
    }

    std::string Name() override {
        return "stps";
    }

    std::string Version() const override {
#ifdef EXT_VERSION
        return EXT_VERSION;
#else
        return "v1.0.0";
#endif
    }
};

} // namespace duckdb

// Entry point for the loadable extension
extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(stps, loader) {
    duckdb::StpsExtension ext;
    ext.Load(loader);
}
}
