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
#include "drop_duplicates_function.hpp"
#include "arrange_function.hpp"
#include "street_split.hpp"
#include "plz_validation.hpp"
#include "address_lookup.hpp"
#include "search_columns_function.hpp"
#include "search_database_function.hpp"
#include "clean_database_function.hpp"
#include "inso_account_function.hpp"
#ifdef HAVE_CURL
#include "ai_functions.hpp"
#include "nextcloud_functions.hpp"
#include "gobd_cloud_reader.hpp"
#endif
#include "account_validation.hpp"
#include "smart_cast_scalar.hpp"
#include "smart_cast_function.hpp"
#include "stps_lambda_function.hpp"
#include "blz_lut_loader.hpp"
#include "zip_functions.hpp"
#include "import_folder_functions.hpp"
// #include "fill_functions.hpp"  // Temporarily disabled

namespace duckdb {
namespace stps {
    void RegisterFilesystemFunctions(ExtensionLoader &loader);
    void RegisterZipFunctions(ExtensionLoader &loader);
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

        // Register drop duplicates function
        stps::RegisterDropDuplicatesFunction(loader);

        // Register arrange function
        stps::RegisterArrangeFunctions(loader);

        // Register street split function
        stps::RegisterStreetSplitFunctions(loader);

        // Register PLZ validation function
        stps::RegisterPlzValidationFunctions(loader);

        // Register address lookup function (Google Impressum search)
        stps::RegisterAddressLookupFunctions(loader);

        // Register smart cast functions
        stps::RegisterSmartCastScalarFunction(loader);
        stps::RegisterSmartCastTableFunctions(loader);

        // Register lambda function
        stps::RegisterLambdaFunction(loader);

        // Register ZIP archive functions
        stps::RegisterZipFunctions(loader);

        // Register folder import functions (local + cloud)
        stps::RegisterImportFolderFunctions(loader);

        // Register Nextcloud/WebDAV function (requires curl)
#ifdef HAVE_CURL
        stps::RegisterNextcloudFunctions(loader);
        stps::RegisterGobdCloudReaderFunctions(loader);
#endif

        // Register search columns function
        stps::RegisterSearchColumnsFunction(loader);

        // Register search database function
        stps::RegisterSearchDatabaseFunction(loader);

        // Register clean database function
        stps::RegisterCleanDatabaseFunction(loader);

        // Register insolvency account mapping function
        stps::RegisterInsoAccountFunction(loader);

        // Register AI functions (Anthropic Claude integration)
#ifdef HAVE_CURL
        stps::RegisterAIFunctions(loader);
#endif

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
