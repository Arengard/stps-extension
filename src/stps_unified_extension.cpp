#include "duckdb.hpp"
#include "duckdb/main/extension.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

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
#include "account_validation.hpp"

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
        stps::RegisterDropNullColumnsFunction(loader);
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
