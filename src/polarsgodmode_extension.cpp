#include "polarsgodmode_extension.hpp"
#include "case_transform.hpp"
#include "text_normalize.hpp"
#include "null_handling.hpp"
#include "uuid_functions.hpp"
#include "io_operations.hpp"
#include "iban_validation.hpp"
#include "xml_parser.hpp"
#include "gobd_reader.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

void PolarsgodmodeExtension::Load(ExtensionLoader &loader) {
    // Register all function categories
    polarsgodmode::RegisterCaseTransformFunctions(loader);
    polarsgodmode::RegisterTextNormalizeFunctions(loader);
    polarsgodmode::RegisterNullHandlingFunctions(loader);
    polarsgodmode::RegisterUuidFunctions(loader);
    polarsgodmode::RegisterIoOperationFunctions(loader);
    polarsgodmode::RegisterIbanValidationFunctions(loader);
    polarsgodmode::RegisterXmlParserFunctions(loader);
    polarsgodmode::RegisterGobdReaderFunctions(loader);
}

std::string PolarsgodmodeExtension::Name() {
    return "polarsgodmode";
}

std::string PolarsgodmodeExtension::Version() const {
#ifdef EXT_VERSION
    return EXT_VERSION;
#else
    return "v0.1.0";
#endif
}

} // namespace duckdb

// Entry point for the loadable extension
extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(polarsgodmode, loader) {
    duckdb::PolarsgodmodeExtension ext;
    ext.Load(loader);
}
}
