# External Integrations

## API Integrations

### Anthropic Claude API
- **Purpose**: AI-powered data functions (gender detection, classification, etc.)
- **Files**: `src/ai_functions.cpp`, `src/include/ai_functions.hpp`
- **Auth**: API key via `stps_set_anthropic_api_key()`
- **Requires**: `HAVE_CURL` compile flag

### Nextcloud/WebDAV
- **Purpose**: Fetch remote files from Nextcloud/WebDAV servers
- **Files**: `src/nextcloud_functions.cpp`
- **Auth**: Basic auth (username/password)
- **Requires**: `HAVE_CURL` compile flag

### Brave Search API
- **Purpose**: Web search for address lookup
- **Files**: `src/ai_functions.cpp`
- **Auth**: API key via `stps_set_brave_api_key()`

### Google Custom Search API
- **Purpose**: Alternative web search provider
- **Files**: `src/ai_functions.cpp`
- **Auth**: API key + CSE ID

## Data Sources

### BLZ LUT (Bankleitzahl Lookup Table)
- **Source**: Bundesbank
- **Files**: `src/blz_lut_loader.cpp`
- **Purpose**: German bank code validation
- **Cache**: `~/.stps/blz.lut`

## File Format Support

| Format | Read | Write | Library |
|--------|------|-------|---------|
| ZIP | ✅ | ❌ | miniz (bundled) |
| CSV | ✅ | ❌ | Custom parser |
| XML | ✅ | ❌ | Custom parser |
| Parquet | ✅ | ❌ | DuckDB native |
| Excel | ✅ | ❌ | Via spatial extension |
