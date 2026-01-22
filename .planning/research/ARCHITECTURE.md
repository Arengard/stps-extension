# Architecture Research

**Domain:** DuckDB C++ extension for data quality and enrichment
**Researched:** 2026-01-22
**Confidence:** MEDIUM

## Standard Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────────┐
│                         DuckDB Engine                        │
├─────────────────────────────────────────────────────────────┤
│                     STPS Extension Layer                     │
│  ┌─────────────┐  ┌─────────────┐  ┌───────────────────────┐ │
│  │ Scalar Fns  │  │ Table Fns   │  │ Optional Integrations │ │
│  └──────┬──────┘  └──────┬──────┘  └───────────┬───────────┘ │
│         │               │                     │             │
│  ┌──────┴───────────────┴─────────────────────┴───────────┐ │
│  │                   Shared Utilities                     │ │
│  │   parsing, validation, caching, filesystem, http        │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility | Typical Implementation |
|-----------|----------------|------------------------|
| Extension entry | Register functions | `stps_unified_extension.cpp` |
| Scalar functions | 1-row -> 1-value ops | `src/*_functions.cpp` |
| Table functions | Set-returning functions | Bind/Init/Scan pattern |
| Shared utilities | Common helpers | `src/shared/*` and `src/utils.cpp` |
| Integrations | Network/API access | `curl_utils.cpp`, AI modules |

## Recommended Project Structure

```
src/
├── include/                 # Public headers
├── shared/                  # Reusable utilities
├── integrations/            # External APIs (curl/AI)
├── functions/
│   ├── scalar/              # Scalar functions by domain
│   └── table/               # Table functions by domain
└── stps_unified_extension.cpp
```

### Structure Rationale

- **functions/** separates feature domains and clarifies ownership.
- **integrations/** isolates optional network-dependent code.

## Architectural Patterns

### Pattern 1: Registration Aggregator

**What:** Central entry point calls `Register*Functions`.
**When to use:** Always, to keep module boundaries clean.
**Trade-offs:** Requires strict naming conventions.

### Pattern 2: Bind -> Init -> Scan (Table Functions)

**What:** Bind validates inputs, Init prepares state, Scan streams chunks.
**When to use:** Any table function with external IO or large data.
**Trade-offs:** Slightly more boilerplate but avoids memory spikes.

### Pattern 3: Optional Feature Guards

**What:** `#ifdef HAVE_CURL` blocks for network functionality.
**When to use:** Optional deps or platform-specific features.
**Trade-offs:** More conditional code; needs CI coverage.

## Data Flow

### Request Flow

```
User SQL
   ↓
DuckDB Binder/Planner
   ↓
STPS Bind (validate + schema)
   ↓
STPS Init (open resources)
   ↓
STPS Scan (produce chunks)
   ↓
Query result
```

### Key Data Flows

1. **File IO functions:** path -> open -> parse -> chunked output.
2. **AI functions:** input -> request -> parse -> structured output.
3. **Validation functions:** input -> validate -> scalar output.

## Scaling Considerations

| Scale | Architecture Adjustments |
|-------|--------------------------|
| Small datasets | Simple in-memory processing is fine. |
| Large datasets | Stream results in Scan; avoid full materialization. |
| Remote data | Add caching and retry with rate limits. |

### Scaling Priorities

1. **First bottleneck:** memory spikes from full file reads.
2. **Second bottleneck:** network latency and rate limits.

## Anti-Patterns

### Anti-Pattern 1: Materializing full results

**What people do:** Load entire file or dataset into memory before output.
**Why it's wrong:** Crashes on large files and defeats DuckDB streaming.
**Do this instead:** Stream DataChunks in Scan.

### Anti-Pattern 2: Mixing optional dependencies across core code

**What people do:** Scatter curl usage across unrelated files.
**Why it's wrong:** Increases build complexity and platform bugs.
**Do this instead:** Centralize in integrations layer with guards.

## Integration Points

### External Services

| Service | Integration Pattern | Notes |
|---------|---------------------|-------|
| Anthropic API | HTTP request + JSON parse | Requires key and rate limiting. |
| Brave Search | HTTP request + JSON parse | Optional enrichment. |
| Nextcloud/WebDAV | HTTP basic auth | Should reuse curl helper. |

### Internal Boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| Functions -> shared utils | Direct call | Keep helpers stateless. |
| Functions -> integrations | Direct call | Guard with compile flags. |

## Sources

- https://duckdb.org/docs/stable/dev/building/building_extensions.html — extension build
- https://duckdb.org/docs/stable/extensions/overview.html — extension model
- https://github.com/duckdb/extension-template — canonical structure

---
*Architecture research for: DuckDB extension (data quality and enrichment)*
*Researched: 2026-01-22*
