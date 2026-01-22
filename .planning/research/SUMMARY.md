# Project Research Summary

**Project:** STPS DuckDB Extension
**Domain:** DuckDB C++ extension for data quality and enrichment
**Researched:** 2026-01-22
**Confidence:** MEDIUM

## Executive Summary

STPS sits in a growing DuckDB extension ecosystem where users expect compact,
composable SQL functions rather than monolithic ETL tooling. The most valuable
new functionality concentrates on data quality: profiling summaries, validation
helpers, deterministic masking, and fuzzy matching. These are repeatedly useful
across datasets and align with the existing extension focus on cleansing and
validation.

The recommended approach is to stay within DuckDB's extension patterns (C++17,
Bind/Init/Scan for table functions, optional dependency guards) and to avoid
duplicating core extensions like httpfs, json, or spatial. Key risks are API
drift across DuckDB versions, memory spikes from full materialization, and
unstable network integrations; mitigate with version pinning, streaming scans,
and centralized HTTP helpers.

## Key Findings

### Recommended Stack

The current stack (C++17, DuckDB SDK, CMake, extension-ci-tools) is aligned
with DuckDB's official extension guidance and should remain the base. Optional
dependencies like ICU or re2 are only needed if adding locale-heavy parsing or
large-scale regex functionality.

**Core technologies:**
- C++17: extension implementation — required by DuckDB SDK
- DuckDB SDK v1.4.3: engine linkage — keeps ABI aligned
- CMake 3.15+: build system — required by DuckDB tooling

### Expected Features

**Must have (table stakes):**
- Profiling summary table function — users need quick health checks
- Validation helpers (email/phone/date/number) — basic data QA

**Should have (competitive):**
- Fuzzy matching utilities — dedupe and linkage
- Deterministic masking — compliance workflows

**Defer (v2+):**
- Rule engine with saved rules — higher complexity
- AI-assisted normalization — external keys and guardrails

### Architecture Approach

Use the existing pattern: a central registration entry point, module-based
function groups, and Bind/Init/Scan table functions for streaming. Keep network
integrations centralized under compile guards to reduce platform variability.

**Major components:**
1. Function registry — registers all modules
2. Scalar/table function modules — domain features
3. Shared utilities — parsing, validation, caching

### Critical Pitfalls

1. **DuckDB version drift** — pin SDK and align CI builds
2. **Memory spikes from materialization** — stream DataChunks
3. **Unstable network features** — centralized HTTP config + retries

## Implications for Roadmap

Based on research, suggested phase structure:

### Phase 1: Data Quality Foundation
**Rationale:** Establish reliable, low-risk features first.
**Delivers:** Profiling summary, validation helpers, error-message upgrades.
**Addresses:** Table-stakes features for user trust.
**Avoids:** Version drift and poor error messaging.

### Phase 2: Enrichment and Matching
**Rationale:** Build on validated core utilities.
**Delivers:** Deterministic masking, fuzzy matching, improved locale parsing.
**Uses:** Optional libraries if needed (ICU/re2).
**Implements:** Extended shared utilities + new table functions.

### Phase 3: Advanced Intelligence
**Rationale:** Higher complexity and external dependency risk.
**Delivers:** Rule engine, AI-assisted normalization, drift reporting.
**Uses:** Existing AI integration patterns and caching strategies.

### Phase Ordering Rationale

- Phase 1 creates stable primitives used by later features.
- Phase 2 depends on similarity and validation helpers.
- Phase 3 depends on trust in earlier outputs and stable integrations.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 2:** Fuzzy matching algorithms and locale parsing libraries.
- **Phase 3:** Rule engine design and AI cost/latency controls.

Phases with standard patterns (skip research-phase):
- **Phase 1:** Profiling and validation are well-established patterns.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | Based on official DuckDB docs and repo stack. |
| Features | MEDIUM | Derived from ecosystem and data-quality tools. |
| Architecture | HIGH | Aligns with DuckDB extension patterns. |
| Pitfalls | MEDIUM | Informed by repo concerns and common extension risks. |

**Overall confidence:** MEDIUM

### Gaps to Address

- Validate which profiling functions users miss most (survey or issue review).
- Confirm which fuzzy matching algorithms are most desired.

## Sources

### Primary (HIGH confidence)
- https://duckdb.org/docs/stable/extensions/overview.html — extension model
- https://duckdb.org/docs/stable/dev/building/building_extensions.html — build guidance

### Secondary (MEDIUM confidence)
- https://duckdb.org/community_extensions/list_of_extensions.html — ecosystem survey
- https://github.com/duckdb/extension-template — reference structure

---
*Research completed: 2026-01-22*
*Ready for roadmap: yes*
