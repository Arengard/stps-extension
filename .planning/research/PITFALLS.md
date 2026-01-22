# Pitfalls Research

**Domain:** DuckDB extension for data quality and enrichment
**Researched:** 2026-01-22
**Confidence:** MEDIUM

## Critical Pitfalls

### Pitfall 1: DuckDB version drift

**What goes wrong:**
Extensions compile but fail or crash due to API/ABI changes.

**Why it happens:**
DuckDB evolves quickly; extensions pin to older headers.

**How to avoid:**
Pin DuckDB SDK version and align CI builds and docs.

**Warning signs:**
Sudden build failures or function registration errors after bump.

**Phase to address:**
Phase 1 (foundation and build alignment).

---

### Pitfall 2: Memory spikes from full materialization

**What goes wrong:**
Large files or table functions load into memory and crash.

**Why it happens:**
Convenient parsing paths skip streaming or chunked output.

**How to avoid:**
Use Bind/Init/Scan and stream DataChunks; add file-size guards.

**Warning signs:**
OOM errors on large archives or CSVs.

**Phase to address:**
Phase 1 (core table functions and utilities).

---

### Pitfall 3: Unstable network features

**What goes wrong:**
AI and web lookups fail due to TLS, proxies, or rate limits.

**Why it happens:**
Network environments vary; missing retries and backoff.

**How to avoid:**
Centralize HTTP client config, add retries, expose timeouts.

**Warning signs:**
Intermittent errors across OSes.

**Phase to address:**
Phase 2 (integrations hardening).

---

### Pitfall 4: Poor error messages

**What goes wrong:**
Users cannot diagnose failed calls or invalid inputs.

**Why it happens:**
Exceptions omit expected format and input context.

**How to avoid:**
Standardize error helpers with structured context.

**Warning signs:**
Repeated issue reports without reproducible info.

**Phase to address:**
Phase 1 (baseline usability).

---

### Pitfall 5: Validation false positives

**What goes wrong:**
Validation rejects correct data in real-world datasets.

**Why it happens:**
Rules too strict and no locale/config options.

**How to avoid:**
Support locale flags and "strict vs permissive" modes.

**Warning signs:**
High failure rates in user datasets.

**Phase to address:**
Phase 2 (advanced validation).

---

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Duplicate parsing logic | Faster shipping | Inconsistent behavior | Only for quick experiments. |
| Ad hoc caching in functions | Short-term speed | Memory leaks and stale data | Only for temporary hotfixes. |
| Hard-coded paths | Quick tests | OS-specific bugs | Never. |

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| Anthropic API | Logging raw prompts/keys | Redact and avoid logging secrets. |
| Brave Search | No rate limiting | Add backoff and cache. |
| WebDAV/Nextcloud | Incorrect auth headers | Use standard Basic Auth in curl helper. |

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Full file buffering | Memory spikes | Stream scan output | >500MB files |
| Regex on large columns | Slow queries | Pre-filter + compiled regex | >10M rows |
| Unbounded caching | RAM growth | TTL + size limits | Long-running sessions |

## Security Mistakes

| Mistake | Risk | Prevention |
|---------|------|------------|
| Storing API keys in logs | Key leakage | Redact and avoid logging secrets. |
| Disabling TLS verification | MITM risk | Keep TLS enabled; allow CA override. |
| Path traversal in file ops | File access outside scope | Normalize and validate paths. |

## UX Pitfalls

| Pitfall | User Impact | Better Approach |
|---------|-------------|-----------------|
| Silent NULLs on errors | Hard to debug | Throw with clear error message. |
| Inconsistent naming | Confusing API | Keep `stps_` and `snake_case`. |
| Missing docs/examples | Low adoption | Provide README examples and tests. |

## "Looks Done But Isn't" Checklist

- [ ] **AI functions:** Missing timeout/backoff settings — verify configurable.
- [ ] **Validation helpers:** Missing locale flags — verify defaults and options.
- [ ] **Archive functions:** Missing streaming path — verify large files work.
- [ ] **New table functions:** Missing tests — add SQL logic tests.

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Version drift | MEDIUM | Pin SDK, rebuild, re-run tests. |
| Memory spikes | HIGH | Add streaming, add size guards. |
| Network errors | MEDIUM | Add retries/timeouts and cache. |

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| Version drift | Phase 1 | CI passes on all OSes. |
| Memory spikes | Phase 1 | Large-file test passes. |
| Network errors | Phase 2 | Integration tests with mock endpoints. |
| Poor error messages | Phase 1 | Docs include error examples. |
| Validation false positives | Phase 2 | Sample datasets validate as expected. |

## Sources

- https://duckdb.org/docs/stable/extensions/overview.html — extension lifecycle
- https://duckdb.org/docs/stable/dev/building/building_extensions.html — build constraints
- .planning/codebase/CONCERNS.md — repo-specific risks

---
*Pitfalls research for: DuckDB extension (data quality and enrichment)*
*Researched: 2026-01-22*
