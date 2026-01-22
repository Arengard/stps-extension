# Requirements: STPS DuckDB Extension

**Defined:** 2026-01-22
**Core Value:** DuckDB extension that adds SQL functions for data cleansing, validation, file/archive access, German business data handling, and optional AI enrichment.

## v1 Requirements

Requirements for initial release. Each maps to roadmap phases.

### Profiling

- [ ] **PROF-01**: User can run a profiling summary that reports null %, distinct %, min/max, and top values per column.

### Validation

- [ ] **VAL-01**: User can validate common formats (email, phone, date, number) with clear pass/fail outputs.

### Error Handling

- [ ] **ERR-01**: User receives descriptive error messages that include expected formats and invalid input context.

### Hashing and Masking

- [ ] **HASH-01**: User can generate deterministic hashes with optional salt for pseudonymization.

### Regex Utilities

- [ ] **REGX-01**: User can extract and replace values using regex helpers in SQL.

### Locale Parsing

- [ ] **LOCL-01**: User can parse locale-specific dates, decimals, and currency formats with explicit locale options.

## v2 Requirements

Deferred to future release. Tracked but not in current roadmap.

### Rule Engine

- **RULE-01**: User can define reusable data quality rules that run across tables.

### Fuzzy Matching

- **FUZZ-01**: User can perform fuzzy matching/deduplication using similarity functions.

### PII Detection and Masking

- **PII-01**: User can detect and mask common PII fields (email, phone, IBAN, names).

### Drift Reporting

- **DRFT-01**: User can compare tables and produce drift reports for schema and statistics.

### AI Normalization

- **AI-01**: User can optionally use AI-assisted normalization for ambiguous values.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Full ETL orchestration | Scope expansion beyond extension focus |
| Replacing DuckDB core extensions | Duplicates built-in functionality |
| UI/dashboard inside extension | Not appropriate for C++ extension layer |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| PROF-01 | TBD | Pending |
| VAL-01 | TBD | Pending |
| ERR-01 | TBD | Pending |
| HASH-01 | TBD | Pending |
| REGX-01 | TBD | Pending |
| LOCL-01 | TBD | Pending |

**Coverage:**
- v1 requirements: 6 total
- Mapped to phases: 0
- Unmapped: 6 ⚠️

---
*Requirements defined: 2026-01-22*
*Last updated: 2026-01-22 after initial definition*
