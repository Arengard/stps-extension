# Feature Research

**Domain:** DuckDB extension for data quality, validation, and enrichment
**Researched:** 2026-01-22
**Confidence:** MEDIUM

## Feature Landscape

### Table Stakes (Users Expect These)

Features users assume exist. Missing these = product feels incomplete.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Column profiling summary | Fast understanding of data health | MEDIUM | Null %, distinct %, min/max, top values. |
| Validation helpers | Data quality gates | MEDIUM | Email/phone/date/number parsing and checks. |
| Robust error messages | Ease of use | LOW | Include bad input and expected format. |
| Deterministic hashing | Pseudonymization | LOW | Hash with salt, consistent across runs. |
| Regex extract/replace | Common cleaning tasks | LOW | Often used in SQL pipelines. |
| Locale-aware parsing | International datasets | MEDIUM | Dates, decimals, currency parsing. |

### Differentiators (Competitive Advantage)

Features that set the product apart. Not required, but valuable.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Data quality rule engine | Reusable validation rules | HIGH | Rules compiled to SQL for speed. |
| Fuzzy matching / dedupe | Better record linkage | HIGH | Jaro-Winkler/Levenshtein and blocking. |
| PII detection + masking | Compliance help | MEDIUM | Email/phone/IBAN/name patterns. |
| Table diff + drift report | Track changes over time | MEDIUM | Compare schema and stats. |
| AI-assisted normalization | Higher accuracy | HIGH | Optional, keyed APIs. |

### Anti-Features (Commonly Requested, Often Problematic)

Features that seem good but create problems.

| Feature | Why Requested | Why Problematic | Alternative |
|---------|---------------|-----------------|-------------|
| Full ETL orchestration | "All-in-one" desire | Scope explosion and maintenance | Keep composable SQL functions. |
| Replacing DuckDB core extensions | Avoid extra installs | Duplicates built-ins | Leverage httpfs/json/spatial/fts. |
| UI/dashboard inside extension | Immediate visuals | Out of scope for C++ extension | Provide SQL outputs for BI tools. |

## Feature Dependencies

```
[Data quality rule engine]
    └──requires──> [Profiling summary]
                       └──requires──> [Robust error messages]

[Fuzzy matching]
    └──requires──> [Similarity functions]

[PII masking]
    └──requires──> [Regex + validation helpers]
```

### Dependency Notes

- **Rule engine requires profiling:** Rules need reliable stats and metadata.
- **Fuzzy matching requires similarity functions:** Jaro-Winkler, Levenshtein, trigram.
- **PII masking builds on validation helpers:** Accurate detection reduces false positives.

## MVP Definition

### Launch With (v1)

Minimum viable product — what's needed to validate the concept.

- [ ] Profiling summary table function — proves value quickly
- [ ] Validation helpers (email/phone/date/number) — core use case
- [ ] Better error messages — reduces support and misuse

### Add After Validation (v1.x)

Features to add once core is working.

- [ ] Deterministic hashing + masking — compliance workflows
- [ ] Fuzzy matching utilities — dedupe and linkage

### Future Consideration (v2+)

Features to defer until product-market fit is established.

- [ ] Rule engine with saved rule sets — more complex lifecycle
- [ ] AI-assisted normalization — requires keys and guardrails

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| Profiling summary | HIGH | MEDIUM | P1 |
| Validation helpers | HIGH | MEDIUM | P1 |
| Error message upgrades | MEDIUM | LOW | P1 |
| Deterministic hashing | MEDIUM | LOW | P2 |
| Fuzzy matching | HIGH | HIGH | P2 |
| Rule engine | HIGH | HIGH | P3 |
| AI-assisted normalization | MEDIUM | HIGH | P3 |

**Priority key:**
- P1: Must have for launch
- P2: Should have, add when possible
- P3: Nice to have, future consideration

## Competitor Feature Analysis

| Feature | Competitor A | Competitor B | Our Approach |
|---------|--------------|--------------|--------------|
| Data quality checks | Great Expectations | Soda Core | SQL-native functions with low overhead. |
| Profiling summary | dbt docs / adapters | Deequ | Lightweight table function for fast profiling. |
| Fuzzy matching | OpenRefine | Dedupe | Built-in similarity functions and blocking. |

## Sources

- https://duckdb.org/docs/stable/extensions/overview.html — extension capabilities
- https://duckdb.org/community_extensions/list_of_extensions.html — ecosystem feature survey
- https://docs.greatexpectations.io/ — data quality expectations (reference baseline)
- https://docs.soda.io/soda-core/ — rule-based checks (reference baseline)

---
*Feature research for: DuckDB extension (data quality and enrichment)*
*Researched: 2026-01-22*
