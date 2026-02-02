# stps_inso_account Design

## Purpose

Table function that maps commercial bookings to insolvency chart of accounts (Einnahme-/Ausgaberechnung). Filters transactions by bank account and adds EA-Konto mapping columns.

## Usage

```sql
SELECT * FROM stps_inso_account('rl.buchungen', bank_account='180000');
```

## Mapping Rules

| Counter-account kontoart | Mapping logic | EA-Konto |
|---|---|---|
| D (Debitor) | Fixed | 8200 |
| Erloes | Fixed | 8200 |
| K (Kreditor) | Per-Kreditor: dominant Aufwand account -> its EA mapping | Varies |
| Aufwand | Prefix match + name similarity to Ausgabekonten | Varies |
| Sachkonto (14xxxx Vorsteuer) | Fixed | 1780 |
| Sachkonto (other) | Best-effort match | Match or NULL |
| Geldkonto | Fixed | 1360 |

## Output Schema

All original buchungen columns, plus:
- counter_konto (DECIMAL): non-bank side account number
- counter_kontobezeichnung (VARCHAR): non-bank side account name
- counter_kontoart (VARCHAR): non-bank side account type
- ea_konto (VARCHAR): mapped insolvency account number
- ea_kontobezeichnung (VARCHAR): mapped insolvency account name
- mapping_source (VARCHAR): explanation of how mapping was determined

## Implementation

Pure C++ table function. Lookup maps built at init time, applied per-row at scan time.

### Files
- src/include/inso_account_function.hpp
- src/inso_account_function.cpp
- Register in src/stps_unified_extension.cpp
- Add to CMakeLists.txt
