# 🎉 Build erfolgreich!

Die STPS Extension wurde erfolgreich gebaut!

## 📦 Extension-Datei

```
build\stps.duckdb_extension
```

## ⚡ Schnellstart

### Mit Python (Empfohlen):

```powershell
pip install duckdb
python test-extension.py
```

### Mit DuckDB CLI:

```powershell
# DuckDB installieren (einmalig):
scoop install duckdb

# Extension nutzen:
duckdb -unsigned
```

```sql
LOAD 'build/stps.duckdb_extension';
SELECT stps_generate_uuid();
```

## 📚 Dokumentation

- **EXTENSION_VERWENDUNG.md** - Wie man die Extension verwendet
- **STPS_FUNCTIONS.md** - Alle verfügbaren Funktionen
- **test-extension.py** - Automatischer Test

## 🔄 Rebuild

```powershell
.\build-windows.bat
```

Viel Erfolg! 🚀

