# 🎉 STPS Extension erfolgreich gebaut!

## ✅ Build-Ergebnis

Die Extension wurde erfolgreich kompiliert!

**Extension-Datei:**
```
C:\Users\Ramon\Documents\stps-extension\build\stps.duckdb_extension
```

**Dateigröße:** Prüfen Sie mit:
```powershell
Get-Item build\stps.duckdb_extension | Select-Object Length, LastWriteTime
```

---

## 📦 Extension verwenden

### Option 1: DuckDB installieren und Extension laden

#### 1. DuckDB installieren:

**Windows (Scoop):**
```powershell
scoop install duckdb
```

**Windows (Chocolatey):**
```powershell
choco install duckdb
```

**Manueller Download:**
1. Gehen Sie zu: https://duckdb.org/docs/installation/
2. Laden Sie `duckdb_cli-windows-amd64.zip` herunter
3. Entpacken Sie `duckdb.exe`
4. Fügen Sie es zu Ihrem PATH hinzu oder kopieren Sie es ins Projektverzeichnis

#### 2. Extension laden und testen:

```powershell
# Im Projektverzeichnis
duckdb -unsigned
```

In DuckDB:
```sql
-- Extension laden
LOAD 'build/stps.duckdb_extension';

-- Extension testen
SELECT stps_is_valid_iban('DE89370400440532013000') AS is_valid;
-- Erwartet: true

-- Weitere Funktionen testen
SELECT stps_upper('hello world') AS upper_text;
-- Erwartet: HELLO WORLD

SELECT stps_lower('HELLO WORLD') AS lower_text;
-- Erwartet: hello world

SELECT stps_generate_uuid() AS uuid;
-- Erwartet: z.B. 550e8400-e29b-41d4-a716-446655440000

-- Alle STPS-Funktionen anzeigen
SELECT function_name, return_type, parameters
FROM duckdb_functions()
WHERE function_name LIKE 'stps_%'
ORDER BY function_name;
```

---

### Option 2: Extension installieren (Persistent)

```powershell
# Kopiere Extension ins DuckDB-Extension-Verzeichnis
$ExtensionDir = "$env:USERPROFILE\.duckdb\extensions\v1.4.3\windows_amd64"
New-Item -ItemType Directory -Force -Path $ExtensionDir
Copy-Item "build\stps.duckdb_extension" -Destination "$ExtensionDir\stps.duckdb_extension"
```

Dann in DuckDB:
```sql
-- Extension wird automatisch gefunden
INSTALL stps;
LOAD stps;

-- Oder direkt:
LOAD stps;
```

---

### Option 3: Extension in anderen Projekten verwenden

#### Python (duckdb-python):
```python
import duckdb

# Verbindung erstellen
conn = duckdb.connect()

# Extension laden
conn.execute("LOAD 'C:/Users/Ramon/Documents/stps-extension/build/stps.duckdb_extension'")

# Verwenden
result = conn.execute("SELECT stps_is_valid_iban('DE89370400440532013000')").fetchall()
print(result)  # [(True,)]

# UUID generieren
uuid = conn.execute("SELECT stps_generate_uuid()").fetchone()[0]
print(uuid)
```

#### Node.js (duckdb-node):
```javascript
const duckdb = require('duckdb');

const db = new duckdb.Database(':memory:');
const conn = db.connect();

// Extension laden
conn.run("LOAD 'C:/Users/Ramon/Documents/stps-extension/build/stps.duckdb_extension'");

// Verwenden
conn.all("SELECT stps_is_valid_iban('DE89370400440532013000') AS is_valid", (err, rows) => {
    console.log(rows); // [{ is_valid: true }]
});
```

---

## 🧪 Schnelltest ohne Installation

Wenn Sie DuckDB nicht installieren möchten, können Sie die Extension in Python testen:

```powershell
# Python DuckDB installieren
pip install duckdb

# Python-Testskript
python
```

```python
import duckdb

# In-Memory Datenbank
con = duckdb.connect(':memory:')

# Extension laden
con.execute("LOAD 'build/stps.duckdb_extension'")

# Testen
print(con.execute("SELECT stps_is_valid_iban('DE89370400440532013000')").fetchone())
print(con.execute("SELECT stps_generate_uuid()").fetchone())
print(con.execute("SELECT stps_upper('test')").fetchone())
```

---

## 📋 Verfügbare Funktionen

Die STPS Extension bietet folgende Funktionen:

### String-Funktionen:
- `stps_upper(text)` - Konvertiert zu Großbuchstaben
- `stps_lower(text)` - Konvertiert zu Kleinbuchstaben
- `stps_normalize_text(text)` - Normalisiert Text (Umlaute, etc.)

### Validierung:
- `stps_is_valid_iban(iban)` - Validiert IBAN
- `stps_validate_iban(iban)` - Detaillierte IBAN-Validierung

### UUID:
- `stps_generate_uuid()` - Generiert UUID v4
- `stps_generate_uuid_v1()` - Generiert UUID v1

### Filesystem:
- `stps_path_join(path1, path2)` - Pfade zusammenfügen
- `stps_read_file(path)` - Datei lesen
- `stps_write_file(path, content)` - Datei schreiben

### GoBD:
- `read_gobd(path)` - GoBD-Daten lesen
- `scan_gobd_folder(path)` - GoBD-Ordner scannen

Siehe `STPS_FUNCTIONS.md` für vollständige Dokumentation.

---

## 🔄 Rebuild

Bei Code-Änderungen:

```powershell
# Schneller Rebuild (nur Extension)
cd build
nmake stps_loadable_extension

# Vollständiger Rebuild
cd ..
.\build-windows.bat
```

---

## 📦 Extension verteilen

### Für andere Benutzer:

1. **Kopiere die Extension-Datei:**
   ```
   build\stps.duckdb_extension
   ```

2. **Bereitstellung:**
   - GitHub Releases
   - Interner File-Server
   - Package Registry

3. **Installation bei Benutzern:**
   ```sql
   LOAD 'pfad/zur/stps.duckdb_extension';
   ```

---

## 🐛 Fehlerbehebung

### Extension lädt nicht:

```sql
-- Prüfe ob Extension-Datei existiert
-- In PowerShell:
Test-Path build\stps.duckdb_extension

-- In DuckDB mit Fehlerdetails:
LOAD 'build/stps.duckdb_extension';
```

### Falsche DuckDB-Version:

Die Extension wurde für DuckDB v1.4.3 gebaut. Stellen Sie sicher, dass Sie eine kompatible Version verwenden:

```sql
SELECT version();
```

Sollte `v1.4.x` sein.

---

## 🎯 Nächste Schritte

1. **DuckDB installieren** (siehe oben)
2. **Extension testen** mit den Beispiel-Queries
3. **In Ihre Projekte integrieren**
4. **Dokumentation lesen:** `STPS_FUNCTIONS.md`, `USAGE.md`

---

## ✅ Zusammenfassung

**Sie haben erfolgreich:**
- ✅ Visual Studio richtig konfiguriert
- ✅ Windows SDK installiert  
- ✅ DuckDB kompiliert
- ✅ STPS Extension gebaut

**Die Extension ist einsatzbereit unter:**
```
C:\Users\Ramon\Documents\stps-extension\build\stps.duckdb_extension
```

**Viel Erfolg mit der STPS Extension!** 🚀

