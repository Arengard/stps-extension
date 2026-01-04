# Windows Build Setup - Lösungsanleitung

## Problem

Sie versuchen, die STPS Extension auf Windows zu bauen, erhalten aber den Fehler:
```
CMake Error: CMAKE_C_COMPILER not set, after EnableLanguage
CMake Error: CMAKE_CXX_COMPILER not set, after EnableLanguage
```

## Ursache

Visual Studio 2022 ist auf Ihrem System installiert, aber die **"Desktop development with C++"** Workload fehlt. CMake kann daher keinen C/C++-Compiler finden.

## Lösung 1: Visual Studio C++ Workload installieren (Empfohlen)

### Schritte:

1. **Visual Studio Installer öffnen:**
   - Suchen Sie im Startmenü nach "Visual Studio Installer"
   - Oder öffnen Sie: `C:\Program Files (x86)\Microsoft Visual Studio\Installer\vs_installer.exe`

2. **Visual Studio 2022 Community modifizieren:**
   - Klicken Sie auf "Modify" neben Visual Studio 2022 Community

3. **C++ Workload installieren:**
   - Wählen Sie **"Desktop development with C++"** (Desktop-Entwicklung mit C++)
   - Stellen Sie sicher, dass folgende Komponenten ausgewählt sind:
     - MSVC v143 - VS 2022 C++ x64/x86 build tools
     - Windows 10 SDK (oder Windows 11 SDK)
     - C++ CMake tools for Windows

4. **Installation durchführen:**
   - Klicken Sie auf "Modify" und warten Sie, bis die Installation abgeschlossen ist
   - Dies kann 5-15 Minuten dauern

5. **Nach der Installation:**
   ```powershell
   # Terminal schließen und neu öffnen
   cd C:\Users\Ramon\Documents\stps-extension
   Remove-Item -Recurse -Force build
   
   # CMake in Developer PowerShell ausführen
   # Oder direkt:
   cmd /c "call `"C:\Program Files\Microsoft Visual Studio\2022\Community\Common7\Tools\VsDevCmd.bat`" && cmake -S . -B build -G `"Visual Studio 17 2022`" -A x64"
   
   # Dann bauen:
   cmd /c "call `"C:\Program Files\Microsoft Visual Studio\2022\Community\Common7\Tools\VsDevCmd.bat`" && cmake --build build --config Release"
   ```

## Lösung 2: Vorkompilierte Binaries verwenden (Schnellste Lösung)

Falls Sie die Extension nur nutzen möchten ohne sie zu kompilieren:

```powershell
# Warten Sie, bis GitHub Actions die Binaries gebaut hat
# Oder laden Sie sie von der Releases-Seite herunter

# Dann:
duckdb -unsigned
```

In DuckDB:
```sql
LOAD './binaries/stps_windows_amd64.duckdb_extension';
SELECT stps_is_valid_iban('DE89370400440532013000');
```

## Lösung 3: WSL (Windows Subsystem for Linux) verwenden

Falls Sie lieber in einer Linux-Umgebung entwickeln:

```powershell
# WSL installieren (falls noch nicht vorhanden)
wsl --install

# Nach dem Neustart:
wsl
```

In WSL:
```bash
cd /mnt/c/Users/Ramon/Documents/stps-extension
./fast-build.sh
```

## Lösung 4: MinGW verwenden

Alternative zum Visual Studio:

1. **MSYS2 installieren:**
   - Download: https://www.msys2.org/
   - Installieren Sie MSYS2

2. **MinGW installieren:**
   ```bash
   # In MSYS2 Terminal:
   pacman -S mingw-w64-x86_64-gcc mingw-w64-x86_64-cmake mingw-w64-x86_64-make
   ```

3. **Build durchführen:**
   ```powershell
   # In PowerShell (nach Hinzufügen von MSYS2 zu PATH):
   Remove-Item -Recurse -Force build
   cmake -S . -B build -G "MinGW Makefiles"
   cmake --build build
   ```

## Empfohlener Workflow

### Für Entwickler:
1. Installieren Sie die C++ Workload in Visual Studio 2022
2. Verwenden Sie die **Developer PowerShell for VS 2022** für alle Build-Befehle
3. Oder verwenden Sie WSL für ein Linux-ähnliches Entwicklungserlebnis

### Für Nutzer:
1. Warten Sie auf die vorkompilierten Binaries im `binaries/` Ordner
2. Oder laden Sie sie von GitHub Releases herunter
3. Keine Kompilierung erforderlich!

## Überprüfung der Installation

Nach Installation der C++ Workload:

```powershell
# Developer PowerShell for VS 2022 öffnen
where cl
# Sollte ausgeben: C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Tools\MSVC\...\bin\Hostx64\x64\cl.exe
```

## Weitere Hilfe

- [Visual Studio Workloads](https://learn.microsoft.com/en-us/visualstudio/install/workload-component-id-vs-community)
- [CMake Generator Documentation](https://cmake.org/cmake/help/latest/manual/cmake-generators.7.html)
- [DuckDB Extension Template](https://github.com/duckdb/extension-template)

## Quick Commands Reference

```powershell
# Build mit Visual Studio (nach Installation der C++ Workload)
cmd /c "call `"C:\Program Files\Microsoft Visual Studio\2022\Community\Common7\Tools\VsDevCmd.bat`" && cmake -S . -B build -G `"Visual Studio 17 2022`" -A x64 && cmake --build build --config Release"

# Oder verwenden Sie das bereitgestellte Skript:
.\build-windows.bat

# Extension testen:
.\scripts\install-extension.bat
```

