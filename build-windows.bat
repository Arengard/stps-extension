@echo off
REM Windows build script for STPS Extension

echo Building STPS Extension on Windows...

REM Check if git submodules are initialized
if not exist "duckdb\.git" (
    echo Initializing submodules...
    git submodule update --init --recursive
)

if not exist "extension-ci-tools\.git" (
    echo Initializing extension-ci-tools...
    git submodule update --init --recursive
)

REM Build the extension
echo Starting build process...
echo.

REM Execute entire build in one cmd context to preserve environment
cmd /c "call build-windows-internal.bat"

if %errorlevel% neq 0 (
    goto :error
)

goto :find_extension

:find_extension
REM Check if build was successful
echo.
echo === Checking for extension files ===

REM Check common locations
if exist "build\release\extension\stps\stps.duckdb_extension" (
    set EXTENSION_FILE=build\release\extension\stps\stps.duckdb_extension
    echo Found extension: %EXTENSION_FILE%
    goto :test_extension
)

if exist "build\Release\extension\stps\stps.duckdb_extension" (
    set EXTENSION_FILE=build\Release\extension\stps\stps.duckdb_extension
    echo Found extension: %EXTENSION_FILE%
    goto :test_extension
)

REM Fallback: search recursively
for /r build %%f in (*.duckdb_extension) do (
    echo Found extension: %%f
    set EXTENSION_FILE=%%f
    goto :test_extension
)

echo No extension files found!
echo Searched in build\release and build\Release directories
goto :error

:test_extension
echo.
echo === Testing extension ===
REM Find DuckDB executable

REM Check common locations first
if exist "build\release\duckdb.exe" (
    set DUCKDB_EXE=build\release\duckdb.exe
    echo Found DuckDB: %DUCKDB_EXE%
    goto :run_test
)

if exist "build\Release\duckdb.exe" (
    set DUCKDB_EXE=build\Release\duckdb.exe
    echo Found DuckDB: %DUCKDB_EXE%
    goto :run_test
)

if exist "build\duckdb.exe" (
    set DUCKDB_EXE=build\duckdb.exe
    echo Found DuckDB: %DUCKDB_EXE%
    goto :run_test
)

REM Fallback: search recursively
for /r build %%f in (duckdb.exe) do (
    echo Found DuckDB: %%f
    set DUCKDB_EXE=%%f
    goto :run_test
)

REM DuckDB not found - but that's OK, extension was built
echo DuckDB executable not found in build directory.
echo This is normal - DuckDB CLI is not built by default.
echo.
echo Extension successfully built at: %EXTENSION_FILE%
goto :success_no_test

:run_test
echo Testing extension loading...
"%DUCKDB_EXE%" -c "INSTALL '%EXTENSION_FILE%'; LOAD stps; SELECT 'Extension loaded successfully!' as status;"

if %errorlevel% equ 0 (
    echo.
    echo === SUCCESS! ===
    echo Extension built and tested successfully!
    goto :end
) else (
    echo.
    echo === FAILED! ===
    echo Extension failed to load properly
    goto :error
)

:success_no_test
echo.
echo ===================================================================
echo === BUILD SUCCESS! ===
echo ===================================================================
echo.
echo Extension successfully built at:
echo   %EXTENSION_FILE%
echo.
echo Next steps:
echo.
echo 1. Test the extension (recommended):
echo    python test-extension.py
echo.
echo 2. Install DuckDB and load extension:
echo    Download from: https://duckdb.org/docs/installation/
echo    Then run:
echo      duckdb -unsigned
echo      LOAD '%EXTENSION_FILE%';
echo      SELECT stps_is_valid_iban('DE89370400440532013000');
echo.
echo 3. Use in Python:
echo    pip install duckdb
echo    python
echo      import duckdb
echo      con = duckdb.connect(':memory:')
echo      con.execute("LOAD '%EXTENSION_FILE%'")
echo      print(con.execute("SELECT stps_generate_uuid()").fetchone())
echo.
echo For more information, see: EXTENSION_VERWENDUNG.md
echo ===================================================================
goto :end

:error
echo.
echo === BUILD FAILED ===
echo Check the output above for errors
exit /b 1

:end
echo.
echo === BUILD COMPLETE ===
pause
