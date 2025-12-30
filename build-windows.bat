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
make debug

REM Check if build was successful
echo.
echo === Checking for extension files ===
for /r build %%f in (*.duckdb_extension) do (
    echo Found extension: %%f
    set EXTENSION_FILE=%%f
    goto :test_extension
)

echo No extension files found!
goto :error

:test_extension
echo.
echo === Testing extension ===
REM Find DuckDB executable
for /r build %%f in (duckdb.exe) do (
    echo Found DuckDB: %%f
    set DUCKDB_EXE=%%f
    goto :run_test
)

echo No DuckDB executable found!
goto :error

:run_test
echo Testing extension loading...
"%DUCKDB_EXE%" -c "INSTALL '%EXTENSION_FILE%'; LOAD polarsgodmode; SELECT 'Extension loaded successfully!' as status;"

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

:error
echo.
echo === BUILD FAILED ===
echo Check the output above for errors
exit /b 1

:end
echo.
echo === BUILD COMPLETE ===
pause
