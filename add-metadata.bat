@echo off
REM Manual metadata append script
REM This adds the required DuckDB extension metadata to the extension file

echo.
echo ===================================================================
echo Adding DuckDB Extension Metadata
echo ===================================================================
echo.

set EXTENSION_FILE=build\stps.duckdb_extension
set PLATFORM_FILE=build\duckdb\duckdb_platform_out
set NULL_FILE=duckdb\scripts\null.txt
set VERSION=v1.4.3
set EXT_VERSION=v1.0.0
set ABI_TYPE=CPP

echo Extension file: %EXTENSION_FILE%
echo Platform file: %PLATFORM_FILE%
echo DuckDB version: %VERSION%
echo Extension version: %EXT_VERSION%
echo ABI Type: %ABI_TYPE%
echo.

if not exist "%EXTENSION_FILE%" (
    echo ERROR: Extension file not found: %EXTENSION_FILE%
    echo Please build the extension first with: .\build-windows.bat
    exit /b 1
)

if not exist "%PLATFORM_FILE%" (
    echo ERROR: Platform file not found: %PLATFORM_FILE%
    echo This should have been created during the build.
    exit /b 1
)

echo Adding metadata footer...
cmake -DABI_TYPE=%ABI_TYPE% ^
      -DEXTENSION=%EXTENSION_FILE% ^
      -DPLATFORM_FILE=%PLATFORM_FILE% ^
      -DVERSION_FIELD=%VERSION% ^
      -DEXTENSION_VERSION=%EXT_VERSION% ^
      -DNULL_FILE=%NULL_FILE% ^
      -P duckdb\scripts\append_metadata.cmake

if %errorlevel% neq 0 (
    echo.
    echo === FAILED ===
    echo Metadata append failed!
    exit /b 1
)

echo.
echo === SUCCESS ===
echo.
echo Metadata successfully added to extension!
echo Extension is now ready to use: %EXTENSION_FILE%
echo.
echo Test with:
echo   python test-extension.py
echo.

pause

