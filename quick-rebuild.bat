@echo off
REM Quick rebuild script - only rebuilds the extension, not all of DuckDB

echo.
echo ===================================================================
echo STPS Extension - Quick Rebuild
echo ===================================================================
echo.

echo This will rebuild only the STPS extension (much faster than full build)
echo.

REM Execute build in VS environment
cmd /c "call `"C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat`" x64 && cd build && nmake stps_loadable_extension"

if %errorlevel% neq 0 (
    echo.
    echo === REBUILD FAILED ===
    exit /b 1
)

echo.
echo === REBUILD SUCCESS ===
echo.
echo Extension updated at: build\stps.duckdb_extension
echo.
echo Test with:
echo   python test-extension.py
echo.

pause

