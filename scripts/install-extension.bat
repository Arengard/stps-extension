@echo off
setlocal enabledelayedexpansion

REM STPS Extension Installation Script for Windows
REM This script installs the STPS extension to %USERPROFILE%\.duckdb\extensions\ and
REM optionally configures autoloading via %USERPROFILE%\.duckdbrc

echo.
echo ====================================
echo STPS Extension Installation Script
echo ====================================
echo.

REM Step 1: Determine DuckDB version
where duckdb >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] duckdb command not found
    echo Please install DuckDB first: https://duckdb.org/docs/installation/
    exit /b 1
)

for /f "tokens=*" %%i in ('duckdb --version 2^>^&1') do set DUCKDB_OUTPUT=%%i
for /f "tokens=2 delims= " %%a in ("%DUCKDB_OUTPUT%") do set DUCKDB_VERSION=%%a

if "%DUCKDB_VERSION%"=="" (
    echo [ERROR] Could not determine DuckDB version
    exit /b 1
)

echo [OK] Detected DuckDB version: %DUCKDB_VERSION%

REM Get script directory and project root
set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..

REM Step 2: Check if extension is built for this version
set EXTENSION_FILE=%PROJECT_ROOT%\build\%DUCKDB_VERSION%\extension\stps\stps.duckdb_extension

if not exist "%EXTENSION_FILE%" (
    echo [WARNING] Extension not built for version %DUCKDB_VERSION%
    echo.
    echo Building extension for %DUCKDB_VERSION%...

    if not exist "%SCRIPT_DIR%build-for-version.sh" (
        echo [ERROR] build-for-version.sh not found
        echo Please run the build script manually
        exit /b 1
    )

    call "%SCRIPT_DIR%build-for-version.sh" %DUCKDB_VERSION%

    if not exist "%EXTENSION_FILE%" (
        echo [ERROR] Build failed
        exit /b 1
    )
)

echo [OK] Extension binary found: %EXTENSION_FILE%

REM Step 3: Create installation directory
set INSTALL_DIR=%USERPROFILE%\.duckdb\extensions\stps\%DUCKDB_VERSION%

if not exist "%INSTALL_DIR%" (
    mkdir "%INSTALL_DIR%"
)
echo [OK] Created installation directory: %INSTALL_DIR%

REM Step 4: Copy extension
copy /Y "%EXTENSION_FILE%" "%INSTALL_DIR%\" >nul
echo [OK] Installed extension to: %INSTALL_DIR%\stps.duckdb_extension

REM Step 5: Configure autoloading
echo.
echo Configure autoloading?
echo   1) Yes - Configure %%USERPROFILE%%\.duckdbrc for CLI autoloading
echo   2) No  - Skip autoloading configuration (manual setup)
set /p CONFIGURE_AUTOLOAD="Choice [1]: "
if "%CONFIGURE_AUTOLOAD%"=="" set CONFIGURE_AUTOLOAD=1

if "%CONFIGURE_AUTOLOAD%"=="1" (
    set DUCKDBRC=%USERPROFILE%\.duckdbrc

    if exist "!DUCKDBRC!" (
        echo [WARNING] File %%USERPROFILE%%\.duckdbrc already exists
        echo.
        type "!DUCKDBRC!"
        echo.
        echo Options:
        echo   1) Backup and replace
        echo   2) Append to existing file
        echo   3) Skip autoloading configuration
        set /p BACKUP_CHOICE="Choice [2]: "
        if "!BACKUP_CHOICE!"=="" set BACKUP_CHOICE=2

        if "!BACKUP_CHOICE!"=="1" (
            copy "!DUCKDBRC!" "!DUCKDBRC!.backup.%date:~-4%%date:~-7,2%%date:~-10,2%_%time:~0,2%%time:~3,2%%time:~6,2%" >nul
            echo [OK] Backed up existing %%USERPROFILE%%\.duckdbrc

            (
                echo -- Autoload stps extension
                echo .echo Loading stps extension...
                echo LOAD '%USERPROFILE%/.duckdb/extensions/stps/%DUCKDB_VERSION%/stps.duckdb_extension';
                echo .echo stps extension loaded successfully!
            ) > "!DUCKDBRC!"
            echo [OK] Created new %%USERPROFILE%%\.duckdbrc

        ) else if "!BACKUP_CHOICE!"=="2" (
            (
                echo.
                echo -- Autoload stps extension (added %date% %time%)
                echo .echo Loading stps extension...
                echo LOAD '%USERPROFILE%/.duckdb/extensions/stps/%DUCKDB_VERSION%/stps.duckdb_extension';
                echo .echo stps extension loaded successfully!
            ) >> "!DUCKDBRC!"
            echo [OK] Appended to existing %%USERPROFILE%%\.duckdbrc
        ) else (
            echo [WARNING] Skipped autoloading configuration
        )
    ) else (
        (
            echo -- Autoload stps extension
            echo .echo Loading stps extension...
            echo LOAD '%USERPROFILE%/.duckdb/extensions/stps/%DUCKDB_VERSION%/stps.duckdb_extension';
            echo .echo stps extension loaded successfully!
        ) > "!DUCKDBRC!"
        echo [OK] Created %%USERPROFILE%%\.duckdbrc
    )
)

REM Summary
echo.
echo ======================================
echo Installation Complete!
echo ======================================
echo.
echo Extension installed to:
echo   %INSTALL_DIR%\stps.duckdb_extension
echo.
echo Test the installation:
echo   CLI:    duckdb -unsigned
echo   Python: See duckdb_helpers.py for helper function
echo.
echo For more information, see:
echo   docs\plans\2026-01-02-autoload-setup.md
echo.

endlocal
