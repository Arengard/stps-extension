@echo off
REM Internal build script - called from build-windows.bat with environment preserved

echo Step 1: Setting up Visual Studio environment...

REM Try different Visual Studio editions
if exist "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat" (
    call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat" x64
    goto :check_compiler
)

if exist "C:\Program Files\Microsoft Visual Studio\2022\Professional\VC\Auxiliary\Build\vcvarsall.bat" (
    call "C:\Program Files\Microsoft Visual Studio\2022\Professional\VC\Auxiliary\Build\vcvarsall.bat" x64
    goto :check_compiler
)

if exist "C:\Program Files\Microsoft Visual Studio\2022\Enterprise\VC\Auxiliary\Build\vcvarsall.bat" (
    call "C:\Program Files\Microsoft Visual Studio\2022\Enterprise\VC\Auxiliary\Build\vcvarsall.bat" x64
    goto :check_compiler
)

echo ERROR: Visual Studio 2022 not found!
echo Please install Visual Studio 2022 from https://visualstudio.microsoft.com/
exit /b 1

:check_compiler
where cl >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo ===================================================================
    echo ERROR: C++ compiler not found!
    echo ===================================================================
    echo.
    echo Visual Studio 2022 is installed, but the C++ compiler is missing.
    echo.
    echo SOLUTION:
    echo 1. Open Visual Studio Installer
    echo 2. Click "Modify" on Visual Studio 2022
    echo 3. Select "Desktop development with C++" workload
    echo 4. Install these components:
    echo    - MSVC v143 - VS 2022 C++ x64/x86 build tools
    echo    - Windows 10/11 SDK
    echo    - C++ CMake tools for Windows
    echo.
    echo See WINDOWS_BUILD_SETUP.md for detailed instructions.
    echo ===================================================================
    exit /b 1
)

echo Visual Studio C++ compiler found:
where cl | findstr /C:"cl.exe"

REM Check for Windows SDK (rc.exe)
where rc >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo ===================================================================
    echo ERROR: Windows SDK not found!
    echo ===================================================================
    echo.
    echo The Resource Compiler ^(rc.exe^) is missing from Windows SDK.
    echo This is REQUIRED for building Windows applications.
    echo.
    echo SOLUTION:
    echo 1. Open Visual Studio Installer
    echo 2. Click "Modify" on Visual Studio 2022 Community
    echo 3. Go to "Individual components" tab
    echo 4. Search for and install:
    echo    - Windows 11 SDK or Windows 10 SDK
    echo    - MSVC v143 - VS 2022 C++ x64/x86 build tools
    echo.
    echo OR install the full workload:
    echo    - Go to "Workloads" tab
    echo    - Select "Desktop development with C++"
    echo    - Ensure "Windows SDK" is checked on the right side
    echo.
    echo See WINDOWS_BUILD_SETUP.md for detailed instructions.
    echo ===================================================================
    exit /b 1
)

echo Windows SDK Resource Compiler found:
where rc | findstr /C:"rc.exe"

echo.
echo Step 2: Configuring CMake...
if exist build (
    echo Cleaning old build directory...
    rmdir /s /q build
)

REM Get the MSVC compiler path
for /f "delims=" %%i in ('where cl') do set CL_PATH=%%i
echo Using compiler: %CL_PATH%

REM Set compiler environment variables for CMake
set CC=cl
set CXX=cl

cmake -S . -B build -G "NMake Makefiles" -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=cl -DCMAKE_CXX_COMPILER=cl
if %errorlevel% neq 0 (
    echo CMake configuration failed!
    exit /b 1
)

echo.
echo Step 3: Building extension (Release mode)...
cmake --build build
if %errorlevel% neq 0 (
    echo Build failed!
    exit /b 1
)

echo.
echo Build completed successfully!
exit /b 0

