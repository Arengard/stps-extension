@echo off
cd /d C:\stps-extension
if exist build rmdir /s /q build
call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat" x64
if %errorlevel% neq 0 exit /b 1
cmake -S . -B build -G "NMake Makefiles" -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=cl -DCMAKE_CXX_COMPILER=cl
if %errorlevel% neq 0 exit /b 1
cmake --build build
exit /b %errorlevel%
