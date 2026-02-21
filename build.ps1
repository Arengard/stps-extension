# STPS Extension - Windows Build Script
# Requires: VS 2022, CMake, vcpkg at C:\Users\Ramon\vcpkg
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File build.ps1          # full build (configure + build)
#   powershell -ExecutionPolicy Bypass -File build.ps1 -Quick   # quick rebuild (extension only)

param([switch]$Quick)

# Enter VS Developer Shell
Import-Module "C:\Program Files\Microsoft Visual Studio\2022\Community\Common7\Tools\Microsoft.VisualStudio.DevShell.dll"
Enter-VsDevShell -VsInstallPath "C:\Program Files\Microsoft Visual Studio\2022\Community" -SkipAutomaticLocation -DevCmdArguments "-arch=x64"

# Add PowerShell to PATH (needed for vcpkg MSBuild post-build steps)
# Deduplicate to stay under cmd.exe 8191-char PATH limit
if ($env:PATH -notlike "*WindowsPowerShell*") {
    $env:PATH = "$env:SystemRoot\System32\WindowsPowerShell\v1.0;$env:PATH"
}
$env:PATH = ($env:PATH -split ';' | Select-Object -Unique) -join ';'

Set-Location C:\stps-extension

$vcpkgToolchain = "C:\Users\Ramon\vcpkg\scripts\buildsystems\vcpkg.cmake"

# Configure if needed (first build or clean build)
if (-not $Quick -and -not (Test-Path "build\release\CMakeCache.txt")) {
    Write-Host "=== CMake Configure ==="
    if (-not (Test-Path "build\release")) { New-Item -ItemType Directory -Path "build\release" -Force | Out-Null }
    cmake -DEXTENSION_STATIC_BUILD=1 `
          -DDUCKDB_EXTENSION_CONFIGS="C:/stps-extension/extension_config.cmake" `
          -DVCPKG_BUILD=1 `
          -DCMAKE_TOOLCHAIN_FILE="$vcpkgToolchain" `
          -DVCPKG_MANIFEST_DIR="C:/stps-extension" `
          -DCMAKE_BUILD_TYPE=Release `
          -S ./duckdb/ -B build/release
    if ($LASTEXITCODE -ne 0) { Write-Host "CMAKE CONFIGURE FAILED"; exit 1 }
}

Set-Location C:\stps-extension\build\release

if ($Quick) {
    Write-Host "=== Quick Rebuild (extension only) ==="
    cmake --build . --config Release --target stps_loadable_extension
} else {
    Write-Host "=== Full Build ==="
    cmake --build . --config Release
}

if ($LASTEXITCODE -ne 0) { Write-Host "BUILD FAILED"; exit 1 }

Write-Host ""
Write-Host "=== BUILD COMPLETE ==="
Get-ChildItem -Path "C:\stps-extension\build\release\extension\stps\*.duckdb_extension" -ErrorAction SilentlyContinue | ForEach-Object { Write-Host $_.FullName }
