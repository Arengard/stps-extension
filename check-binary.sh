#!/bin/bash
# Script to help users quickly identify and use the correct pre-built binary

set -e

# Detect platform
OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
    Linux*)
        if [ "$ARCH" = "x86_64" ]; then
            PLATFORM="linux_amd64"
        else
            echo "⚠️  Warning: No pre-built binary for Linux $ARCH"
            echo "You may need to build from source."
            exit 1
        fi
        ;;
    Darwin*)
        if [ "$ARCH" = "arm64" ]; then
            PLATFORM="osx_arm64"
        elif [ "$ARCH" = "x86_64" ]; then
            PLATFORM="osx_amd64"
        else
            echo "⚠️  Warning: No pre-built binary for macOS $ARCH"
            echo "You may need to build from source."
            exit 1
        fi
        ;;
    MINGW*|MSYS*|CYGWIN*)
        PLATFORM="windows_amd64"
        ;;
    *)
        echo "⚠️  Warning: Unknown operating system: $OS"
        echo "Available platforms: linux_amd64, osx_arm64, osx_amd64, windows_amd64"
        exit 1
        ;;
esac

BINARY_PATH="binaries/$PLATFORM/polarsgodmode.duckdb_extension"

echo "🔍 Detected platform: $PLATFORM"
echo ""

if [ -f "$BINARY_PATH" ]; then
    echo "✅ Pre-built binary found at: $BINARY_PATH"
    echo ""
    echo "To use this extension in DuckDB, run:"
    echo ""
    echo "  duckdb"
    echo ""
    echo "Then in DuckDB:"
    echo ""
    echo "  INSTALL './$BINARY_PATH';"
    echo "  LOAD polarsgodmode;"
    echo "  SELECT stps_uuid();"
    echo ""
else
    echo "❌ Pre-built binary not found at: $BINARY_PATH"
    echo ""
    echo "The binary might not have been built yet. Please:"
    echo "  1. Pull the latest changes: git pull"
    echo "  2. Check if binaries exist: ls -la binaries/"
    echo "  3. Or build from source: make release"
    echo ""
fi
