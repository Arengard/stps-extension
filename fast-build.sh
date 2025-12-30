#!/bin/bash

# Fast Development Build Script
# This script optimizes local builds for development

set -e

echo "🚀 Starting fast development build..."

# Detect number of CPU cores
if [[ "$OSTYPE" == "darwin"* ]]; then
    CORES=$(sysctl -n hw.ncpu)
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    CORES=$(nproc)
else
    CORES=4  # Default fallback
fi

echo "📊 Using $CORES CPU cores for parallel build"

# Set parallel build flags
export MAKEFLAGS="-j$CORES"

# Create build directory if it doesn't exist
mkdir -p build/release

# Check if DuckDB submodule is initialized
if [ ! -f "duckdb/CMakeLists.txt" ]; then
    echo "🔧 Initializing DuckDB submodule..."
    git submodule update --init --recursive --depth=1
fi

# Build with optimizations
echo "🔨 Building extension with $CORES parallel jobs..."
time make -j$CORES

# Check if build was successful
if [ $? -eq 0 ]; then
    echo "✅ Build completed successfully!"
    echo "🎯 Extension built: build/release/extension/polarsgodmode/polarsgodmode.duckdb_extension"

    # Show build artifacts
    if [ -f "build/release/extension/polarsgodmode/polarsgodmode.duckdb_extension" ]; then
        echo "📦 Extension size: $(du -h build/release/extension/polarsgodmode/polarsgodmode.duckdb_extension | cut -f1)"
    fi
else
    echo "❌ Build failed!"
    exit 1
fi

echo "🏁 Fast build complete!"
