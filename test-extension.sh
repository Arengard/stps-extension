#!/bin/bash

# Test script for the STPS extension

echo "Testing STPS Extension..."

# Build the extension first
echo "Building extension..."
make debug

# Find the extension file
EXTENSION_FILE=$(find build -name "*.duckdb_extension" -type f | head -1)

if [ -z "$EXTENSION_FILE" ]; then
    echo "❌ Extension build failed - no .duckdb_extension file found"
    echo ""
    echo "Debug information:"
    echo "=== Looking for extension files ==="
    find build -name "*extension*" -type f 2>/dev/null || echo "No extension files found"
    echo ""
    echo "=== Build directory contents ==="
    ls -la build/ 2>/dev/null || echo "build/ directory not found"
    exit 1
fi

echo "✅ Extension built successfully"
echo "Extension file found: $EXTENSION_FILE"

# Test loading the extension
echo "Testing extension loading..."
./build/debug/duckdb -c "
INSTALL '$EXTENSION_FILE';
LOAD stps;
SELECT 'Extension loaded successfully!' as status;
"

if [ $? -eq 0 ]; then
    echo "✅ Extension loads and works correctly!"
else
    echo "❌ Extension failed to load properly"
    exit 1
fi

echo "🎉 All tests passed!"
