#!/bin/bash

echo "Checking build status..."
echo ""

if ps aux | grep -v grep | grep "make" | grep -q "stps"; then
    echo "✓ Build is RUNNING"
    echo ""
    echo "Last 10 lines of build log:"
    tail -10 build.log 2>/dev/null || echo "No log yet"
    echo ""
    echo "To see full log: tail -f build.log"
else
    if [ -f "build/release/extension/stps/stps.duckdb_extension" ]; then
        echo "🎉 BUILD COMPLETE!"
        echo ""
        ls -lh build/release/extension/stps/stps.duckdb_extension
        echo ""
        echo "To test:"
        echo "  duckdb"
        echo "  LOAD '$(pwd)/build/release/extension/stps/stps.duckdb_extension';"
        echo "  SELECT pgm_uuid();"
    else
        echo "❌ Build not running and no extension found"
        echo ""
        if [ -f "build.log" ]; then
            echo "Last 20 lines of build log:"
            tail -20 build.log
        fi
    fi
fi
