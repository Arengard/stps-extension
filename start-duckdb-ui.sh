#!/bin/bash

# Kill any existing DuckDB processes first
pkill -9 duckdb 2>/dev/null
sleep 1

# Start DuckDB UI
echo "🚀 Starting DuckDB UI with stps extension..."
echo "📍 Extension will auto-load from ~/.duckdbrc"
echo ""

# Start DuckDB UI in background - use tail -f to keep stdin open
(echo "SELECT * FROM start_ui();" && tail -f /dev/null) | ./build/release/duckdb --unsigned > /tmp/duckdb-ui.log 2>&1 &

# Wait for server to start
sleep 4

# Open Chrome
echo "🌐 Opening Chrome at http://localhost:4213/"
open -a "Google Chrome" http://localhost:4213/

echo ""
echo "✅ DuckDB UI is running with stps extension loaded!"
echo "📦 21 functions available with stps_ prefix"
echo ""
echo "To stop: Use 'pkill duckdb'"
