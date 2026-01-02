#!/bin/bash
set -e

# STPS Extension Installation Script for macOS/Linux
# This script installs the STPS extension to ~/.duckdb/extensions/ and
# optionally configures autoloading via ~/.duckdbrc

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "📦 STPS Extension Installation Script"
echo "======================================"
echo ""

# Step 1: Determine DuckDB version
if ! command -v duckdb &> /dev/null; then
    echo -e "${RED}❌ Error: duckdb command not found${NC}"
    echo "Please install DuckDB first: https://duckdb.org/docs/installation/"
    exit 1
fi

DUCKDB_VERSION=$(duckdb --version 2>&1 | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' | head -1)

if [ -z "$DUCKDB_VERSION" ]; then
    echo -e "${RED}❌ Error: Could not determine DuckDB version${NC}"
    exit 1
fi

echo -e "${GREEN}✓${NC} Detected DuckDB version: $DUCKDB_VERSION"

# Step 2: Check if extension is built for this version
EXTENSION_FILE="$PROJECT_ROOT/build/$DUCKDB_VERSION/extension/stps/stps.duckdb_extension"

if [ ! -f "$EXTENSION_FILE" ]; then
    echo -e "${YELLOW}⚠${NC}  Extension not built for version $DUCKDB_VERSION"
    echo ""
    echo "Building extension for $DUCKDB_VERSION..."

    if [ ! -f "$SCRIPT_DIR/build-for-version.sh" ]; then
        echo -e "${RED}❌ Error: build-for-version.sh not found${NC}"
        exit 1
    fi

    "$SCRIPT_DIR/build-for-version.sh" "$DUCKDB_VERSION"

    if [ ! -f "$EXTENSION_FILE" ]; then
        echo -e "${RED}❌ Error: Build failed${NC}"
        exit 1
    fi
fi

echo -e "${GREEN}✓${NC} Extension binary found: $EXTENSION_FILE"

# Step 3: Create installation directory
INSTALL_DIR="$HOME/.duckdb/extensions/stps/$DUCKDB_VERSION"
mkdir -p "$INSTALL_DIR"
echo -e "${GREEN}✓${NC} Created installation directory: $INSTALL_DIR"

# Step 4: Copy extension
cp "$EXTENSION_FILE" "$INSTALL_DIR/"
echo -e "${GREEN}✓${NC} Installed extension to: $INSTALL_DIR/stps.duckdb_extension"

# Step 5: Configure autoloading
echo ""
echo "Configure autoloading?"
echo "  1) Yes - Configure ~/.duckdbrc for CLI autoloading"
echo "  2) No  - Skip autoloading configuration (manual setup)"
read -p "Choice [1]: " CONFIGURE_AUTOLOAD
CONFIGURE_AUTOLOAD=${CONFIGURE_AUTOLOAD:-1}

if [ "$CONFIGURE_AUTOLOAD" = "1" ]; then
    DUCKDBRC="$HOME/.duckdbrc"

    if [ -f "$DUCKDBRC" ]; then
        echo -e "${YELLOW}⚠${NC}  File ~/.duckdbrc already exists"
        echo ""
        cat "$DUCKDBRC"
        echo ""
        echo "Options:"
        echo "  1) Backup and replace"
        echo "  2) Append to existing file"
        echo "  3) Skip autoloading configuration"
        read -p "Choice [2]: " BACKUP_CHOICE
        BACKUP_CHOICE=${BACKUP_CHOICE:-2}

        if [ "$BACKUP_CHOICE" = "1" ]; then
            cp "$DUCKDBRC" "$DUCKDBRC.backup.$(date +%Y%m%d_%H%M%S)"
            echo -e "${GREEN}✓${NC} Backed up existing ~/.duckdbrc"

            cat > "$DUCKDBRC" << EOF
-- Autoload stps extension
.echo Loading stps extension...
LOAD '$INSTALL_DIR/stps.duckdb_extension';
.echo stps extension loaded successfully!
EOF
            echo -e "${GREEN}✓${NC} Created new ~/.duckdbrc"

        elif [ "$BACKUP_CHOICE" = "2" ]; then
            echo "" >> "$DUCKDBRC"
            echo "-- Autoload stps extension (added $(date))" >> "$DUCKDBRC"
            echo ".echo Loading stps extension..." >> "$DUCKDBRC"
            echo "LOAD '$INSTALL_DIR/stps.duckdb_extension';" >> "$DUCKDBRC"
            echo ".echo stps extension loaded successfully!" >> "$DUCKDBRC"
            echo -e "${GREEN}✓${NC} Appended to existing ~/.duckdbrc"
        else
            echo -e "${YELLOW}⚠${NC}  Skipped autoloading configuration"
        fi
    else
        cat > "$DUCKDBRC" << EOF
-- Autoload stps extension
.echo Loading stps extension...
LOAD '$INSTALL_DIR/stps.duckdb_extension';
.echo stps extension loaded successfully!
EOF
        echo -e "${GREEN}✓${NC} Created ~/.duckdbrc"
    fi
fi

# Summary
echo ""
echo "======================================"
echo -e "${GREEN}✅ Installation Complete!${NC}"
echo "======================================"
echo ""
echo "Extension installed to:"
echo "  $INSTALL_DIR/stps.duckdb_extension"
echo ""
echo "Test the installation:"
echo "  CLI:    duckdb -unsigned"
echo "  Python: See duckdb_helpers.py for helper function"
echo ""
echo "For more information, see:"
echo "  docs/plans/2026-01-02-autoload-setup.md"
echo ""
