#!/usr/bin/env bash
set -euo pipefail

# Build DuckDB extension for a specific DuckDB version
# Usage: ./scripts/build-for-version.sh <version>
# Example: ./scripts/build-for-version.sh v1.4.3

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ORIGINAL_PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$ORIGINAL_PROJECT_ROOT"
DUCKDB_DIR="$PROJECT_ROOT/duckdb"

# Handle directories with spaces in their names
# Create a temporary symlink without spaces if needed
SYMLINK_CREATED=false
SYMLINK_PATH=""
if [[ "$PROJECT_ROOT" == *" "* ]]; then
    SYMLINK_PATH="/tmp/duckdb-ext-build-$$"
    ln -s "$PROJECT_ROOT" "$SYMLINK_PATH"
    SYMLINK_CREATED=true
    PROJECT_ROOT="$SYMLINK_PATH"
    DUCKDB_DIR="$PROJECT_ROOT/duckdb"
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored messages
info() { echo -e "${BLUE}ℹ${NC} $*"; }
success() { echo -e "${GREEN}✅${NC} $*"; }
warning() { echo -e "${YELLOW}⚠${NC} $*"; }
error() { echo -e "${RED}❌${NC} $*"; }

# Function to cleanup on exit
cleanup() {
    local exit_code=$?
    if [[ -n "${ORIGINAL_COMMIT:-}" ]]; then
        info "Restoring submodule to original state..."
        cd "$DUCKDB_DIR"
        git checkout "$ORIGINAL_COMMIT" >/dev/null 2>&1 || {
            error "Failed to restore submodule to $ORIGINAL_COMMIT"
            error "Please manually run: cd duckdb && git checkout $ORIGINAL_COMMIT"
        }
        cd "$PROJECT_ROOT"
        success "Submodule restored"
    fi
    if [[ "$SYMLINK_CREATED" == "true" ]] && [[ -n "$SYMLINK_PATH" ]]; then
        rm -f "$SYMLINK_PATH"
    fi
    exit $exit_code
}

# Set up trap for cleanup
trap cleanup EXIT INT TERM

# Check arguments
if [[ $# -ne 1 ]]; then
    error "Usage: $0 <version>"
    echo "Example: $0 v1.4.3"
    echo ""
    echo "Available recent tags:"
    git -C "$DUCKDB_DIR" tag | grep '^v1' | tail -5
    exit 1
fi

VERSION="$1"
BUILD_DIR="$ORIGINAL_PROJECT_ROOT/build/$VERSION"

info "Building extension for DuckDB $VERSION"

# Check if duckdb submodule exists
if [[ ! -e "$DUCKDB_DIR/.git" ]]; then
    error "DuckDB submodule not initialized"
    info "Run: git submodule update --init --recursive"
    exit 1
fi

# Save current submodule state
cd "$DUCKDB_DIR"
ORIGINAL_COMMIT=$(git rev-parse HEAD)
info "Saved current submodule state: ${ORIGINAL_COMMIT:0:12}"

# Check for dirty submodule
if [[ -n $(git status --porcelain) ]]; then
    error "DuckDB submodule has uncommitted changes"
    info "Please commit or stash changes in duckdb/ directory"
    exit 1
fi

# Verify version exists
if ! git rev-parse "$VERSION" >/dev/null 2>&1; then
    error "Version '$VERSION' not found in duckdb submodule"
    echo ""
    echo "Available tags:"
    git tag | grep '^v1' | tail -10
    echo ""
    echo "Or use branch names like 'main' for development version"
    exit 1
fi

# Checkout target version
info "Checking out DuckDB $VERSION..."
git checkout "$VERSION" >/dev/null 2>&1
CHECKED_OUT_COMMIT=$(git rev-parse HEAD)
success "Checked out $VERSION (${CHECKED_OUT_COMMIT:0:12})"

cd "$PROJECT_ROOT"

# Build using Makefile (builds to build/release by default)
info "Building extension and DuckDB binary (this may take a few minutes)..."
cd "$PROJECT_ROOT"

# Clean previous build to ensure we're building fresh
make clean >/dev/null 2>&1 || true

# Build the extension
# Use PWD override to make the Makefile see the symlink path instead of the real path
if [[ "$SYMLINK_CREATED" == "true" ]]; then
    if ! PWD="$PROJECT_ROOT" make release -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4) 2>&1 | \
        grep -v "^make\[" | grep -v "Entering directory" | grep -v "Leaving directory" | \
        grep -v "^\[" | grep -v "^--"; then
        error "Build failed"
        exit 1
    fi
else
    if ! make release -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4) 2>&1 | \
        grep -v "^make\[" | grep -v "Entering directory" | grep -v "Leaving directory" | \
        grep -v "^\[" | grep -v "^--"; then
        error "Build failed"
        exit 1
    fi
fi

# Copy build artifacts to version-specific directory
info "Copying build artifacts to $BUILD_DIR..."
mkdir -p "$BUILD_DIR"
cp -r build/release/* "$BUILD_DIR/"

success "Build complete!"

# Verify extension was built
EXTENSION_PATH="$BUILD_DIR/extension/polarsgodmode/polarsgodmode.duckdb_extension"
if [[ ! -f "$EXTENSION_PATH" ]]; then
    error "Extension not found at expected path: $EXTENSION_PATH"
    exit 1
fi

# Verify DuckDB binary was built
DUCKDB_BINARY="$BUILD_DIR/duckdb"
if [[ ! -f "$DUCKDB_BINARY" ]]; then
    error "DuckDB binary not found at expected path: $DUCKDB_BINARY"
    exit 1
fi

# Get DuckDB version
DUCKDB_VERSION=$("$DUCKDB_BINARY" --version | head -1)

echo ""
success "Extension built successfully for $VERSION"
echo ""
echo "📍 Extension: $EXTENSION_PATH"
echo "🔧 DuckDB:    $DUCKDB_BINARY"
echo "   Version:   $DUCKDB_VERSION"
echo ""
info "Next steps:"
echo "  1. Run tests:  ./scripts/test-version.sh $VERSION"
echo "  2. Or use manually:"
echo "     duckdb -unsigned"
echo "     > LOAD '$EXTENSION_PATH';"
echo ""
