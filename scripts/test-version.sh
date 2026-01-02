#!/usr/bin/env bash
set -euo pipefail

# Test DuckDB extension for a specific version
# Usage: ./scripts/test-version.sh <version>
# Example: ./scripts/test-version.sh v1.4.3

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

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

# Check arguments
if [[ $# -ne 1 ]]; then
    error "Usage: $0 <version>"
    echo "Example: $0 v1.4.3"
    echo ""
    echo "Available builds:"
    ls -d "$PROJECT_ROOT/build"/*/ 2>/dev/null | xargs -n1 basename || echo "  (none)"
    exit 1
fi

VERSION="$1"
BUILD_DIR="$PROJECT_ROOT/build/$VERSION"
EXTENSION_PATH="$BUILD_DIR/extension/stps/stps.duckdb_extension"
BUILT_DUCKDB="$BUILD_DIR/duckdb"
TEST_DIR="$PROJECT_ROOT/test"

# Verify build exists
if [[ ! -d "$BUILD_DIR" ]]; then
    error "Build directory not found: $BUILD_DIR"
    info "Run: ./scripts/build-for-version.sh $VERSION"
    exit 1
fi

if [[ ! -f "$EXTENSION_PATH" ]]; then
    error "Extension not found: $EXTENSION_PATH"
    info "Run: ./scripts/build-for-version.sh $VERSION"
    exit 1
fi

echo ""
success "Testing $VERSION extension"
echo "📍 Extension: $EXTENSION_PATH"

# Detect DuckDB binary
DUCKDB_BIN=""
DUCKDB_SOURCE=""

# Try system DuckDB first
if command -v duckdb >/dev/null 2>&1; then
    SYSTEM_DUCKDB=$(which duckdb)
    SYSTEM_VERSION=$(duckdb --version 2>/dev/null | head -1 || echo "unknown")

    # Check if version matches
    if [[ "$SYSTEM_VERSION" == *"$VERSION"* ]] || [[ "$VERSION" == "v"* && "$SYSTEM_VERSION" == *"${VERSION#v}"* ]]; then
        DUCKDB_BIN="$SYSTEM_DUCKDB"
        DUCKDB_SOURCE="system"
    else
        warning "System DuckDB version doesn't match: $SYSTEM_VERSION"
        info "Expected: $VERSION"
    fi
fi

# Fall back to built DuckDB
if [[ -z "$DUCKDB_BIN" ]]; then
    if [[ -f "$BUILT_DUCKDB" ]]; then
        DUCKDB_BIN="$BUILT_DUCKDB"
        DUCKDB_SOURCE="built"
    else
        error "No compatible DuckDB binary found"
        info "System DuckDB version mismatch and built binary not found"
        exit 1
    fi
fi

DUCKDB_VERSION=$("$DUCKDB_BIN" --version 2>/dev/null | head -1 || echo "unknown")
echo "🔧 DuckDB:    $DUCKDB_BIN ($DUCKDB_SOURCE)"
echo "   Version:   $DUCKDB_VERSION"
echo ""

# Find test files
TEST_FILES=()
if [[ -d "$TEST_DIR" ]]; then
    while IFS= read -r -d '' file; do
        TEST_FILES+=("$file")
    done < <(find "$TEST_DIR" -name "*.sql" -print0 2>/dev/null)
fi

# If no test directory or files, look for test_*.sql in project root
if [[ ${#TEST_FILES[@]} -eq 0 ]]; then
    while IFS= read -r -d '' file; do
        TEST_FILES+=("$file")
    done < <(find "$PROJECT_ROOT" -maxdepth 1 -name "test_*.sql" -print0 2>/dev/null)
fi

if [[ ${#TEST_FILES[@]} -eq 0 ]]; then
    warning "No test files found in $TEST_DIR or $PROJECT_ROOT/test_*.sql"
    info "Creating a basic test..."

    # Run a basic test directly
    info "Running basic IBAN function test..."

    TEST_SQL=$(cat <<EOF
LOAD '$EXTENSION_PATH';

SELECT 'Testing stps_is_valid_iban:' as test;
SELECT stps_is_valid_iban('DE89370400440532013000') as is_valid;

SELECT 'Testing stps_format_iban:' as test;
SELECT stps_format_iban('DE89370400440532013000') as formatted;

SELECT 'All stps functions:' as test;
SELECT function_name
FROM duckdb_functions()
WHERE function_name LIKE 'stps%'
ORDER BY function_name;
EOF
)

    if echo "$TEST_SQL" | "$DUCKDB_BIN" -unsigned 2>&1; then
        echo ""
        success "Basic tests passed!"
        exit 0
    else
        echo ""
        error "Basic tests failed"
        exit 1
    fi
fi

# Run test files
info "Running tests:"
FAILED_TESTS=0
PASSED_TESTS=0

for test_file in "${TEST_FILES[@]}"; do
    test_name=$(basename "$test_file")

    # Create temporary test file with LOAD statement prepended
    TEMP_TEST=$(mktemp)
    echo "LOAD '$EXTENSION_PATH';" > "$TEMP_TEST"
    cat "$test_file" >> "$TEMP_TEST"

    # Run test
    if "$DUCKDB_BIN" -unsigned < "$TEMP_TEST" >/dev/null 2>&1; then
        success "$test_name passed"
        ((PASSED_TESTS++))
    else
        error "$test_name failed"
        ((FAILED_TESTS++))

        # Show error output
        echo "  Error output:"
        "$DUCKDB_BIN" -unsigned < "$TEMP_TEST" 2>&1 | sed 's/^/    /' || true
    fi

    rm -f "$TEMP_TEST"
done

echo ""
echo "Results: $PASSED_TESTS passed, $FAILED_TESTS failed"

if [[ $FAILED_TESTS -eq 0 ]]; then
    success "All tests passed!"
    exit 0
else
    error "Some tests failed"
    exit 1
fi
