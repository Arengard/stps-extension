"""
DuckDB Connection Helpers with STPS Extension

This module provides helper functions to create DuckDB connections with the
STPS extension automatically loaded. This eliminates the need to manually
load the extension in every script or notebook.

Installation:
    1. Install the STPS extension using scripts/install-extension.sh (or .bat)
    2. Copy this file to your project or add to your Python path
    3. Import and use get_duckdb_connection() instead of duckdb.connect()

Example:
    >>> from duckdb_helpers import get_duckdb_connection
    >>>
    >>> # Create connection with stps extension pre-loaded
    >>> conn = get_duckdb_connection()
    >>>
    >>> # All stps functions are immediately available
    >>> result = conn.execute("SELECT stps_uuid()").fetchone()
    >>> print(f"Generated UUID: {result[0]}")
    >>>
    >>> # Test IBAN validation
    >>> is_valid = conn.execute(
    ...     "SELECT stps_is_valid_iban('DE89370400440532013000')"
    ... ).fetchone()[0]
    >>> print(f"Valid IBAN: {is_valid}")

For more information, see:
    docs/plans/2026-01-02-autoload-setup.md
"""

import duckdb
import sys
from pathlib import Path
from typing import Optional, Union


def get_duckdb_connection(database: Union[str, Path] = ':memory:') -> duckdb.DuckDBPyConnection:
    """
    Create DuckDB connection with stps extension pre-loaded.

    This function creates a DuckDB connection and automatically loads the stps
    extension, making all stps_* functions immediately available without manual
    LOAD commands.

    The extension is loaded from ~/.duckdb/extensions/stps/ (or the Windows
    equivalent %USERPROFILE%\\.duckdb\\extensions\\stps\\). The version is
    automatically detected based on the installed extension.

    Args:
        database: Database file path or ':memory:' for in-memory database.
                  Default is ':memory:'.
                  Can be a string or Path object.

    Returns:
        DuckDB connection object with stps extension loaded and ready to use.

    Raises:
        FileNotFoundError: If the stps extension is not installed in the
                          expected location.
        duckdb.IOException: If the extension fails to load (e.g., version
                           mismatch with DuckDB).

    Example:
        >>> # In-memory database
        >>> conn = get_duckdb_connection()
        >>> result = conn.execute("SELECT stps_uuid()").fetchone()

        >>> # Persistent database
        >>> conn = get_duckdb_connection('my_data.duckdb')
        >>> conn.execute("CREATE TABLE users (id VARCHAR, name VARCHAR)")
        >>> conn.execute("INSERT INTO users VALUES (stps_uuid(), 'Alice')")

    Note:
        - The extension must be installed first using scripts/install-extension.sh
          (or scripts/install-extension.bat on Windows)
        - Run DuckDB with -unsigned flag when using from CLI: duckdb -unsigned
        - This function automatically detects the platform (macOS/Linux/Windows)
    """
    # Create connection with unsigned extensions allowed
    conn = duckdb.connect(str(database), config={'allow_unsigned_extensions': 'true'})

    # Determine extension base path (works on macOS, Linux, and Windows)
    home = Path.home()
    extensions_base = home / '.duckdb' / 'extensions' / 'stps'

    # Try to auto-detect the installed version
    ext_path = _find_extension_binary(extensions_base)

    if not ext_path:
        # Fallback: try common version (update this after installing)
        ext_path = extensions_base / 'v1.4.3' / 'stps.duckdb_extension'

        if not ext_path.exists():
            raise FileNotFoundError(
                f"STPS extension not found at {ext_path}\n"
                f"Please install the extension first:\n"
                f"  macOS/Linux: ./scripts/install-extension.sh\n"
                f"  Windows:     scripts\\install-extension.bat\n"
                f"\nSee docs/plans/2026-01-02-autoload-setup.md for details."
            )

    # Load extension
    try:
        conn.execute(f"LOAD '{ext_path}'")
    except duckdb.IOException as e:
        raise duckdb.IOException(
            f"Failed to load STPS extension from {ext_path}\n"
            f"Original error: {e}\n"
            f"This may indicate a DuckDB version mismatch.\n"
            f"Try rebuilding the extension for your DuckDB version:\n"
            f"  ./scripts/build-for-version.sh $(duckdb --version | grep -oE 'v[0-9]+\\.[0-9]+\\.[0-9]+')\n"
            f"  ./scripts/install-extension.sh"
        ) from e

    return conn


def _find_extension_binary(extensions_base: Path) -> Optional[Path]:
    """
    Auto-detect the installed extension binary.

    Searches for installed extension versions in the extensions directory
    and returns the path to the most recent version.

    Args:
        extensions_base: Base path to search for extensions
                        (e.g., ~/.duckdb/extensions/stps/)

    Returns:
        Path to extension binary if found, None otherwise
    """
    if not extensions_base.exists():
        return None

    # Look for version directories (e.g., v1.4.3, v1.5.0)
    version_dirs = [
        d for d in extensions_base.iterdir()
        if d.is_dir() and d.name.startswith('v')
    ]

    if not version_dirs:
        return None

    # Sort by version (newest first) and return the first valid one
    version_dirs.sort(reverse=True)

    for version_dir in version_dirs:
        ext_path = version_dir / 'stps.duckdb_extension'
        if ext_path.exists():
            return ext_path

    return None


def get_extension_version() -> Optional[str]:
    """
    Get the version of the installed STPS extension.

    Returns:
        Version string (e.g., 'v1.4.3') or None if not found

    Example:
        >>> version = get_extension_version()
        >>> print(f"Installed STPS extension version: {version}")
    """
    home = Path.home()
    extensions_base = home / '.duckdb' / 'extensions' / 'stps'

    ext_path = _find_extension_binary(extensions_base)

    if ext_path:
        # Extract version from path (e.g., ~/.duckdb/extensions/stps/v1.4.3/...)
        return ext_path.parent.name

    return None


def check_extension_installed() -> bool:
    """
    Check if the STPS extension is installed.

    Returns:
        True if extension is installed, False otherwise

    Example:
        >>> if not check_extension_installed():
        ...     print("Please install the STPS extension first")
        ...     print("Run: ./scripts/install-extension.sh")
    """
    home = Path.home()
    extensions_base = home / '.duckdb' / 'extensions' / 'stps'

    return _find_extension_binary(extensions_base) is not None


# Convenience function for quick testing
def test_connection():
    """
    Test the STPS extension by creating a connection and running sample queries.

    This function creates a test connection and verifies that common STPS
    functions work correctly.

    Example:
        >>> from duckdb_helpers import test_connection
        >>> test_connection()
        Testing STPS extension...
        ✓ Connection created successfully
        ✓ Extension loaded
        ✓ stps_uuid() works
        ✓ stps_is_valid_iban() works
        All tests passed!
    """
    try:
        print("Testing STPS extension...")

        # Create connection
        conn = get_duckdb_connection()
        print("✓ Connection created successfully")
        print(f"✓ Extension loaded (version: {get_extension_version()})")

        # Test UUID function
        uuid_result = conn.execute("SELECT stps_uuid()").fetchone()
        print(f"✓ stps_uuid() works: {uuid_result[0]}")

        # Test IBAN validation
        iban_result = conn.execute(
            "SELECT stps_is_valid_iban('DE89370400440532013000')"
        ).fetchone()
        print(f"✓ stps_is_valid_iban() works: {iban_result[0]}")

        # List all stps functions
        functions = conn.execute(
            "SELECT function_name FROM duckdb_functions() "
            "WHERE function_name LIKE 'stps_%' "
            "ORDER BY function_name"
        ).fetchall()

        print(f"\n✓ Found {len(functions)} stps functions:")
        for func in functions[:10]:  # Show first 10
            print(f"  - {func[0]}")
        if len(functions) > 10:
            print(f"  ... and {len(functions) - 10} more")

        print("\nAll tests passed!")

        conn.close()

    except Exception as e:
        print(f"\n✗ Test failed: {e}", file=sys.stderr)
        raise


if __name__ == '__main__':
    # Run tests if executed directly
    test_connection()
