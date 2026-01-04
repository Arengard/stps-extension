#!/usr/bin/env python3
"""
STPS Extension Test Script
Testet die gebaute Extension ohne DuckDB CLI zu benötigen
"""

import sys
import os

try:
    import duckdb
except ImportError:
    print("❌ DuckDB Python-Modul nicht installiert!")
    print("\nInstallieren Sie es mit:")
    print("  pip install duckdb")
    sys.exit(1)

def test_extension():
    print("=" * 70)
    print("🧪 STPS Extension Test")
    print("=" * 70)
    print()

    # Pfad zur Extension
    extension_path = os.path.join(os.path.dirname(__file__), "build", "stps.duckdb_extension")

    if not os.path.exists(extension_path):
        print(f"❌ Extension nicht gefunden: {extension_path}")
        print("\nBitte bauen Sie die Extension zuerst:")
        print("  .\\build-windows.bat")
        sys.exit(1)

    print(f"✅ Extension gefunden: {extension_path}")
    print(f"📦 Dateigröße: {os.path.getsize(extension_path):,} Bytes")
    print()

    # DuckDB-Verbindung
    print("🔌 Verbinde mit DuckDB...")
    con = duckdb.connect(':memory:')
    print(f"✅ DuckDB Version: {con.execute('SELECT version()').fetchone()[0]}")
    print()

    # Extension laden
    print(f"📥 Lade Extension...")
    try:
        con.execute(f"LOAD '{extension_path.replace(os.sep, '/')}'")
        print("✅ Extension erfolgreich geladen!")
    except Exception as e:
        print(f"❌ Fehler beim Laden der Extension:")
        print(f"   {e}")
        sys.exit(1)

    print()
    print("=" * 70)
    print("🧪 Funktions-Tests")
    print("=" * 70)
    print()

    tests = [
        {
            "name": "IBAN Validierung",
            "query": "SELECT stps_is_valid_iban('DE89370400440532013000') AS result",
            "expected": True
        },
        {
            "name": "String Upper",
            "query": "SELECT stps_upper('hello world') AS result",
            "expected": "HELLO WORLD"
        },
        {
            "name": "String Lower",
            "query": "SELECT stps_lower('HELLO WORLD') AS result",
            "expected": "hello world"
        },
        {
            "name": "UUID Generierung",
            "query": "SELECT stps_generate_uuid() AS result",
            "expected": None  # UUID ist immer unterschiedlich
        },
    ]

    passed = 0
    failed = 0

    for i, test in enumerate(tests, 1):
        print(f"Test {i}/{len(tests)}: {test['name']}")
        try:
            result = con.execute(test['query']).fetchone()[0]

            if test['expected'] is None:
                # Für UUID nur prüfen ob ein Wert zurückkommt
                if result:
                    print(f"  ✅ PASSED - Result: {result}")
                    passed += 1
                else:
                    print(f"  ❌ FAILED - Kein Ergebnis")
                    failed += 1
            elif result == test['expected']:
                print(f"  ✅ PASSED - Result: {result}")
                passed += 1
            else:
                print(f"  ❌ FAILED - Expected: {test['expected']}, Got: {result}")
                failed += 1
        except Exception as e:
            print(f"  ❌ ERROR - {e}")
            failed += 1
        print()

    # Liste alle STPS-Funktionen auf
    print("=" * 70)
    print("📋 Verfügbare STPS-Funktionen")
    print("=" * 70)
    print()

    try:
        functions = con.execute("""
            SELECT function_name, return_type, parameters
            FROM duckdb_functions()
            WHERE function_name LIKE 'stps_%'
            ORDER BY function_name
        """).fetchall()

        if functions:
            for func_name, return_type, params in functions:
                print(f"  • {func_name}({params or ''}) → {return_type}")
        else:
            print("  ⚠️  Keine STPS-Funktionen gefunden")
            print("     (Möglicherweise wurde die Extension nicht korrekt geladen)")
    except Exception as e:
        print(f"  ❌ Fehler beim Abrufen der Funktionen: {e}")

    print()
    print("=" * 70)
    print("📊 Test-Zusammenfassung")
    print("=" * 70)
    print(f"  ✅ Bestanden: {passed}")
    print(f"  ❌ Fehlgeschlagen: {failed}")
    print(f"  📈 Erfolgsquote: {(passed/(passed+failed)*100):.0f}%")
    print("=" * 70)
    print()

    if failed == 0:
        print("🎉 Alle Tests bestanden! Die Extension funktioniert einwandfrei.")
        return 0
    else:
        print("⚠️  Einige Tests sind fehlgeschlagen. Prüfen Sie die Ausgabe oben.")
        return 1

if __name__ == "__main__":
    sys.exit(test_extension())

