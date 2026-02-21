"""
strip_column_spaces.py
─────────────────────
Rename every column in every table inside a DuckDB file whose name has
leading or trailing whitespace, stripping it in-place.

Usage (Windows):
    python strip_column_spaces.py C:\Bayan\backend\.data\local-20260220-0436.duckdb

Usage (Mac/Linux):
    python strip_column_spaces.py /path/to/file.duckdb

Requires:  pip install duckdb
"""

import sys
import duckdb


def strip_column_whitespace(db_path: str) -> None:
    con = duckdb.connect(db_path)

    # Fetch all user tables
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_type = 'BASE TABLE' "
        "ORDER BY table_name"
    ).fetchall()

    if not tables:
        print("No tables found.")
        con.close()
        return

    total_renames = 0

    for (table_name,) in tables:
        cols = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'main' AND table_name = ? "
            "ORDER BY ordinal_position",
            [table_name]
        ).fetchall()

        renames = [(c[0], c[0].strip()) for c in cols if c[0] != c[0].strip()]

        if not renames:
            print(f"  {table_name}: no whitespace in column names, skipped.")
            continue

        print(f"\n  {table_name}: renaming {len(renames)} column(s)…")
        for old_name, new_name in renames:
            quoted_table = f'"{table_name}"'
            quoted_old = f'"{old_name}"'
            quoted_new = f'"{new_name}"'
            sql = f"ALTER TABLE {quoted_table} RENAME COLUMN {quoted_old} TO {quoted_new}"
            try:
                con.execute(sql)
                print(f"    '{old_name}'  →  '{new_name}'")
                total_renames += 1
            except Exception as e:
                print(f"    ERROR renaming '{old_name}': {e}")

    con.close()
    print(f"\nDone. {total_renames} column(s) renamed across {len(tables)} table(s).")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python strip_column_spaces.py <path_to_duckdb_file>")
        sys.exit(1)

    path = sys.argv[1]
    print(f"Opening: {path}\n")
    strip_column_whitespace(path)
