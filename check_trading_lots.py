"""
Diagnostic: why do Deposits/Withdrawals differ between This Week (Mar 2-4) and This Month (Mar 1-4)?
Hypothesis: Daily_Positions has data on Sunday March 1st.
"""
import duckdb

DUCK_DB_PATH = "/Users/mohammed/Documents/Bayan/backend/.data/new-20251030-1557.duckdb"

con = duckdb.connect(DUCK_DB_PATH, read_only=True)

print("=== Daily_Positions: daily breakdown Mar 1-4 ===")
try:
    rows = con.execute("""
        SELECT created_at,
               strftime(created_at, '%A') AS day_name,
               SUM("Client Deposits")    AS deposits,
               SUM("Client Withdrawals") AS withdrawals,
               COUNT(*)                  AS rows
        FROM Daily_Positions
        WHERE created_at >= '2026-03-01' AND created_at < '2026-03-05'
        GROUP BY created_at
        ORDER BY created_at
    """).fetchall()
    for r in rows:
        print(f"  {r[0]}  ({r[1]})  deposits={r[2]:,.2f}  withdrawals={r[3]:,.2f}  rows={r[4]}")
except Exception as e:
    print(f"  ERROR: {e}")

print()
print("=== THIS WEEK sum (Mar 2-4) ===")
try:
    r = con.execute("""
        SELECT SUM("Client Deposits") as dep, SUM("Client Withdrawals") as wd
        FROM Daily_Positions
        WHERE created_at >= '2026-03-02' AND created_at < '2026-03-05'
    """).fetchone()
    print(f"  Deposits={r[0]:,.2f}  Withdrawals={r[1]:,.2f}")
except Exception as e:
    print(f"  ERROR: {e}")

print()
print("=== THIS MONTH sum (Mar 1-4) ===")
try:
    r = con.execute("""
        SELECT SUM("Client Deposits") as dep, SUM("Client Withdrawals") as wd
        FROM Daily_Positions
        WHERE created_at >= '2026-03-01' AND created_at < '2026-03-05'
    """).fetchone()
    print(f"  Deposits={r[0]:,.2f}  Withdrawals={r[1]:,.2f}")
except Exception as e:
    print(f"  ERROR: {e}")

print()
print("=== created_at column type ===")
try:
    r = con.execute("""
        SELECT typeof(created_at), created_at
        FROM Daily_Positions
        WHERE created_at >= '2026-03-01' AND created_at < '2026-03-05'
        LIMIT 3
    """).fetchall()
    for row in r:
        print(f"  type={row[0]}  value={row[1]}")
except Exception as e:
    print(f"  ERROR: {e}")

print()
print("=== Sample rows for Mar 1 (Sunday) ===")
try:
    r = con.execute("""
        SELECT * FROM Daily_Positions
        WHERE created_at = '2026-03-01'
        LIMIT 5
    """).fetchall()
    cols = [d[0] for d in con.description]
    print(f"  columns: {cols}")
    for row in r:
        print(f"  {dict(zip(cols, row))}")
except Exception as e:
    print(f"  ERROR: {e}")

con.close()
