r"""
Test script to verify Deposits calculation in DuckDB
Run this from: C:\Bayan\backend\
Command: python test_deposits.py
"""
import duckdb
import sys

# Connect to DuckDB (using in-memory or your database file)
# If you have a database file, replace ':memory:' with the path
try:
    # conn = duckdb.connect(':memory:')  # Change this if you have a persistent DB file
    conn = duckdb.connect('C:/Bayan/backend/.data/datamodel-20251102-1740.duckdb')
    # You might need to attach your database if it's not the default
    # Uncomment and adjust if needed:
    # conn.execute("ATTACH 'path/to/your/database.duckdb' AS main")
    
    print("Testing VaultID=1, ClientCode=BOP (matching Excel filter)...")
    print("=" * 80)
    
    # Test query to see raw values - WITH JOIN
    test_sql = """
    SELECT 
        s.CurrencyName,
        s.VaultID,
        j1.EnName AS VaultName,
        s.Category1, s.Category2, s.Category3, s.Category4, 
        s.Category5, s.Category6, s.Category7, s.Category8,
        (COALESCE(TRY_CAST(s.Category1 AS DOUBLE), 0) + 
         COALESCE(TRY_CAST(s.Category2 AS DOUBLE), 0) + 
         COALESCE(TRY_CAST(s.Category3 AS DOUBLE), 0) + 
         COALESCE(TRY_CAST(s.Category4 AS DOUBLE), 0) + 
         COALESCE(TRY_CAST(s.Category5 AS DOUBLE), 0) + 
         COALESCE(TRY_CAST(s.Category6 AS DOUBLE), 0) + 
         COALESCE(TRY_CAST(s.Category7 AS DOUBLE), 0) + 
         COALESCE(TRY_CAST(s.Category8 AS DOUBLE), 0)) as calculated_deposits
    FROM main.View_Client_Vault_Report_By_Category1 AS s
    LEFT JOIN main.SYS_Lookups AS j1 ON s.VaultID = j1.LKP_ID AND j1.LKP_Type = 'Vault'
    WHERE s.VaultID = 1 AND s.ClientID = '2'
    ORDER BY s.OrderDate DESC;
    """
    
    result = conn.execute(test_sql).fetchall()
    
    if not result:
        print("❌ No rows found! Check if the view exists and has data.")
        print("\nTrying to list available tables...")
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"Available tables: {tables}")
    else:
        print(f"✅ Found {len(result)} rows\n")
        # Show first 10 and last 10 rows
        rows_to_show = result[:10] + (result[-10:] if len(result) > 20 else [])
        shown_count = 0
        for i, row in enumerate(result):
            if i < 10 or i >= len(result) - 10:
                if i == 10 and len(result) > 20:
                    print(f"\n... ({len(result) - 20} more rows) ...\n")
                print(f"Row {i+1}/{len(result)}:")
                print(f"Currency: {row[0]} | VaultID: {row[1]} | VaultName: {row[2]}")
                print(f"  Cat1(20):  {row[3]}")
                print(f"  Cat2(50):  {row[4]}")
                print(f"  Cat3(100): {row[5]}")
                print(f"  Cat4(200): {row[6]}")
                print(f"  Cat5(5):   {row[7]}")
                print(f"  Cat6(10):  {row[8]}")
                print(f"  Cat7(1):   {row[9]}")
                print(f"  Cat8(2):   {row[10]}")
                print(f"  ➜ TOTAL:   {row[11]}")
                print("-" * 60)
    
    # Test aggregated sum for VaultID=1, ClientCode=BOP
    print("\n" + "=" * 80)
    print("Testing aggregated SUM for VaultID=1, ClientCode=BOP (matching Excel)...")
    print("=" * 80)
    
    agg_sql = """
    SELECT 
        s.CurrencyName,
        COUNT(*) as row_count,
        SUM(COALESCE(TRY_CAST(s.Category4 AS DOUBLE), 0)) as cat4_200_sum,
        SUM(COALESCE(TRY_CAST(s.Category2 AS DOUBLE), 0)) as cat2_50_sum,
        SUM(
            COALESCE(TRY_CAST(s.Category1 AS DOUBLE), 0) + 
            COALESCE(TRY_CAST(s.Category2 AS DOUBLE), 0) + 
            COALESCE(TRY_CAST(s.Category3 AS DOUBLE), 0) + 
            COALESCE(TRY_CAST(s.Category4 AS DOUBLE), 0) + 
            COALESCE(TRY_CAST(s.Category5 AS DOUBLE), 0) + 
            COALESCE(TRY_CAST(s.Category6 AS DOUBLE), 0) + 
            COALESCE(TRY_CAST(s.Category7 AS DOUBLE), 0) + 
            COALESCE(TRY_CAST(s.Category8 AS DOUBLE), 0)
        ) as total_deposits
    FROM main.View_Client_Vault_Report_By_Category1 AS s
    WHERE s.VaultID = 1 AND s.ClientID = '2'
    GROUP BY s.CurrencyName
    ORDER BY s.CurrencyName;
    """
    
    agg_result = conn.execute(agg_sql).fetchall()
    
    if agg_result:
        print(f"\n✅ Aggregated results by Currency:\n")
        for row in agg_result:
            print(f"  {row[0]}: {row[1]} rows")
            print(f"    Category4 (200): {row[2]:,.2f}")
            print(f"    Category2 (50):  {row[3]:,.2f}")
            print(f"    TOTAL Deposits:  {row[4]:,.2f}")
    
    conn.close()
    print("\n✅ Test completed successfully!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
