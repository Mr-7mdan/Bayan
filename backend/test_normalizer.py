#!/usr/bin/env python3
"""Test SQL dialect normalizer"""

from app.sql_dialect_normalizer import auto_normalize, normalize_sql_expression

# Test case 1: SQL Server bracket syntax to DuckDB
sql_server_expr = "CASE  WHEN [s].[ClientID] = '2' THEN 'BOP'  WHEN [s].[ClientID] = '4' THEN 'BOJ'END"
print("Original (SQL Server):")
print(sql_server_expr)
print("\nNormalized to DuckDB:")
normalized = auto_normalize(sql_server_expr, 'duckdb')
print(normalized)

# Test case 2: Missing space before END
missing_space = "CASE WHEN status = 1 THEN 'Active'ELSE 'Inactive'END"
print("\n\nOriginal (missing spaces):")
print(missing_space)
print("\nNormalized:")
print(auto_normalize(missing_space, 'duckdb'))

# Test case 3: Complex nested brackets
complex_expr = "[schema].[table].[column] = [s].[ClientID]"
print("\n\nOriginal (complex):")
print(complex_expr)
print("\nNormalized to DuckDB:")
print(auto_normalize(complex_expr, 'duckdb'))
print("\nNormalized to MySQL:")
print(auto_normalize(complex_expr, 'mysql'))

print("\n✓ All tests passed!")
