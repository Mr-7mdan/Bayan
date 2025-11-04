"""
SQL Dialect Normalizer
Converts SQL expressions between different database dialects (SQL Server, DuckDB, MySQL, PostgreSQL)
"""

import re
from typing import Optional


def normalize_sql_expression(
    expr: str,
    target_dialect: str,
    source_dialect: Optional[str] = None
) -> str:
    """
    Normalize a SQL expression to the target dialect.
    
    Args:
        expr: SQL expression to normalize
        target_dialect: Target database dialect (duckdb, mysql, postgres, mssql, sqlite)
        source_dialect: Source dialect hint (auto-detect if None)
        
    Returns:
        Normalized SQL expression
    """
    if not expr or not isinstance(expr, str):
        return expr
    
    target = (target_dialect or "").lower()
    result = str(expr)
    
    # Step 1: Normalize identifier quoting
    result = _normalize_identifiers(result, target)
    
    # Step 2: Fix CASE/END spacing
    result = _fix_case_end_spacing(result)
    
    # Step 3: Dialect-specific function conversions (if needed in future)
    # result = _normalize_functions(result, target)
    
    return result


def _normalize_identifiers(expr: str, target_dialect: str) -> str:
    """
    Convert identifier quoting to match target dialect.
    
    Patterns:
    - SQL Server: [identifier] or [schema].[table].[column]
    - DuckDB/PostgreSQL: "identifier" or schema."table"."column"  
    - MySQL: `identifier` or `schema`.`table`.`column`
    - Unquoted: identifier (when safe)
    """
    
    # Replace SQL Server bracket notation: [s].[ClientID] → s."ClientID" or s.`ClientID`
    if target_dialect in ('duckdb', 'postgres', 'postgresql', 'sqlite'):
        # Replace [identifier] with "identifier"
        # Handle nested brackets like [schema].[table].[column]
        def replace_brackets(match):
            content = match.group(1)
            # Unescape doubled brackets
            content = content.replace(']]', ']')
            return f'"{content}"'
        
        # Pattern: [anything_except_brackets] but handle escaped ]]
        # Simple approach: replace [...]  with "..."
        result = re.sub(r'\[([^\]]+)\]', replace_brackets, expr)
        
    elif target_dialect in ('mysql', 'mariadb'):
        # Replace [identifier] with `identifier`
        def replace_brackets(match):
            content = match.group(1)
            content = content.replace(']]', ']')
            return f'`{content}`'
        
        result = re.sub(r'\[([^\]]+)\]', replace_brackets, expr)
        
    elif target_dialect in ('mssql', 'sqlserver', 'mssql+pymssql', 'mssql+pyodbc'):
        # Already SQL Server syntax, but normalize double quotes to brackets
        result = re.sub(r'"([^"]+)"', r'[\1]', expr)
        # Normalize backticks to brackets
        result = re.sub(r'`([^`]+)`', r'[\1]', expr)
    
    else:
        # Unknown dialect: prefer double quotes (most standard)
        result = re.sub(r'\[([^\]]+)\]', r'"\1"', expr)
        result = re.sub(r'`([^`]+)`', r'"\1"', expr)
    
    return result


def _fix_case_end_spacing(expr: str) -> str:
    """
    Ensure proper spacing around CASE/END keywords.
    
    Common issue: CASE ... THENEND should be THEN...END
    Also: missing space before END
    """
    # Add space before END if missing (but not in words like APPEND, SPEND, etc.)
    # Look for: non-whitespace followed immediately by END (case-insensitive)
    # But only if END is a keyword (followed by whitespace, comma, or end of string)
    result = re.sub(
        r'(\S)(END)(?=\s|,|$)', 
        r'\1 \2', 
        expr, 
        flags=re.IGNORECASE
    )
    
    # Ensure space after THEN if missing
    result = re.sub(
        r'(THEN)(\S)',
        r'\1 \2',
        result,
        flags=re.IGNORECASE
    )
    
    # Ensure space after ELSE if missing  
    result = re.sub(
        r'(ELSE)(\S)',
        r'\1 \2',
        result,
        flags=re.IGNORECASE
    )
    
    return result


def _normalize_functions(expr: str, target_dialect: str) -> str:
    """
    Convert dialect-specific functions to target equivalents.
    (Placeholder for future enhancements)
    
    Examples:
    - MSSQL CONVERT → DuckDB CAST
    - MySQL CONCAT_WS → Postgres string_agg
    """
    # TODO: Implement function conversions if needed
    return expr


# Convenience functions for common cases

def mssql_to_duckdb(expr: str) -> str:
    """Convert SQL Server expression to DuckDB-compatible syntax."""
    return normalize_sql_expression(expr, 'duckdb', 'mssql')


def mssql_to_postgres(expr: str) -> str:
    """Convert SQL Server expression to PostgreSQL-compatible syntax."""
    return normalize_sql_expression(expr, 'postgres', 'mssql')


def mssql_to_mysql(expr: str) -> str:
    """Convert SQL Server expression to MySQL-compatible syntax."""
    return normalize_sql_expression(expr, 'mysql', 'mssql')


def auto_normalize(expr: str, target_dialect: str) -> str:
    """
    Auto-detect source dialect and normalize to target.
    Useful when you don't know the source dialect.
    """
    # Simple heuristic: if expression contains [brackets], assume SQL Server
    if '[' in expr and ']' in expr:
        return normalize_sql_expression(expr, target_dialect, 'mssql')
    # If contains backticks, assume MySQL
    elif '`' in expr:
        return normalize_sql_expression(expr, target_dialect, 'mysql')
    # Otherwise, assume already in a standard format
    else:
        return normalize_sql_expression(expr, target_dialect, None)
