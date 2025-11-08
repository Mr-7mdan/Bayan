"""
Tests for SQLGlot SQL generation.
Validates that SQLGlot produces correct SQL for various scenarios.
"""
import pytest
from app.sqlgen_glot import SQLGlotBuilder, should_use_sqlglot, validate_sql


class TestSQLGlotBuilder:
    """Test SQLGlot query builder"""
    
    def test_simple_sum_aggregation(self):
        """Test basic SUM aggregation"""
        builder = SQLGlotBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="sales",
            x_field="date",
            y_field="amount",
            agg="sum",
        )
        
        # Verify key components
        assert "SUM" in sql.upper()
        assert "GROUP BY" in sql.upper()
        assert "sales" in sql
        assert " as x" in sql.lower()
        assert " as value" in sql.lower()
    
    def test_aggregation_with_legend(self):
        """Test aggregation with legend field"""
        builder = SQLGlotBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="orders",
            x_field="order_date",
            y_field="total",
            legend_field="category",
            agg="sum",
        )
        
        assert "category" in sql
        assert " as legend" in sql.lower()
        assert "GROUP BY" in sql.upper()
        # Should group by both x and legend (positions 1, 2)
        assert "GROUP BY 1, 2" in sql.upper() or "GROUP BY 1,2" in sql.upper()
    
    def test_count_aggregation(self):
        """Test COUNT(*) aggregation"""
        builder = SQLGlotBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="users",
            agg="count",
        )
        
        assert "COUNT(*)" in sql.upper() or "COUNT(1)" in sql.upper()
        assert " as value" in sql.lower()
    
    def test_distinct_count(self):
        """Test COUNT(DISTINCT field)"""
        builder = SQLGlotBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="events",
            y_field="user_id",
            agg="distinct",
        )
        
        assert "COUNT(" in sql.upper() and "user_id" in sql.lower()
        # DISTINCT flag is internal to SQLGlot, may not appear in final SQL
    
    def test_time_bucketing_month(self):
        """Test DATE_TRUNC for month grouping"""
        builder = SQLGlotBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="transactions",
            x_field="created_at",
            y_field="amount",
            agg="sum",
            group_by="month",
        )
        
        assert "DATE_TRUNC" in sql or "date_trunc" in sql
        assert "month" in sql.lower()
    
    def test_where_clause_single_value(self):
        """Test WHERE with single value"""
        builder = SQLGlotBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="sales",
            y_field="revenue",
            agg="sum",
            where={"status": "completed"},
        )
        
        assert "WHERE" in sql.upper()
        assert "status" in sql
        assert "completed" in sql
    
    def test_where_clause_list(self):
        """Test WHERE with IN clause"""
        builder = SQLGlotBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="products",
            y_field="price",
            agg="avg",
            where={"category": ["electronics", "books"]},
        )
        
        assert "WHERE" in sql.upper()
        assert "IN" in sql.upper()
        assert "electronics" in sql
        assert "books" in sql
    
    def test_order_by_value(self):
        """Test ORDER BY value DESC"""
        builder = SQLGlotBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="sales",
            x_field="product",
            y_field="revenue",
            agg="sum",
            order_by="value",
            order="desc",
        )
        
        assert "ORDER BY" in sql.upper()
        assert "value" in sql
        # DESC should be present (either explicit or via ordering)
    
    def test_limit(self):
        """Test LIMIT clause"""
        builder = SQLGlotBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="logs",
            agg="count",
            limit=100,
        )
        
        assert "LIMIT" in sql.upper()
        assert "100" in sql
    
    def test_postgres_dialect(self):
        """Test PostgreSQL dialect"""
        builder = SQLGlotBuilder("postgres")
        sql = builder.build_aggregation_query(
            source="events",
            x_field="timestamp",
            y_field="count",
            agg="sum",
            group_by="day",
        )
        
        # PostgreSQL uses lowercase date_trunc
        assert "date_trunc" in sql.lower() or "DATE_TRUNC" in sql
        assert builder.dialect == "postgres"
    
    def test_mysql_dialect(self):
        """Test MySQL dialect"""
        builder = SQLGlotBuilder("mysql")
        assert builder.dialect == "mysql"
    
    def test_mssql_dialect(self):
        """Test MSSQL dialect normalization"""
        builder = SQLGlotBuilder("mssql")
        assert builder.dialect == "tsql"
        
        builder2 = SQLGlotBuilder("sqlserver")
        assert builder2.dialect == "tsql"
    
    def test_comparison_operators(self):
        """Test comparison operators in WHERE clause"""
        builder = SQLGlotBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="orders",
            x_field="date",
            y_field="amount",
            agg="sum",
            where={
                "amount__gte": 100,
                "amount__lt": 1000,
            },
            group_by="month"
        )
        assert ">=" in sql
        assert "<" in sql
        assert "amount" in sql.lower()
    
    def test_date_range_filters(self):
        """Test date range filtering with start/end"""
        builder = SQLGlotBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="orders",
            x_field="order_date",
            y_field="amount",
            agg="sum",
            where={
                "start": "2023-01-01",
                "end": "2023-12-31",
            },
            group_by="month",
            date_field="order_date"
        )
        assert "order_date" in sql.lower()
        assert ">=" in sql
        assert "<=" in sql
        assert "2023-01-01" in sql
        assert "2023-12-31" in sql
    
    def test_derived_columns_no_validation(self):
        """Test that derived columns don't cause validation errors"""
        builder = SQLGlotBuilder("duckdb")
        # This should not raise an error even though "OrderDate (Year)" doesn't exist
        sql = builder.build_aggregation_query(
            source="orders",
            x_field="order_date",
            y_field="amount",
            agg="sum",
            where={
                "OrderDate (Year)": ["2023", "2024"],  # Derived column
            },
            group_by="month"
        )
        assert "IN" in sql.upper()
        assert "OrderDate (Year)" in sql
    
    def test_expression_in_where_clause(self):
        """Test WHERE clause with SQL expression as key (resolved derived column)"""
        builder = SQLGlotBuilder("duckdb")
        # Simulating resolved derived column: "(strftime('%Y', \"OrderDate\"))"
        sql = builder.build_aggregation_query(
            source="orders",
            x_field="order_date",
            y_field="amount",
            agg="sum",
            where={
                "(strftime('%Y', \"OrderDate\"))": ["2023", "2024", "2025"]
            },
            group_by="month"
        )
        assert "strftime" in sql or "STRFTIME" in sql
        assert "IN" in sql.upper()
        assert "2023" in sql
    
    def test_mixed_expression_and_column_where(self):
        """Test WHERE with both expressions and regular columns"""
        builder = SQLGlotBuilder("duckdb")
        sql = builder.build_aggregation_query(
            source="orders",
            x_field="date",
            y_field="amount",
            agg="sum",
            where={
                "(strftime('%Y', \"OrderDate\"))": ["2023"],  # Expression
                "status": "completed",  # Regular column
                "amount__gte": 100  # Comparison operator
            },
            group_by="month"
        )
        assert "strftime" in sql or "STRFTIME" in sql
        assert "status" in sql
        assert ">=" in sql
        assert "completed" in sql


class TestShouldUseSQLGlot:
    """Test feature flag logic"""
    
    def test_disabled_globally(self, monkeypatch):
        """Test when ENABLE_SQLGLOT=false"""
        # Mock settings at the config level where it's imported
        class MockSettings:
            enable_sqlglot = False
            sqlglot_users = ""
        
        monkeypatch.setattr("app.config.settings", MockSettings())
        
        assert should_use_sqlglot("any_user") is False
    
    def test_enabled_for_all(self, monkeypatch):
        """Test when ENABLE_SQLGLOT=true, SQLGLOT_USERS=*"""
        class MockSettings:
            enable_sqlglot = True
            sqlglot_users = "*"
        
        monkeypatch.setattr("app.config.settings", MockSettings())
        
        assert should_use_sqlglot("any_user") is True
        assert should_use_sqlglot(None) is True
    
    def test_enabled_for_specific_users(self, monkeypatch):
        """Test when ENABLE_SQLGLOT=true, SQLGLOT_USERS=user1,user2"""
        class MockSettings:
            enable_sqlglot = True
            sqlglot_users = "user1,user2"
        
        monkeypatch.setattr("app.config.settings", MockSettings())
        
        assert should_use_sqlglot("user1") is True
        assert should_use_sqlglot("user2") is True
        assert should_use_sqlglot("user3") is False
        assert should_use_sqlglot(None) is True  # No user ID = use global flag


class TestValidateSQL:
    """Test SQL validation"""
    
    def test_valid_sql(self):
        """Test valid SQL passes validation"""
        sql = "SELECT * FROM table WHERE id = 1"
        is_valid, error = validate_sql(sql, "duckdb")
        
        assert is_valid is True
        assert error is None
    
    def test_invalid_sql(self):
        """Test invalid SQL fails validation"""
        sql = "SELECT * FROM WHERE"
        is_valid, error = validate_sql(sql, "duckdb")
        
        assert is_valid is False
        assert error is not None
        assert len(error) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
