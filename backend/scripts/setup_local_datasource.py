#!/usr/bin/env python3
"""
Setup script to create a local DuckDB datasource with custom columns.
Run this once to enable custom column support for local tables.
"""
import sys
import os
import json

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.db import get_db_session
from app.models import Datasource
from sqlalchemy.orm import Session
import uuid

def create_local_duckdb_datasource():
    """Create a local DuckDB datasource with ClientCode custom column"""
    
    # Custom columns configuration
    transforms = {
        "customColumns": [
            {
                "name": "ClientCode",
                "type": "case",
                "cases": [
                    {
                        "when": "ClientID IS NOT NULL",
                        "then": "ClientID"
                    }
                ],
                "else": "'Unknown'",
                "scope": {
                    "level": "datasource"
                }
            }
        ],
        "transforms": [],
        "joins": [],
        "defaults": {}
    }
    
    with get_db_session() as db:
        # Check if a local DuckDB datasource already exists
        existing = db.query(Datasource).filter(
            Datasource.type == "duckdb",
            Datasource.connection_encrypted == None
        ).first()
        
        if existing:
            print(f"Found existing DuckDB datasource: {existing.id}")
            print("Updating transforms...")
            existing.options_json = json.dumps({"transforms": transforms})
            db.commit()
            print("✓ Updated successfully")
            print(f"\nDatasource ID: {existing.id}")
        else:
            print("Creating new local DuckDB datasource...")
            ds_id = str(uuid.uuid4())
            ds = Datasource(
                id=ds_id,
                name="Local DuckDB",
                type="duckdb",
                user_id=None,  # Global datasource (or set to specific user ID)
                connection_encrypted=None,  # No connection = use default local path
                options_json=json.dumps({"transforms": transforms})
            )
            db.add(ds)
            db.commit()
            print("✓ Created successfully")
            print(f"\nDatasource ID: {ds_id}")
        
        print("\nNOTE: You can customize the ClientCode definition by editing this datasource via the UI")
        print("or by modifying this script and re-running it.")

if __name__ == "__main__":
    try:
        create_local_duckdb_datasource()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
