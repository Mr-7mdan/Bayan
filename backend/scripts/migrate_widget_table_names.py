#!/usr/bin/env python3
"""
Migrate widget configs to use table IDs and update broken table references.

This script:
1. Finds all widgets with old table names in querySpec.source
2. Checks if those tables exist in DuckDB
3. If not, looks for renamed tables in datasource tableIdMappings
4. Updates widget configs with current table names and adds sourceTableId
"""

import sys
import json
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.models import SessionLocal, Dashboard, Datasource
from app.db import open_duck_native
from app.config import settings

def get_existing_tables(ds_id: str) -> set:
    """Get list of tables that actually exist in DuckDB"""
    try:
        with open_duck_native(settings.duckdb_path) as conn:
            result = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
            return {row[0] for row in result}
    except Exception as e:
        print(f"Failed to get tables: {e}")
        return set()

def find_current_table_name(ds, old_name: str) -> str | None:
    """Find current name for a table that may have been renamed"""
    try:
        opts = json.loads(ds.options_json or "{}")
        mappings = opts.get("tableIdMappings", {})
        
        # Check if this old name appears in mappings
        for table_id, current_name in mappings.items():
            if old_name in table_id or table_id.endswith(f"__{old_name}"):
                return current_name
        
        # Check if old name without UUID prefix exists
        if "__" in old_name:
            short_name = old_name.split("__", 1)[1] if old_name.count("__") >= 1 else old_name
            return short_name
    except Exception as e:
        print(f"Error finding current name: {e}")
    
    return None

def migrate_widgets():
    """Main migration function"""
    db = SessionLocal()
    
    try:
        # Get all datasources
        datasources = db.query(Datasource).all()
        ds_map = {ds.id: ds for ds in datasources}
        
        # Get all dashboards
        dashboards = db.query(Dashboard).all()
        
        updated_count = 0
        error_count = 0
        skipped_count = 0
        
        print(f"Found {len(dashboards)} dashboards to check...")
        
        for dashboard in dashboards:
            try:
                definition = json.loads(dashboard.definition_json or "{}")
                widgets_obj = definition.get("widgets", {})
                changed = False
                
                print(f"\nDashboard: {dashboard.name} ({len(widgets_obj)} widgets)")
                
                for widget_id, widget in widgets_obj.items():
                    if not isinstance(widget, dict):
                        continue
                    
                    widget_id = widget.get("id", "unknown")
                    widget_type = widget.get("type", "unknown")
                    
                    query_spec = widget.get("querySpec")
                    if not query_spec or not isinstance(query_spec, dict):
                        print(f"  Widget {widget_id} ({widget_type}): No querySpec")
                        continue
                    
                    source = query_spec.get("source")
                    ds_id = widget.get("datasourceId")
                    
                    if not source or not ds_id:
                        print(f"  Widget {widget_id} ({widget_type}): Missing source or datasourceId")
                        continue
                    
                    # Check if already has sourceTableId
                    has_table_id = query_spec.get("sourceTableId")
                    print(f"  Widget {widget_id} ({widget_type}): source={source}, hasTableId={bool(has_table_id)}")
                    
                    if has_table_id:
                        skipped_count += 1
                        continue
                    
                    # Get datasource
                    ds = ds_map.get(ds_id)
                    if not ds:
                        continue
                    
                    # Check if source table exists
                    existing_tables = get_existing_tables(ds_id)
                    
                    if source not in existing_tables:
                        # Table doesn't exist - try to find current name
                        print(f"Widget {widget.get('id')} references non-existent table: {source}")
                        
                        current_name = find_current_table_name(ds, source)
                        
                        if current_name and current_name in existing_tables:
                            print(f"  → Found current name: {current_name}")
                            query_spec["source"] = current_name
                            query_spec["sourceTableId"] = f"{ds_id}__{source}"
                            changed = True
                            updated_count += 1
                        else:
                            print(f"  ✗ Could not find current table name")
                            error_count += 1
                    else:
                        # Table exists - just add sourceTableId
                        query_spec["sourceTableId"] = f"{ds_id}__{source}"
                        changed = True
                        updated_count += 1
                
                if changed:
                    definition["widgets"] = widgets_obj
                    dashboard.definition_json = json.dumps(definition)
                    print(f"✓ Updated dashboard: {dashboard.name} ({dashboard.id})")
            
            except Exception as e:
                print(f"Error processing dashboard {dashboard.id}: {e}")
                error_count += 1
                continue
        
        # Commit all changes
        db.commit()
        print(f"\n✓ Migration complete!")
        print(f"  Updated: {updated_count} widgets")
        print(f"  Skipped (already has tableId): {skipped_count} widgets")
        print(f"  Errors: {error_count} widgets")
        
    except Exception as e:
        db.rollback()
        print(f"Migration failed: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    print("Starting widget table name migration...")
    migrate_widgets()
