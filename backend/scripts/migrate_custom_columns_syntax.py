#!/usr/bin/env python3
"""
Migration script to normalize custom column expressions in existing datasources.
Converts SQL Server bracket syntax to dialect-appropriate syntax.
"""

import sys
import os
import json
import sqlite3
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.sql_dialect_normalizer import auto_normalize


def migrate_datasource_custom_columns(db_path: str, dry_run: bool = True):
    """Migrate custom column expressions in all datasources."""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all datasources with options_json
    cursor.execute("SELECT id, name, type, options_json FROM datasources WHERE options_json IS NOT NULL")
    datasources = cursor.fetchall()
    
    print(f"Found {len(datasources)} datasources with options")
    
    updated_count = 0
    
    for ds_id, ds_name, ds_type, options_json in datasources:
        try:
            opts = json.loads(options_json)
            transforms = opts.get('transforms', {})
            custom_columns = transforms.get('customColumns', [])
            
            if not custom_columns:
                continue
            
            print(f"\n📦 Datasource: {ds_name} ({ds_id})")
            print(f"   Type: {ds_type}")
            print(f"   Custom columns: {len(custom_columns)}")
            
            # Determine target dialect
            target_dialect = (ds_type or 'duckdb').lower()
            
            modified = False
            for cc in custom_columns:
                name = cc.get('name', '')
                expr = cc.get('expr', '')
                
                if not expr:
                    continue
                
                # Normalize the expression
                normalized = auto_normalize(expr, target_dialect)
                
                if normalized != expr:
                    print(f"\n   ✏️  {name}:")
                    print(f"      Before: {expr[:100]}{'...' if len(expr) > 100 else ''}")
                    print(f"      After:  {normalized[:100]}{'...' if len(normalized) > 100 else ''}")
                    cc['expr'] = normalized
                    modified = True
            
            if modified:
                # Update the datasource
                new_options_json = json.dumps(opts)
                
                if not dry_run:
                    cursor.execute(
                        "UPDATE datasources SET options_json = ? WHERE id = ?",
                        (new_options_json, ds_id)
                    )
                    print(f"   ✅ Updated in database")
                else:
                    print(f"   🔍 Would update (dry run)")
                
                updated_count += 1
        
        except Exception as e:
            print(f"   ❌ Error processing {ds_id}: {e}")
            continue
    
    if not dry_run:
        conn.commit()
        print(f"\n✅ Migration complete! Updated {updated_count} datasources.")
    else:
        print(f"\n🔍 Dry run complete! Would update {updated_count} datasources.")
        print("   Run with --apply to apply changes.")
    
    conn.close()


if __name__ == "__main__":
    # Default to metadata database
    db_path = os.path.join(os.path.dirname(__file__), '../.data/meta.sqlite')
    
    # Check for --apply flag
    dry_run = '--apply' not in sys.argv
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        sys.exit(1)
    
    print(f"🔧 Migrating custom column expressions")
    print(f"📁 Database: {db_path}")
    print(f"🔍 Mode: {'DRY RUN' if dry_run else 'APPLY CHANGES'}\n")
    
    migrate_datasource_custom_columns(db_path, dry_run=dry_run)
