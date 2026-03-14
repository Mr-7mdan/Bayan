"""
1. Print full options_json for the DuckDB datasource [9eb8f9f7] (joins config).
2. Search dashboard JSONs for the New Markup / PriceGateway computed column.
"""
import json, sqlite3, re

META_DB_PATH = "/Users/mohammed/Documents/Bayan/backend/.data/meta.sqlite"
meta = sqlite3.connect(META_DB_PATH)
meta.row_factory = sqlite3.Row

# ── 1. Full config for the DuckDB (New) datasource ───────────────────────────
print("=" * 90)
print("DuckDB datasource [9eb8f9f7] full options_json")
print("=" * 90)
ds = meta.execute(
    "SELECT id, name, options_json FROM datasources WHERE id = '9eb8f9f7-1dbe-40f8-bcf8-2a0bcf96f5f8' OR id LIKE '9eb8f9f7%'"
).fetchone()
if ds:
    opts = json.loads(ds["options_json"] or "{}")
    print(json.dumps(opts, indent=2))
else:
    # Try by name
    ds = meta.execute("SELECT id, name, options_json FROM datasources WHERE type='duckdb'").fetchone()
    if ds:
        opts = json.loads(ds["options_json"] or "{}")
        print(f"id={ds['id']}  name={ds['name']}")
        print(json.dumps(opts, indent=2))
    else:
        print("  Not found")

# ── 2. Search all dashboard JSONs broadly ─────────────────────────────────────
print()
print("=" * 90)
print("All dashboards — searching for 'Markup', 'Price', 'customCol', 'expr' ...")
print("=" * 90)
dashboards = meta.execute("SELECT id, name, definition_json FROM dashboards ORDER BY name").fetchall()
for db in dashboards:
    defn = db["definition_json"] or ""
    hits = []
    for keyword in ["Markup", "PriceGateway", "customCol", "ContractSize", "RateProfit"]:
        if keyword.lower() in defn.lower():
            hits.append(keyword)
    if hits:
        print(f"\n  Dashboard: [{db['id'][:8]}] {db['name']}")
        print(f"  Keywords found: {hits}")
        # Extract surrounding context for each keyword
        for kw in hits[:2]:  # first 2 only
            idx = defn.lower().find(kw.lower())
            snippet = defn[max(0, idx-100):idx+300]
            print(f"  ... context for '{kw}':")
            print(f"  {snippet}")
            print()
    else:
        print(f"  [{db['id'][:8]}] {db['name']}  — no markup/price keywords")

meta.close()
