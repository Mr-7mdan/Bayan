# Custom Columns Import/Export Debugging Guide

## Issue
After importing dashboard "Cash GeoFlow" and mapping datasources/tables, custom column `ToClientCode` is not being resolved, causing:
```
_duckdb.BinderException: Binder Error: Referenced column "ToClientCode" not found in FROM clause!
```

## What Should Happen
1. **Export**: Dashboard export includes datasource with custom columns in `options.transforms.customColumns`
2. **Import**: Frontend merges custom columns from exported datasource into mapped target datasource
3. **Query**: Backend applies scope filtering and includes relevant custom columns in SQL generation

## Export Structure (Cash_GeoFlow-export-20251126-1700.json)

The export DOES include custom columns:
- Line 650-657: `ToClientCode` with `scope.level: "table"` and `scope.table: "main.View_CIT_Invoice_Details_PriceList3"`
- Line 540-547: `FromClientCode` with `scope.level: "datasource"` and `scope.table: null`

## Debug Logging Added

### Backend (query.py)
Added detailed logging to show:
1. **Before scope filtering**: All custom columns in datasource with their scope
2. **During scope filtering**: Each custom column and whether it matches the source table
3. **After scope filtering**: Total custom columns included

Look for these log lines:
```
[Query] Applying scope filter for source: <table_name>
[Query] Total custom columns before scope filter: <count>
[Query]   - <column_name>: level=<level>, table=<table>
[Query] Custom column '<name>' scope table '<scope_table>' vs source '<source>': MATCH/NO MATCH
```

## Frontend Import Logic (page.tsx lines 293-409)

The import process:
1. Maps datasource IDs (old → new)
2. Creates new datasources if no mapping exists
3. **Merges transforms** for mapped datasources:
   - Fetches target datasource
   - Clones transforms from export
   - **Remaps scope.table** using tableNameMap (lines 314-321)
   - Merges custom columns, transforms, joins
   - Updates target datasource

Look for frontend console logs:
```
[Import] Merging transforms from <name> into target datasource <id>
[Import]   Remapping custom column "<name>" scope: <old_table> → <new_table>
[Import]   Total custom columns after merge: <count>
[Import]     - <name> (scope: <table>)
```

## Debugging Steps

### 1. Check Frontend Logs
After import, check browser console for:
- Did transforms get merged?
- Was `ToClientCode` remapped correctly?
- What was the table name mapping?

### 2. Check Backend Logs
When widgets fail to load, check server logs for:
- What custom columns exist in the datasource?
- What source table is being queried?
- Did `ToClientCode` match the source table during scope filtering?

### 3. Verify Datasource Options
Query the database or use API to check if the custom columns were actually saved:
```sql
SELECT id, name, options_json FROM datasources WHERE id = '<target_datasource_id>';
```

Check if `options_json` contains the `ToClientCode` custom column with correct scope.

## Common Issues

### Issue 1: Table Name Not Remapped
**Symptom**: `ToClientCode` has `scope.table: "main.View_CIT_Invoice_Details_PriceList3"` but query uses different schema/prefix
**Solution**: Check `tableNameMap` during import, ensure correct mapping was provided by user

### Issue 2: Custom Column Not Saved
**Symptom**: Backend logs show 0 custom columns
**Solution**: Check if frontend merge succeeded, check for errors in `updateDatasource` call

### Issue 3: Scope Table Doesn't Match
**Symptom**: Backend logs show "NO MATCH" for scope table comparison
**Root Cause**: Table name normalization might be failing, or schema prefix differs
**Debug**: Check `_matches_table` function - it compares last segment (object name) case-insensitively

## Next Steps

1. **Deploy** updated backend (version 1.2) with new debug logging
2. **Import** the dashboard again
3. **Collect logs** from both frontend console and backend stderr
4. **Share logs** to identify exactly where the process fails

## Files Modified

### Backend
- `/backend/app/routers/query.py`: Added debug logging to scope filtering in both pivot and regular query paths

### Frontend  
- Already has logging in `/frontend/src/app/(app)/dashboards/mine/page.tsx` (lines 298, 317, 397-402)

## Testing Checklist

- [ ] Export dashboard with custom columns
- [ ] Verify custom columns exist in exported JSON
- [ ] Import dashboard on different machine
- [ ] Map datasource to existing datasource
- [ ] Map table names (if schemas differ)
- [ ] Check frontend console for merge logs
- [ ] Open dashboard and trigger widget queries
- [ ] Check backend logs for scope filtering details
- [ ] Verify which custom columns were included/excluded
- [ ] If NO MATCH, compare exact table names in logs
