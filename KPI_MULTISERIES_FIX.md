# KPI Multi-Series Fix (No Delta Mode)

## Issue
When adding multiple data series (e.g., Amount_JOD and Amount_USD) to a KPI card **without delta/comparison mode enabled**, the card displayed a dash "-" instead of showing both values.

## Root Cause
The KPI card's multi-series rendering only worked when delta mode was ON:
1. `useKpiData` hook only fetches data when `activeDeltaMode` is set
2. When delta is OFF, it falls back to a single `baseValue` number
3. Multi-series rendering checked for `kpi.data?.bySeries`, which is `undefined` when delta is OFF

## Solution
Added `baseBySeries` state to fetch multi-series values independently when delta mode is disabled.

## Changes Made

### `/frontend/src/components/widgets/KpiCard.tsx`

1. **Added `baseBySeries` state** (lines 623-666):
   - Fetches each series separately using `QueryApi.querySpec`
   - Runs only when delta is OFF and multiple series are configured
   - Stores results as `Record<string, number>` mapping series labels to totals

2. **Updated multi-series rendering** (lines 1579-1584):
   - Changed condition to check both `kpi.data?.bySeries` (delta ON) and `baseBySeries` (delta OFF)
   - Maps `baseBySeries` to the expected format with current/previous/delta structure

3. **Updated data ready check** (line 570):
   - Includes `baseBySeries` and `baseByLegend` in the data availability check
   - Ensures snapshot/export waits for all data to load

4. **Added debug logging**:
   - Logs when `baseBySeries` is loaded successfully
   - Logs errors during fetch

## Testing
After deploying these changes:
1. Create a KPI card with multiple series (e.g., Amount_JOD and Amount_USD)
2. Keep delta mode OFF (no year-over-year comparison)
3. Both values should now display correctly, each with its own label

## Example
**Before:** KPI shows "-"
**After:** KPI shows:
```
Amount_JOD
132.2M

Amount_USD
58.8M
```
