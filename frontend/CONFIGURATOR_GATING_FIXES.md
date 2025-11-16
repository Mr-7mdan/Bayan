# Configurator Auto-Expansion Gating Fixes

## Problem
The configurator panel was auto-expanding when users interacted with:
1. Widget actions menu (gear icon) - specifically when opening Select dropdowns inside the menu
2. Widget filterbars (per-widget filter controls)
3. Global filters bar (top of canvas)

The configurator should only expand when explicitly triggered (e.g., clicking on a widget), not during menu/filterbar interactions.

## Root Cause
The original gating mechanism (`document.body.dataset.actionsMenuOpen`) was only tracking the top-level menu open state, not internal popovers like:
- Tremor Select dropdowns inside the actions menu
- Radix UI popover components
- Filterbar dropdown menus

When users opened these internal popovers, the gating flag was not set, so the configurator would expand on hover.

## Solution

### 1. Unified Gating Counter (`window.__actionsMenuState`)
Created a global state object on `window` that tracks all active menus/popovers with a reference counter:
```typescript
window.__actionsMenuState = {
  count: 0,        // Number of active menus/popovers
  timeoutId: null  // Cooldown timer after last menu closes
}
```

### 2. Component-Level Gating
Updated these components to increment/decrement the counter:
- **WidgetActionsMenu** (gear icon menu)
- **WidgetKebabMenu** (3-dots menu)
- **FilterbarShell** (widget-level filterbars)
- **FilterbarControl** (global filters bar)

Each component:
- Increments `count` when opening
- Decrements `count` when closing
- Sets `document.body.dataset.actionsMenuOpen = '1'` while count > 0
- Clears the flag after a 300ms cooldown when count reaches 0

### 3. Global Popover Detector (MutationObserver)
Added a DOM mutation observer in `page.tsx` that watches for any popover elements:
```typescript
const popoverSelectors = [
  '[data-radix-popper-content-wrapper]',
  '[data-radix-portal]',
  '[role="listbox"]',
  '[role="menu"]',
  '[role="dialog"]',
  '.tremor-Select-popover',
  '.filterbar-popover',
]
```

When these elements appear/disappear in the DOM, the counter is adjusted automatically.

This catches:
- Tremor Select dropdowns inside the actions menu
- Radix UI Select/Popover components
- Any other popover-like elements

### 4. Configurator Hover Gating
The configurator panel's `onMouseEnter` handler checks the gate:
```typescript
onMouseEnter={() => {
  if (document.body?.dataset?.actionsMenuOpen === '1') return  // GATED
  if (dragging) return                                          // GATED
  if (gridInteracting) return                                   // GATED
  // ... expand configurator
}}
```

## Debug Logging
Added console.debug statements to trace gating behavior:
- `[WidgetActionsMenu] Gate ON/OFF`
- `[WidgetKebabMenu] Gate ON/OFF`
- `[FilterbarShell] Gate ON/OFF`
- `[FilterbarControl] Gate ON/OFF`
- `[PopoverDetector] Gate ON/OFF (popover added/removed)`

Check browser console to verify gating is working during interactions.

## Deprecation Warning Fix
The `util._extend` deprecation warning comes from old Node.js dependencies (not our code). Suppressed by adding `NODE_OPTIONS='--no-deprecation'` to the dev script in `package.json`.

## Testing Checklist
- [ ] Click gear icon → open actions menu → hover configurator → should NOT expand
- [ ] Inside actions menu, open a Select dropdown → hover configurator → should NOT expand
- [ ] Click filterbar (widget-level) → open popover → hover configurator → should NOT expand
- [ ] Click global filters bar → open preset dropdown → hover configurator → should NOT expand
- [ ] Click 3-dots kebab menu → hover configurator → should NOT expand
- [ ] Drag/resize widgets → configurator should NOT expand
- [ ] Click on a widget card body → configurator SHOULD appear (not gated)

## Files Modified
1. `/Users/mohammed/Documents/Bayan/frontend/src/app/page.tsx` - Added MutationObserver
2. `/Users/mohammed/Documents/Bayan/frontend/src/components/widgets/WidgetActionsMenu.tsx` - Unified gating
3. `/Users/mohammed/Documents/Bayan/frontend/src/components/widgets/WidgetKebabMenu.tsx` - Unified gating
4. `/Users/mohammed/Documents/Bayan/frontend/src/components/shared/FilterbarControl.tsx` - Unified gating (both Shell and Control)
5. `/Users/mohammed/Documents/Bayan/frontend/package.json` - Deprecation warning suppression
