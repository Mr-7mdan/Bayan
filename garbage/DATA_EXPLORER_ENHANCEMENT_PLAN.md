# Data Explorer Enhancement Plan
*Design System: Data-Dense Dashboard | Fira Code + Fira Sans*

## 🎯 Objective
Integrate Advanced SQL transformation capabilities directly into the Data Explorer modal with improved UX, plus add sorting and column filtering to the preview panel.

---

## 📊 Design System Applied

**Style:** Data-Dense Dashboard
- Multiple widgets, data tables, KPI cards
- Minimal padding, grid layout
- Maximum data visibility
- Space-efficient

**Colors:**
- Primary: `#1E40AF` (Blue-700)
- Secondary: `#3B82F6` (Blue-500)
- CTA: `#F59E0B` (Amber-500)
- Background: `#F8FAFC` (Slate-50)
- Text: `#1E3A8A` (Blue-900)

**Typography:**
- Code/Data: `Fira Code` (monospace)
- UI Text: `Fira Sans` (sans-serif)

**Effects:**
- Hover tooltips
- Row highlighting on hover
- Smooth filter animations
- Data loading spinners

---

## 🏗️ Current Structure Analysis

### Data Explorer Modal (Current)
```
┌─────────────────────────────────────────────────────┐
│  Header: [DB Icon] Datasource Name                  │
├─────────────────┬───────────────────────────────────┤
│ Schema Tree     │  Preview Panel                    │
│ (272px fixed)   │  - Table header                   │
│                 │  - Data rows                      │
│ - Search        │  - Pagination (Prev/Next)         │
│ - Refresh       │                                   │
│ - Tree          │  ⚠️ No sorting                    │
│                 │  ⚠️ No column filtering           │
│                 │  ⚠️ No transformations            │
└─────────────────┴───────────────────────────────────┘
```

### Advanced SQL Modal (Current)
- Tabs: Custom Columns, Transforms, Joins, Sort, Preview
- JSON editor (technical, not user-friendly)
- Builders: Case, Replace, Computed, Unpivot, Null, Join
- Scope selection: Datasource, Table, Widget
- Preview with SQL output

---

## ✨ Enhanced Layout Proposal

### Three-Column Layout
```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Header: [DB Icon] Datasource Name · Data Explorer & Transformations       │
├─────────────┬───────────────────────────┬───────────────────────────────────┤
│ Schema      │  Transformation Panel     │  Preview Panel                    │
│ (240px)     │  (400px, collapsible)     │  (flexible)                       │
│             │                           │                                   │
│ • Search    │  🔧 Transformations       │  🎯 Selected: table_name          │
│ • Refresh   │                           │  ┌─────────────────────────────┐  │
│ • Tree      │  [+ Custom Column]        │  │ 🔍 Filter columns...        │  │
│             │  [+ Join Table]           │  └─────────────────────────────┘  │
│ ▸ Schema    │  [+ Transform]            │                                   │
│   ▸ Table1  │                           │  ┌─────┬─────────┬──────────┐    │
│     • col1  │  ─────────────────────    │  │ ↕ ID │ ↕ Name  │ ↕ Value  │    │
│     • col2  │  📊 Active Items:         │  ├─────┼─────────┼──────────┤    │
│   ▸ Table2  │                           │  │ 1   │ Alice   │ 100      │    │
│             │  • ClientType             │  │ 2   │ Bob     │ 200      │    │
│             │    Custom Column          │  └─────┴─────────┴──────────┘    │
│             │    [Edit] [Delete] [↑↓]   │                                   │
│             │                           │  [Prev 1-500] [Next 501-1000]     │
│             │  • Total                  │                                   │
│             │    Computed (SUM)         │  💡 Tip: Click ↕ to sort          │
│             │    [Edit] [Delete] [↑↓]   │                                   │
│             │                           │                                   │
│             │  ─────────────────────    │                                   │
│             │                           │                                   │
│             │  [< Collapse Panel]       │                                   │
└─────────────┴───────────────────────────┴───────────────────────────────────┘
```

---

## 🎨 Feature Details

### 1. Transformation Panel (New - Middle Column)

#### Header
```typescript
┌─────────────────────────────────────┐
│ 🔧 Transformations                  │
│ [+ Custom Column ▾] [+ Join ▾] [...│
└─────────────────────────────────────┘
```

**Add Buttons (Dropdown):**
- **Custom Column** → Opens inline form
- **Join Table** → Opens join builder
- **Transform** → Sub-menu:
  - Computed Expression
  - Case/When
  - Replace Values
  - Handle Nulls
  - Unpivot

#### Active Items List
```typescript
┌─────────────────────────────────────┐
│ 📊 Active Items (3)                 │
│                                     │
│ • ClientType                        │
│   Custom Column · Datasource scope  │
│   LEFT(CAST(login AS VARCHAR), 2)   │
│   [✏️ Edit] [🗑️ Delete] [↕️ Move]   │
│                                     │
│ • TotalRevenue                      │
│   Join · Table: invoices            │
│   SUM(amount) AS TotalRevenue       │
│   [✏️ Edit] [🗑️ Delete] [↕️ Move]   │
│                                     │
│ • Status                            │
│   Case/When · 3 conditions          │
│   [✏️ Edit] [🗑️ Delete] [↕️ Move]   │
└─────────────────────────────────────┘
```

**Each item shows:**
- Name (bold, primary color)
- Type badge (Custom Column, Join, Computed, etc.)
- Scope badge (Datasource, Table, Widget)
- Expression preview (truncated, monospace font)
- Actions: Edit, Delete, Reorder

#### Inline Builders
When clicking "+ Custom Column" or "Edit":

```typescript
┌─────────────────────────────────────┐
│ ✨ New Custom Column                │
│                                     │
│ Name                                │
│ [ClientType____________]            │
│                                     │
│ Expression                          │
│ [LEFT(CAST(login AS VARCHAR), 2)] │
│                                     │
│ Scope                               │
│ ○ Datasource                        │
│ ● Table: [mt5_deals ▾]             │
│ ○ Widget: [widget_abc ▾]           │
│                                     │
│ [Cancel] [Save]                     │
└─────────────────────────────────────┘
```

**Benefits over current Advanced SQL modal:**
- ✅ Immediate visual feedback (see item in list)
- ✅ No need to understand JSON structure
- ✅ Clear scope selection with labels
- ✅ Expression syntax highlighting
- ✅ Drag-to-reorder for execution order

#### Collapse/Expand
```typescript
[< Collapse Transformations]  // When expanded
[> Show Transformations (3)]  // When collapsed (shows count badge)
```

---

### 2. Preview Panel Enhancements

#### A. Column Filtering (Search)
```typescript
┌─────────────────────────────────────┐
│ 🔍 Filter columns...                │
└─────────────────────────────────────┘
```

**Behavior:**
- Filters visible columns in real-time
- Shows match count: "5 of 25 columns"
- Keeps filtered columns highlighted
- Case-insensitive search

#### B. Column Sorting (Click Headers)
```typescript
┌────────┬──────────┬───────────┐
│ ↕ ID   │ ↕ Name   │ ↕ Value   │  ← Default: no sort
├────────┼──────────┼───────────┤
│ ↓ ID   │   Name   │   Value   │  ← Ascending sort
├────────┼──────────┼───────────┤
│ ↑ ID   │   Name   │   Value   │  ← Descending sort
└────────┴──────────┴───────────┘
```

**Implementation:**
- Click header → ASC → DESC → No Sort (cycle)
- Visual indicator: `↕` (default), `↓` (asc), `↑` (desc)
- Sort icon always visible on hover
- Sorts current page client-side
- Option to sort full dataset server-side

#### C. Column Actions Menu (Right-Click)
```typescript
┌─────────────────────────┐
│ ✏️  Rename               │
│ 🔢  Set Data Type        │
│ 🧮  Create Computed Col  │
│ 👁️  Hide Column          │
│ 📋  Copy Column Name     │
└─────────────────────────┘
```

**Quick Actions:**
- Right-click column header → Context menu
- "Create Computed Col" → Opens builder with column pre-selected
- "Hide Column" → Adds to hidden columns list (reversible)

#### D. Enhanced Table UI
```typescript
<th className="group cursor-pointer select-none">
  <div className="flex items-center gap-1.5 justify-between">
    <span className="font-semibold">Column Name</span>
    <button className="opacity-0 group-hover:opacity-100 transition-opacity">
      {sortState === 'none' && <RiArrowUpDownLine />}
      {sortState === 'asc' && <RiArrowUpLine className="text-primary" />}
      {sortState === 'desc' && <RiArrowDownLine className="text-primary" />}
    </button>
  </div>
</th>
```

---

## 🔄 User Workflows

### Workflow 1: Create Custom Column
1. User selects table from schema tree
2. Preview loads data
3. User clicks **[+ Custom Column]**
4. Inline form appears in Transformation Panel
5. User enters name, expression, scope
6. Clicks **[Save]**
7. Column appears in Active Items list
8. Preview updates showing new column
9. User can immediately filter/sort it

### Workflow 2: Create Join
1. User has table selected
2. Clicks **[+ Join ▾]**
3. Join builder appears inline
4. Selects target table, keys, columns
5. Sets scope (datasource/table/widget)
6. Saves
7. Joined columns appear in preview
8. Can sort/filter joined columns

### Workflow 3: Sort & Filter Preview
1. User types in column filter: "amount"
2. Only columns matching "amount" are visible
3. User clicks `↕ Amount` header
4. Data sorts ascending (↓)
5. Click again → descending (↑)
6. Click again → no sort (↕)

---

## 🎯 Implementation Plan

### Phase 1: Preview Panel Enhancements (Quick Win)
**Files:** `DataExplorerDialog.tsx`

1. **Column Filter Search**
   - Add search input above table
   - Filter `cols` array by search term
   - Show match count

2. **Sortable Headers**
   - Add sort state: `{col: string, dir: 'asc'|'desc'|'none'}`
   - Sort `rows` client-side when header clicked
   - Add visual indicators (icons)

3. **Column Context Menu**
   - Add right-click handler
   - Show context menu with actions
   - Implement "Hide Column" and "Copy Name"

**Complexity:** Low | **Time:** 2-3 hours | **Impact:** High

---

### Phase 2: Transformation Panel Integration (Major)
**Files:** New `DataExplorerDialog.tsx` + Transform components

1. **Three-Column Layout**
   - Refactor to three columns
   - Make transformation panel collapsible
   - Add resize handles (optional)

2. **Transformation Panel UI**
   - Header with Add buttons
   - Active items list
   - Inline builders for each type

3. **Custom Column Builder**
   - Reuse `AdvancedSqlCustomColumnBuilder`
   - Adapt to inline context
   - Add scope selector

4. **Other Builders**
   - Join: `AdvancedSqlJoinBuilder`
   - Computed: `AdvancedSqlComputedBuilder`
   - Case: `AdvancedSqlCaseBuilder`
   - Replace: `AdvancedSqlReplaceBuilder`
   - Null: `AdvancedSqlNullBuilder`
   - Unpivot: `AdvancedSqlUnpivotBuilder`

5. **Active Items Management**
   - List all transforms
   - Edit/Delete/Reorder
   - Show scope badges
   - Expression preview

6. **Save & Apply**
   - Save to datasource transforms API
   - Refresh preview with transforms applied
   - Show success/error notifications

**Complexity:** High | **Time:** 8-12 hours | **Impact:** Very High

---

### Phase 3: Advanced Features (Optional)
1. **Server-Side Sorting**
   - Add `ORDER BY` to query
   - Full dataset sorting
   - Pagination with sort

2. **Column Data Type Editor**
   - Change column types
   - Add CAST transforms automatically

3. **Expression Autocomplete**
   - Column name suggestions
   - Function suggestions
   - Syntax highlighting

4. **Drag-to-Reorder**
   - Reorder transforms
   - Visual drag handles
   - Update execution order

---

## 📋 Technical Specifications

### State Management
```typescript
interface DataExplorerState {
  // Existing
  schema: IntrospectResponse | null
  loading: boolean
  sel: Sel | null
  
  // New
  transforms: DatasourceTransforms
  transformPanelOpen: boolean
  editingTransform: {type: string, index: number} | null
  
  // Preview enhancements
  columnFilter: string
  sortCol: string | null
  sortDir: 'asc' | 'desc' | 'none'
  hiddenColumns: Set<string>
}
```

### API Calls
```typescript
// Existing
Api.introspect(dsId) → schema

// New
Api.getDatasourceTransforms(dsId) → transforms
Api.saveDatasourceTransforms(dsId, payload) → success

// Enhanced preview
Api.query({
  sql: buildSqlWithTransforms(sel.table, transforms),
  datasourceId: dsId,
  limit: 500,
  offset: 0,
  orderBy: sortCol,
  sortDir: sortDir
})
```

### Component Structure
```
DataExplorerDialog
├── Header
├── SchemaTree (existing)
├── TransformationPanel (new)
│   ├── AddButtons
│   ├── ActiveItemsList
│   └── InlineBuilders
│       ├── CustomColumnBuilder
│       ├── JoinBuilder
│       ├── ComputedBuilder
│       ├── CaseBuilder
│       └── ...
└── PreviewPanel (enhanced)
    ├── ColumnFilter
    ├── SortableTable
    └── Pagination
```

---

## 🎨 Visual Design Mockup

### Color Scheme Application
```css
/* Primary Actions */
.btn-add-transform {
  background: #1E40AF;  /* Primary */
  color: white;
  &:hover { background: #1E3A8A; }
}

/* CTA (Save, Apply) */
.btn-save {
  background: #F59E0B;  /* Amber CTA */
  color: white;
}

/* Item Cards */
.transform-item {
  background: #F8FAFC;  /* Light bg */
  border-left: 3px solid #3B82F6;  /* Secondary */
  
  &:hover {
    background: #EFF6FF;  /* Blue-50 */
  }
}

/* Scope Badges */
.badge-datasource { background: #DBEAFE; color: #1E40AF; }
.badge-table     { background: #FEF3C7; color: #92400E; }
.badge-widget    { background: #DCFCE7; color: #166534; }
```

### Typography
```css
/* Headers */
.transform-panel-title {
  font-family: 'Fira Sans', sans-serif;
  font-weight: 600;
  font-size: 14px;
}

/* Expressions & Code */
.expression-preview {
  font-family: 'Fira Code', monospace;
  font-size: 11px;
  background: #F1F5F9;
  padding: 4px 8px;
  border-radius: 4px;
}

/* Data Table */
.preview-table {
  font-family: 'Fira Code', monospace;
  font-size: 11px;
}
```

---

## ✅ Benefits Over Current Advanced SQL Modal

| Aspect | Current (Advanced SQL) | Enhanced (Data Explorer) |
|--------|------------------------|--------------------------|
| **Visibility** | Hidden in separate modal | Always visible alongside data |
| **Context** | No data preview | Live preview with transforms applied |
| **Discoverability** | Technical JSON editor | Visual list of transforms |
| **Learning Curve** | High (JSON structure) | Low (form-based) |
| **Feedback** | Preview tab only | Immediate in data grid |
| **Workflow** | Modal → Edit → Save → Close | Inline → Edit → See Results |
| **Column Selection** | Manual typing | Click column in preview |
| **Scope Clarity** | JSON field | Visual badges + selectors |

---

## 🚀 Next Steps

1. **Approval** - Review and approve this plan
2. **Phase 1 Implementation** - Start with preview enhancements
3. **User Testing** - Validate sorting/filtering UX
4. **Phase 2 Implementation** - Build transformation panel
5. **Integration Testing** - Ensure transforms save/load correctly
6. **Documentation** - Update user guide with new features

---

## 📝 Notes

- Keep existing Advanced SQL modal for power users who prefer JSON
- Add "Open in Advanced SQL" link in Data Explorer for JSON editing
- Ensure mobile responsiveness (collapse transformation panel on small screens)
- Add keyboard shortcuts: `Cmd+K` for column filter, `Cmd+T` for add transform
- Consider adding "Quick Transforms" templates (e.g., "Remove Nulls", "Add Running Total")

