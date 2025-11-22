export type TableOptions = {
  tableType?: 'data' | 'pivot'
  // Pivot renderer: 'legacy' uses react-pivottable, 'matrix' uses our custom PivotMatrixView
  pivotRenderer?: 'legacy' | 'matrix'
  // react-pivottable integration
  pivotUI?: boolean // show interactive controls (default: true)
  pivotConfig?: {
    rows?: string[]
    cols?: string[]
    vals?: string[]
    aggregatorName?: string
    rendererName?: string
    rowTotals?: boolean
    colTotals?: boolean
    labels?: Record<string, string>
    filters?: Record<string, string[]>
  }
  // Pivot table styling options (apply when tableType='pivot')
  pivotStyle?: {
    // Row heights in pixels
    headerRowHeight?: number
    cellRowHeight?: number
    // Font sizes in pixels
    headerFontSize?: number
    cellFontSize?: number
    // Typography
    headerFontWeight?: 'normal' | 'medium' | 'semibold' | 'bold'
    headerFontStyle?: 'normal' | 'italic'
    cellFontWeight?: 'normal' | 'medium' | 'semibold' | 'bold'
    cellFontStyle?: 'normal' | 'italic'
    // Persistence of manual column widths
    rowHeaderWidths?: number[] // widths for each row header column (left side)
    valueColWidths?: number[] // widths for each value column (grid cells)
    // UI tweaks
    hideColAxisLabel?: boolean // hide the column that contains the column-axis labels in the header
    // Row zebra and hover effects (for PivotMatrixView)
    alternateRows?: boolean
    rowHover?: boolean
    // Emphasize leaf rows with slightly stronger background
    leafRowEmphasis?: boolean
    // Tint row header background by depth level
    rowHeaderDepthHue?: boolean
    // Tint column header background by depth level
    colHeaderDepthHue?: boolean
    // CSS border-collapse toggle (true = collapse, false = separate)
    collapseBorders?: boolean
    // Optional subtotal row per parent group with separator
    showSubtotals?: boolean
    // Expand/Collapse icon style for tree toggles
    expandIconStyle?:
      | 'plusMinusLine'
      | 'plusMinusFill'
      | 'arrowLine'
      | 'arrowFill'
      | 'arrowWide'
      | 'arrowDrop'
    // Value formatting for PivotMatrixView cells and totals
    valueFormat?:
      | 'none'
      | 'short'
      | 'abbrev'
      | 'currency'
      | 'percent'
      | 'bytes'
      | 'wholeNumber'
      | 'number'
      | 'thousands'
      | 'millions'
      | 'billions'
      | 'oneDecimal'
      | 'twoDecimals'
      | 'percentWhole'
      | 'percentOneDecimal'
    valuePrefix?: string
    valueSuffix?: string
  }
  // Pivot data fetch controls (client-side fetch for react-pivottable)
  // pivotChunkSize: per-request page size (500–5000), default 2000
  // pivotMaxRows: overall cap on total rows fetched, default 20000
  pivotChunkSize?: number
  pivotMaxRows?: number
  // Server-side pivot toggle (uses /query/pivot instead of client aggregation)
  serverPivot?: boolean
  // Show Expand All / Collapse All / Download Excel controls for pivot matrix
  showControls?: boolean
  theme?: 'quartz' | 'balham' | 'material' | 'alpine'
  density?: 'comfortable' | 'compact'
  rowHeight?: number
  headerHeight?: number
  defaultCol?: {
    sortable?: boolean
    filter?: boolean
    resizable?: boolean
    floatingFilter?: boolean
  }
  pivot?: {
    mode?: 'auto' | 'on' | 'off'
    sideBar?: boolean
    pivotPanelShow?: 'never' | 'onlyWhenPivoting' | 'always'
    clientSimulate?: boolean
  }
  aggregation?: {
    defaultAgg?: 'sum' | 'avg' | 'min' | 'max' | 'count' | 'distinct'
    groupTotalRow?: 'top' | 'bottom' | 'off'
    grandTotalRow?: 'top' | 'bottom' | 'pinnedTop' | 'pinnedBottom' | 'off'
    omitAggNameInHeader?: boolean
  }
  filtering?: { quickFilter?: boolean }
  selection?: { mode?: 'none' | 'single' | 'multiple'; checkbox?: boolean }
  interactions?: {
    columnMove?: boolean
    columnResize?: boolean
    columnHoverHighlight?: boolean
    suppressRowHoverHighlight?: boolean
  }
  columns?: Record<string, {
    headerName?: string
    hide?: boolean
    pinned?: 'left' | 'right'
    width?: number
    minWidth?: number
    maxWidth?: number
    type?: 'numericColumn' | 'rightAligned'
    aggFunc?: 'sum' | 'avg' | 'min' | 'max' | 'count' | 'distinct'
    valueFormatter?: 'none' | 'short' | 'currency' | 'percent' | 'bytes'
  }>
  export?: { csv?: boolean; excel?: boolean; fileName?: string; onlySelected?: boolean }
  state?: { columnState?: any }
  performance?: { domLayout?: 'autoHeight' | 'normal'; rowBuffer?: number }
  // Auto-fit columns for Data Table
  autoFit?: {
    mode?: 'content' | 'window'
    sampleRows?: number
  }
}

export type CompositionComponent =
  | { id?: string; kind: 'title'; text?: string; align?: 'left'|'center'|'right'; span?: number }
  | { id?: string; kind: 'subtitle'; text?: string; align?: 'left'|'center'|'right'; span?: number }
  | { id?: string; kind: 'kpi'; span?: number; refId?: string; label?: string; props?: { preset?: 'basic'|'badge'|'withPrevious'|'donut'|'spark'|'progress'|'categoryBar'|'multiProgress' } }
  | { id?: string; kind: 'chart'; span?: number; refId?: string; label?: string; props?: { type?: 'line'|'bar'|'area'|'column'|'donut'|'categoryBar'|'spark'|'combo'|'badges'|'progress'|'tracker'|'scatter'|'tremorTable'|'heatmap'|'barList'|'gantt'|'sankey' } }
  | { id?: string; kind: 'table'; span?: number; refId?: string; label?: string; props?: { tableType?: 'data'|'pivot' } }

export type WidgetConfig = {
  id: string
  type: 'kpi' | 'chart' | 'table' | 'text' | 'spacer' | 'composition'
  title: string
  sql: string
  queryMode?: 'sql' | 'spec'
  querySpec?: {
    source: string
    sourceTableId?: string  // Stable table ID for rename resilience
    select?: string[]
    where?: Record<string, unknown>
    limit?: number
    offset?: number
    // Aggregated spec semantics
    x?: string
    y?: string
    agg?: 'none' | 'count' | 'distinct' | 'avg' | 'sum' | 'min' | 'max'
    groupBy?: 'none' | 'hour' | 'day' | 'week' | 'month' | 'quarter' | 'year'
    measure?: string
    series?: Array<{
      id?: string
      name?: string
      x: string
      y: string
      agg?: 'none' | 'count' | 'distinct' | 'avg' | 'sum' | 'min' | 'max'
      label?: string
      colorToken?: 1 | 2 | 3 | 4 | 5
      colorKey?: 'blue' | 'emerald' | 'violet' | 'amber' | 'gray' | 'rose' | 'indigo' | 'cyan' | 'pink' | 'lime' | 'fuchsia'
      stackId?: string
      style?: 'solid' | 'gradient'
      conditionalRules?: Array<{
        when: '>' | '>=' | '<' | '<=' | 'between' | 'equals'
        value: number | [number, number]
      }>
    }>
  }
  chartType?: 'line' | 'bar' | 'area' | 'column' | 'donut' | 'categoryBar' | 'spark' | 'combo' | 'badges' | 'progress' | 'tracker' | 'scatter' | 'tremorTable' | 'heatmap' | 'barList' | 'gantt' | 'sankey'
  datasourceId?: string
  options?: {
    // Card-level options
    showCardHeader?: boolean
    cardFill?: 'default' | 'transparent' | 'custom'
    cardCustomColor?: string
    autoFitCardContent?: boolean
    showLegend?: boolean
    // Label casing controls
    legendLabelCase?: 'lowercase' | 'capitalize' | 'uppercase' | 'capitalcase' | 'proper'
    xLabelCase?: 'lowercase' | 'capitalize' | 'uppercase' | 'capitalcase' | 'proper'
    // Category label casing (applies to the category part of virtual labels);
    // categoryLabelCaseMap allows per-category overrides by original category key
    categoryLabelCase?: 'lowercase' | 'capitalize' | 'uppercase' | 'capitalcase' | 'proper'
    categoryLabelCaseMap?: Record<string, 'lowercase' | 'capitalize' | 'uppercase' | 'capitalcase' | 'proper'>
    yAxisFormat?:
      | 'none'
      | 'short'
      | 'abbrev'
      | 'currency'
      | 'percent'
      | 'bytes'
      | 'wholeNumber'
      | 'number'
      | 'thousands'
      | 'millions'
      | 'billions'
      | 'oneDecimal'
      | 'twoDecimals'
      | 'percentWhole'
      | 'percentOneDecimal'
      | 'timeHours'
      | 'timeMinutes'
      | 'distance-km'
      | 'distance-mi'
    color?: 'blue' | 'violet' | 'indigo' | 'emerald' | 'teal' | 'amber' | 'rose'
    colorToken?: 1 | 2 | 3 | 4 | 5
    // Coloring strategy: default palette cycling vs value-based gradient using a base color
    colorMode?: 'palette' | 'valueGradient'
    colorBaseKey?: 'blue' | 'emerald' | 'violet' | 'amber' | 'gray' | 'rose' | 'indigo' | 'cyan' | 'pink' | 'lime' | 'fuchsia'
    yMin?: number
    yMax?: number
    // Value formatting enhancements
    valueFormatLocale?: string
    valueCurrency?: string
    valuePrefix?: string
    valueSuffix?: string
    // Charts enhancements
    advancedMode?: boolean
    areaType?: 'default' | 'stacked' | 'percent'
    donutVariant?: 'donut' | 'pie' | 'sunburst' | 'nightingale'
    autoCondenseXLabels?: boolean
    xDenseThreshold?: number
    // Visual styling knobs (to be gradually implemented)
    barRounded?: boolean
    barGradient?: boolean
    barMode?: 'default' | 'grouped' | 'stacked'
    barGap?: number
    conditionalRules?: Array<{
      when: '>' | '>=' | '<' | '<=' | 'between' | 'equals'
      value: number | [number, number]
      color?: 'blue' | 'emerald' | 'violet' | 'amber' | 'gray' | 'rose' | 'indigo' | 'cyan' | 'pink' | 'lime' | 'fuchsia'
    }>
    legendPosition?: 'top' | 'bottom' | 'none'
    maxLegendItems?: number
    legendDotShape?: 'circle' | 'square' | 'rect'
    legendMode?: 'flat' | 'nested'
    // Show small badges in header for Sort / Top N (including datasource defaults)
    showDataDefaultsBadges?: boolean
    // Tooltip options
    tooltipHideZeros?: boolean
    colorPreset?: 'default' | 'muted' | 'vibrant' | 'corporate'
    richTooltip?: boolean
    tooltipShowPercent?: boolean
    tooltipShowDelta?: boolean
    downIsGood?: boolean
    // Tracker density control
    trackerMaxPills?: number
    // Tabs from a filter field
    tabsField?: string
    tabsVariant?: 'line' | 'solid'
    tabsMaxItems?: number
    tabsStretch?: boolean
    tabsShowAll?: boolean
    tabsSort?: { by?: 'x' | 'value'; direction?: 'asc' | 'desc' }
    // Tab label casing: follow legend (default) or override just for tabs
    tabsLabelCase?: 'legend' | 'lowercase' | 'capitalize' | 'uppercase' | 'capitalcase' | 'proper'
    // Tremor Table (as a chart type)
    tremorTable?: {
      alternatingRows?: boolean
      badgeColumns?: string[]
      progressColumns?: string[]
      showTotalRow?: boolean
      // Per-column value formatting overrides
      formatByColumn?: Record<string,
        | 'none'
        | 'short'
        | 'abbrev'
        | 'currency'
        | 'percent'
        | 'bytes'
        | 'wholeNumber'
        | 'number'
        | 'thousands'
        | 'millions'
        | 'billions'
        | 'oneDecimal'
        | 'twoDecimals'
        | 'percentWhole'
        | 'percentOneDecimal'
        | 'timeHours'
        | 'timeMinutes'
        | 'distance-km'
        | 'distance-mi'
      >
      // Optional row click action (builder mode emits a DOM event)
      rowClick?: {
        type?: 'emit'
        eventName?: string // default: 'tremor-table-click'
      }
    }
    // Gantt chart options
    gantt?: {
      categoryField?: string
      startField?: string
      endField?: string
      durationField?: string
      durationUnit?: 'seconds' | 'minutes' | 'hours' | 'days' | 'weeks' | 'months'
      mode?: 'startEnd' | 'startDuration' // Determines which fields to use
      colorField?: string
      barHeight?: number
    }
    // Tremor Bar List (new chart type)
    barList?: {
      topN?: number
    }
    // Heatmap presets
    heatmap?: {
      // Compact preview mode used in configurator tiles
      preview?: boolean
      preset?: 'calendarMonthly' | 'weekdayHour' | 'calendarAnnual'
      calendarMonthly?: { dateField?: string; valueField?: string; month?: string }
      calendarAnnual?: { dateField?: string; valueField?: string; year?: string }
      weekdayHour?: { timeField?: string; valueField?: string; agg?: 'sum' | 'count' | 'avg' }
      // Display
      valueFormat?:
        | 'none'
        | 'short'
        | 'currency'
        | 'percent'
        | 'bytes'
        | 'oneDecimal'
        | 'twoDecimals'
      visualMap?: {
        orient?: 'horizontal' | 'vertical'
        position?: 'top' | 'bottom' | 'left' | 'right'
      }
    }
    // Badges-specific presets
    badgesPreset?: 'badge1' | 'badge2' | 'badge3' | 'badge4' | 'badge5'
    // Badges fine-tune toggles
    badgesShowCategoryLabel?: boolean
    badgesLabelInside?: boolean
    badgesShowValue?: boolean
    badgesShowDelta?: boolean
    badgesShowDeltaPct?: boolean
    badgesShowPercentOfTotal?: boolean
    // Filters UI (show pivot filters as filterbars above the chart)
    filtersUI?: 'off' | 'filterbars'
    // Per-field override to expose/hide a specific filterbar regardless of filtersUI
    filtersExpose?: Record<string, boolean>
    // Deltas configuration
    deltaUI?: 'none' | 'filterbar' | 'preconfigured'
    deltaMode?: 'off' | 'TD_YSTD' | 'TW_LW' | 'MONTH_LMONTH' | 'MTD_LMTD' | 'TY_LY' | 'YTD_LYTD' | 'TQ_LQ' | 'Q_TY_VS_Q_LY' | 'QTD_TY_VS_QTD_LY' | 'M_TY_VS_M_LY' | 'MTD_TY_VS_MTD_LY'
    deltaDateField?: string
    deltaWeekStart?: 'sat' | 'sun' | 'mon'
    seriesStackMap?: Record<string, string>
    // Data labels (advanced mode)
    dataLabelsShow?: boolean
    // New semantic positions; legacy values retained for compat
    dataLabelPosition?: 'center' | 'insideEnd' | 'insideBase' | 'outsideEnd' | 'callout' | 'inside' | 'outside' | 'top' | 'right'
    // Line/Area appearance
    lineWidth?: number
    // Spark-specific controls
    sparkDownIsGood?: boolean
    sparkLabelMaxLines?: number
    // Axis controls (advanced mode)
    xTickAngle?: 0 | 30 | 45 | 60 | 90
    xTickCount?: number
    yTickCount?: number
    axisFontSize?: number
    // Per-axis font controls (applies to both Advanced and Tremor charts)
    xAxisFontWeight?: 'normal' | 'bold'
    xAxisFontSize?: number
    xAxisFontColor?: string
    yAxisFontWeight?: 'normal' | 'bold'
    yAxisFontSize?: number
    yAxisFontColor?: string
    xLabelFormat?: 'none' | 'short' | 'datetime'
    // Week grouping convention for groupBy=week
    xWeekStart?: 'mon' | 'sun'
    // Chart title placement/format (when not using the card header)
    chartTitlePosition?: 'none' | 'header' | 'above' | 'below'
    chartTitleAlign?: 'left' | 'center'
    chartTitleSize?: number
    chartTitleWeight?: 'normal' | 'medium' | 'semibold' | 'bold'
    chartTitleColor?: string
    // Table-specific options (AG Grid)
    table?: TableOptions
    // Per-widget data overrides for Sort/Top N (adv12)
    dataDefaults?: {
      // When true, render badges in the card header for these overrides
      showHeaderBadges?: boolean
      useDatasourceDefaults?: boolean
      sort?: { by?: 'x' | 'value'; direction?: 'asc' | 'desc' }
      topN?: { n?: number; by?: 'x' | 'value'; direction?: 'asc' | 'desc' }
    }
    // KPI-specific options
    kpi?: {
      preset?: 'basic' | 'badge' | 'withPrevious' | 'donut' | 'spark' | 'progress' | 'categoryBar' | 'multiProgress'
      topN?: number // default Top N for category-based presets
      downIsGood?: boolean // e.g., bounce rate
      target?: number // used by donut/progress; alternatively computed from total
      labelCase?: 'lowercase' | 'capitalize' | 'uppercase' | 'capitalcase' | 'proper'
      sparkType?: 'line' | 'area' | 'bar'
      // Aggregation mode for KPI tiles (affects category totals)
      aggregationMode?: 'none' | 'sum' | 'count' | 'distinctCount' | 'avg' | 'min' | 'max' | 'first' | 'last'
    }
    // Text card options
    text?: {
      html?: string
      labels?: Array<{ id?: string; text: string; style?: 'h1'|'h2'|'h3'|'p'|'small'|'metric'|'label'; align?: 'left'|'center'|'right'; color?: string }>
      imageUrl?: string
      imageAlt?: string
      imageAlign?: 'left'|'center'|'right'
      imageWidth?: number // px
      sanitizeHtml?: boolean
    }
    // Composition card options
    composition?: {
      components: CompositionComponent[]
      columns?: 6 | 8 | 12 // grid columns for layout (default 12)
      gap?: number // tailwind spacing scale, e.g., 2
      layout?: 'grid' | 'stack' // grid = side-by-side per span; stack = vertical order
      innerInteractive?: boolean // enable inner selection/interaction mode
    }
    // Spacer card options
    spacer?: {
      minW?: number // minimum grid columns width
    }
    // Prefer routing this widget's queries to local DuckDB when available
    preferLocalDuck?: boolean
  }
  // Reusable measures available to drag into Values in the Pivot
  measures?: Array<{
    id: string
    name: string
    formula: string
  }>
  // Row-level computed columns (QuerySpec only). Available as fields in Pivot and Charts
  customColumns?: Array<{
    id?: string
    name: string
    formula: string
    type?: 'number' | 'string' | 'date' | 'boolean'
  }>
  // New: user-defined data series for charts (PowerBI/Tableau-like)
  series?: Array<{
    id?: string
    name?: string
    x: string
    y: string
    agg?: 'none' | 'count' | 'distinct' | 'avg' | 'sum' | 'min' | 'max'
    colorToken?: 1 | 2 | 3 | 4 | 5
    formula?: string
    secondaryAxis?: boolean
  }>
  // Optional axis settings (e.g., date grouping for X axis)
  xAxis?: { groupBy?: 'none' | 'hour' | 'day' | 'week' | 'month' | 'quarter' | 'year' }
  yAxis?: { scale?: 'linear' | 'log' }
  // Table: pivot assignments (rows/columns/values/filters)
  pivot?: {
    x?: string
    legend?: string
    values: Array<{
      field?: string
      measureId?: string
      agg?: 'none' | 'count' | 'distinct' | 'avg' | 'sum' | 'min' | 'max'
      label?: string
      secondaryAxis?: boolean
    }>
    filters: string[]
  }
}
