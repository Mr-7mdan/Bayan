// Shared chart type union for ChartCard and its (future) per-type renderers.
// Extracted verbatim from ChartCard.tsx's inline prop union (behavior-preserving).
export type ChartType =
  | 'line'
  | 'bar'
  | 'area'
  | 'column'
  | 'donut'
  | 'categoryBar'
  | 'spark'
  | 'combo'
  | 'badges'
  | 'progress'
  | 'tracker'
  | 'scatter'
  | 'tremorTable'
  | 'barList'
  | 'gantt'
  | 'sankey'
