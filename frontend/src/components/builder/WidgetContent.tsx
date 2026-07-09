"use client"

import { memo } from 'react'
import KpiCard from '@/components/widgets/KpiCard'
import ChartCard from '@/components/widgets/ChartCard'
import HeatmapCard from '@/components/widgets/HeatmapCard'
import TableCard from '@/components/widgets/TableCard'
import TextCard from '@/components/widgets/TextCard'
import SpacerCard from '@/components/widgets/SpacerCard'
import ErrorBoundary from '@/components/dev/ErrorBoundary'
import type { WidgetConfig } from '@/types/widgets'

type Props = {
  cfg: WidgetConfig
  reservedTop: number
}

/**
 * Pure display widgets (KPI / chart / heatmap / table / text / spacer).
 *
 * Memoized so the expensive ECharts / ag-grid instances do NOT re-render when
 * unrelated builder state changes (widget selection, drag-stop layout commits,
 * actions-menu open/close). Props are `cfg` (a stable reference until that
 * widget's config actually changes) and `reservedTop` (a number), so the
 * default shallow comparison is sufficient.
 *
 * Composition / report widgets are rendered inline by the builder instead —
 * they depend on the live `allWidgets` map and callbacks, so memoizing them
 * here would provide no benefit.
 */
function WidgetContentInner({ cfg, reservedTop }: Props) {
  switch (cfg.type) {
    case 'kpi':
      return (
        <ErrorBoundary name="KpiCard">
          <KpiCard
            title={cfg.title}
            sql={cfg.sql}
            datasourceId={cfg.datasourceId}
            queryMode={cfg.queryMode}
            querySpec={cfg.querySpec as any}
            options={cfg.options}
            pivot={cfg.pivot as any}
            widgetId={cfg.id}
          />
        </ErrorBoundary>
      )
    case 'chart':
      return (
        <ErrorBoundary name="ChartCard">
          {((cfg as any).chartType === 'heatmap') ? (
            <HeatmapCard
              title={cfg.title}
              sql={cfg.sql}
              datasourceId={cfg.datasourceId}
              options={cfg.options}
              queryMode={cfg.queryMode as any}
              querySpec={cfg.querySpec as any}
              widgetId={cfg.id}
            />
          ) : (
            <ChartCard
              title={cfg.title}
              sql={cfg.sql}
              datasourceId={cfg.datasourceId}
              type={(cfg as any).chartType || 'line'}
              options={cfg.options}
              queryMode={cfg.queryMode}
              querySpec={cfg.querySpec as any}
              customColumns={cfg.customColumns}
              widgetId={cfg.id}
              pivot={cfg.pivot}
              layout="measure"
              reservedTop={reservedTop}
            />
          )}
        </ErrorBoundary>
      )
    case 'table':
      return (
        <ErrorBoundary name="TableCard">
          <TableCard
            title={cfg.title}
            sql={cfg.sql}
            datasourceId={cfg.datasourceId}
            options={cfg.options as any}
            queryMode={cfg.queryMode as any}
            querySpec={cfg.querySpec as any}
            widgetId={cfg.id}
            customColumns={cfg.customColumns as any}
            pivot={cfg.pivot as any}
          />
        </ErrorBoundary>
      )
    case 'text':
      return (
        <ErrorBoundary name="TextCard">
          <TextCard title={cfg.title} options={cfg.options} />
        </ErrorBoundary>
      )
    case 'spacer':
      return (
        <ErrorBoundary name="SpacerCard">
          <SpacerCard options={cfg.options} />
        </ErrorBoundary>
      )
    default:
      return null
  }
}

export default memo(WidgetContentInner)
