"use client"

import KpiCard from '@/components/widgets/KpiCard'
import type { QuerySpec } from '@/lib/api'
import type { WidgetConfig } from '@/types/widgets'

export default function KpiDemosPage() {
  // Shared spec targeting the local sample dataset
  const spec: QuerySpec = {
    source: 'local.main.orders',
    // x is useful for spark and for resolving delta date field in some cases
    x: 'order_date',
    y: 'quantity',
    agg: 'sum',
    legend: 'notes',
  }
  // Common delta settings for the demo (meaningful periods)
  const baseDelta: WidgetConfig['options'] = {
    deltaUI: 'preconfigured',
    deltaMode: 'MTD_LMTD',
    deltaDateField: 'order_date',
    kpi: { topN: 3 },
  }
  const cardStyle = 'rounded-lg border p-3'

  return (
    <div className="p-4 space-y-4">
      <h1 className="text-xl font-semibold">KPI Presets Demo</h1>
      <p className="text-sm text-muted-foreground">Quick visual QA for KPI presets using the local sample dataset.</p>
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
        <div className={cardStyle}>
          <div className="text-sm font-medium mb-2">Basic</div>
          <KpiCard
            title="Basic"
            sql=""
            queryMode="spec"
            querySpec={spec}
            options={{ ...baseDelta, kpi: { ...baseDelta.kpi, preset: 'basic' } }}
          />
        </div>
        <div className={cardStyle}>
          <div className="text-sm font-medium mb-2">Badge</div>
          <KpiCard
            title="Badge"
            sql=""
            queryMode="spec"
            querySpec={spec}
            options={{ ...baseDelta, kpi: { ...baseDelta.kpi, preset: 'badge' } }}
          />
        </div>
        <div className={cardStyle}>
          <div className="text-sm font-medium mb-2">With Previous</div>
          <KpiCard
            title="With Previous"
            sql=""
            queryMode="spec"
            querySpec={spec}
            options={{ ...baseDelta, kpi: { ...baseDelta.kpi, preset: 'withPrevious' } }}
          />
        </div>
        <div className={cardStyle}>
          <div className="text-sm font-medium mb-2">Donut (target override)</div>
          <KpiCard
            title="Donut"
            sql=""
            queryMode="spec"
            querySpec={spec}
            options={{ ...baseDelta, kpi: { ...baseDelta.kpi, preset: 'donut', target: 100 } }}
          />
        </div>
        <div className={cardStyle}>
          <div className="text-sm font-medium mb-2">Category Bar (Top N)</div>
          <KpiCard
            title="Category Bar"
            sql=""
            queryMode="spec"
            querySpec={spec}
            options={{ ...baseDelta, kpi: { ...baseDelta.kpi, preset: 'categoryBar', topN: 5 } }}
          />
        </div>
        <div className={cardStyle}>
          <div className="text-sm font-medium mb-2">Multi Progress (Top N)</div>
          <KpiCard
            title="Multi Progress"
            sql=""
            queryMode="spec"
            querySpec={spec}
            options={{ ...baseDelta, kpi: { ...baseDelta.kpi, preset: 'multiProgress', topN: 5 } }}
          />
        </div>
        <div className={cardStyle}>
          <div className="text-sm font-medium mb-2">Spark</div>
          <KpiCard
            title="Spark"
            sql=""
            queryMode="spec"
            querySpec={{ ...spec, groupBy: 'day' as any }}
            options={{ ...baseDelta, kpi: { ...baseDelta.kpi, preset: 'spark', sparkType: 'line' } }}
          />
        </div>
        <div className={cardStyle}>
          <div className="text-sm font-medium mb-2">Progress</div>
          <KpiCard
            title="Progress"
            sql=""
            queryMode="spec"
            querySpec={spec}
            options={{ ...baseDelta, kpi: { ...baseDelta.kpi, preset: 'progress', target: 100 } }}
          />
        </div>
      </div>
    </div>
  )
}
