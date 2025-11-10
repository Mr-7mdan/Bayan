"use client"

import React, { useMemo } from 'react'
import dynamic from 'next/dynamic'
import { useQuery } from '@tanstack/react-query'
import { useFilters } from '@/components/providers/FiltersProvider'
import type { WidgetConfig } from '@/types/widgets'
import type { QuerySpec } from '@/lib/api'
import { QueryApi } from '@/lib/api'
import ErrorBoundary from '@/components/dev/ErrorBoundary'

// ECharts client-only wrapper
const ReactECharts: any = dynamic(() => import('echarts-for-react').then(m => (m as any).default), { ssr: false }) as any

function isDark(): boolean {
  if (typeof window === 'undefined') return false
  return document.documentElement.classList.contains('dark')
}

function parseDateLoose(v: any): Date | null {
  try {
    if (v == null) return null
    if (v instanceof Date) return isNaN(v.getTime()) ? null : v
    const s = String(v)
    if (/^\d{10,13}$/.test(s)) { const n = Number(s); const ms = s.length === 10 ? n * 1000 : n; const d = new Date(ms); return isNaN(d.getTime()) ? null : d }
    const m1 = s.replace(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(?::\d{2})?)$/, '$1T$2')
    let d = new Date(m1); if (!isNaN(d.getTime())) return d
    const m2 = s.match(/^(\d{4})-(\d{2})-(\d{2})$/)
    if (m2) { d = new Date(`${m2[1]}-${m2[2]}-${m2[3]}T00:00:00`); return isNaN(d.getTime()) ? null : d }
    return null
  } catch { return null }
}

export default function GanttCard({
  title,
  datasourceId,
  options,
  queryMode = 'spec',
  querySpec,
  widgetId,
  pivot,
}: {
  title: string
  datasourceId?: string
  options?: WidgetConfig['options']
  queryMode?: 'sql' | 'spec'
  querySpec?: QuerySpec
  widgetId?: string
  pivot?: {
    x?: string | string[]
    legend?: string | string[]
    values: Array<{ field?: string; measureId?: string; agg?: string }>
    filters: string[]
  }
}) {
  const { filters } = useFilters()
  const echartsRef = React.useRef<any>(null)
  
  // Handle download actions from kebab menu
  React.useEffect(() => {
    const handleDownload = (e: CustomEvent) => {
      const { widgetId: targetId, format } = e.detail || {}
      if (targetId !== widgetId) return
      
      const instance = echartsRef.current?.getEchartsInstance?.()
      if (!instance) {
        console.warn('[GanttCard] No ECharts instance available for download')
        return
      }
      
      try {
        const fileName = `${title || 'gantt'}_${new Date().toISOString().split('T')[0]}`
        
        if (format === 'png') {
          const url = instance.getDataURL({
            type: 'png',
            pixelRatio: 2,
            backgroundColor: '#fff'
          })
          const link = document.createElement('a')
          link.href = url
          link.download = `${fileName}.png`
          link.click()
        } else if (format === 'svg') {
          const url = instance.getDataURL({
            type: 'svg',
            backgroundColor: '#fff'
          })
          const link = document.createElement('a')
          link.href = url
          link.download = `${fileName}.svg`
          link.click()
        }
      } catch (err) {
        console.error('[GanttCard] Download failed:', err)
      }
    }
    
    window.addEventListener('widget-download-chart' as any, handleDownload as any)
    return () => {
      window.removeEventListener('widget-download-chart' as any, handleDownload as any)
    }
  }, [widgetId, title])
  const gantt = (options?.gantt || {}) as NonNullable<WidgetConfig['options']>['gantt']
  const mode = gantt?.mode || 'startEnd'
  const durationUnit = gantt?.durationUnit || 'hours'
  const barH = Math.max(6, Math.min(24, Number(gantt?.barHeight ?? 10)))
  
  // Map pivot assignments to gantt fields
  // X = Start field, Legend = Category field, Value[0] = End/Duration field
  const startField = Array.isArray(pivot?.x) ? pivot.x[0] : pivot?.x
  const catField = Array.isArray(pivot?.legend) ? pivot.legend[0] : pivot?.legend
  const valueField = pivot?.values?.[0]?.field
  const endField = mode === 'startEnd' ? valueField : undefined
  const durationField = mode === 'startDuration' ? valueField : undefined
  const colorField = undefined // Can be added later via filters or additional legend

  const q = useQuery({
    queryKey: ['gantt', title, datasourceId, querySpec, options, filters],
    enabled: queryMode === 'spec' && !!querySpec?.source,
    queryFn: async () => {
      if (!querySpec?.source) return { columns: [], rows: [] }
      // Build a raw select spec pulling only needed fields; include existing where
      const select: string[] = []
      if (catField) select.push(catField)
      if (startField) select.push(startField)
      if (mode === 'startEnd' && endField) select.push(endField)
      if (mode === 'startDuration' && durationField) select.push(durationField)
      if (colorField) select.push(colorField)
      // Fallback: if no mapping provided, attempt best-effort heuristic
      const sel = select.length ? select : (querySpec.select && querySpec.select.length ? querySpec.select : [])
      const spec: QuerySpec = {
        source: querySpec.source,
        select: sel.length ? sel : undefined,
        where: querySpec.where as any,
        limit: querySpec.limit ?? 5000,
        offset: querySpec.offset ?? 0,
      }
      return QueryApi.querySpec({ spec, datasourceId, limit: spec.limit ?? 5000, offset: spec.offset ?? 0, includeTotal: false })
    },
  })

  const { categories, items } = useMemo(() => {
    const cols = (q.data?.columns as string[]) || []
    const rows: any[] = (q.data?.rows as any[]) || []
    if (!cols.length || !rows.length) return { categories: [] as string[], items: [] as any[] }

    const idxOf = (name?: string, fallbackMatch?: RegExp): number => {
      if (!name) return -1
      const i = cols.indexOf(name)
      if (i >= 0) return i
      if (fallbackMatch) {
        const j = cols.findIndex(c => fallbackMatch.test(c))
        return j >= 0 ? j : -1
      }
      return -1
    }
    const ci = idxOf(catField, /(name|task|owner|category)/i)
    const si = idxOf(startField, /(start|begin|from)/i)
    const ei = mode === 'startEnd' ? idxOf(endField, /(end|finish|to)/i) : -1
    const di = mode === 'startDuration' ? (durationField ? cols.indexOf(durationField) : -1) : -1
    const coli = colorField ? cols.indexOf(colorField) : -1
    
    // Duration multipliers in milliseconds
    const durationMultipliers: Record<string, number> = {
      seconds: 1000,
      minutes: 60 * 1000,
      hours: 60 * 60 * 1000,
      days: 24 * 60 * 60 * 1000,
      weeks: 7 * 24 * 60 * 60 * 1000,
      months: 30 * 24 * 60 * 60 * 1000, // Approximate
    }
    
    const items: Array<{ cat: string; start: number; end: number; color?: string }> = []
    const cats: string[] = []
    rows.forEach((r: any) => {
      const cat = String(ci >= 0 ? r[ci] : (r?.[catField as any]))
      const sv = si >= 0 ? r[si] : (r?.[startField as any])
      const col = (coli >= 0 ? r[coli] : (colorField ? r?.[colorField as any] : undefined))
      const sd = parseDateLoose(sv)
      if (!cat || !sd) return
      
      let ed: Date | null = null
      if (mode === 'startEnd') {
        const ev = ei >= 0 ? r[ei] : (r?.[endField as any])
        ed = parseDateLoose(ev)
      } else if (mode === 'startDuration') {
        const dv = di >= 0 ? r[di] : (r?.[durationField as any])
        const duration = Number(dv)
        if (Number.isFinite(duration) && duration >= 0) {
          const durationMs = duration * (durationMultipliers[durationUnit] || durationMultipliers.hours)
          ed = new Date(sd.getTime() + durationMs)
        }
      }
      
      if (!ed) return
      if (!cats.includes(cat)) cats.push(cat)
      items.push({ cat, start: sd.getTime(), end: ed.getTime(), color: col != null ? String(col) : undefined })
    })
    // Sort categories by name
    cats.sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }))
    return { categories: cats, items }
  }, [q.data])

  if (q.isLoading) return (<div className="space-y-2 animate-pulse"><div className="h-6 bg-muted rounded w-1/2" /><div className="h-[280px] bg-muted rounded" /></div>)
  if (q.error) return (<div className="text-sm text-red-600">Failed to load Gantt data</div>)

  // Build ECharts custom series data: encode yIndex per category
  const catIndex = new Map<string, number>(categories.map((c, i) => [c, i]))
  const data = items.map(it => ({
    name: it.cat,
    value: [it.start, it.end, catIndex.get(it.cat) ?? 0],
    itemStyle: it.color ? { color: String(it.color) } : undefined,
  }))

  const dark = isDark()
  const axisTextColor = dark ? '#94a3b8' : '#475569'
  const borderColor = dark ? '#1f2937' : '#e2e8f0'

  // Custom renderer for horizontal bars
  const renderItem = (params: any, api: any) => {
    const start = api.value(0)
    const end = api.value(1)
    const yIndex = api.value(2)
    const startCoord = api.coord([start, yIndex])
    const endCoord = api.coord([end, yIndex])
    const y = startCoord[1]
    const h = barH
    const x = startCoord[0]
    const w = Math.max(1, endCoord[0] - startCoord[0])
    return {
      type: 'rect',
      shape: { x, y: y - h / 2, width: w, height: h },
      style: api.style(),
    }
  }

  const option = {
    tooltip: {
      trigger: 'item',
      backgroundColor: 'transparent', borderWidth: 0, extraCssText: 'box-shadow:none;padding:0;',
      formatter: (p: any) => {
        try {
          const name = String(p?.name || '')
          const v = p?.value as [number, number, number]
          const s = new Date(v[0])
          const e = new Date(v[1])
          const durMs = Math.max(0, v[1] - v[0])
          const durH = (durMs / 3600000)
          const fmt = (d: Date) => `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')} ${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`
          const bg = (typeof document !== 'undefined' && document.documentElement.classList.contains('dark')) ? 'hsla(199, 98.5%, 8.1%, 0.9)' : 'rgba(255,255,255,0.95)'
          const fg = dark ? 'hsl(var(--foreground))' : '#0f172a'
          const border = dark ? '1px solid hsl(var(--border))' : '1px solid rgba(148,163,184,.35)'
          return `<div style="padding:6px 8px;border:${border};background:${bg};color:${fg};border-radius:6px;font-size:12px;line-height:1.1;">
            <div style="font-weight:600;margin-bottom:6px;text-align:left">${name}</div>
            <div style="display:grid;grid-template-columns:auto 1fr;gap:4px 8px">
              <div>Start</div><div style="text-align:right">${fmt(s)}</div>
              <div>End</div><div style="text-align:right">${fmt(e)}</div>
              <div>Duration</div><div style="text-align:right">${durH.toFixed(1)} h</div>
            </div>
          </div>`
        } catch { return '' }
      },
    },
    grid: { left: 80, right: 20, top: 10, bottom: 40, containLabel: false },
    xAxis: { type: 'time', axisLabel: { color: axisTextColor } },
    yAxis: { type: 'category', data: categories, inverse: false, axisLabel: { color: axisTextColor } },
    dataZoom: [
      { type: 'inside', xAxisIndex: 0 },
      { type: 'slider', xAxisIndex: 0, height: 24, bottom: 8 }
    ],
    series: [
      {
        type: 'custom',
        renderItem,
        encode: { x: [0, 1], y: 2 },
        data,
        itemStyle: { borderColor, borderWidth: 0 },
      }
    ]
  }

  return (
    <ErrorBoundary name="GanttCard">
      <div className="absolute inset-0">
        <ReactECharts ref={echartsRef} key={`${widgetId || title}-gantt`} option={option} style={{ height: '100%', width: '100%' }} notMerge={true} lazyUpdate={true} opts={{ renderer: 'svg' }} />
      </div>
    </ErrorBoundary>
  )
}
