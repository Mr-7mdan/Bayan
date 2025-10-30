"use client"

import React, { ComponentType } from 'react'
import dynamic from 'next/dynamic'

// Keep ECharts client-side
const ReactECharts: any = dynamic(() => import('echarts-for-react').then(m => (m as any).default), { ssr: false }) as ComponentType<any>

// Shared helpers
function toDateISO(v: any): string | null {
  try {
    if (!v && v !== 0) return null
    const d = v instanceof Date ? v : new Date(String(v))
    if (isNaN(d.getTime())) return null
    const yyyy = d.getFullYear()
    const mm = String(d.getMonth() + 1).padStart(2, '0')
    const dd = String(d.getDate()).padStart(2, '0')
    return `${yyyy}-${mm}-${dd}`
  } catch { return null }
}

function extent(nums: number[]): { min: number; max: number } {
  let min = Number.POSITIVE_INFINITY
  let max = Number.NEGATIVE_INFINITY
  for (const n of nums) { if (Number.isFinite(n)) { if (n < min) min = n; if (n > max) max = n } }
  if (!isFinite(min)) min = 0
  if (!isFinite(max)) max = 1
  if (min === max) { if (max === 0) max = 1; else min = 0 }
  return { min, max }
}

function formatValue(v: number, fmt?: string, currency?: string) {
  const n = Number(v)
  if (!Number.isFinite(n)) return ''
  try {
    switch (fmt) {
      case 'short': return new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 }).format(n)
      case 'currency': return new Intl.NumberFormat(undefined, { style: 'currency', currency: currency || 'USD', maximumFractionDigits: 0 }).format(n)
      case 'percent': return new Intl.NumberFormat(undefined, { style: 'percent', maximumFractionDigits: 1 }).format(n)
      case 'bytes': {
        const units = ['B','KB','MB','GB','TB']; let x=n, i=0; while (x>=1024 && i<units.length-1) { x/=1024; i++ } return `${x.toFixed(1)} ${units[i]}`
      }
      case 'oneDecimal': return new Intl.NumberFormat(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 }).format(n)
      case 'twoDecimals': return new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(n)
      case 'none': default: return new Intl.NumberFormat().format(n)
    }
  } catch { return String(n) }
}

// 1) Calendar Heatmap (Monthly Heatmap - per Day)
export type CalendarMonthlyArgs = {
  chartInstanceKey: any
  data: Array<{ date: string | Date; value: number }> | Array<[string | Date, number]>
  month?: string // 'YYYY-MM'. If not provided, inferred from data
  title?: string
  valueFormat?: 'none'|'short'|'currency'|'percent'|'bytes'|'oneDecimal'|'twoDecimals'
  currency?: string
  visualMap?: { orient?: 'horizontal'|'vertical'; position?: 'top'|'bottom'|'left'|'right' }
}
export function renderCalendarHeatmapMonthlyDays({ chartInstanceKey, data, month, title, valueFormat, currency, visualMap }: CalendarMonthlyArgs) {
  const pairs = (Array.isArray(data) ? data : []).map((r: any) => Array.isArray(r) ? r : [r.date, r.value])
  const rows: [string, number][] = pairs.map(([d, v]) => [toDateISO(d)!, Number(v)]).filter(([d, v]) => !!d && Number.isFinite(v)) as any
  const monthRange = month || (() => {
    const first = rows.find(Boolean)?.[0]
    if (!first) return new Date().toISOString().slice(0, 7)
    return first.slice(0, 7)
  })()
  const { min, max } = extent(rows.map(([, v]) => v))
  const vmOrient = visualMap?.orient || 'horizontal'
  const vmPos = visualMap?.position || 'top'
  const option = {
    title: title ? { left: 'center', text: title } : undefined,
    tooltip: { position: 'top' as const, formatter: (p: any) => `${p?.value?.[0]}: ${formatValue(Number(p?.value?.[1]||0), valueFormat, currency)}` },
    visualMap: { min, max, calculable: true, orient: vmOrient, left: (vmPos==='left'?'left':'center'), top: (vmPos==='top'?0:undefined), right: (vmPos==='right'?0:undefined), bottom: (vmPos==='bottom'?0:undefined), dimension: 1, seriesIndex: 0, formatter: (val: number) => formatValue(val, valueFormat, currency) },
    calendar: {
      top: 40,
      left: 20,
      right: 20,
      cellSize: [18, 18],
      range: monthRange,
      itemStyle: { borderWidth: 0.5, borderColor: 'rgba(148,163,184,.35)' },
      splitLine: { lineStyle: { color: 'rgba(148,163,184,.4)', width: 1 } },
    },
    series: [
      { type: 'heatmap', coordinateSystem: 'calendar', data: rows, encode: { value: 1 } },
    ],
  }
  return (
    <div className="absolute inset-0">
      <ReactECharts key={chartInstanceKey} option={option} notMerge={true} lazyUpdate style={{ height: '100%' }} />
    </div>
  )
}

// 2) Heatmap on Cartesian (Weekday Heatmap - per Hour)
export type WeekdayHourArgs = {
  chartInstanceKey: any
  // Triples of [hour(0-23), weekday(0=Mon..6=Sun), value]
  data: Array<[number, number, number]>
  title?: string
  valueFormat?: 'none'|'short'|'currency'|'percent'|'bytes'|'oneDecimal'|'twoDecimals'
  currency?: string
  visualMap?: { orient?: 'horizontal'|'vertical'; position?: 'top'|'bottom'|'left'|'right' }
}
export function renderHeatmapWeekdayByHour({ chartInstanceKey, data, title, valueFormat, currency, visualMap }: WeekdayHourArgs) {
  const hours = Array.from({ length: 24 }, (_, i) => i)
  const days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
  const triples = (Array.isArray(data) ? data : []).map((t) => [Number(t[0]), Number(t[1]), Number(t[2])])
  const { min, max } = extent(triples.map(([, , v]) => v))
  const vmOrient = visualMap?.orient || 'vertical'
  const vmPos = visualMap?.position || 'right'
  const option = {
    title: title ? { text: title, left: 'center' } : undefined,
    tooltip: { position: 'top' as const, formatter: (p: any) => {
      try {
        const h = Number(p?.data?.[0] ?? 0)
        const d = Number(p?.data?.[1] ?? 0)
        const v = Number(p?.data?.[2] ?? 0)
        return `${days[d]} ${String(h).padStart(2,'0')}:00 — ${formatValue(v, valueFormat, currency)}`
      } catch { return '' }
    } },
    grid: { left: 60, right: 40, top: 40, bottom: 40 },
    xAxis: { type: 'category' as const, data: hours.map(h => `${h}:00`), splitArea: { show: true } },
    yAxis: { type: 'category' as const, data: days, splitArea: { show: true } },
    visualMap: { min, max, calculable: true, orient: vmOrient, left: (vmPos==='left'?'left':'right'), top: (vmPos==='top'?'top':'center'), bottom: (vmPos==='bottom'?0:undefined), dimension: 2, seriesIndex: 0, formatter: (val: number) => formatValue(val, valueFormat, currency) },
    series: [
      {
        name: 'Intensity',
        type: 'heatmap',
        data: triples,
        label: { show: false },
        encode: { x: 0, y: 1, value: 2 },
        emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.5)' } },
      },
    ],
  }
  return (
    <div className="absolute inset-0">
      <ReactECharts key={chartInstanceKey} option={option} notMerge={true} lazyUpdate style={{ height: '100%' }} />
    </div>
  )
}

// 3) Simple Calendar (Annual Heatmap - per Month or Day)
export type CalendarAnnualArgs = {
  chartInstanceKey: any
  data: Array<{ date: string | Date; value: number }> | Array<[string | Date, number]>
  year?: string // 'YYYY'
  title?: string
  valueFormat?: 'none'|'short'|'currency'|'percent'|'bytes'|'oneDecimal'|'twoDecimals'
  currency?: string
  visualMap?: { orient?: 'horizontal'|'vertical'; position?: 'top'|'bottom'|'left'|'right' }
}
export function renderCalendarHeatmapAnnual({ chartInstanceKey, data, year, title, valueFormat, currency, visualMap }: CalendarAnnualArgs) {
  const pairs = (Array.isArray(data) ? data : []).map((r: any) => Array.isArray(r) ? r : [r.date, r.value])
  const rows: [string, number][] = pairs.map(([d, v]) => [toDateISO(d)!, Number(v)]).filter(([d, v]) => !!d && Number.isFinite(v)) as any
  const yearRange = year || (() => {
    const first = rows.find(Boolean)?.[0]
    if (!first) return String(new Date().getFullYear())
    return first.slice(0, 4)
  })()
  const { min, max } = extent(rows.map(([, v]) => v))
  const vmOrient = visualMap?.orient || 'horizontal'
  const vmPos = visualMap?.position || 'top'
  const option = {
    title: title ? { left: 'center', text: title } : undefined,
    tooltip: { position: 'top' as const, formatter: (p: any) => `${p?.value?.[0]}: ${formatValue(Number(p?.value?.[1]||0), valueFormat, currency)}` },
    visualMap: { min, max, calculable: true, orient: vmOrient, left: (vmPos==='left'?'left':'center'), top: (vmPos==='top'?0:undefined), right: (vmPos==='right'?0:undefined), bottom: (vmPos==='bottom'?0:undefined), formatter: (val: number) => formatValue(val, valueFormat, currency) },
    calendar: {
      top: 40,
      left: 20,
      right: 20,
      range: yearRange,
      splitLine: { lineStyle: { color: 'rgba(148,163,184,.4)', width: 1 } },
      itemStyle: { borderWidth: 0.5, borderColor: 'rgba(148,163,184,.35)' },
    },
    series: [
      { type: 'heatmap', coordinateSystem: 'calendar', data: rows },
    ],
  }
  return (
    <div className="absolute inset-0">
      <ReactECharts key={chartInstanceKey} option={option} notMerge={true} lazyUpdate style={{ height: '100%' }} />
    </div>
  )
}

// 4) Correlation Matrix (Heatmap)
export type CorrelationHeatmapArgs = {
  chartInstanceKey: any
  // Option A: provide correlation matrix directly
  labels?: string[]
  matrix?: number[][] // values in [-1, 1]
  // Option B: provide raw vectors to compute Pearson correlation
  seriesVectors?: Record<string, number[]>
  title?: string
  valueFormat?: 'none'|'short'|'currency'|'percent'|'bytes'|'oneDecimal'|'twoDecimals'
  currency?: string
}
function computeCorrelationMatrix(vectors: Record<string, number[]>): { labels: string[]; matrix: number[][] } {
  const keys = Object.keys(vectors || {})
  const labels = keys
  const vals = keys.map(k => (vectors[k] || []).filter(v => Number.isFinite(v)))
  const n = labels.length
  const mat: number[][] = Array.from({ length: n }, () => Array.from({ length: n }, () => 0))
  const mean = (arr: number[]) => (arr.length ? arr.reduce((a,b)=>a+b,0)/arr.length : 0)
  const std = (arr: number[], m: number) => {
    if (arr.length === 0) return 0
    const v = arr.reduce((s,x)=>s+(x-m)*(x-m),0)/arr.length
    return Math.sqrt(v)
  }
  for (let i=0;i<n;i++) {
    const xi = vals[i]
    const mi = mean(xi)
    const si = std(xi, mi) || 1
    for (let j=0;j<n;j++) {
      if (j < i) { mat[i][j] = mat[j][i]; continue }
      const xj = vals[j]
      const mj = mean(xj)
      const sj = std(xj, mj) || 1
      const len = Math.min(xi.length, xj.length)
      let cov = 0
      for (let k=0;k<len;k++) cov += ((xi[k] ?? 0) - mi) * ((xj[k] ?? 0) - mj)
      cov /= len || 1
      const r = cov / (si * sj)
      mat[i][j] = Math.max(-1, Math.min(1, r))
      mat[j][i] = mat[i][j]
    }
  }
  return { labels, matrix: mat }
}
export function renderCorrelationMatrixHeatmap({ chartInstanceKey, labels, matrix, seriesVectors, title, valueFormat, currency }: CorrelationHeatmapArgs) {
  let labs: string[] = labels || []
  let mat: number[][] | undefined = matrix
  if ((!matrix || !labels) && seriesVectors && Object.keys(seriesVectors).length >= 2) {
    const r = computeCorrelationMatrix(seriesVectors)
    labs = r.labels
    mat = r.matrix
  }
  const n = labs.length
  const triples: Array<[number, number, number]> = []
  if (mat && n) {
    for (let i=0;i<n;i++) for (let j=0;j<n;j++) triples.push([i, j, Number((mat[i]?.[j] ?? 0))])
  }
  const option = {
    title: title ? { text: title, left: 'center' } : undefined,
    tooltip: {
      position: 'top' as const,
      formatter: (p: any) => {
        const i = p?.data?.[0]; const j = p?.data?.[1]; const v = Number(p?.data?.[2] ?? 0)
        return `${labs[i]} × ${labs[j]}: ${formatValue(v, valueFormat || 'twoDecimals', currency)}`
      }
    },
    grid: { left: 80, right: 40, top: 40, bottom: 40, containLabel: true },
    xAxis: { type: 'category' as const, data: labs, splitArea: { show: false }, axisLabel: { rotate: 45 } },
    yAxis: { type: 'category' as const, data: labs, splitArea: { show: false } },
    visualMap: {
      min: -1,
      max: 1,
      calculable: false,
      orient: 'vertical' as const,
      left: 'right',
      top: 'center',
      inRange: { color: ['#5588EE', '#FFFFFF', '#EE4444'] },
    },
    series: [
      {
        name: 'Correlation',
        type: 'heatmap',
        data: triples,
        label: { show: false },
        emphasis: { itemStyle: { shadowBlur: 8, shadowColor: 'rgba(0,0,0,0.35)' } },
      }
    ],
  }
  return (
    <div className="absolute inset-0">
      <ReactECharts key={chartInstanceKey} option={option} notMerge={true} lazyUpdate style={{ height: '100%' }} />
    </div>
  )
}
