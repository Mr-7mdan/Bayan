// Unified calculation helpers for charts, tables, KPIs, and tracker
// This module centralizes axis alignment, per-label aggregation, pivot accumulation,
// timeline bucketing, and label normalization so all renderers can share the same numbers.

export type AxisLabel = string
export type CategoryKey = string

export type TimelineOptions = {
  yMax?: number
  trackerMaxPills?: number // for condensing very long timelines (default 60)
}

export type TimelineContext = {
  labels: AxisLabel[]
  rowsByLabel: Record<AxisLabel, Record<string, number | string>>
  totalsByLabel: Record<AxisLabel, number>
  rowMaxByCat: Record<CategoryKey, number>
}

// Tolerant date parser (duplicates tolerant logic used in ChartCard)
function parseDateLoose(v: any): Date | null {
  if (v == null) return null
  if (v instanceof Date) return isNaN(v.getTime()) ? null : v
  const s = String(v).trim()
  if (!s) return null
  if (/^\d{10,13}$/.test(s)) { const n = Number(s); const ms = s.length === 10 ? n*1000 : n; const d = new Date(ms); return isNaN(d.getTime())?null:d }
  const norm = s.replace(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(:\d{2})?)$/, '$1T$2')
  let d = new Date(norm); if (!isNaN(d.getTime())) return d
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})$/); if (iso) { d = new Date(`${iso[1]}-${iso[2]}-${iso[3]}T00:00:00`); return isNaN(d.getTime())?null:d }
  const m = s.match(/^([0-1]?\d)\/([0-3]?\d)\/(\d{4})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?$/)
  if (m) { const mm=Number(m[1])-1, dd=Number(m[2]), yyyy=Number(m[3]); const t=m[4]||'00:00:00'; d = new Date(`${yyyy}-${String(mm+1).padStart(2,'0')}-${String(dd).padStart(2,'0')}T${t.length===5?t+':00':t}`); return isNaN(d.getTime())?null:d }
  return null
}

const formatKey = (d: Date) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`

export function normalizeCategoryLabel(label: string): string {
  const raw = String(label ?? '')
  if (raw.toLowerCase() === 'null') return 'None'
  return raw
}

// Safe numeric coercion
function toNum(x: any, fallback = 0): number {
  const n = Number(x)
  return Number.isFinite(n) ? n : fallback
}

// Accumulate duplicate [x, legend] rows when incoming shape is arrays + columns
export function pivotArrayRows(rows: any[], columns: string[], categories?: string[]): any[] {
  if (!Array.isArray(rows) || !Array.isArray(columns)) return rows || []
  const xIdx = Math.max(columns.indexOf('x'), 0)
  const legendIdx = columns.indexOf('legend')
  const valueIdx = columns.indexOf('value')
  // If shape is [x, legend, value], pivot and accumulate
  if (legendIdx !== -1 && valueIdx !== -1) {
    const byX = new Map<any, any>()
    rows.forEach((r) => {
      const x = r?.[xIdx]
      const lg = String(r?.[legendIdx])
      const val = toNum(r?.[valueIdx])
      const obj = byX.get(x) || { x }
      const prev = toNum((obj as any)[lg], 0)
      ;(obj as any)[lg] = prev + val
      byX.set(x, obj)
    })
    const out = Array.from(byX.values())
    if (Array.isArray(categories) && categories.length > 0) {
      out.forEach((o) => { categories.forEach((c) => { if ((o as any)[c] == null) (o as any)[c] = 0 }) })
    }
    return out
  }
  // Otherwise map arrays by columns if present
  if (columns.length > 0) {
    return rows.map((r) => {
      const o: any = {}
      columns.forEach((c, i) => { o[c] = r?.[i] })
      return o
    })
  }
  return rows
}

// Build an aligned and optionally bucketed timeline context (per-label aggregation included)
export function buildTimelineContext(
  rows: Array<Record<string, any>>,
  categories: string[],
  opts: TimelineOptions = {}
): TimelineContext {
  const yMax = typeof opts.yMax === 'number' ? opts.yMax : undefined
  const maxPills = Math.max(10, Number(opts.trackerMaxPills ?? 60))

  const rawX = rows.map((r) => (r as any)?.x)
  const parsedDates = rawX.map((x) => parseDateLoose(x))
  const isDateX = parsedDates.some((d) => !!d)

  let axisLabels: string[] = []
  const rowByKey: Record<string, any> = {}

  if (isDateX) {
    const valid = parsedDates.filter((d): d is Date => !!d)
    if (valid.length > 0) {
      const min = new Date(Math.min(...valid.map((d) => d.getTime())))
      const max = new Date(Math.max(...valid.map((d) => d.getTime())))
      // Aggregate all rows per day key across categories
      rows.forEach((r) => {
        const d = parseDateLoose((r as any)?.x)
        if (!d) return
        const key = formatKey(d)
        const cur = rowByKey[key] || { x: key }
        categories.forEach((c) => {
          const v = toNum((r as any)?.[c])
          cur[c] = toNum(cur[c], 0) + v
        })
        rowByKey[key] = cur
      })
      const start = new Date(min.getFullYear(), min.getMonth(), min.getDate())
      const end = new Date(max.getFullYear(), max.getMonth(), max.getDate())
      for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
        axisLabels.push(formatKey(d))
      }
    }
  }

  if (axisLabels.length === 0) {
    // Fallback: use exact x values, aggregated per-label
    rows.forEach((r) => {
      const key = String((r as any)?.x)
      const cur = rowByKey[key] || { x: key }
      categories.forEach((c) => {
        const v = toNum((r as any)?.[c])
        cur[c] = toNum(cur[c], 0) + v
      })
      rowByKey[key] = cur
    })
    axisLabels = rawX.map((v) => String(v))
  }

  // Optional bucketing for long ranges
  let labels = axisLabels.slice()
  let rowsByLabel: Record<string, any> = { ...rowByKey }
  if (axisLabels.length > maxPills) {
    const bucketSize = Math.ceil(axisLabels.length / maxPills)
    const newLabels: string[] = []
    const newMap: Record<string, any> = {}
    for (let i = 0; i < axisLabels.length; i += bucketSize) {
      const slice = axisLabels.slice(i, i + bucketSize)
      const start = slice[0]
      const end = slice[slice.length - 1]
      const label = start === end ? start : `${start} – ${end}`
      newLabels.push(label)
      const agg: any = { x: label }
      categories.forEach((c) => { agg[c] = 0 })
      slice.forEach((k) => {
        const row = rowByKey[k] || {}
        categories.forEach((c) => { agg[c] += toNum(row?.[c]) })
      })
      newMap[label] = agg
    }
    labels = newLabels
    rowsByLabel = newMap
  }

  // Totals per label (row) and per-category max
  const totalsByLabel: Record<string, number> = {}
  labels.forEach((label) => {
    const r = rowsByLabel[label]
    totalsByLabel[label] = categories.reduce((s, c) => s + toNum(r?.[c]), 0)
  })

  const rowMaxByCat: Record<string, number> = {}
  categories.forEach((c) => {
    const maxVal = labels.reduce((m, label) => Math.max(m, toNum(rowsByLabel[label]?.[c])), 0)
    rowMaxByCat[c] = typeof yMax === 'number' ? yMax : maxVal
  })

  return { labels, rowsByLabel, totalsByLabel, rowMaxByCat }
}

// Aggregate category columns across rows
export function aggregateCategories(
  rows: Array<Record<string, any>>,
  categories: string[],
  mode: 'sum' | 'last' | 'avg' = 'sum'
): Record<string, number> {
  const acc: Record<string, { sum: number; last: number; count: number }> = {}
  categories.forEach((c) => (acc[c] = { sum: 0, last: 0, count: 0 }))
  rows.forEach((r) => {
    categories.forEach((c) => {
      const v = toNum((r as any)?.[c], NaN)
      if (Number.isFinite(v)) {
        acc[c].sum += v
        acc[c].last = v
        acc[c].count += 1
      }
    })
  })
  const out: Record<string, number> = {}
  categories.forEach((c) => {
    const a = acc[c]
    if (mode === 'sum') out[c] = a.sum
    else if (mode === 'last') out[c] = a.last
    else out[c] = a.count > 0 ? (a.sum / a.count) : 0
  })
  return out
}

// Column maxima by category
export function computeColMax(rows: Array<Record<string, any>>, categories: string[]): Record<string, number> {
  const out: Record<string, number> = {}
  categories.forEach((c) => {
    let m = 0
    rows.forEach((r) => { m = Math.max(m, toNum((r as any)?.[c], 0)) })
    out[c] = m
  })
  return out
}

// Advanced aggregation for KPI and charts
export type AggregationMode = 'none' | 'sum' | 'count' | 'distinctCount' | 'avg' | 'min' | 'max' | 'first' | 'last'

export function aggregateCategoriesAdvanced(
  rows: Array<Record<string, any>>,
  categories: string[],
  mode: AggregationMode
): Record<string, number> {
  const out: Record<string, number> = {}
  const getValues = (c: string) => rows.map((r) => toNum((r as any)?.[c], NaN)).filter((v) => Number.isFinite(v))
  categories.forEach((c) => {
    const vals = getValues(c)
    if (vals.length === 0) { out[c] = 0; return }
    switch (mode) {
      case 'none':
        out[c] = vals[vals.length - 1]
        break
      case 'sum':
      case 'count':
      case 'distinctCount':
        // Note: for time-bucketed series, count/distinctCount from server are per-bucket; summing is the closest aggregate across the whole range
        out[c] = vals.reduce((a, b) => a + b, 0)
        break
      case 'avg':
        out[c] = vals.reduce((a, b) => a + b, 0) / vals.length
        break
      case 'min':
        out[c] = Math.min(...vals)
        break
      case 'max':
        out[c] = Math.max(...vals)
        break
      case 'first':
        out[c] = vals[0]
        break
      case 'last':
      default:
        out[c] = vals[vals.length - 1]
        break
    }
  })
  return out
}
