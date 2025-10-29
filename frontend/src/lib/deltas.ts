import { Api } from '@/lib/api'
import { periodTotalsCached } from '@/lib/periodTotalsCache'

export type DeltaMode =
  | 'TD_YSTD'
  | 'TW_LW'
  | 'MONTH_LMONTH'
  | 'MTD_LMTD'
  | 'TY_LY'
  | 'YTD_LYTD'
  | 'TQ_LQ'

export type SeriesSpec = {
  label?: string
  y?: string
  measure?: string
  agg?: 'none' | 'count' | 'distinct' | 'avg' | 'sum' | 'min' | 'max'
}

export type PeriodDeltaRequest = {
  source: string
  datasourceId?: string
  dateField: string
  where?: Record<string, unknown>
  legend?: string | string[]
  series?: SeriesSpec[]
  agg?: 'none' | 'count' | 'distinct' | 'avg' | 'sum' | 'min' | 'max'
  y?: string
  measure?: string
  mode: DeltaMode
  tzOffsetMinutes?: number
  weekStart?: 'sat' | 'sun' | 'mon'
}

export type PeriodDeltaResult = {
  deltas: Record<string, number>
  curTotals?: Record<string, number>
  prevTotals?: Record<string, number>
  curTotal?: number
  prevTotal?: number
  // Legend path enhancements
  curOverall?: number
  prevOverall?: number
  curShareTotals?: Record<string, number> // percent (0..100)
  prevShareTotals?: Record<string, number> // percent (0..100)
}

export async function computePeriodDeltas(req: PeriodDeltaRequest): Promise<PeriodDeltaResult> {
  const {
    mode,
    tzOffsetMinutes,
    weekStart,
    source,
    datasourceId,
    dateField,
    where,
    legend,
    series,
    agg,
    y,
    measure,
  } = req
  const { curStart, curEnd, prevStart, prevEnd } = await Api.resolvePeriods({
    mode,
    tzOffsetMinutes,
    weekStart: (weekStart || 'mon') as any,
  })
  // Strip any date constraints on the same delta date field to avoid intersecting with period bounds
  const stripDateFromWhere = (w?: Record<string, unknown>, df?: string) => {
    const base: Record<string, unknown> = { ...(w || {}) }
    if (df) {
      delete base[df]
      delete (base as any)[`${df}__gte`]
      delete (base as any)[`${df}__lte`]
      delete (base as any)[`${df}__gt`]
      delete (base as any)[`${df}__lt`]
    }
    return Object.keys(base).length ? base : undefined
  }
  const whereEff = stripDateFromWhere(where, dateField)

  // Legend totals (per-category) with prev2 fallback, using compare endpoint
  if (legend) {
    const aggEff = (agg as any)
    const cmp = await Api.periodTotalsCompare({
      source,
      datasourceId,
      dateField,
      start: curStart,
      end: curEnd,
      prevStart,
      prevEnd,
      where: whereEff,
      legend: legend as any,
      agg: aggEff,
      y: y as any,
      measure: measure as any,
    })
    let cur = cmp.cur || {}
    let prev = cmp.prev || {}
    let keys = new Set<string>([...Object.keys((cur as any).totals || {}), ...Object.keys((prev as any).totals || {})])
    let curSum = 0
    let prevSum = 0
    keys.forEach((k) => { curSum += Number(((cur as any).totals || {})[k] || 0); prevSum += Number(((prev as any).totals || {})[k] || 0) })
    if (prevSum === 0 && curSum > 0) {
      try {
        const cs = new Date(curStart)
        const ce = new Date(curEnd)
        const spanMs = Math.max(1, ce.getTime() - cs.getTime())
        const p2Start = new Date(new Date(prevStart).getTime() - spanMs)
        const p2End = new Date(new Date(prevStart).getTime() - 1)
        const cmp2 = await Api.periodTotalsCompare({
          source,
          datasourceId,
          dateField,
          start: curStart,
          end: curEnd,
          prevStart: p2Start.toISOString(),
          prevEnd: p2End.toISOString(),
          where: whereEff,
          legend: legend as any,
          agg: aggEff,
          y: y as any,
          measure: measure as any,
        })
        const prev2 = cmp2.prev || {}
        const keys2 = new Set<string>([...Object.keys((cur as any).totals || {}), ...Object.keys((prev2 as any).totals || {})])
        let prevSum2 = 0
        keys2.forEach((k) => { prevSum2 += Number(((prev2 as any).totals || {})[k] || 0) })
        if (prevSum2 > 0) { prev = prev2; keys = keys2 }
      } catch {}
    }
    const deltas: Record<string, number> = {}
    let curSumOut = 0
    let prevSumOut = 0
    keys.forEach((k) => {
      const a = Number(((cur as any).totals || {})[k] || 0)
      const b = Number(((prev as any).totals || {})[k] || 0)
      deltas[k] = a - b
      curSumOut += a
      prevSumOut += b
    })
    // Overall totals without legend (for share-of-total)
    const whereNoLegend = (() => {
      const w: any = { ...(whereEff || {}) }
      const legends = Array.isArray(legend) ? legend : [legend]
      legends.filter(Boolean).forEach((lg) => {
        const key = String(lg)
        delete w[key]
        delete w[`${key}__gte`]
        delete w[`${key}__lte`]
        delete w[`${key}__gt`]
        delete w[`${key}__lt`]
      })
      return Object.keys(w).length ? w : undefined
    })()
    const cmpAll = await Api.periodTotalsCompare({ source, datasourceId, dateField, start: curStart, end: curEnd, prevStart, prevEnd, where: whereNoLegend, agg: aggEff, y: y as any, measure: measure as any })
    const curOverall = Number((cmpAll.cur || {}).total || 0)
    const prevOverall = Number((cmpAll.prev || {}).total || 0)
    const safeDiv = (num: number, den: number) => (den === 0 ? (num !== 0 ? 100 : 0) : ((num / Math.abs(den)) * 100))
    const curShareTotals: Record<string, number> = {}
    const prevShareTotals: Record<string, number> = {}
    keys.forEach((k) => {
      const a = Number(((cur as any).totals || {})[k] || 0)
      const b = Number(((prev as any).totals || {})[k] || 0)
      curShareTotals[String(k)] = safeDiv(a, curOverall)
      prevShareTotals[String(k)] = safeDiv(b, prevOverall)
    })
    return { deltas, curTotals: (cur as any).totals || {}, prevTotals: (prev as any).totals || {}, curTotal: curSumOut, prevTotal: prevSumOut, curOverall, prevOverall, curShareTotals, prevShareTotals }
  }

  // Multi-series explicit
  if (Array.isArray(series) && series.length > 0) {
    // Batch both cur and prev totals for all series in one call
    const requests: Array<any> = []
    series.forEach((s, idx) => {
      const aggS = ((s.agg ?? agg) as any)
      requests.push({ key: `s${idx}_cur`, source, datasourceId, dateField, start: curStart, end: curEnd, where: whereEff, agg: aggS, y: s.y as any, measure: s.measure as any })
      requests.push({ key: `s${idx}_prev`, source, datasourceId, dateField, start: prevStart, end: prevEnd, where: whereEff, agg: aggS, y: s.y as any, measure: s.measure as any })
    })
    const batch = await Api.periodTotalsBatch({ requests })
    const pairs = series.map((s, idx) => {
      const label = s.label || s.y || s.measure || `series_${idx + 1}`
      const cur = batch.results[`s${idx}_cur`] || {}
      const prev = batch.results[`s${idx}_prev`] || {}
      return [label, Number((cur as any).total || 0) - Number((prev as any).total || 0), Number((cur as any).total || 0), Number((prev as any).total || 0)] as const
    })
    const deltas: Record<string, number> = {}
    const curTotalsBy: Record<string, number> = {}
    const prevTotalsBy: Record<string, number> = {}
    let curOverall = 0
    let prevOverall = 0
    pairs.forEach(([label, d, c, p]) => {
      deltas[String(label)] = d
      curTotalsBy[String(label)] = c
      prevTotalsBy[String(label)] = p
      curOverall += c
      prevOverall += p
    })
    const safeDiv = (num: number, den: number) => (den === 0 ? (num !== 0 ? 100 : 0) : ((num / Math.abs(den)) * 100))
    const curShareTotals: Record<string, number> = {}
    const prevShareTotals: Record<string, number> = {}
    Object.keys(curTotalsBy).forEach((k) => { curShareTotals[k] = safeDiv(curTotalsBy[k] || 0, curOverall) })
    Object.keys(prevTotalsBy).forEach((k) => { prevShareTotals[k] = safeDiv(prevTotalsBy[k] || 0, prevOverall) })
    return { deltas, curTotals: curTotalsBy, prevTotals: prevTotalsBy, curOverall, prevOverall, curShareTotals, prevShareTotals }
  }

  // Single series
  const aggEff = (y ? ((agg === 'count' || agg === 'none') ? 'sum' : agg) : agg) as any
  const cmpSingle = await Api.periodTotalsCompare({ source, datasourceId, dateField, start: curStart, end: curEnd, prevStart, prevEnd, where: whereEff, agg: aggEff, y: (y as any), measure: (measure as any) })
  let c = Number((cmpSingle.cur || {}).total || 0)
  let p = Number((cmpSingle.prev || {}).total || 0)
  if (p === 0 && c > 0) {
    try {
      const cs = new Date(curStart)
      const ce = new Date(curEnd)
      const spanMs = Math.max(1, ce.getTime() - cs.getTime())
      const p2Start = new Date(new Date(prevStart).getTime() - spanMs)
      const p2End = new Date(new Date(prevStart).getTime() - 1)
      const cmp2 = await Api.periodTotalsCompare({ source, datasourceId, dateField, start: curStart, end: curEnd, prevStart: p2Start.toISOString(), prevEnd: p2End.toISOString(), where: whereEff, agg: aggEff, y: (y as any), measure: (measure as any) })
      const p2 = Number((cmp2.prev || {}).total || 0)
      if (p2 > 0) { p = p2 }
    } catch {}
  }
  return {
    deltas: { value: c - p },
    curTotal: c,
    prevTotal: p,
  }
}

export function computeChangePercent(cur: number, prev: number): number {
  const a = Number(cur || 0)
  const b = Number(prev || 0)
  if (!Number.isFinite(a) && !Number.isFinite(b)) return 0
  if (b === 0) return a !== 0 ? 100 : 0
  return ((a - b) / Math.abs(b)) * 100
}
