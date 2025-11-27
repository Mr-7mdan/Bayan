"use client"

import { useMemo, useEffect, useState } from 'react'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'
import { useQuery } from '@tanstack/react-query'
import type { QuerySpec } from '@/lib/api'
import { Api } from '@/lib/api'
import { computePeriodDeltas, computeChangePercent } from '@/lib/deltas'
import type { WidgetConfig } from '@/types/widgets'

export type KpiValue = {
  current: number
  previous: number
  absoluteDelta: number
  percentChange: number
}

export type KpiResult = {
  mode?: string
  legendField?: string | string[]
  seriesLabels?: string[]
  // Single-value mode
  single?: KpiValue
  // Per-legend (category) mode
  byLegend?: Record<string, KpiValue>
  // Per-series mode
  bySeries?: Record<string, KpiValue>
  // Totals across categories or series
  totals?: KpiValue
  // Overall totals across the entire dataset (no legend split)
  overall?: KpiValue
}

function pctChange(curr: number, prev: number): number { return computeChangePercent(curr, prev) }

export function useKpiData({
  title,
  datasourceId,
  querySpec,
  options,
  effectiveWhere,
  visible,
}: {
  title?: string
  datasourceId?: string
  querySpec: QuerySpec
  options?: WidgetConfig['options']
  effectiveWhere?: Record<string, any>
  visible?: boolean
}) {
  const { env } = useEnvironment()
  // (removed) tolerant date parser; not needed when resolving by real NOW
  const series = (querySpec as any)?.series as Array<{ label?: string; x?: string; y?: string; agg?: string; groupBy?: string; measure?: string }> | undefined
  const legendField = (querySpec as any)?.legend as string | string[] | undefined
  const aggTop = (((querySpec as any)?.agg) || (((querySpec as any)?.y) ? 'sum' : 'count')) as string
  const activeDeltaMode = useMemo(() => {
    const deltaUI = options?.deltaUI || 'none'
    const preconfiguredMode = options?.deltaMode && options.deltaMode !== 'off' ? options.deltaMode : undefined
    const filterbarMode = undefined // KpiCard can override via its own state; for now only preconfigured
    return (deltaUI === 'filterbar' ? filterbarMode : preconfiguredMode) as string | undefined
  }, [options?.deltaMode, options?.deltaUI])
  const deltaDateField = options?.deltaDateField
  const deltaWeekStart = (options?.deltaWeekStart || env.weekStart) as 'sat'|'sun'|'mon'
  const tzOffsetMinutes = (typeof window !== 'undefined') ? new Date().getTimezoneOffset() : 0

  const whereKey = useMemo(() => JSON.stringify(effectiveWhere || {}), [effectiveWhere])
  function useDebounced<T>(val: T, delay = 350): T {
    const [v, setV] = useState<T>(val as T)
    useEffect(() => { const t = setTimeout(() => setV(val), delay); return () => { try { clearTimeout(t) } catch {} } }, [val, delay])
    return v
  }
  const debouncedWhereKey = useDebounced(whereKey, 350)

  // Debug inputs and gating
  const enabledFlag = !!(querySpec as any)?.source && !!activeDeltaMode
  try {
    console.log('[KPICardDebug] useKpiData inputs', {
      title,
      datasourceId,
      source: (querySpec as any)?.source,
      activeDeltaMode,
      deltaDateField,
      deltaWeekStart,
      tzOffsetMinutes,
      where: effectiveWhere,
      enabled: enabledFlag,
    })
  } catch {}

  const q = useQuery<KpiResult>({
    queryKey: ['kpi', title, datasourceId, querySpec, activeDeltaMode, deltaDateField, deltaWeekStart, env.weekStart, debouncedWhereKey],
    enabled: enabledFlag && !!visible,
    placeholderData: (prev) => prev as any,
    queryFn: async () => {
      const mode = activeDeltaMode as any
      const source = (querySpec as any).source as string
      const df = deltaDateField as string | undefined
      if (!source || !df) return { mode } as KpiResult
      const legend = (querySpec as any)?.legend as string | string[] | undefined
      const seriesArr = (Array.isArray((querySpec as any)?.series) ? (querySpec as any).series : []) as Array<{ label?: string; y?: string; measure?: string; agg?: any }>
      const reqBase = {
        source,
        datasourceId,
        dateField: df,
        where: (effectiveWhere || undefined) as any,
        agg: (((querySpec as any)?.agg) || ((querySpec as any)?.y ? 'sum' : 'count')) as any,
        y: (querySpec as any)?.y as any,
        measure: (querySpec as any)?.measure as any,
        mode,
        tzOffsetMinutes,
        weekStart: deltaWeekStart,
      } as const

      // If effectiveWhere already specifies a date window on df, use that exact window
      const w = (effectiveWhere || {}) as Record<string, any>
      const gte = (w as any)[`${df}__gte`] as string | undefined
      const lt = (w as any)[`${df}__lt`] as string | undefined
      const hasGlobalWindow = !!gte && !!lt
      if (hasGlobalWindow) {
        try { console.log('[KPICardDebug] useKpiData: using global window', { dateField: df, curStart: gte, curEndExcl: lt }) } catch {}
        // Helper: parse YYYY-MM-DD as UTC and format back
        const parseYmdUtc = (s: string) => {
          const [y, m, d] = s.split('-').map(Number)
          return new Date(Date.UTC(y, (m || 1) - 1, d || 1))
        }
        const fmtYmd = (d: Date) => `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`
        const asIsoZ = (ymd: string) => `${ymd}T00:00:00.000Z`
        const daysBetween = (a: string, b: string) => Math.max(1, Math.round((parseYmdUtc(b).getTime() - parseYmdUtc(a).getTime()) / 86400000))
        const spanDays = daysBetween(gte!, lt!)
        const prevStartYmd = fmtYmd(new Date(parseYmdUtc(gte!).getTime() - spanDays * 86400000))
        const prevEndYmd = gte!
        try { console.log('[KPICardDebug] useKpiData: prev window', { prevStart: asIsoZ(prevStartYmd), prevEnd: asIsoZ(prevEndYmd) }) } catch {}
        const stripDateFromWhere = (inW?: Record<string, any>) => {
          const out: Record<string, any> = { ...(inW || {}) }
          delete out[df]
          delete out[`${df}__gte`]
          delete out[`${df}__lte`]
          delete out[`${df}__gt`]
          delete out[`${df}__lt`]
          return Object.keys(out).length ? out : undefined
        }
        const startIso = asIsoZ(gte!)
        const endIso = asIsoZ(lt!)
        const prevStartIso = asIsoZ(prevStartYmd)
        const prevEndIso = asIsoZ(prevEndYmd)

        // Multi-series + Legend path (both present - create tiles per legend, each with multiple series)
        const hasLegend = Array.isArray(legend) ? legend.length > 0 : !!legend
        if (seriesArr.length > 1 && hasLegend) {
          const requests: Array<any> = []
          seriesArr.forEach((s, idx) => {
            const aggS = ((s.agg ?? reqBase.agg) as any)
            const yS = (s.y as any)
            const measureS = (s.measure as any)
            // Only pass y+agg OR measure, not both
            if (yS) {
              requests.push({ key: `s${idx}_cur`, source, datasourceId, dateField: df, start: startIso, end: endIso, where: stripDateFromWhere(effectiveWhere), legend: legend as any, agg: aggS, y: yS })
              requests.push({ key: `s${idx}_prev`, source, datasourceId, dateField: df, start: prevStartIso, end: prevEndIso, where: stripDateFromWhere(effectiveWhere), legend: legend as any, agg: aggS, y: yS })
            } else if (measureS) {
              requests.push({ key: `s${idx}_cur`, source, datasourceId, dateField: df, start: startIso, end: endIso, where: stripDateFromWhere(effectiveWhere), legend: legend as any, measure: measureS })
              requests.push({ key: `s${idx}_prev`, source, datasourceId, dateField: df, start: prevStartIso, end: prevEndIso, where: stripDateFromWhere(effectiveWhere), legend: legend as any, measure: measureS })
            }
          })
          const batch = await Api.periodTotalsBatch({ requests })
          
          // Collect all legend keys across all series
          const allLegendKeys = new Set<string>()
          seriesArr.forEach((s, idx) => {
            const curResult = batch.results[`s${idx}_cur`] || {}
            const prevResult = batch.results[`s${idx}_prev`] || {}
            Object.keys((curResult as any).totals || {}).forEach(k => allLegendKeys.add(String(k)))
            Object.keys((prevResult as any).totals || {}).forEach(k => allLegendKeys.add(String(k)))
          })
          
          // Build byLegend structure with bySeries for each legend value
          const byLegend: Record<string, any> = {}
          allLegendKeys.forEach(legendKey => {
            const bySeries: Record<string, KpiValue> = {}
            let legendCurTotal = 0
            let legendPrevTotal = 0
            
            seriesArr.forEach((s, idx) => {
              const seriesLabel = s.label || s.y || s.measure || `series_${idx + 1}`
              const curResult = batch.results[`s${idx}_cur`] || {}
              const prevResult = batch.results[`s${idx}_prev`] || {}
              const c = Number(((curResult as any).totals || {})[legendKey] || 0)
              const p = Number(((prevResult as any).totals || {})[legendKey] || 0)
              bySeries[seriesLabel] = {
                current: c,
                previous: p,
                absoluteDelta: c - p,
                percentChange: pctChange(c, p),
              }
              legendCurTotal += c
              legendPrevTotal += p
            })
            
            byLegend[legendKey] = {
              bySeries,
              totals: {
                current: legendCurTotal,
                previous: legendPrevTotal,
                absoluteDelta: legendCurTotal - legendPrevTotal,
                percentChange: pctChange(legendCurTotal, legendPrevTotal),
              },
            }
          })
          
          // Compute overall totals
          let overallCur = 0
          let overallPrev = 0
          Object.values(byLegend).forEach((leg: any) => {
            overallCur += leg.totals?.current || 0
            overallPrev += leg.totals?.previous || 0
          })
          
          return {
            mode,
            legendField: legend,
            byLegend,
            totals: {
              current: overallCur,
              previous: overallPrev,
              absoluteDelta: overallCur - overallPrev,
              percentChange: pctChange(overallCur, overallPrev),
            },
          } as any
        }

        // Multi-series only path (no legend)
        if (seriesArr.length > 1) {
          const requests: Array<any> = []
          seriesArr.forEach((s, idx) => {
            const aggS = ((s.agg ?? reqBase.agg) as any)
            requests.push({ key: `s${idx}_cur`, source, datasourceId, dateField: df, start: startIso, end: endIso, where: stripDateFromWhere(effectiveWhere), agg: aggS, y: (s.y as any), measure: (s.measure as any) })
            requests.push({ key: `s${idx}_prev`, source, datasourceId, dateField: df, start: prevStartIso, end: prevEndIso, where: stripDateFromWhere(effectiveWhere), agg: aggS, y: (s.y as any), measure: (s.measure as any) })
          })
          const batch = await Api.periodTotalsBatch({ requests })
          const pairs = seriesArr.map((s, idx) => {
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
          pairs.forEach(([label, d, c, p]) => { deltas[String(label)] = d; curTotalsBy[String(label)] = c; prevTotalsBy[String(label)] = p; curOverall += c; prevOverall += p })
          const safeDiv = (num: number, den: number) => (den === 0 ? (num !== 0 ? 100 : 0) : ((num / Math.abs(den)) * 100))
          const curShareTotals: Record<string, number> = {}
          const prevShareTotals: Record<string, number> = {}
          Object.keys(curTotalsBy).forEach((k) => { curShareTotals[k] = safeDiv(curTotalsBy[k] || 0, curOverall) })
          Object.keys(prevTotalsBy).forEach((k) => { prevShareTotals[k] = safeDiv(prevTotalsBy[k] || 0, prevOverall) })
          return { mode, seriesLabels: Object.keys(curTotalsBy), bySeries: Object.fromEntries(Object.keys(curTotalsBy).map(k => [k, { current: curTotalsBy[k], previous: prevTotalsBy[k], absoluteDelta: curTotalsBy[k]-prevTotalsBy[k], percentChange: pctChange(curTotalsBy[k], prevTotalsBy[k]) } ])), totals: { current: curOverall, previous: prevOverall, absoluteDelta: curOverall - prevOverall, percentChange: pctChange(curOverall, prevOverall) } } as any
        }

        // Legend only path
        const hasLegendOnly = Array.isArray(legend) ? legend.length > 0 : !!legend
        if (hasLegendOnly) {
          const aggEff = (reqBase.agg as any)
          const cmp = await Api.periodTotalsCompare({
            source,
            datasourceId,
            dateField: df,
            start: startIso,
            end: endIso,
            prevStart: prevStartIso,
            prevEnd: prevEndIso,
            where: stripDateFromWhere(effectiveWhere),
            legend: legend as any,
            agg: aggEff,
            y: (reqBase.y as any),
            measure: (reqBase.measure as any),
          })
          const cur = (cmp.cur || {}) as any
          const prev = (cmp.prev || {}) as any
          const keys = new Set<string>([...Object.keys(cur.totals || {}), ...Object.keys(prev.totals || {})])
          const byLegend: Record<string, KpiValue> = {}
          let curSum = 0
          let prevSum = 0
          keys.forEach((k) => {
            const c = Number((cur.totals || {})[k] || 0)
            const p = Number((prev.totals || {})[k] || 0)
            curSum += c
            prevSum += p
            byLegend[(k == null || String(k).trim()==='') ? 'None' : String(k)] = {
              current: c,
              previous: p,
              absoluteDelta: c - p,
              percentChange: pctChange(c, p),
            }
          })
          // Also compute overall totals without legend for share
          const stripLegend = (inW?: Record<string, any>) => {
            const out: any = { ...(stripDateFromWhere(inW) || {}) }
            const legends = Array.isArray(legend) ? legend : [legend]
            legends.filter(Boolean).forEach((lg) => {
              const key = String(lg)
              delete out[key]
              delete out[`${key}__gte`]
              delete out[`${key}__lte`]
              delete out[`${key}__gt`]
              delete out[`${key}__lt`]
            })
            return Object.keys(out).length ? out : undefined
          }
          const cmpAll = await Api.periodTotalsCompare({ source, datasourceId, dateField: df, start: startIso, end: endIso, prevStart: prevStartIso, prevEnd: prevEndIso, where: stripLegend(effectiveWhere), agg: reqBase.agg as any, y: (reqBase.y as any), measure: (reqBase.measure as any) })
          const curOverall = Number((cmpAll.cur || {}).total || 0)
          const prevOverall = Number((cmpAll.prev || {}).total || 0)
          const safeDiv = (num: number, den: number) => (den === 0 ? (num !== 0 ? 100 : 0) : ((num / Math.abs(den)) * 100))
          const curShareTotals: Record<string, number> = {}
          const prevShareTotals: Record<string, number> = {}
          keys.forEach((k) => {
            const a = Number((cur.totals || {})[k] || 0)
            const b = Number((prev.totals || {})[k] || 0)
            curShareTotals[String(k)] = safeDiv(a, curOverall)
            prevShareTotals[String(k)] = safeDiv(b, prevOverall)
          })
          return { mode, legendField: legend, byLegend, totals: { current: curSum, previous: prevSum, absoluteDelta: curSum - prevSum, percentChange: pctChange(curSum, prevSum) }, overall: { current: curOverall, previous: prevOverall, absoluteDelta: curOverall - prevOverall, percentChange: pctChange(curOverall, prevOverall) }, } as any
        }

        // Single series/value path (fallback for hasGlobalWindow without series or legend)
        const cmp = await Api.periodTotalsCompare({ source, datasourceId, dateField: df, start: startIso, end: endIso, prevStart: prevStartIso, prevEnd: prevEndIso, where: stripDateFromWhere(effectiveWhere), agg: (reqBase.agg as any), y: (reqBase.y as any), measure: (reqBase.measure as any) })
        const c = Number((cmp.cur || {}).total || 0)
        const p = Number((cmp.prev || {}).total || 0)
        return { mode, single: { current: c, previous: p, absoluteDelta: c - p, percentChange: pctChange(c, p) } }
      }

      // Multi-series + Legend path (both present)
      const hasLegend = Array.isArray(legend) ? legend.length > 0 : !!legend
      if (seriesArr.length > 1 && hasLegend) {
        // Fetch data for each series with legend breakdown separately
        const seriesResults = await Promise.all(
          seriesArr.map(s => {
            const seriesReq = { ...reqBase, legend }
            // Use individual series y/agg/measure instead of series array
            if (s.y) {
              seriesReq.y = s.y as any
              seriesReq.agg = (s.agg || reqBase.agg) as any
              delete (seriesReq as any).measure
            } else if (s.measure) {
              seriesReq.measure = s.measure as any
              delete (seriesReq as any).y
              delete (seriesReq as any).agg
            }
            return computePeriodDeltas(seriesReq as any)
          })
        )
        
        // Collect all legend keys
        const allLegendKeys = new Set<string>()
        seriesResults.forEach(res => {
          Object.keys(res.curTotals || {}).forEach(k => allLegendKeys.add(String(k)))
          Object.keys(res.prevTotals || {}).forEach(k => allLegendKeys.add(String(k)))
        })
        
        // Build byLegend structure with bySeries for each legend value
        const byLegend: Record<string, any> = {}
        allLegendKeys.forEach(legendKey => {
          const bySeries: Record<string, KpiValue> = {}
          let legendCurTotal = 0
          let legendPrevTotal = 0
          
          seriesArr.forEach((s, idx) => {
            const seriesLabel = s.label || s.y || s.measure || `series_${idx + 1}`
            const res = seriesResults[idx]
            const c = Number((res.curTotals || {})[legendKey] || 0)
            const p = Number((res.prevTotals || {})[legendKey] || 0)
            bySeries[seriesLabel] = {
              current: c,
              previous: p,
              absoluteDelta: c - p,
              percentChange: pctChange(c, p),
            }
            legendCurTotal += c
            legendPrevTotal += p
          })
          
          byLegend[legendKey] = {
            bySeries,
            totals: {
              current: legendCurTotal,
              previous: legendPrevTotal,
              absoluteDelta: legendCurTotal - legendPrevTotal,
              percentChange: pctChange(legendCurTotal, legendPrevTotal),
            },
          }
        })
        
        // Compute overall totals
        let overallCur = 0
        let overallPrev = 0
        Object.values(byLegend).forEach((leg: any) => {
          overallCur += leg.totals?.current || 0
          overallPrev += leg.totals?.previous || 0
        })
        
        const out = {
          mode,
          legendField: legend,
          byLegend,
          totals: {
            current: overallCur,
            previous: overallPrev,
            absoluteDelta: overallCur - overallPrev,
            percentChange: pctChange(overallCur, overallPrev),
          },
        }
        try { console.log('[KPICardDebug] multi-series+legend result (deltas.ts)', out) } catch {}
        return out
      }

      // Multi-series only path (no legend)
      if (seriesArr.length > 1) {
        const res = await computePeriodDeltas({ ...reqBase, series: seriesArr as any })
        const keys = new Set<string>([
          ...Object.keys(res.curTotals || {}),
          ...Object.keys(res.prevTotals || {}),
        ])
        const bySeries: Record<string, KpiValue> = {}
        let curSum = 0
        let prevSum = 0
        keys.forEach((k) => {
          const c = Number((res.curTotals || {})[k] || 0)
          const p = Number((res.prevTotals || {})[k] || 0)
          curSum += c
          prevSum += p
          bySeries[String(k)] = { current: c, previous: p, absoluteDelta: c - p, percentChange: pctChange(c, p) }
        })
        const totals: KpiValue = { current: curSum, previous: prevSum, absoluteDelta: curSum - prevSum, percentChange: pctChange(curSum, prevSum) }
        const out = { mode, seriesLabels: Array.from(keys.values()), bySeries, totals }
        try { console.log('[KPICardDebug] series result (deltas.ts)', out) } catch {}
        return out
      }

      // Legend only path
      const hasLegendOnly = Array.isArray(legend) ? legend.length > 0 : !!legend
      if (hasLegendOnly) {
        const res = await computePeriodDeltas({ ...reqBase, legend })
        const keys = new Set<string>([
          ...Object.keys(res.curTotals || {}),
          ...Object.keys(res.prevTotals || {}),
        ])
        const byLegend: Record<string, KpiValue> = {}
        let curSum = 0
        let prevSum = 0
        keys.forEach((k) => {
          const c = Number((res.curTotals || {})[k] || 0)
          const p = Number((res.prevTotals || {})[k] || 0)
          curSum += c
          prevSum += p
          byLegend[(k == null || String(k).trim()==='') ? 'None' : String(k)] = {
            current: c,
            previous: p,
            absoluteDelta: c - p,
            percentChange: pctChange(c, p),
          }
        })
        const totals: KpiValue = {
          current: Number(res.curTotal || curSum),
          previous: Number(res.prevTotal || prevSum),
          absoluteDelta: Number(res.curTotal || curSum) - Number(res.prevTotal || prevSum),
          percentChange: pctChange(Number(res.curTotal || curSum), Number(res.prevTotal || prevSum)),
        }
        const overall: KpiValue | undefined = (res.curOverall != null || res.prevOverall != null)
          ? {
              current: Number(res.curOverall || 0),
              previous: Number(res.prevOverall || 0),
              absoluteDelta: Number(res.curOverall || 0) - Number(res.prevOverall || 0),
              percentChange: pctChange(Number(res.curOverall || 0), Number(res.prevOverall || 0)),
            }
          : undefined
        const out: KpiResult & { share?: { cur?: Record<string, number>; prev?: Record<string, number> } } = {
          mode,
          legendField: legend,
          byLegend,
          totals,
          overall,
          share: { cur: res.curShareTotals, prev: res.prevShareTotals },
        }
        try { console.log('[KPICardDebug] legend result (deltas.ts)', out) } catch {}
        return out
      }

      // Single-series fallback (when seriesArr.length === 1 or 0)
      if (seriesArr.length === 1) {
        const res = await computePeriodDeltas({ ...reqBase, series: seriesArr as any })
        const keys = new Set<string>([
          ...Object.keys(res.curTotals || {}),
          ...Object.keys(res.prevTotals || {}),
        ])
        const bySeries: Record<string, KpiValue> = {}
        let curSum = 0
        let prevSum = 0
        keys.forEach((k) => {
          const c = Number((res.curTotals || {})[k] || 0)
          const p = Number((res.prevTotals || {})[k] || 0)
          curSum += c
          prevSum += p
          bySeries[String(k)] = { current: c, previous: p, absoluteDelta: c - p, percentChange: pctChange(c, p) }
        })
        const totals: KpiValue = { current: curSum, previous: prevSum, absoluteDelta: curSum - prevSum, percentChange: pctChange(curSum, prevSum) }
        const out = { mode, seriesLabels: Array.from(keys.values()), bySeries, totals }
        try { console.log('[KPICardDebug] series result (deltas.ts)', out) } catch {}
        return out
      }

      // Single series/value path
      const res = await computePeriodDeltas(reqBase as any)
      const single: KpiValue = {
        current: Number(res.curTotal || 0),
        previous: Number(res.prevTotal || 0),
        absoluteDelta: Number(res.curTotal || 0) - Number(res.prevTotal || 0),
        percentChange: pctChange(Number(res.curTotal || 0), Number(res.prevTotal || 0)),
      }
      const out = { mode, single }
      try { console.log('[KPICardDebug] single result (deltas.ts)', out) } catch {}
      return out
    },
  })

  return q
}
