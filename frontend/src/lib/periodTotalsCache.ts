import { Api } from '@/lib/api'

export type PeriodTotalsArgs = {
  source: string
  datasourceId?: string
  dateField: string
  start: string
  end: string
  where?: Record<string, unknown>
  legend?: string | string[]
  agg?: 'none' | 'count' | 'distinct' | 'avg' | 'sum' | 'min' | 'max'
  y?: string
  measure?: string
}

export type PeriodTotalsResp = {
  totals?: Record<string, number>
  total?: number
}

const ttlMsDefault = 30_000 // 30s

const inflight = new Map<string, Promise<PeriodTotalsResp>>()
const cache = new Map<string, { exp: number; val: PeriodTotalsResp }>()

function stableStringify(v: any): string {
  try {
    if (v === null || typeof v !== 'object') return JSON.stringify(v)
    if (Array.isArray(v)) return '[' + v.map(stableStringify).join(',') + ']'
    const keys = Object.keys(v).sort()
    return '{' + keys.map(k => JSON.stringify(k) + ':' + stableStringify((v as any)[k])).join(',') + '}'
  } catch {
    return JSON.stringify(v)
  }
}

function buildKey(args: PeriodTotalsArgs): string {
  const { source, datasourceId, dateField, start, end, where, legend, agg, y, measure } = args
  return stableStringify({ source, datasourceId, dateField, start, end, where, legend, agg, y, measure })
}

export async function periodTotalsCached(args: PeriodTotalsArgs, ttlMs: number = ttlMsDefault): Promise<PeriodTotalsResp> {
  const key = buildKey(args)
  const now = Date.now()
  const hit = cache.get(key)
  if (hit && hit.exp > now) return hit.val
  const pending = inflight.get(key)
  if (pending) return pending
  const p = Api.periodTotals(args as any)
    .then((res: any) => {
      cache.set(key, { exp: now + ttlMs, val: res as PeriodTotalsResp })
      inflight.delete(key)
      return res as PeriodTotalsResp
    })
    .catch((e: any) => { inflight.delete(key); throw e })
  inflight.set(key, p)
  return p
}

export function clearPeriodTotalsCache() {
  cache.clear(); inflight.clear()
}
