// Module-level helpers extracted verbatim from ChartCard.tsx (L75–367).
// These take no component state — pure functions plus two self-contained hooks.
import { useState, useEffect } from 'react'
import { Api, QueryApi } from '@/lib/api'
import { compileFormula, parseReferences } from '@/lib/formula'

// Helper: proper-case labels (Title Case, replace _ and - with spaces)
export function toProperCase(s: string): string {
  const str = String(s ?? '')
  return str
    .replace(/[_-]+/g, ' ')
    .split(/\s+/)
    .map(w => (w ? (w[0].toUpperCase() + w.slice(1).toLowerCase()) : w))
    .join(' ')
}

// Helper: split legend label into base and category parts
// Format: "SeriesName (Category)" => { base: "SeriesName", cat: "Category" }
export function splitLegend(label: string): { base: string; cat: string } {
  const str = String(label || '').trim()
  const match = str.match(/^(.+?)\s*\(([^)]+)\)\s*$/)
  if (match) {
    return { base: match[1].trim(), cat: match[2].trim() }
  }
  return { base: str, cat: '' }
}

// Helper: extract base label without category suffix
export function extractBaseLabel(label: string): string {
  return splitLegend(label).base
}

// Fallback string values from current query result rows
export function fallbackStringsFor(field: string, qdata?: any): string[] {
  try {
    const cols: string[] = (qdata?.columns as string[]) || []
    const rows: any[] = (qdata?.rows as any[]) || []
    if (!cols.length || !rows.length) return []
    const idx = cols.indexOf(field)
    const set = new Set<string>()
    if (idx >= 0) {
      rows.forEach((r: any) => { const v = Array.isArray(r) ? r[idx] : r?.[field]; if (v != null) set.add(String(v)) })
      return Array.from(set.values()).sort()
    }
    if (typeof rows[0] === 'object' && !Array.isArray(rows[0]) && (field in (rows[0] || {}))) {
      rows.forEach((r: any) => { const v = r?.[field]; if (v != null) set.add(String(v)) })
      return Array.from(set.values()).sort()
    }
    return []
  } catch { return [] }
}

// Shared, tolerant date parser for filterbar type inference
export function parseDateLoose(v: any): Date | null {
  if (v == null) return null
  if (v instanceof Date) return isNaN(v.getTime()) ? null : v
  const s = String(v).trim()
  if (!s) return null
  if (/^\d{10,13}$/.test(s)) { const n = Number(s); const ms = s.length === 10 ? n*1000 : n; const d = new Date(ms); return isNaN(d.getTime())?null:d }
  const norm = s.replace(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(:\d{2})?)$/, '$1T$2')
  let d = new Date(norm); if (!isNaN(d.getTime())) return d
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})$/); if (iso) { d = new Date(`${iso[1]}-${iso[2]}-${iso[3]}T00:00:00`); return isNaN(d.getTime())?null:d }
  // Support YYYY-MM (assume first of month)
  const ym = s.match(/^(\d{4})-(\d{2})$/)
  if (ym) { const yyyy = Number(ym[1]); const mm = Math.max(1, Math.min(12, Number(ym[2]))); d = new Date(`${yyyy}-${String(mm).padStart(2,'0')}-01T00:00:00`); return isNaN(d.getTime())?null:d }
  // Support MMM-YYYY or MMM YYYY (English month names)
  const my = s.match(/^([A-Za-z]{3,9})[-\s](\d{4})$/)
  if (my) {
    const map: Record<string, number> = { jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,sept:8,oct:9,nov:10,dec:11 }
    const raw = String(my[1]).toLowerCase()
    const key = raw.startsWith('sept') ? 'sep' : raw.slice(0,3)
    const mi = map[key as keyof typeof map]
    if (mi != null) { const yyyy = Number(my[2]); d = new Date(`${yyyy}-${String(mi+1).padStart(2,'0')}-01T00:00:00`); return isNaN(d.getTime())?null:d }
  }
  const m = s.match(/^([0-1]?\d)\/([0-3]?\d)\/(\d{4})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?$/)
  if (m) { const mm=Number(m[1])-1, dd=Number(m[2]), yyyy=Number(m[3]); const t=m[4]||'00:00:00'; d = new Date(`${yyyy}-${String(mm+1).padStart(2,'0')}-${String(dd).padStart(2,'0')}T${t.length===5?t+':00':t}`); return isNaN(d.getTime())?null:d }
  return null
}

// Helper: load distinct string values for a field (omitting self constraint)
export function useDistinctStrings(
  source?: string,
  datasourceId?: string,
  baseWhere?: Record<string, any>,
  customCols?: Array<{ name: string; formula: string }>,
) {
  const [cache, setCache] = useState<Record<string, string[]>>({})
  const [loading, setLoading] = useState<Record<string, boolean>>({})
  // Reset cache whenever the underlying constraints or source change
  useEffect(() => { setCache({}); setLoading({}) }, [source, datasourceId, JSON.stringify(baseWhere || {}), JSON.stringify(customCols || [])])
  const load = async (field: string) => {
    try {
      if (!source) return
      // Mark as loading
      setLoading((prev) => ({ ...prev, [field]: true }))
      const isCustom = !!(customCols || []).find((c) => String(c?.name ?? '').trim().toLowerCase() === String(field ?? '').trim().toLowerCase())
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        try { console.debug('[ChartCard] [DistinctsDebug] start', { field, isCustom, baseWhere, source }) } catch {}
      }
      const isDev = (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production')
      const safeQuerySpec = async (payload: any) => {
        try { return await QueryApi.querySpec(payload) } catch (err: any) {
          if (isDev) { try { console.debug('[ChartCard] [DistinctsDebug] query-spec-error', { field, message: err?.message || String(err) }) } catch {} }
          return undefined as any
        }
      }
      if (!isCustom) {
        const omit = { ...(baseWhere || {}) }
        // Do not constrain by the same field (and any ops like __gte/__in)
        Object.keys(omit).forEach((k) => { if (k === field || k.startsWith(`${field}__`)) delete (omit as any)[k] })
        // Try backend distinct endpoint first for base fields (fast and authoritative), then fall back to paging
        try {
          if (typeof (Api as any).distinct === 'function') {
            if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
              try { console.debug('[ChartCard] [DistinctsDebug] request-base-api', { field, source, where: Object.keys(omit).length ? omit : undefined, datasourceId }) } catch {}
            }
            const res = await (Api as any).distinct({ source: String(source), field: String(field), where: Object.keys(omit).length ? omit : undefined, datasourceId })
            const arr = ((res?.values || []) as any[]).map((v) => (v != null ? String(v) : null)).filter((v) => v != null) as string[]
            const dedup = Array.from(new Set(arr).values()).sort()
            if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
              try { console.debug('[ChartCard] [DistinctsDebug] result-base-api', { field, count: dedup.length, sample: dedup.slice(0, 20) }) } catch {}
            }
            setCache((prev) => ({ ...prev, [field]: dedup }))
            setLoading((prev) => ({ ...prev, [field]: false }))
            return
          }
        } catch (e: any) {
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try {
              const msg = e?.message || String(e)
              const m = String(msg).match(/HTTP\s+(\d{3})/)
              const code = m ? Number(m[1]) : null
              const logger = (code && code >= 500) ? console.error : console.debug
              logger('[ChartCard] [DistinctsDebug] result-base-api-error', { field, code, message: msg })
            } catch {}
          }
        }
        const pageSize = 5000
        let offset = 0
        const setVals = new Set<string>()
        while (true) {
          const spec: any = { source, select: [field], where: Object.keys(omit).length ? omit : undefined, limit: pageSize, offset }
          const res = await safeQuerySpec({ spec, datasourceId, limit: pageSize, offset, includeTotal: false })
          const cols = ((res?.columns || []) as string[])
          const idx = cols.indexOf(field)
          const rows = ((res?.rows || []) as any[])
          rows.forEach((row: any) => {
            let v: any
            if (idx >= 0) v = Array.isArray(row) ? row[idx] : (row?.[field])
            else if (Array.isArray(row)) v = row[0]
            else v = row?.[field]
            if (v !== null && v !== undefined) setVals.add(String(v))
          })

          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try { console.debug('[ChartCard] [DistinctsDebug] page-base', { field, offset, got: rows.length, setSize: setVals.size, idx, cols }) } catch {}
          }
          if (rows.length === 0 || rows.length < pageSize) break
          offset += pageSize
          if (offset >= 500000) break // safety cap (rows scanned)
        }
        const arr = Array.from(setVals.values()).sort()
        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
          try { console.debug('[ChartCard] [DistinctsDebug] result-base', { field, count: arr.length, sample: arr.slice(0, 20), where: omit }) } catch {}
        }
        setCache((prev) => ({ ...prev, [field]: arr }))
        setLoading((prev) => ({ ...prev, [field]: false }))
        return
      }
      // Custom column path: compute values client-side from base refs
      const c = (customCols || []).find((cc) => cc.name === field)!
      const customNames = new Set<string>((customCols || []).map((cc) => cc.name))
      const DERIVED_RE = /^(.*) \((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/
      const serverWhere: Record<string, any> = {}
      Object.entries(baseWhere || {}).forEach(([k, v]) => { if (!DERIVED_RE.test(k) && !customNames.has(k)) (serverWhere as any)[k] = v })
      // Build dependency graph among custom columns and gather base refs
      const customMap = new Map<string, { name: string; formula: string }>((customCols || []).map((cc) => [cc.name, cc]))
      const visited = new Set<string>()
      const order: string[] = []
      const baseRefs = new Set<string>()
      const visit = (name: string) => {
        if (visited.has(name)) return
        visited.add(name)
        const def = customMap.get(name)
        if (!def) return
        const refs = (parseReferences(def.formula).row || []) as string[]
        refs.forEach((r) => {
          if (customMap.has(r)) visit(r)
          else baseRefs.add(r)
        })
        order.push(name)
      }
      ;((parseReferences(c.formula).row || []) as string[]).forEach((r) => { if (customMap.has(r)) visit(r); else baseRefs.add(r) })
      const selectBase = Array.from(baseRefs.values())
      // Ensure we also fetch any base fields used in serverWhere filters
      Object.keys(serverWhere).forEach((k) => { if (!selectBase.includes(k)) selectBase.push(k) })
      if (selectBase.length === 0) { setCache((prev) => ({ ...prev, [field]: [] })); setLoading((prev) => ({ ...prev, [field]: false })); return }
      const pageSize = 5000
      let offset = 0
      const cf = compileFormula(c.formula)
      const compiledDeps = new Map<string, ReturnType<typeof compileFormula>>()
      order.forEach((name) => { const def = customMap.get(name)!; compiledDeps.set(name, compileFormula(def.formula)) })
      const setVals = new Set<string>()
      // 1) Try server DISTINCT of baseRef tuples to avoid scanning full table
      try {
        if (selectBase.length > 0) {
          // Build SQL: SELECT DISTINCT [b1],[b2],... FROM [source] WHERE ... ORDER BY 1
          const qIdent = (nm: string) => `[${String(nm).replace(/]/g, ']]')}]`
          const qSource = (src: string) => String(src).split('.').map(qIdent).join('.')
          const qLit = (v: any) => { if (v == null) return 'NULL'; if (typeof v === 'number' && Number.isFinite(v)) return String(v); if (v instanceof Date) return `'${v.toISOString().slice(0, 19).replace('T',' ')}'`; const s = String(v); return `'${s.replace(/'/g, "''")}'` }
          const buildWhere = (w?: Record<string, any>) => {
            if (!w || Object.keys(w).length === 0) return ''
            const parts: string[] = []
            Object.entries(w).forEach(([k, val]) => {
              let col = k; let op: string = '='
              if (k.endsWith('__gte')) { col = k.slice(0,-5); op = '>=' }
              else if (k.endsWith('__lte')) { col = k.slice(0,-5); op = '<=' }
              else if (k.endsWith('__gt')) { col = k.slice(0,-4); op = '>' }
              else if (k.endsWith('__lt')) { col = k.slice(0,-4); op = '<' }
              else if (k.endsWith('__in')) { col = k.slice(0,-4); op = 'IN' }
              if (op === 'IN' || Array.isArray(val)) {
                const arr = Array.isArray(val) ? val : [val]
                if (arr.length) parts.push(`${qIdent(col)} IN (${arr.map(qLit).join(', ')})`)
              } else {
                parts.push(`${qIdent(col)} ${op} ${qLit(val)}`)
              }
            })
            return parts.length ? ` WHERE ${parts.join(' AND ')}` : ''
          }
          const colsSql = selectBase.map(qIdent).join(', ')
          const sql = `SELECT DISTINCT ${colsSql} FROM ${qSource(String(source))}${buildWhere(Object.keys(serverWhere).length ? serverWhere : undefined)} ORDER BY 1`
          const r = await Api.query({ sql, datasourceId, limit: 500000, offset: 0 })
          const cols = (r.columns || []) as string[]
          const rows = (r.rows || []) as any[]
          rows.forEach((arr: any[]) => {
            const env: Record<string, any> = {}
            cols.forEach((nm, i) => { env[nm] = arr[i] })
            // compute dependent custom columns into env
            order.forEach((dep) => { const fn = compiledDeps.get(dep); try { env[dep] = fn ? fn.exec({ row: env }) : null } catch { env[dep] = null } })
            try { const v = cf.exec({ row: env }); if (v != null) setVals.add(String(v)) } catch {}
          })
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try { console.debug('[ChartCard] [DistinctsDebug] result-custom-sql', { field, baseTuples: rows.length, setSize: setVals.size, selectBase }) } catch {}
          }
        }
      } catch (e:any) {
        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
          try { console.error('[ChartCard] [DistinctsDebug] result-custom-sql-error', { field, error: e?.message || String(e) }) } catch {}
        }
      }
      // 2) Fallback to incremental paging only if SQL path yielded nothing
      if (setVals.size === 0) {
        while (true) {
          const spec: any = { source, select: selectBase, where: Object.keys(serverWhere).length ? serverWhere : undefined, limit: pageSize, offset }
          const res = await safeQuerySpec({ spec, datasourceId, limit: pageSize, offset, includeTotal: false })
          const cols = ((res?.columns || []) as string[])
          const rows = ((res?.rows || []) as any[])
          rows.forEach((arr: any[]) => {
            const env: Record<string, any> = {}
            cols.forEach((name, i) => { env[name] = arr[i] })
            // compute dependent custom columns into env
            order.forEach((dep) => {
              const fn = compiledDeps.get(dep)
              try { env[dep] = fn ? fn.exec({ row: env }) : null } catch { env[dep] = null }
            })
            try {
              const v = cf.exec({ row: env })
              if (v !== null && v !== undefined) setVals.add(String(v))
            } catch {}
          })
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try { console.debug('[ChartCard] [DistinctsDebug] page-custom', { field, offset, got: rows.length, setSize: setVals.size, selectBase }) } catch {}
          }
          if (rows.length === 0 || rows.length < pageSize) break
          offset += pageSize
          if (offset >= 500000) break // safety cap (rows scanned)
        }
      }
      const out = Array.from(setVals.values()).sort()
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        try { console.debug('[ChartCard] [DistinctsDebug] result-custom', { field, count: out.length, sample: out.slice(0, 20), serverWhere }) } catch {}
      }
      setCache((prev) => ({ ...prev, [field]: out }))
      setLoading((prev) => ({ ...prev, [field]: false }))
    } catch (e: any) {
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        try { console.error('[ChartCard] [DistinctsDebug] error', { field, error: e?.message || String(e) }) } catch {}
      }
      setCache((prev) => ({ ...prev, [field]: [] }))
      setLoading((prev) => ({ ...prev, [field]: false }))
    }
  }
  return { cache, load, loading }
}

export function useDebounced<T>(val: T, delay = 350): T {
  const [v, setV] = useState<T>(val as T)
  useEffect(() => { const t = setTimeout(() => setV(val), delay); return () => { try { clearTimeout(t) } catch {} } }, [val, delay])
  return v
}
