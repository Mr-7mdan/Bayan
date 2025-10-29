// Lightweight Excel-like formula evaluator for QuerySpec custom columns
// Supports:
// - Column tokens: [@col] for row value, [col] for partition range (array)
// - Operators: + - * / % ** (use ^ in formulas; we convert to **)
// - Functions: SUM, AVG, MIN, MAX, COUNT, COUNTIF, IF, AND, OR, NOT,
//              YEAR, QUARTER, MONTH, WEEKNUM, DAY, HOUR, MINUTE,
//              CONCAT, LOWER, UPPER, TRIM, COALESCE, ISBLANK
//
// NOTE: This is intentionally minimal and sandboxed by only exposing FN, RV, RG into the compiled function scope.
// Formulas are assumed to be created by our UI. We still avoid exposing global scope.

export type Row = Record<string, any>
export type Range = Record<string, any[]>

export type CompiledFormula = {
  source: string
  refsRow: string[]
  refsRange: string[]
  exec: (ctx: { row: Row; range?: Range }) => any
  execDebug: (ctx: { row: Row; range?: Range }) => any
}

function toDate(v: any): Date | null {
  if (v == null) return null
  if (v instanceof Date) return isNaN(v.getTime()) ? null : v
  // Numeric epoch
  if (typeof v === 'number') {
    const ms = v < 1e12 ? v * 1000 : v
    const d = new Date(ms)
    return isNaN(d.getTime()) ? null : d
  }
  const s = String(v).trim()
  if (!s) return null
  // Handle integer-like strings as epoch
  if (/^\d{10,13}$/.test(s)) {
    const n = Number(s)
    const ms = s.length === 10 ? n * 1000 : n
    const d = new Date(ms)
    return isNaN(d.getTime()) ? null : d
  }
  // Normalize space to 'T' for ISO-like strings
  const norm = s.replace(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(:\d{2})?)$/, '$1T$2')
  const d = new Date(norm)
  if (!isNaN(d.getTime())) return d
  // Explicit YYYY-MM-DD fallback
  const isoDateOnly = s.match(/^(\d{4})-(\d{2})-(\d{2})$/)
  if (isoDateOnly) {
    const yyyy = Number(isoDateOnly[1])
    const mm = Number(isoDateOnly[2])
    const dd = Number(isoDateOnly[3])
    const d0 = new Date(`${String(yyyy).padStart(4,'0')}-${String(mm).padStart(2,'0')}-${String(dd).padStart(2,'0')}T00:00:00`)
    return isNaN(d0.getTime()) ? null : d0
  }
  // Try MM/DD/YYYY as fallback
  const m = s.match(/^([0-1]?\d)\/([0-3]?\d)\/(\d{4})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?$/)
  if (m) {
    const mm = Number(m[1]) - 1
    const dd = Number(m[2])
    const yyyy = Number(m[3])
    const t = m[4] || '00:00:00'
    const d2 = new Date(`${yyyy}-${String(mm + 1).padStart(2,'0')}-${String(dd).padStart(2,'0')}T${t.length===5? t+':00': t}`)
    return isNaN(d2.getTime()) ? null : d2
  }
  return null
}

function weekNumber(d: Date): number {
  // ISO week number
  const date = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()))
  // Thursday in current week decides the year
  date.setUTCDate(date.getUTCDate() + 4 - (date.getUTCDay() || 7))
  // January 1st of this year
  const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1))
  // Calculate full weeks to the date
  const weekNo = Math.ceil(((date.getTime() - yearStart.getTime()) / 86400000 + 1) / 7)
  return weekNo
}

const FN = {
  SUM: (arr: any[]) => (Array.isArray(arr) ? arr.reduce((a, b) => a + (Number(b) || 0), 0) : 0),
  AVG: (arr: any[]) => (Array.isArray(arr) && arr.length ? arr.reduce((a, b) => a + (Number(b) || 0), 0) / arr.length : 0),
  MIN: (arr: any[]) => (Array.isArray(arr) && arr.length ? Math.min(...arr.map((v) => Number(v) || 0)) : 0),
  MAX: (arr: any[]) => (Array.isArray(arr) && arr.length ? Math.max(...arr.map((v) => Number(v) || 0)) : 0),
  COUNT: (arr: any[]) => (Array.isArray(arr) ? arr.filter((v) => v !== null && v !== undefined).length : 0),
  COUNTIF: (arr: any[], predicate: string) => {
    if (!Array.isArray(arr)) return 0
    // predicate like ">10", ">=5", "=x", "<15", "<>0"
    const m = /^(>=|<=|<>|=|>|<)\s*(.*)$/.exec(String(predicate || '').trim())
    if (!m) return 0
    const op = m[1]
    const rhsRaw = m[2]
    const rhsNum = Number(rhsRaw)
    return arr.filter((v) => {
      const lhsNum = Number(v)
      const lhsStr = String(v)
      const rhsStr = String(rhsRaw)
      switch (op) {
        case '>': return lhsNum > rhsNum
        case '>=': return lhsNum >= rhsNum
        case '<': return lhsNum < rhsNum
        case '<=': return lhsNum <= rhsNum
        case '=': return lhsStr === rhsStr || (isFinite(lhsNum) && isFinite(rhsNum) && lhsNum === rhsNum)
        case '<>': return lhsStr !== rhsStr && (!isFinite(lhsNum) || !isFinite(rhsNum) || lhsNum !== rhsNum)
        default: return false
      }
    }).length
  },
  IF: (cond: any, a: any, b: any) => (cond ? a : b),
  AND: (...args: any[]) => args.every(Boolean),
  OR: (...args: any[]) => args.some(Boolean),
  NOT: (x: any) => !x,
  CONCAT: (...args: any[]) => args.map((x) => (x === null || x === undefined ? '' : String(x))).join(''),
  LOWER: (s: any) => (s === null || s === undefined ? '' : String(s).toLowerCase()),
  UPPER: (s: any) => (s === null || s === undefined ? '' : String(s).toUpperCase()),
  TRIM: (s: any) => (s === null || s === undefined ? '' : String(s).trim()),
  COALESCE: (...args: any[]) => args.find((x) => x !== null && x !== undefined),
  ISBLANK: (x: any) => x === null || x === undefined || x === '',
  IFERROR: (value: any, alt: any) => {
    const v = value
    if (v === null || v === undefined) return alt
    if (typeof v === 'number' && Number.isNaN(v)) return alt
    if (typeof v === 'string' && v.toLowerCase() === 'error') return alt
    return v
  },
  CONTAINS: (text: any, sub: any) => String(text ?? '').includes(String(sub ?? '')),
  STARTSWITH: (text: any, pre: any) => String(text ?? '').startsWith(String(pre ?? '')),
  ENDSWITH: (text: any, suf: any) => String(text ?? '').endsWith(String(suf ?? '')),
  LIKE: (text: any, pattern: any) => {
    // SQL-like pattern: % => .*, _ => .
    const esc = String(pattern ?? '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    const rx = new RegExp('^' + esc.replace(/%/g, '.*').replace(/_/g, '.') + '$', 'i')
    return rx.test(String(text ?? ''))
  },
  DATEVALUE: (v: any) => toDate(v),
  YEAR: (v: any) => { const d = toDate(v); return d ? d.getFullYear() : null },
  QUARTER: (v: any) => { const d = toDate(v); return d ? Math.floor(d.getMonth() / 3) + 1 : null },
  MONTH: (v: any) => { const d = toDate(v); return d ? d.getMonth() + 1 : null },
  WEEKNUM: (v: any) => { const d = toDate(v); return d ? weekNumber(d) : null },
  DAY: (v: any) => { const d = toDate(v); return d ? d.getDate() : null },
  HOUR: (v: any) => { const d = toDate(v); return d ? d.getHours() : null },
  MINUTE: (v: any) => { const d = toDate(v); return d ? d.getMinutes() : null },
}

const ROW_REF_RE = /\[@([^\]]+)\]/g
const RANGE_REF_RE = /\[([^\]@][^\]]*)\]/g

export function parseReferences(formula: string): { row: string[]; range: string[] } {
  const row: string[] = []
  const range: string[] = []
  formula.replace(ROW_REF_RE, (_, name) => { const n = String(name).trim(); if (n && !row.includes(n)) row.push(n); return '' })
  formula.replace(RANGE_REF_RE, (_, name) => { const n = String(name).trim(); if (n && !range.includes(n)) range.push(n); return '' })
  return { row, range }
}

export function compileFormula(source: string): CompiledFormula {
  const refs = parseReferences(source)
  // Replace tokens with RV/RG calls
  let expr = source.replace(ROW_REF_RE, (_, name) => `RV(${JSON.stringify(String(name).trim())})`)
  expr = expr.replace(RANGE_REF_RE, (_, name) => `RG(${JSON.stringify(String(name).trim())})`)
  // Convert ^ to **
  expr = expr.replace(/\^/g, '**')
  // Route known function calls to FN.NAME(...)
  try {
    const names = Object.keys(FN)
    if (names.length) {
      const rx = new RegExp(`\\b(${names.join('|')})\\s*\\(`, 'gi')
      expr = expr.replace(rx, (m: string, name: string) => `FN.${name.toUpperCase()}(`)
    }
  } catch {}

  // Build function. Only FN, RV, RG in scope.
  const fn = new Function('FN', 'RV', 'RG', `"use strict"; return ( ${expr} );`) as any
  const invoke = (ctx: { row: Row; range?: Range }) => {
    try {
      const RV = (name: string) => (ctx.row ? ctx.row[name] : undefined)
      const RG = (name: string) => (ctx.range ? (ctx.range[name] || []) : [])
      const FN_CI = new Proxy(FN as any, {
        get(target: any, prop: PropertyKey, receiver: any) {
          if (typeof prop === 'string') {
            const up = prop.toUpperCase()
            return target[up] ?? target[prop]
          }
          return Reflect.get(target, prop, receiver)
        }
      })
      return fn(FN_CI, RV, RG)
    } catch {
      return null
    }
  }
  const exec = (ctx: { row: Row; range?: Range }) => invoke(ctx)
  const execDebug = (ctx: { row: Row; range?: Range }) => {
    const RV = (name: string) => (ctx.row ? ctx.row[name] : undefined)
    const RG = (name: string) => (ctx.range ? (ctx.range[name] || []) : [])
    const FN_CI = new Proxy(FN as any, {
      get(target: any, prop: PropertyKey) {
        if (typeof prop === 'string') return target[prop.toUpperCase()] ?? target[prop]
        return (target as any)[prop as any]
      }
    })
    // Do not catch to allow preview to show errors
    return fn(FN_CI, RV, RG)
  }
  return { source, refsRow: refs.row, refsRange: refs.range, exec, execDebug }
}

export function evalRow(formula: string, row: Row): any {
  const compiled = compileFormula(formula)
  return compiled.exec({ row })
}
