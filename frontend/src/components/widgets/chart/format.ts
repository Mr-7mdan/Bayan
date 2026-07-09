// Number/date formatting helpers.
// Moved verbatim from ChartCard.tsx tail (L7806–7999) — pure functions, no component state.
// NOTE: the spec's target home is `lib/format.ts` (deduping copies in KpiCard/PivotMatrixView/
// echarts/AreaAdvanced). Those files are outside this change's edit scope, so the shared home
// lives under widgets/chart/ for now; the dedup is a follow-up once those files can be touched.

export type FormatMode =
  | 'none'
  | 'short'
  | 'abbrev'
  | 'currency'
  | 'percent'
  | 'bytes'
  | 'wholeNumber'
  | 'number'
  | 'thousands'
  | 'millions'
  | 'billions'
  | 'oneDecimal'
  | 'twoDecimals'
  | 'percentWhole'
  | 'percentOneDecimal'
  | 'timeHours'
  | 'timeMinutes'
  | 'distance-km'
  | 'distance-mi'

export function formatNumber(n: number, mode: FormatMode): string {
  // Avoid propagating NaN/Infinity to UI
  if (!isFinite(n)) return '0'
  switch (mode) {
    case 'abbrev': {
      const abs = Math.abs(n)
      if (abs >= 1_000_000_000) return `${(n / 1_000_000_000).toLocaleString(undefined, { maximumFractionDigits: 2 })}B`
      if (abs >= 1_000_000) return `${(n / 1_000_000).toLocaleString(undefined, { maximumFractionDigits: 2 })}M`
      if (abs >= 1_000) return `${(n / 1_000).toLocaleString(undefined, { maximumFractionDigits: 2 })}K`
      return n.toLocaleString(undefined, { maximumFractionDigits: 2 })
    }
    case 'short': {
      const abs = Math.abs(n)
      if (abs >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`
      if (abs >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
      if (abs >= 1_000) return `${(n / 1_000).toFixed(1)}K`
      return String(n)
    }
    case 'wholeNumber': {
      return Math.round(n).toLocaleString()
    }
    case 'number': {
      return n.toLocaleString(undefined, { maximumFractionDigits: 2 })
    }
    case 'oneDecimal': {
      return n.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 })
    }
    case 'twoDecimals': {
      return n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    }
    case 'thousands': {
      const abs = Math.abs(n)
      // Always express in thousands, allowing fractions (e.g., 500 -> 0.5K)
      const v = n / 1_000
      return `${v.toLocaleString(undefined, { maximumFractionDigits: 2 })}K`
    }
    case 'millions': {
      const abs = Math.abs(n)
      // Always express in millions, allowing fractions (e.g., 500,000 -> 0.5M)
      const v = n / 1_000_000
      return `${v.toLocaleString(undefined, { maximumFractionDigits: 2 })}M`
    }
    case 'billions': {
      const abs = Math.abs(n)
      // Always express in billions, allowing fractions
      const v = n / 1_000_000_000
      return `${v.toLocaleString(undefined, { maximumFractionDigits: 2 })}B`
    }
    case 'currency': {
      try {
        return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(n)
      } catch {
        return `$${n.toFixed(2)}`
      }
    }
    case 'percent': {
      const v = Math.abs(n) <= 1 ? n * 100 : n
      return `${v.toFixed(1)}%`
    }
    case 'percentWhole': {
      const v = Math.abs(n) <= 1 ? n * 100 : n
      return `${Math.round(v)}%`
    }
    case 'percentOneDecimal': {
      const v = Math.abs(n) <= 1 ? n * 100 : n
      return `${v.toFixed(1)}%`
    }
    case 'bytes': {
      const units = ['B', 'KB', 'MB', 'GB', 'TB']
      let v = n
      let i = 0
      while (Math.abs(v) >= 1024 && i < units.length - 1) {
        v /= 1024
        i++
      }
      return `${v.toFixed(1)} ${units[i]}`
    }
    case 'timeHours': {
      // Interpret value as hours; show 1 decimal for non-integers
      const abs = Math.abs(n)
      const withDec = abs % 1 !== 0
      return withDec ? `${n.toFixed(1)}h` : `${Math.round(n)}h`
    }
    case 'timeMinutes': {
      // Interpret value as minutes; whole numbers preferred
      const abs = Math.abs(n)
      const withDec = abs % 1 !== 0
      return withDec ? `${n.toFixed(1)}m` : `${Math.round(n)}m`
    }
    case 'distance-km': {
      const abs = Math.abs(n)
      const digits = abs >= 100 ? 0 : 1
      return `${n.toFixed(digits)} km`
    }
    case 'distance-mi': {
      const abs = Math.abs(n)
      const digits = abs >= 100 ? 0 : 1
      return `${n.toFixed(digits)} mi`
    }
    default:
      return String(n)
  }
}

// Safe numeric coercion: returns 0 for non-finite values
export function toNum(x: any, fallback = 0): number {
  const n = Number(x)
  return Number.isFinite(n) ? n : fallback
}

// Custom date formatter supporting patterns like:
// YYYY, YY, MM, M, DD, D, MMM, MMMM, ddd/DDD, dddd/DDDD, HH, H, hh, h, mm, m, a, A, q/Q, ww, and bracketed literals [text].
export function formatDatePattern(d: Date, pattern: string): string {
  try {
    if (!pattern) return d.toString()
    const pad2 = (n: number) => String(n).padStart(2, '0')
    const monthShort = d.toLocaleDateString('en-US', { month: 'short' })
    const monthLong = d.toLocaleDateString('en-US', { month: 'long' })
    const weekdayShort = d.toLocaleDateString('en-US', { weekday: 'short' })
    const weekdayLong = d.toLocaleDateString('en-US', { weekday: 'long' })
    const q = Math.floor(d.getMonth() / 3) + 1
    const isoWeek = (() => {
      const _d = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()))
      _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7))
      const yearStart = new Date(Date.UTC(_d.getUTCFullYear(), 0, 1))
      return Math.ceil((((_d.getTime() - yearStart.getTime()) / 86400000) + 1) / 7)
    })()
    const h24 = d.getHours()
    const h12 = h24 % 12 || 12
    const am = h24 < 12

    // Extract literals inside [ ... ] and protect them
    const literals: string[] = []
    let fmt = pattern.replace(/\[(.*?)\]/g, (_m, p1) => {
      literals.push(String(p1))
      return `__L${literals.length - 1}__`
    })

    // Token map (longer tokens first)
    const map: Record<string, string> = {
      'YYYY': String(d.getFullYear()),
      'YY': String(d.getFullYear()).slice(-2),
      'MMMM': monthLong,
      'MMM': monthShort,
      'MM': pad2(d.getMonth() + 1),
      'M': String(d.getMonth() + 1),
      'DD': pad2(d.getDate()),
      'D': String(d.getDate()),
      'dddd': weekdayLong,
      'ddd': weekdayShort,
      'DDDD': weekdayLong, // accept uppercase variants
      'DDD': weekdayShort,
      'HH': pad2(h24),
      'H': String(h24),
      'hh': pad2(h12),
      'h': String(h12),
      'mm': pad2(d.getMinutes()),
      'm': String(d.getMinutes()),
      'a': am ? 'am' : 'pm',
      'A': am ? 'AM' : 'PM',
      'ww': pad2(isoWeek),
      'q': String(q),
      'Q': String(q),
    }
    const tokens = ['YYYY','MMMM','MMM','YY','MM','M','DDDD','dddd','DDD','ddd','DD','D','HH','H','hh','h','mm','m','A','a','ww','Q','q']
    tokens.forEach(tok => {
      fmt = fmt.replace(new RegExp(tok, 'g'), map[tok])
    })
    // Restore literals
    fmt = fmt.replace(/__L(\d+)__/g, (_m, idx) => literals[Number(idx)] ?? '')
    return fmt
  } catch { return d.toString() }
}
