// Pure, side-effect-free helpers used by AlertDialog. Moved verbatim out of the
// AlertDialog monolith so they can be unit-tested and reused. No behavior change.

// Helper: format date as YYYY-MM-DD using local timezone (not UTC)
export function ymd(d: Date): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, '0')
  const da = String(d.getDate()).padStart(2, '0')
  return `${y}-${m}-${da}`
}

export function parseCron(cron?: string) {
  try {
    const s = String(cron || '')
    const parts = s.trim().split(/\s+/)
    if (parts.length < 5) return { hh: '09', mm: '00', dows: [1,2,3,4,5] as number[], doms: [] as number[], mode: 'weekly' as const, everyHours: 1 }
    const mm = parts[0]
    const hh = parts[1]
    const domPart = (parts[2] || '*').trim()
    const dowPart = (parts[4] || '*').trim()
    const doms = domPart === '*' ? [] : domPart.split(',').map((x)=>parseInt(x,10)).filter((n)=>!isNaN(n) && n>=1 && n<=31)
    const dows = dowPart === '*' ? [] : dowPart.split(',').map((x) => parseInt(x, 10)).filter((n) => !isNaN(n) && n>=0 && n<=6)
    // Detect hourly pattern like: mm */N * * *
    if (/^\*\/\d+$/.test(hh)) {
      const n = parseInt(hh.split('*/')[1] || '1', 10)
      return { hh: '00', mm: String(mm).padStart(2,'0'), dows: [], doms: [], mode: 'hourly' as const, everyHours: (isNaN(n) || n<=0 ? 1 : n) }
    }
    const mode = doms.length ? 'monthly' as const : 'weekly' as const
    const wk = dows.length ? dows : [1,2,3,4,5]
    return { hh: String(hh).padStart(2,'0'), mm: String(mm).padStart(2,'0'), dows: wk, doms, mode, everyHours: 1 }
  } catch { return { hh: '09', mm: '00', dows: [1,2,3,4,5] as number[], doms: [] as number[], mode: 'weekly' as const, everyHours: 1 } }
}

export function buildCron(time: string, opts: { mode: 'hourly'|'weekly'|'monthly'; dows: number[]; doms: number[]; everyHours?: number }) {
  try {
    const [hh, mm] = time.split(':').map((t)=>parseInt(t,10));
    if (opts.mode === 'hourly') {
      const every = Math.max(1, Math.min(24, Number(opts.everyHours || 1)))
      return `0 */${isNaN(every)?1:every} * * *`
    }
    if (opts.mode === 'monthly') {
      const domList = (opts.doms||[]).join(',') || '*'
      return `${isNaN(mm)?0:mm} ${isNaN(hh)?0:hh} ${domList} * *`
    }
    // Convert from standard cron DOW (0=Sun,1=Mon..6=Sat) to APScheduler DOW (0=Mon..6=Sun)
    const apsDows = (opts.dows||[]).map(d => d === 0 ? 6 : d - 1)
    const dowList = apsDows.join(',') || '*'
    return `${isNaN(mm)?0:mm} ${isNaN(hh)?0:hh} * * ${dowList}`
  } catch { return '0 9 * * 0,1,2,3,4' }
}

export function defaultAggFromSpec(spec: any): string {
  try {
    if (spec?.agg && spec.agg !== 'none') return String(spec.agg)
    if (spec?.measure || spec?.y) return 'sum'
    if (Array.isArray(spec?.series) && spec.series.length) return String(spec.series?.[0]?.agg || 'sum')
  } catch {}
  return 'count'
}
