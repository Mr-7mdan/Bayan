/**
 * Conditional formatting for report variables (Excel-style icon sets + color scales).
 * Mirrors backend/app/alerts_service.py:_match_conditional_rule().
 */

export type ConditionalOp = '>' | '>=' | '<' | '<=' | '==' | '!=' | 'between'

export type ConditionalIcon =
  | 'none'
  | 'arrow_up'
  | 'arrow_down'
  | 'arrow_flat'
  | 'circle'

export interface ConditionalRule {
  op: ConditionalOp
  value: number
  value2?: number
  icon?: ConditionalIcon
  iconColor?: string
  textColor?: string
  bgColor?: string
}

export interface ConditionalFormat {
  enabled?: boolean
  rules: ConditionalRule[]
}

export const OP_OPTIONS: { value: ConditionalOp; label: string }[] = [
  { value: '>',       label: '>' },
  { value: '>=',      label: '≥' },
  { value: '<',       label: '<' },
  { value: '<=',      label: '≤' },
  { value: '==',      label: '=' },
  { value: '!=',      label: '≠' },
  { value: 'between', label: 'between' },
]

export const ICON_OPTIONS: { value: ConditionalIcon; label: string; glyph: string }[] = [
  { value: 'none',        label: 'None',        glyph: ''   },
  { value: 'arrow_up',    label: 'Arrow Up',    glyph: '▲'  },
  { value: 'arrow_down',  label: 'Arrow Down',  glyph: '▼'  },
  { value: 'arrow_flat',  label: 'Arrow Flat',  glyph: '▬'  },
  { value: 'circle',      label: 'Circle',      glyph: '●'  },
]

export const ICON_GLYPH: Record<ConditionalIcon, string> = {
  none: '',
  arrow_up: '▲',
  arrow_down: '▼',
  arrow_flat: '▬',
  circle: '●',
}

/** Returns the first rule that matches *num*, or null. */
export function matchConditionalRule(
  num: number,
  cf: ConditionalFormat | undefined | null,
): ConditionalRule | null {
  if (!cf || cf.enabled === false || !cf.rules || cf.rules.length === 0) return null
  if (num == null || Number.isNaN(num)) return null
  for (const r of cf.rules) {
    if (ruleMatches(num, r)) return r
  }
  return null
}

export function ruleMatches(num: number, r: ConditionalRule): boolean {
  const v = Number(r.value)
  switch (r.op) {
    case '>':  return num > v
    case '>=': return num >= v
    case '<':  return num < v
    case '<=': return num <= v
    case '==': return num === v
    case '!=': return num !== v
    case 'between': {
      const v2 = Number(r.value2 ?? r.value)
      const lo = Math.min(v, v2)
      const hi = Math.max(v, v2)
      return num >= lo && num <= hi
    }
    default: return false
  }
}

/** Preset builders — return a fresh ConditionalFormat object. */
export function presetTrendArrows(): ConditionalFormat {
  return {
    enabled: true,
    rules: [
      { op: '>',  value: 0, icon: 'arrow_up',   iconColor: '#16a34a' },
      { op: '<',  value: 0, icon: 'arrow_down', iconColor: '#dc2626' },
      { op: '==', value: 0, icon: 'arrow_flat', iconColor: '#6b7280' },
    ],
  }
}

export function preset4CircleSet(): ConditionalFormat {
  // Order matters: highest band first so the first-match-wins rule yields
  // contiguous non-overlapping coverage: (75, ∞], (50, 75], (25, 50], (−∞, 25].
  return {
    enabled: true,
    rules: [
      { op: '>',  value: 75, icon: 'circle', iconColor: '#16a34a' }, // green
      { op: '>',  value: 50, icon: 'circle', iconColor: '#eab308' }, // yellow
      { op: '>',  value: 25, icon: 'circle', iconColor: '#f97316' }, // orange
      { op: '<=', value: 25, icon: 'circle', iconColor: '#dc2626' }, // red (catch-all)
    ],
  }
}

export function presetHeatmap3(): ConditionalFormat {
  return {
    enabled: true,
    rules: [
      { op: '>',  value: 0, icon: 'none', textColor: '#16a34a', bgColor: '#dcfce7' },
      { op: '==', value: 0, icon: 'none', textColor: '#374151', bgColor: '#f3f4f6' },
      { op: '<',  value: 0, icon: 'none', textColor: '#dc2626', bgColor: '#fee2e2' },
    ],
  }
}

/** Short human-readable description of a rule, for preview display. */
export function describeRule(r: ConditionalRule): string {
  if (r.op === 'between') {
    const v1 = Math.min(Number(r.value), Number(r.value2 ?? r.value))
    const v2 = Math.max(Number(r.value), Number(r.value2 ?? r.value))
    return `between ${v1} and ${v2}`
  }
  const opLbl = OP_OPTIONS.find(o => o.value === r.op)?.label ?? r.op
  return `${opLbl} ${r.value}`
}
