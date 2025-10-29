// Centralized chart color utilities for Tremor charts
// This mirrors Tremor's documented approach and keeps our classes visible to Tailwind.

export type AvailableChartColorsKeys =
  | 'blue'
  | 'emerald'
  | 'violet'
  | 'amber'
  | 'gray'
  | 'rose'
  | 'indigo'
  | 'cyan'
  | 'pink'
  | 'lime'
  | 'fuchsia'

export type ColorPreset = 'default' | 'muted' | 'vibrant' | 'corporate'

// Default palette order we cycle through. First 5 represent our token mapping 1..5
export const chartColors: AvailableChartColorsKeys[] = [
  'blue',
  'rose',
  'amber',
  'violet',
  'emerald',
  'cyan',
  'fuchsia',
  'lime',
  'indigo',
  'pink',
  'gray',
]

const presetPalettes: Record<ColorPreset, AvailableChartColorsKeys[]> = {
  default: chartColors,
  muted: ['gray', 'emerald', 'amber', 'indigo', 'rose', 'cyan', 'violet', 'lime', 'pink', 'blue', 'fuchsia'],
  vibrant: ['rose', 'emerald', 'amber', 'violet', 'cyan', 'fuchsia', 'lime', 'indigo', 'pink', 'blue', 'gray'],
  corporate: ['blue', 'amber', 'indigo', 'emerald', 'rose', 'cyan', 'violet', 'gray', 'lime', 'fuchsia', 'pink'],
}

export function getPresetPalette(preset: ColorPreset = 'default'): AvailableChartColorsKeys[] {
  return presetPalettes[preset] || chartColors
}

// Map the first five to numeric tokens (used by our pivot values)
const tokenMap: Record<1 | 2 | 3 | 4 | 5, AvailableChartColorsKeys> = {
  1: 'blue',
  2: 'rose',
  3: 'amber',
  4: 'violet',
  5: 'emerald',
}

const inverseTokenMap: Record<AvailableChartColorsKeys, 1 | 2 | 3 | 4 | 5 | undefined> = {
  blue: 1,
  rose: 2,
  amber: 3,
  violet: 4,
  emerald: 5,
  gray: undefined,
  cyan: undefined,
  pink: undefined,
  lime: undefined,
  fuchsia: undefined,
  indigo: undefined,
}

export function tokenToColorKey(token?: 1 | 2 | 3 | 4 | 5): AvailableChartColorsKeys {
  return token ? tokenMap[token] : 'blue'
}

export function colorKeyToToken(name?: AvailableChartColorsKeys): 1 | 2 | 3 | 4 | 5 {
  return inverseTokenMap[name ?? 'blue'] ?? 1
}

export function getDefaultSeriesColors(count: number, base: AvailableChartColorsKeys[] = chartColors): AvailableChartColorsKeys[] {
  if (count <= base.length) return base.slice(0, count)
  const out: AvailableChartColorsKeys[] = []
  for (let i = 0; i < count; i++) out.push(base[i % base.length])
  return out
}

export function constructCategoryColors(categories: string[], colors: AvailableChartColorsKeys[] = chartColors): Record<string, AvailableChartColorsKeys> {
  const map: Record<string, AvailableChartColorsKeys> = {}
  const size = categories.length
  const cyc = getDefaultSeriesColors(size, colors)
  categories.forEach((c, i) => (map[c] = cyc[i]))
  return map
}

export function tremorNameToHex(name: AvailableChartColorsKeys): string {
  switch (name) {
    case 'blue': return '#3b82f6'
    case 'emerald': return '#10b981'
    case 'violet': return '#8b5cf6'
    case 'amber': return '#f59e0b'
    case 'gray': return '#6b7280'
    case 'rose': return '#f43f5e'
    case 'indigo': return '#6366f1'
    case 'cyan': return '#06b6d4'
    case 'pink': return '#ec4899'
    case 'lime': return '#84cc16'
    case 'fuchsia': return '#d946ef'
    default: return '#3b82f6'
  }
}

// ----- Value-gradient utilities -----
function clamp01(x: number) { return Math.max(0, Math.min(1, x)) }

export function hexToRgb(hex: string): { r: number; g: number; b: number } | null {
  const m = /^#?([\da-f]{2})([\da-f]{2})([\da-f]{2})$/i.exec(hex.trim())
  if (!m) return null
  return { r: parseInt(m[1], 16), g: parseInt(m[2], 16), b: parseInt(m[3], 16) }
}

export function rgbToHsl(r: number, g: number, b: number): { h: number; s: number; l: number } {
  r /= 255; g /= 255; b /= 255
  const max = Math.max(r, g, b), min = Math.min(r, g, b)
  let h = 0, s = 0, l = (max + min) / 2
  if (max !== min) {
    const d = max - min
    s = l > 0.5 ? d / (2 - max - min) : d / (max + min)
    switch (max) {
      case r: h = (g - b) / d + (g < b ? 6 : 0); break
      case g: h = (b - r) / d + 2; break
      default: h = (r - g) / d + 4
    }
    h /= 6
  }
  return { h: h * 360, s, l }
}

export function hslToHex(h: number, s: number, l: number): string {
  h = ((h % 360) + 360) % 360; s = clamp01(s); l = clamp01(l)
  const c = (1 - Math.abs(2 * l - 1)) * s
  const x = c * (1 - Math.abs(((h / 60) % 2) - 1))
  const m = l - c / 2
  let r = 0, g = 0, b = 0
  if (h < 60) { r = c; g = x; b = 0 }
  else if (h < 120) { r = x; g = c; b = 0 }
  else if (h < 180) { r = 0; g = c; b = x }
  else if (h < 240) { r = 0; g = x; b = c }
  else if (h < 300) { r = x; g = 0; b = c }
  else { r = c; g = 0; b = x }
  const toHex = (v: number) => {
    const n = Math.round((v + m) * 255)
    return n.toString(16).padStart(2, '0')
  }
  return `#${toHex(r)}${toHex(g)}${toHex(b)}`
}

// Compute a new hex by adjusting saturation based on pct in [0,1]
export function saturateHexBy(baseHex: string, pct: number, minS = 0.25, maxS = 0.95): string {
  const rgb = hexToRgb(baseHex)
  if (!rgb) return baseHex
  const hsl = rgbToHsl(rgb.r, rgb.g, rgb.b)
  const s = minS + (maxS - minS) * clamp01(pct)
  return hslToHex(hsl.h, s, hsl.l)
}

// y-axis helpers (minimal): compute domain hints
export function getYAxisDomain({ minValue, maxValue }: { minValue?: number; maxValue?: number }) {
  const domain: [number | 'auto', number | 'auto'] = ['auto', 'auto']
  if (typeof minValue === 'number') domain[0] = minValue
  if (typeof maxValue === 'number') domain[1] = maxValue
  return domain
}
