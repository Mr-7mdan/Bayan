"use client"
import React, { useState } from 'react'
import { useTranslations } from 'next-intl'

export function SectionCard({ title, badge, children, className }: {
  title: string
  badge?: React.ReactNode
  children: React.ReactNode
  className?: string
}) {
  return (
    <div className={`rounded-lg border bg-card overflow-hidden ${className || ''}`}>
      <div className="flex items-center justify-between px-3 py-2 border-b bg-[hsl(var(--secondary)/0.5)]">
        <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">{title}</span>
        {badge && <div>{badge}</div>}
      </div>
      <div className="p-3 space-y-3">{children}</div>
    </div>
  )
}

export function FormRow({ label, children, full }: {
  label: string
  children: React.ReactNode
  full?: boolean
}) {
  if (full) return (
    <div className="space-y-1.5">
      <label className="block text-xs text-muted-foreground">{label}</label>
      {children}
    </div>
  )
  return (
    <div className="flex items-center justify-between gap-3 min-h-7">
      <span className="text-xs text-muted-foreground flex-1 leading-tight">{label}</span>
      <div className="flex items-center justify-end shrink-0">{children}</div>
    </div>
  )
}

export function ActiveBadge({ count }: { count: number }) {
  if (!count) return null
  return (
    <span className="inline-flex items-center justify-center min-w-[18px] h-[18px] px-1 text-[10px] font-bold rounded-full bg-[hsl(var(--primary))] text-primary-foreground">
      {count}
    </span>
  )
}

export const inputCls = (extra = '') =>
  `w-full h-8 px-2.5 text-xs rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary))] focus:outline-none focus:ring-1 focus:ring-[hsl(var(--primary))] ${extra}`

export const selectCls = (extra = '') =>
  `h-8 px-2 text-xs rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary))] ${extra}`

// ── ColorField ────────────────────────────────────────────────────────────────
// Token-palette swatch picker + custom hex fallback. Always emits a #rrggbb hex
// string (theme swatches are resolved to hex at click time) so widget config
// output stays identical to the old raw <input type="color">.
const PALETTE_TOKENS: { labelKey: string; varName: string }[] = [
  { labelKey: 'chart1', varName: '--chart-1' },
  { labelKey: 'chart2', varName: '--chart-2' },
  { labelKey: 'chart3', varName: '--chart-3' },
  { labelKey: 'chart4', varName: '--chart-4' },
  { labelKey: 'chart5', varName: '--chart-5' },
  { labelKey: 'primary', varName: '--primary' },
  { labelKey: 'muted', varName: '--muted-foreground' },
  { labelKey: 'foreground', varName: '--foreground' },
  { labelKey: 'border', varName: '--border' },
  { labelKey: 'destructive', varName: '--destructive' },
]

function resolveTokenHex(varName: string): string {
  if (typeof window === 'undefined') return '#000000'
  try {
    const el = document.createElement('span')
    el.style.color = `hsl(var(${varName}))`
    el.style.display = 'none'
    document.body.appendChild(el)
    const rgb = getComputedStyle(el).color // "rgb(r, g, b)"
    document.body.removeChild(el)
    const m = rgb.match(/\d+/g)
    if (!m || m.length < 3) return '#000000'
    const [r, g, b] = m.map(Number)
    return '#' + [r, g, b].map(x => Math.max(0, Math.min(255, x)).toString(16).padStart(2, '0')).join('')
  } catch { return '#000000' }
}

export function ColorField({ value, onChange, className }: {
  value: string
  onChange: (hex: string) => void
  className?: string
}) {
  const t = useTranslations('configurator')
  const [open, setOpen] = useState(false)
  const hex = value || '#000000'
  const validHex = /^#[0-9a-fA-F]{6}$/.test(hex)
  return (
    <div className={`relative ${className || ''}`}>
      <button type="button" onClick={() => setOpen(o => !o)}
        className="h-8 w-full rounded-md border cursor-pointer overflow-hidden"
        style={{ backgroundColor: hex }}
        aria-label={t('common.pickColor')} title={hex} />
      {open && (
        <>
          <div className="fixed inset-0 z-40" onClick={() => setOpen(false)} />
          <div className="absolute z-50 mt-1 end-0 w-52 rounded-md border bg-card p-2 shadow-card space-y-2">
            <div className="grid grid-cols-5 gap-1.5">
              {PALETTE_TOKENS.map(tok => (
                <button key={tok.varName} type="button" title={t(`options.palette.${tok.labelKey}`)}
                  onClick={() => { onChange(resolveTokenHex(tok.varName)); setOpen(false) }}
                  className="h-6 rounded border cursor-pointer hover:scale-110 transition-transform"
                  style={{ backgroundColor: `hsl(var(${tok.varName}))` }} />
              ))}
            </div>
            <div className="flex items-center gap-1.5 pt-1 border-t">
              <input type="color" value={validHex ? hex : '#000000'}
                onChange={e => onChange(e.target.value)}
                className="h-8 w-10 shrink-0 rounded border cursor-pointer" aria-label={t('common.customColor')} />
              <input type="text" value={value || ''}
                onChange={e => onChange(e.target.value)}
                placeholder="#rrggbb"
                className="h-8 flex-1 min-w-0 px-2 text-xs rounded-md border bg-[hsl(var(--secondary))] font-mono focus:outline-none focus:ring-1 focus:ring-[hsl(var(--primary))]" />
            </div>
          </div>
        </>
      )}
    </div>
  )
}
