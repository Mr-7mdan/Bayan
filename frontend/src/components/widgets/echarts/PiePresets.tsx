"use client"

import React from 'react'
import dynamic from 'next/dynamic'

// ECharts client-only wrapper
const ReactECharts: any = dynamic(() => import('echarts-for-react').then(m => (m as any).default), { ssr: false }) as any

function isDark(): boolean {
  if (typeof window === 'undefined') return false
  return document.documentElement.classList.contains('dark')
}

function buildTooltipHtml(header: string, lines: Array<{ label: string; value: string; right?: string }>) {
  const dark = isDark()
  const bg = dark ? 'hsla(199, 98.5%, 8.1%, 0.9)' : 'rgba(255,255,255,0.95)'
  const fg = dark ? 'hsl(var(--foreground))' : '#0f172a'
  const border = dark ? '1px solid hsl(var(--border))' : '1px solid rgba(148,163,184,.35)'
  const rows = lines.map(l => (
    `<tr><td style="padding:2px 6px;opacity:.85;text-align:left;white-space:nowrap">${l.label}</td>`+
    `<td style="padding:2px 6px;text-align:right">${l.value}</td>`+
    (l.right?`<td style=\"padding:2px 6px;opacity:.75;text-align:right\">${l.right}</td>`:'')+`</tr>`
  )).join('')
  return `<div style="padding:6px 8px;border:${border};background:${bg};color:${fg};border-radius:6px;font-size:12px;line-height:1.1;font-variant-numeric:tabular-nums;">`+
         `<div style="font-weight:600;margin-bottom:6px;text-align:left">${header}</div>`+
         `<table style="border-collapse:separate;border-spacing:0 2px;min-width:240px"><tbody>${rows}</tbody></table>`+
         `</div>`
}

function stripSeriesPrefix(name: string): string {
  try {
    const s = String(name || '')
    if (!s) return s
    const seps = [' • ', ' - ', ' — ']
    for (const sep of seps) {
      if (s.includes(sep)) {
        const parts = s.split(sep)
        return parts[parts.length - 1]
      }
    }
    return s
  } catch { return String(name || '') }
}

export type LabelPosition = 'center'|'insideEnd'|'insideBase'|'outsideEnd'|'callout'
export type PieDatum = { name: string; value: number }

function pieLabelConfigs(show: boolean, position: LabelPosition | undefined) {
  const dark = isDark()
  const textColor = dark ? '#e5e7eb' : '#0f172a'
  const shadowColor = dark ? 'rgba(0,0,0,0.35)' : 'rgba(255,255,255,0.7)'
  const lineColor = dark ? 'rgba(255,255,255,0.55)' : 'rgba(15,23,42,0.55)'
  const pos = (position || 'outsideEnd')
  const isOutside = pos === 'outsideEnd' || pos === 'callout'
  const label: any = {
    show: !!show,
    color: textColor,
    textShadowColor: shadowColor,
    textShadowBlur: 2,
    formatter: (p: any) => stripSeriesPrefix(String(p?.name ?? '')),
  }
  if (pos === 'center') label.position = 'center'
  else if (pos === 'insideEnd' || pos === 'insideBase') label.position = 'inside'
  else label.position = 'outside'
  const labelLine: any = {
    show: !!show && isOutside,
    lineStyle: { color: lineColor, width: (pos === 'callout' ? 1.25 : 1) },
    length: (pos === 'callout' ? 16 : 10),
    length2: (pos === 'callout' ? 10 : 6),
  }
  return { label, labelLine }
}

export function renderDonut({ chartInstanceKey, data, colors, valueFormatterAction, showLabels, labelPosition }: { chartInstanceKey: string; data: PieDatum[]; colors?: string[]; valueFormatterAction: (n:number)=>string; showLabels?: boolean; labelPosition?: LabelPosition }) {
  const option = {
    color: Array.isArray(colors) && colors.length ? colors : undefined,
    legend: { show: false },
    tooltip: {
      trigger: 'item',
      backgroundColor: 'transparent', borderWidth: 0, extraCssText: 'box-shadow:none;padding:0;',
      formatter: (p: any) => {
        const name = String(p?.name ?? '')
        const v = Number(p?.value ?? 0)
        const pct = Number(p?.percent ?? 0)
        return buildTooltipHtml(stripSeriesPrefix(name), [ { label: 'Value', value: valueFormatterAction(v), right: `${pct.toFixed(1)}%` } ])
      }
    },
    series: [
      {
        type: 'pie',
        radius: ['45%', '70%'],
        avoidLabelOverlap: true,
        ...(() => { const cfg = pieLabelConfigs(!!showLabels, labelPosition); return { label: cfg.label, labelLine: cfg.labelLine } })(),
        data,
      }
    ]
  }
  return (
    <div className="absolute inset-0">
      <ReactECharts key={chartInstanceKey} option={option} style={{ height: '100%', width: '100%' }} notMerge={true} lazyUpdate={true} opts={{ renderer: 'svg' }} />
    </div>
  )
}

export function renderPie({ chartInstanceKey, data, colors, valueFormatterAction, showLabels, labelPosition }: { chartInstanceKey: string; data: PieDatum[]; colors?: string[]; valueFormatterAction: (n:number)=>string; showLabels?: boolean; labelPosition?: LabelPosition }) {
  const option = {
    color: Array.isArray(colors) && colors.length ? colors : undefined,
    legend: { show: false },
    tooltip: {
      trigger: 'item',
      backgroundColor: 'transparent', borderWidth: 0, extraCssText: 'box-shadow:none;padding:0;',
      formatter: (p: any) => {
        const name = String(p?.name ?? '')
        const v = Number(p?.value ?? 0)
        const pct = Number(p?.percent ?? 0)
        return buildTooltipHtml(stripSeriesPrefix(name), [ { label: 'Value', value: valueFormatterAction(v), right: `${pct.toFixed(1)}%` } ])
      }
    },
    series: [
      {
        type: 'pie',
        radius: '70%',
        avoidLabelOverlap: true,
        ...(() => { const cfg = pieLabelConfigs(!!showLabels, labelPosition); return { label: cfg.label, labelLine: cfg.labelLine } })(),
        data,
      }
    ]
  }
  return (
    <div className="absolute inset-0">
      <ReactECharts option={option} style={{ height: '100%', width: '100%' }} notMerge={false} lazyUpdate={true} opts={{ renderer: 'svg' }} />
    </div>
  )
}

export function renderNightingale({ chartInstanceKey, data, colors, valueFormatterAction, showLabels, labelPosition }: { chartInstanceKey: string; data: PieDatum[]; colors?: string[]; valueFormatterAction: (n:number)=>string; showLabels?: boolean; labelPosition?: LabelPosition }) {
  const option = {
    color: Array.isArray(colors) && colors.length ? colors : undefined,
    legend: { show: false },
    tooltip: {
      trigger: 'item',
      backgroundColor: 'transparent', borderWidth: 0, extraCssText: 'box-shadow:none;padding:0;',
      formatter: (p: any) => {
        const name = String(p?.name ?? '')
        const v = Number(p?.value ?? 0)
        const pct = Number(p?.percent ?? 0)
        return buildTooltipHtml(stripSeriesPrefix(name), [ { label: 'Value', value: valueFormatterAction(v), right: `${pct.toFixed(1)}%` } ])
      }
    },
    series: [
      {
        type: 'pie',
        radius: ['10%','70%'],
        roseType: 'radius',
        avoidLabelOverlap: true,
        ...(() => { const cfg = pieLabelConfigs(!!showLabels, labelPosition); return { label: cfg.label, labelLine: cfg.labelLine } })(),
        data,
      }
    ]
  }
  return (
    <div className="absolute inset-0">
      <ReactECharts option={option} style={{ height: '100%', width: '100%' }} notMerge={false} lazyUpdate={true} opts={{ renderer: 'svg' }} />
    </div>
  )
}

export function renderSunburst({ chartInstanceKey, data, colors, valueFormatterAction, showLabels }: { chartInstanceKey: string; data: PieDatum[]; colors?: string[]; valueFormatterAction: (n:number)=>string; showLabels?: boolean }) {
  const total = data.reduce((a, b) => a + Number(b.value || 0), 0)
  const sbData = data.map(d => ({ name: d.name, value: d.value }))
  const option = {
    color: Array.isArray(colors) && colors.length ? colors : undefined,
    legend: { show: false },
    tooltip: {
      trigger: 'item',
      backgroundColor: 'transparent', borderWidth: 0, extraCssText: 'box-shadow:none;padding:0;',
      formatter: (p: any) => {
        const name = String(p?.name ?? '')
        const v = Number(p?.value ?? 0)
        const pct = total > 0 ? (v / total) * 100 : 0
        return buildTooltipHtml(stripSeriesPrefix(name), [ { label: 'Value', value: valueFormatterAction(v), right: `${pct.toFixed(1)}%` } ])
      }
    },
    series: [
      {
        type: 'sunburst',
        radius: ['0%', '80%'],
        data: sbData,
        sort: undefined,
        label: (() => { const dark = isDark(); const textColor = dark ? '#e5e7eb' : '#0f172a'; const shadow = dark ? 'rgba(0,0,0,0.35)' : 'rgba(255,255,255,0.7)'; return { rotate: 'radial', show: !!showLabels, color: textColor, textShadowColor: shadow, textShadowBlur: 2, formatter: (p: any) => stripSeriesPrefix(String(p?.name ?? '')) } })(),
      }
    ]
  }
  return (
    <div className="absolute inset-0">
      <ReactECharts option={option} style={{ height: '100%', width: '100%' }} notMerge={false} lazyUpdate={true} opts={{ renderer: 'svg' }} />
    </div>
  )
}
