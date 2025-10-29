"use client"

import React from 'react'
import { RiArrowUpSFill, RiArrowDownSFill, RiSubtractLine } from '@remixicon/react'

export type TooltipRow = {
  color: string
  name: string
  valueStr: string
  shareStr: string
  aggLabel: string
  changeStr: string
  deltaStr: string
  prevStr: string
  changeColor?: string
}

export function TooltipTable({ header, prevLabel, rows }: { header: string; prevLabel?: string; rows: TooltipRow[] }) {
  const Arrow = ({ text, color }: { text: string; color?: string }) => {
    const t = String(text || '').trim()
    if (!t) return <RiSubtractLine size={12} style={{ color: color || 'inherit' }} />
    if (t.startsWith('-')) return <RiArrowDownSFill size={12} style={{ color: color || 'inherit' }} />
    if (t.startsWith('+')) return <RiArrowUpSFill size={12} style={{ color: color || 'inherit' }} />
    return <RiSubtractLine size={12} style={{ color: color || 'inherit' }} />
  }
  return (
    <div style={{
      padding: '4px 8px',
      border: '1px solid hsl(var(--border))',
      background: 'hsl(var(--card))',
      color: 'hsl(var(--foreground))',
      borderRadius: 6,
      fontSize: 12,
      lineHeight: 1.1,
      fontVariantNumeric: 'tabular-nums',
    }}>
      <div style={{ fontWeight: 600, marginBottom: 6, textAlign: 'left' }}>
        {header}
        {prevLabel ? (
          <span style={{ fontWeight: 400, opacity: 0.8, fontSize: 11 }}> (vs {prevLabel})</span>
        ) : null}
      </div>
      <table style={{ borderCollapse: 'separate', borderSpacing: '0 2px', minWidth: 320 }}>
        <thead>
          <tr>
            <th style={{ textAlign: 'left', padding: '2px 6px' }} />
            <th style={{ textAlign: 'left', padding: '2px 6px', opacity: 0.85 }}>Category</th>
            <th style={{ textAlign: 'right', padding: '2px 6px', opacity: 0.85 }}>Value</th>
            <th style={{ textAlign: 'right', padding: '2px 6px', opacity: 0.85 }}>Share</th>
            <th style={{ textAlign: 'right', padding: '2px 6px', opacity: 0.85 }}>Agg</th>
            <th style={{ width: 8 }} />
            <th style={{ textAlign: 'right', padding: '2px 6px', opacity: 0.85 }}>Change</th>
            <th style={{ textAlign: 'right', padding: '2px 6px', opacity: 0.85 }}>Delta</th>
            <th style={{ textAlign: 'right', padding: '2px 6px', opacity: 0.85 }}>Prev</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              <td style={{ padding: '2px 6px', verticalAlign: 'middle' }}>
                <span style={{ display: 'inline-block', width: 8, height: 8, background: r.color, borderRadius: 2 }} />
              </td>
              <td style={{ padding: '2px 6px', verticalAlign: 'middle', whiteSpace: 'nowrap', maxWidth: 220, overflow: 'hidden', textOverflow: 'ellipsis' }}>{r.name}</td>
              <td style={{ padding: '2px 6px', verticalAlign: 'middle', textAlign: 'right' }}>{r.valueStr}</td>
              <td style={{ padding: '2px 6px', verticalAlign: 'middle', textAlign: 'right' }}>{r.shareStr}</td>
              <td style={{ padding: '2px 6px', verticalAlign: 'middle', textAlign: 'right' }}>{r.aggLabel}</td>
              <td style={{ padding: '2px 6px', verticalAlign: 'middle' }} />
              <td style={{ padding: '2px 6px', verticalAlign: 'middle', textAlign: 'right' }}>
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, lineHeight: 1 }}>
                  <Arrow text={r.changeStr} color={r.changeColor} />
                  <span style={{ color: r.changeColor, lineHeight: 1 }}>{r.changeStr}</span>
                </span>
              </td>
              <td style={{ padding: '2px 6px', verticalAlign: 'middle', textAlign: 'right' }}>
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, lineHeight: 1 }}>
                  <Arrow text={r.deltaStr} color={r.changeColor} />
                  <span style={{ color: r.changeColor, lineHeight: 1 }}>{r.deltaStr}</span>
                </span>
              </td>
              <td style={{ padding: '2px 6px', verticalAlign: 'middle', textAlign: 'right' }}>{r.prevStr}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export default TooltipTable
