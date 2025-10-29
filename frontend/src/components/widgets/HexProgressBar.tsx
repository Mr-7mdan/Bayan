import React from 'react'
import { hexToRgb } from '@/lib/chartUtils'

export default function HexProgressBar({
  value,
  total,
  color,
  height = 12,
  backgroundColor = '#e5e7eb',
  gradient = 'vertical',
  className,
}: {
  value: number
  total: number
  color: string // hex like #3b82f6
  height?: number
  backgroundColor?: string
  gradient?: 'none' | 'vertical'
  className?: string
}) {
  const pct = total > 0 ? Math.max(0, Math.min(100, (value / total) * 100)) : 0
  const toRgba = (hex: string, a: number) => {
    const rgb = hexToRgb(hex)
    if (!rgb) return hex
    return `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, ${a})`
    }
  const fillStyle: React.CSSProperties = {
    width: `${pct}%`,
    height,
    borderRadius: height / 2,
    background: gradient === 'vertical'
      ? `linear-gradient(180deg, ${toRgba(color, 0.9)} 0%, ${toRgba(color, 0.3)} 100%)`
      : color,
  }
  return (
    <div className={["relative overflow-hidden", className].filter(Boolean).join(' ')} style={{ height, backgroundColor, borderRadius: height/2 }}>
      <div style={fillStyle} />
    </div>
  )
}
