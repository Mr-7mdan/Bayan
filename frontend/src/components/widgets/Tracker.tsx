"use client"

import React from 'react'

export type TrackerDatum = { color?: string; tooltip?: string }

export function Tracker({ className, data }: { className?: string; data: TrackerDatum[] }) {
  return (
    <div className={['flex items-center gap-[2px]', className].filter(Boolean).join(' ')}>
      {data.map((d, i) => (
        <div key={i} className="group relative">
          {(() => {
            const color = d.color || 'bg-gray-300'
            const isDirectColor = typeof color === 'string' && (color.startsWith('#') || color.startsWith('rgb'))
            return (
              <div
                className={['w-[8px] h-[14px] rounded-sm', isDirectColor ? '' : color].join(' ')}
                style={isDirectColor ? ({ backgroundColor: color } as React.CSSProperties) : undefined}
              />
            )
          })()}
          {d.tooltip && (
            <div className="pointer-events-none absolute -top-6 left-1/2 -translate-x-1/2 whitespace-nowrap px-1.5 py-0.5 rounded border bg-card text-[10px] opacity-0 group-hover:opacity-100 transition-opacity">
              {d.tooltip}
            </div>
          )}
        </div>
      ))}
    </div>
  )
}
