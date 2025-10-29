"use client"

import * as Popover from "@radix-ui/react-popover"
import React, { useMemo, useState } from "react"

export default function DatePickerField({
  value,
  onChangeAction,
  disabled,
  placeholder = "YYYY-MM-DD",
  className = "",
  ariaLabel,
}: {
  value?: string
  onChangeAction: (v?: string) => void
  disabled?: boolean
  placeholder?: string
  className?: string
  ariaLabel?: string
}) {
  const parsed = useMemo(() => parseLocal(value), [value])
  const [open, setOpen] = useState(false)
  const [month, setMonth] = useState<Date>(() => new Date(parsed?.getFullYear() || new Date().getFullYear(), (parsed?.getMonth() ?? new Date().getMonth()), 1))

  const label = value || placeholder

  return (
    <Popover.Root open={open} onOpenChange={setOpen}>
      <Popover.Trigger asChild>
        <button
          type="button"
          disabled={disabled}
          aria-label={ariaLabel || "Pick a date"}
          className={`date-trigger inline-flex items-center gap-2 px-2 h-8 rounded-md border text-[12px] bg-[hsl(var(--card))] border-[hsl(var(--border))] text-[hsl(var(--foreground))] focus:outline-none focus:ring-2 focus:ring-tremor-brand-muted ${disabled ? 'opacity-60 cursor-not-allowed' : ''} ${className}`}
        >
          <span className="min-w-[92px] text-left tabular-nums">{label}</span>
          <CalendarIcon className="w-4 h-4 text-[hsl(var(--foreground))]" />
        </button>
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Content sideOffset={6} className="z-50 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--popover))] text-[hsl(var(--foreground))] p-2 shadow-none">
          <CalendarView
            month={month}
            selected={parsed || undefined}
            onSelect={(d) => { onChangeAction(d ? formatLocal(d) : undefined); setOpen(false) }}
            onMonthChange={setMonth}
          />
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  )
}

function CalendarView({ month, selected, onSelect, onMonthChange }: { month: Date; selected?: Date; onSelect: (d?: Date) => void; onMonthChange: (d: Date) => void }) {
  const y = month.getFullYear()
  const m = month.getMonth()
  const first = new Date(y, m, 1)
  const startDow = first.getDay() // 0..6, Sun=0
  const daysInMonth = new Date(y, m + 1, 0).getDate()
  const prevDays = startDow
  const cells = [] as { d: Date; inMonth: boolean }[]

  for (let i = prevDays; i > 0; i--) cells.push({ d: new Date(y, m, 1 - i), inMonth: false })
  for (let i = 1; i <= daysInMonth; i++) cells.push({ d: new Date(y, m, i), inMonth: true })
  while (cells.length % 7 !== 0) cells.push({ d: new Date(y, m, daysInMonth + (cells.length % 7) + 1), inMonth: false })

  const header = new Intl.DateTimeFormat(undefined, { month: 'long', year: 'numeric' }).format(first)

  return (
    <div className="w-[268px]">
      <div className="flex items-center justify-between px-1 pb-2 text-[13px]">
        <button type="button" className="p-1 rounded hover:bg-[hsl(var(--muted))]" onClick={() => onMonthChange(new Date(y, m - 1, 1))} aria-label="Previous month">‹</button>
        <div className="font-medium">{header}</div>
        <button type="button" className="p-1 rounded hover:bg-[hsl(var(--muted))]" onClick={() => onMonthChange(new Date(y, m + 1, 1))} aria-label="Next month">›</button>
      </div>
      <div className="grid grid-cols-7 gap-1 px-1 pb-1 text-[11px] text-[hsl(var(--muted-foreground))]">
        {['S','M','T','W','T','F','S'].map((d, i) => (<div key={`${d}-${i}`} className="text-center">{d}</div>))}
      </div>
      <div className="grid grid-cols-7 gap-1 px-1 pb-1">
        {cells.map(({ d, inMonth }, idx) => {
          const isSel = selected ? sameDate(d, selected) : false
          const isToday = sameDate(d, new Date())
          const base = "h-8 w-8 rounded flex items-center justify-center text-[12px]"
          const tone = isSel
            ? "bg-blue-600 text-white"
            : isToday
              ? "border border-[hsl(var(--border))]"
              : inMonth
                ? "hover:bg-[hsl(var(--muted))]"
                : "text-[hsl(var(--muted-foreground))]/70"
          return (
            <button key={idx} type="button" className={`${base} ${tone}`} onClick={() => onSelect(d)}>
              {d.getDate()}
            </button>
          )
        })}
      </div>
      <div className="flex items-center justify-between pt-1 px-1 text-[12px]">
        <button type="button" className="text-blue-500 hover:underline" onClick={() => onSelect(undefined)}>Clear</button>
        <button type="button" className="text-blue-500 hover:underline" onClick={() => onSelect(new Date())}>Today</button>
      </div>
    </div>
  )
}

function sameDate(a: Date, b: Date): boolean {
  return a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth() && a.getDate() === b.getDate()
}

function parseLocal(s?: string | null): Date | null {
  if (!s) return null
  const m = String(s).match(/^(\d{4})-(\d{2})-(\d{2})$/)
  if (!m) return null
  const y = Number(m[1]); const mm = Number(m[2]) - 1; const d = Number(m[3])
  return new Date(y, mm, d)
}

function formatLocal(d: Date): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, '0')
  const day = String(d.getDate()).padStart(2, '0')
  return `${y}-${m}-${day}`
}

function CalendarIcon({ className = "w-4 h-4" }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <rect x="3" y="4" width="18" height="18" rx="2" ry="2" />
      <line x1="16" y1="2" x2="16" y2="6" />
      <line x1="8" y1="2" x2="8" y2="6" />
      <line x1="3" y1="10" x2="21" y2="10" />
    </svg>
  )
}
