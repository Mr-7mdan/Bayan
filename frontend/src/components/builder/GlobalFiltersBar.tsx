"use client"

import { useState } from 'react'
import { useFilters } from '@/components/providers/FiltersProvider'
import FilterbarControl from '@/components/shared/FilterbarControl'
import DateFieldMappingDialog from '@/components/builder/DateFieldMappingDialog'
import DatePickerField from '@/components/shared/DatePickerField'
import type { WidgetConfig } from '@/types/widgets'
import { RiLink, RiLinkUnlink } from '@remixicon/react'

export default function GlobalFiltersBar({ widgets, onApplyMappingAction, disabled }: { widgets?: Record<string, WidgetConfig>; onApplyMappingAction?: (map: Record<string, string | undefined>) => void; disabled?: boolean }) {
  const { filters, setFilters, reset } = useFilters()
  const [showMapping, setShowMapping] = useState(false)
  const [showBreakPanel, setShowBreakPanel] = useState(false)
  
  // Use filterPreset from filters context (defaults to 'all' if not set)
  const selectedPreset = filters.filterPreset || 'all'

  const presetLabels: Record<string, string> = {
    all: 'All time',
    today: 'Today',
    '7d': 'Last 7 days',
    '30d': 'Last 30 days',
    'this-week': 'This week',
    'last-week': 'Last week',
    'this-month': 'This month',
    'prev-month': 'Previous month',
    mtd: 'Month to date',
    'this-quarter': 'This quarter',
    'last-quarter': 'Last quarter',
    'this-year': 'This year',
    'last-year': 'Last year',
    ytd: 'Year to date',
  }
  const presetOptions = [
    'all','today','7d','30d',
    'this-week','last-week',
    'this-month','prev-month','mtd',
    'this-quarter','last-quarter',
    'this-year','last-year','ytd',
  ] as const

  function applyPreset(v: string) {
    const now = new Date()
    const fmt = (d: Date) => {
      const y = d.getFullYear()
      const m = String(d.getMonth() + 1).padStart(2, '0')
      const da = String(d.getDate()).padStart(2, '0')
      return `${y}-${m}-${da}`
    }
    const addDays = (d: Date, n: number) => {
      const dd = new Date(d.getFullYear(), d.getMonth(), d.getDate())
      dd.setDate(dd.getDate() + n)
      return dd
    }
    const startOfWeekMon = (d: Date) => {
      const dd = new Date(d.getFullYear(), d.getMonth(), d.getDate())
      const dow = dd.getDay() // 0=Sun..6=Sat
      const delta = (dow + 6) % 7 // Monday=0
      dd.setDate(dd.getDate() - delta)
      return dd
    }
    const startOfWeekSun = (d: Date) => {
      const dd = new Date(d.getFullYear(), d.getMonth(), d.getDate())
      const dow = dd.getDay() // 0=Sun..6=Sat
      dd.setDate(dd.getDate() - dow) // Sunday=0
      return dd
    }
    const startOfQuarter = (d: Date) => {
      const qStartMonth = Math.floor(d.getMonth() / 3) * 3
      return new Date(d.getFullYear(), qStartMonth, 1)
    }
    const monthEnd = (d: Date) => new Date(d.getFullYear(), d.getMonth() + 1, 0)
    const endOfQuarter = (d: Date) => {
      const qStartMonth = Math.floor(d.getMonth() / 3) * 3
      return new Date(d.getFullYear(), qStartMonth + 3, 0)
    }

    const todayStr = fmt(now)
    const startOfMonth = new Date(now.getFullYear(), now.getMonth(), 1)
    const endOfMonth = new Date(now.getFullYear(), now.getMonth() + 1, 1) // exclusive end
    const prevMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1)
    const prevMonthEnd = new Date(now.getFullYear(), now.getMonth(), 1) // exclusive end
    const daysAgo = (n: number) => fmt(addDays(now, -n))

    switch (v) {
      case 'all':
        setFilters({ filterPreset: v })
        break
      case 'today':
        setFilters({ startDate: todayStr, endDate: todayStr, filterPreset: v })
        break
      case '7d':
        setFilters({ startDate: daysAgo(7), endDate: todayStr, filterPreset: v })
        break
      case '30d':
        // Last 30 days: start = today-30, end = today
        setFilters({ startDate: daysAgo(30), endDate: todayStr, filterPreset: v })
        break
      case 'this-week': {
        const sun = startOfWeekSun(now)
        setFilters({ startDate: fmt(sun), endDate: todayStr, filterPreset: v })
        break
      }
      case 'last-week': {
        const sun = startOfWeekSun(now)
        const lastSun = addDays(sun, -7)
        const lastSat = addDays(sun, -1)
        setFilters({ startDate: fmt(lastSun), endDate: fmt(lastSat), filterPreset: v })
        break
      }
      case 'this-month':
        setFilters({ startDate: fmt(startOfMonth), endDate: fmt(monthEnd(now)), filterPreset: v })
        break
      case 'prev-month': {
        const prevEnd = monthEnd(prevMonthStart)
        setFilters({ startDate: fmt(prevMonthStart), endDate: fmt(prevEnd), filterPreset: v })
        break
      }
      case 'mtd':
        setFilters({ startDate: fmt(startOfMonth), endDate: todayStr, filterPreset: v })
        break
      case 'this-quarter': {
        const soq = startOfQuarter(now)
        const eoq = endOfQuarter(now)
        setFilters({ startDate: fmt(soq), endDate: fmt(eoq), filterPreset: v })
        break
      }
      case 'last-quarter': {
        const thisQ = startOfQuarter(now)
        const lastQ = new Date(thisQ.getFullYear(), thisQ.getMonth() - 3, 1)
        const lastQEnd = endOfQuarter(lastQ)
        setFilters({ startDate: fmt(lastQ), endDate: fmt(lastQEnd), filterPreset: v })
        break
      }
      case 'this-year': {
        const yStart = new Date(now.getFullYear(), 0, 1)
        setFilters({ startDate: fmt(yStart), endDate: todayStr, filterPreset: v })
        break
      }
      case 'last-year': {
        const lastYStart = new Date(now.getFullYear() - 1, 0, 1)
        const lastYEnd = new Date(now.getFullYear() - 1, 11, 31)
        setFilters({ startDate: fmt(lastYStart), endDate: fmt(lastYEnd), filterPreset: v })
        break
      }
      case 'ytd': {
        const yStart = new Date(now.getFullYear(), 0, 1)
        setFilters({ startDate: fmt(yStart), endDate: todayStr, filterPreset: v })
        break
      }
    }
  }

  return (
    <div className="flex items-center gap-3">
      <FilterbarControl
        active={selectedPreset}
        options={presetOptions}
        labels={presetLabels}
        onChange={applyPreset}
        disabled={!!disabled}
      />
      <div className="flex items-center gap-2">
        <label className="text-[11px] text-[hsl(var(--muted-foreground))]">Start</label>
        <DatePickerField
          value={filters.startDate}
          onChangeAction={(v) => {
            setFilters({ ...filters, startDate: v, filterPreset: undefined })
          }}
          disabled={!!disabled}
          ariaLabel="Start date"
        />
        <label className="text-[11px] text-[hsl(var(--muted-foreground))]">End</label>
        <DatePickerField
          value={filters.endDate}
          onChangeAction={(v) => {
            setFilters({ ...filters, endDate: v, filterPreset: undefined })
          }}
          disabled={!!disabled}
          ariaLabel="End date"
        />
        <button
          type="button"
          className={`text-[12px] h-8 px-2 rounded-md border ${disabled?'opacity-60 cursor-not-allowed':'hover:bg-[hsl(var(--muted))]'} bg-[hsl(var(--card))] text-[hsl(var(--foreground))] border-[hsl(var(--border))]`}
          onClick={disabled ? undefined : () => {
            reset()
          }}
          disabled={!!disabled}
          title="Clear filters"
        >
          Clear
        </button>
        {widgets && onApplyMappingAction && (
          <>
            <button
            type="button"
            className="text-[12px] h-8 px-2 rounded-md border bg-[hsl(var(--card))] text-[hsl(var(--foreground))] border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))]"
            onClick={() => setShowMapping(true)}
            title="Map date fields for widgets"
          >
            <span className="inline-flex items-center gap-1"><RiLink className="w-4 h-4" /> Map Date Fields</span>
          </button>
          <div className="relative">
            <button
              type="button"
              className="text-[12px] h-8 px-2 rounded-md border bg-[hsl(var(--card))] text-[hsl(var(--foreground))] border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))]"
              onClick={() => setShowBreakPanel(v => !v)}
              title="Break global filter link for selected widgets"
            >
              <span className="inline-flex items-center gap-1"><RiLinkUnlink className="w-4 h-4" /> Break Filter Link</span>
            </button>
            {showBreakPanel && (
              <div className="absolute right-0 z-20 mt-1 w-[280px] max-h-[260px] overflow-auto rounded-md border bg-card p-2 shadow">
                <div className="text-[11px] text-muted-foreground mb-1">Disable global date filters for widgets</div>
                <ul className="space-y-1 text-[12px]">
                    {Object.values(widgets || {}).map((w) => {
                      const id = String(w.id)
                      let checked = false
                      try { checked = (typeof window !== 'undefined') && localStorage.getItem(`gf_break_${id}`) === '1' } catch {}
                      return (
                        <li key={id} className="flex items-center justify-between gap-2">
                          <span className="truncate" title={w.title}>{w.title}</span>
                          <label className="inline-flex items-center gap-1">
                            <input
                              type="checkbox"
                              className="accent-[hsl(var(--primary))]"
                              defaultChecked={checked}
                              onChange={(e) => {
                                try {
                                  if (e.target.checked) localStorage.setItem(`gf_break_${id}`, '1')
                                  else localStorage.removeItem(`gf_break_${id}`)
                                  // Notify the affected widget to recompute effective where
                                  if (typeof window !== 'undefined') window.dispatchEvent(new CustomEvent('global-filters-break-change', { detail: { widgetId: id } } as any))
                                } catch {}
                              }}
                              disabled={!!disabled}
                            />
                            <span className="text-[11px]">Break</span>
                          </label>
                        </li>
                      )
                    })}
                  </ul>
                  <div className="flex items-center justify-end mt-2">
                    <button type="button" className="text-[12px] h-7 px-2 rounded-md border bg-[hsl(var(--card))] hover:bg-[hsl(var(--muted))]" onClick={() => setShowBreakPanel(false)}>Close</button>
                  </div>
                </div>
              )}
            </div>
            <DateFieldMappingDialog
              open={showMapping}
              onOpenChangeAction={setShowMapping}
              widgets={widgets}
              onApplyAction={(map) => { onApplyMappingAction(map) }}
            />
          </>
        )}
      </div>
    </div>
  )
}
