"use client"

import React from 'react'
import { Switch } from '@/components/Switch'

export default function TabsControls({ local, setLocalAction, updateConfigAction, allFieldNames }: {
  local: any
  setLocalAction: (next: any) => void
  updateConfigAction: (next: any) => void
  allFieldNames: string[]
}) {
  return (
    <>
      <div className="border rounded-md p-2 space-y-2">
        <div className="text-[11px] font-medium text-muted-foreground">Tabs</div>
        <div className="grid grid-cols-[minmax(96px,120px),minmax(0,1fr)] gap-x-3 gap-y-2 items-center">
          <label className="text-xs text-muted-foreground">Field</label>
          <select
            className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
            value={String(((local.options as any)?.tabsField || ''))}
            onChange={(e) => {
              const val = e.target.value || undefined
              const opts = { ...(local.options || {}), tabsField: val }
              const next = { ...local, options: opts }
              setLocalAction(next); updateConfigAction(next)
            }}
          >
            <option value="">Off</option>
            {allFieldNames.map((n) => (
              <option key={n} value={n}>{n}</option>
            ))}
          </select>
          <label className="text-xs text-muted-foreground">Variant</label>
          <select
            className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
            value={String(((local.options as any)?.tabsVariant || 'line'))}
            onChange={(e) => { const opts = { ...(local.options || {}), tabsVariant: e.target.value as any }; const next = { ...local, options: opts }; setLocalAction(next); updateConfigAction(next) }}
          >
            <option value="line">line</option>
            <option value="solid">solid</option>
          </select>
          <label className="text-xs text-muted-foreground">Max items</label>
          <input
            type="number" min={1} max={24}
            className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
            value={Number(((local.options as any)?.tabsMaxItems ?? 8))}
            onChange={(e) => { const n = Math.max(1, Math.min(24, Number(e.target.value || 8))); const opts = { ...(local.options || {}), tabsMaxItems: n }; const next = { ...local, options: opts }; setLocalAction(next); updateConfigAction(next) }}
          />
          <label className="text-xs text-muted-foreground">Stretch to full width</label>
          <label className="flex items-center gap-2 text-xs">
            <Switch checked={!!(local.options as any)?.tabsStretch}
              onChangeAction={(checked) => { const opts = { ...(local.options || {}), tabsStretch: checked }; const next = { ...local, options: opts }; setLocalAction(next); updateConfigAction(next) }} />
            <span>Stretch</span>
          </label>
          <label className="text-xs text-muted-foreground">Tab sort by</label>
          <select
            className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
            value={String(((local.options as any)?.tabsSort?.by || ''))}
            onChange={(e) => {
              const val = e.target.value
              const prev = (local.options as any)?.tabsSort || {}
              const nextSort = val ? { ...prev, by: val as any } : undefined
              const opts = { ...(local.options || {}), ...(nextSort ? { tabsSort: nextSort } : { tabsSort: undefined }) }
              const next = { ...local, options: opts }
              setLocalAction(next); updateConfigAction(next)
            }}
          >
            <option value="">None</option>
            <option value="x">X Axis value</option>
            <option value="value">Aggregate value</option>
          </select>
          <label className="text-xs text-muted-foreground">Direction</label>
          <select
            className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
            value={String(((local.options as any)?.tabsSort?.direction || 'asc'))}
            onChange={(e) => {
              const prev = (local.options as any)?.tabsSort || {}
              const nextSort = { ...prev, direction: e.target.value as any }
              const opts = { ...(local.options || {}), tabsSort: nextSort }
              const next = { ...local, options: opts }
              setLocalAction(next); updateConfigAction(next)
            }}
          >
            {String(((local.options as any)?.tabsSort?.by || '')) === 'value' ? (
              <>
                <option value="desc">Largest → Smallest</option>
                <option value="asc">Smallest → Largest</option>
              </>
            ) : (
              <>
                <option value="asc">Asc (A → Z / Oldest → Newest)</option>
                <option value="desc">Desc (Z → A / Newest → Oldest)</option>
              </>
            )}
          </select>
          <label className="text-xs text-muted-foreground">Tab label case</label>
          <select
            className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
            value={String(((local.options as any)?.tabsLabelCase || 'legend'))}
            onChange={(e) => {
              const opts = { ...(local.options || {}), tabsLabelCase: e.target.value as any }
              const next = { ...local, options: opts }
              setLocalAction(next); updateConfigAction(next)
            }}
          >
            <option value="legend">Follow legend case</option>
            <option value="lowercase">lowercase</option>
            <option value="capitalize">Capitalize</option>
            <option value="proper">Proper Case</option>
          </select>
          <label className="text-xs text-muted-foreground">Show &quot;All&quot; tab</label>
          <label className="flex items-center gap-2 text-xs">
            <Switch
              checked={!!(local.options as any)?.tabsShowAll}
              onChangeAction={(checked) => {
                const opts = { ...(local.options || {}), tabsShowAll: checked }
                const next = { ...local, options: opts }
                setLocalAction(next); updateConfigAction(next)
              }}
            />
            <span>Include an &quot;All&quot; tab (first)</span>
          </label>
        </div>
      </div>
      <div className="text-[11px] text-muted-foreground">Creates one tab per distinct value of the selected field (after filters). Each tab shows the same content filtered to that value.</div>
    </>
  )
}
