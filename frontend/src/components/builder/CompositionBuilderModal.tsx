"use client"

import { createPortal } from 'react-dom'
import { useEffect, useMemo, useState } from 'react'
import type React from 'react'
import type { CompositionComponent } from '@/types/widgets'

export default function CompositionBuilderModal(props: any) {
  const { open, onClose, value, columns, onChange, choices, onQuickAdd } = props as {
    open: boolean
    onClose: () => void
    value: CompositionComponent[]
    columns: 6 | 8 | 12
    onChange: (next: CompositionComponent[]) => void
    choices?: Array<{ id: string; title: string; type: string }>
    onQuickAdd?: (kind: 'kpi'|'chart'|'table', opts?: { addToLayout?: boolean }) => string
  }
  const [local, setLocal] = useState<CompositionComponent[]>(value || [])
  useEffect(() => { if (open) setLocal(value || []) }, [open, value])

  const add = (kind: CompositionComponent['kind']) => {
    const id = Math.random().toString(36).slice(2)
    if (kind === 'title' || kind === 'subtitle') {
      setLocal((prev) => ([...prev, { id, kind: kind as any, text: kind === 'title' ? 'Title' : 'Subtitle', align: 'left', span: columns }]))
    } else if (kind === 'kpi' || kind === 'chart' || kind === 'table') {
      setLocal((prev) => ([...prev, { id, kind: kind as any, span: Math.min(columns, 4) }]))
    }
  }

  const move = (idx: number, dir: -1 | 1) => {
    setLocal((prev) => {
      const next = [...prev]
      const ni = idx + dir
      if (ni < 0 || ni >= next.length) return prev
      const tmp = next[idx]
      next[idx] = next[ni]
      next[ni] = tmp
      return next
    })
  }

  const remove = (idx: number) => setLocal((prev) => prev.filter((_, i) => i !== idx))

  const save = () => { onChange(local); onClose() }

  const spanOptions = useMemo(() => Array.from({ length: columns }, (_, i) => i + 1), [columns])

  if (!open || typeof window === 'undefined') return null
  return createPortal(
    <div className="fixed inset-0 z-[999] flex items-center justify-center">
      <div className="absolute inset-0 bg-black/40" onClick={onClose} />
      <div className="relative z-[1000] w-[900px] max-w-[95vw] max-h-[90vh] overflow-auto rounded-lg border bg-card p-4 shadow-none">
        <div className="flex items-center justify-between mb-2">
          <div className="text-sm font-medium">Card Composition Builder</div>
          <div className="flex items-center gap-2">
            <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={onClose}>Close</button>
            <button className="text-xs px-2 py-1 rounded-md border bg-[hsl(var(--btn3))] text-black" onClick={save}>Save</button>
          </div>
        </div>
        <div className="grid grid-cols-[220px,1fr] gap-3">
          <div className="rounded-md p-2 bg-[hsl(var(--secondary))]">
            <div className="text-xs text-muted-foreground mb-1">Add components</div>
            <div className="grid grid-cols-2 gap-2">
              {(['title','subtitle','kpi','chart','table'] as const).map((k) => (
                <button key={k} className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => add(k as any)}>{k}</button>
              ))}
            </div>
            <div className="text-[11px] text-muted-foreground mt-2">Drag and drop coming soon — use Up/Down arrows to reorder.</div>
          </div>
          <div className="space-y-2">
            {local.length === 0 && (
              <div className="text-xs text-muted-foreground">No components yet. Add from the left.</div>
            )}
            {local.map((c, idx) => (
              <div
                key={c.id || idx}
                className="grid grid-cols-[1fr,auto] gap-2 rounded-md border p-2 hover:border-[hsl(var(--primary))]"
                draggable
                onDragStart={(e: React.DragEvent<HTMLDivElement>) => {
                  e.dataTransfer.setData('text/plain', String(idx))
                  e.dataTransfer.effectAllowed = 'move'
                }}
                onDragOver={(e: React.DragEvent<HTMLDivElement>) => { e.preventDefault(); e.dataTransfer.dropEffect = 'move' }}
                onDrop={(e: React.DragEvent<HTMLDivElement>) => {
                  e.preventDefault()
                  const fromStr = e.dataTransfer.getData('text/plain')
                  const from = Number(fromStr)
                  if (!Number.isFinite(from) || from === idx) return
                  setLocal((prev) => {
                    const arr = [...prev]
                    const [item] = arr.splice(from, 1)
                    arr.splice(idx, 0, item)
                    return arr
                  })
                }}
              >
                <div className="space-y-2">
                  <div className="flex items-center gap-2">
                    <span className="cursor-grab select-none" title="Drag to reorder">≡</span>
                    <span className="inline-flex items-center px-1.5 py-0.5 rounded bg-[hsl(var(--secondary))] text-[11px] capitalize">{c.kind}</span>
                    <label className="text-[11px] text-muted-foreground">Span</label>
                    <select
                      className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                      value={String(Math.min(columns, Math.max(1, c.span || 1)))}
                      onChange={(e) => {
                        const span = Math.min(columns, Math.max(1, Number(e.target.value) || 1))
                        setLocal((prev) => prev.map((x, i) => i === idx ? ({ ...x, span }) : x))
                      }}
                    >
                      {spanOptions.map((n) => (<option key={n} value={n}>{n}</option>))}
                    </select>
                  </div>
                  {(c.kind === 'title' || c.kind === 'subtitle') && (
                    <div className="grid grid-cols-2 gap-2 items-center">
                      <label className="text-[11px] text-muted-foreground">Text</label>
                      <input
                        className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                        value={String((c as any).text || '')}
                        onChange={(e) => setLocal((prev) => prev.map((x, i) => i === idx ? ({ ...x, text: e.target.value }) : x))}
                      />
                      <label className="text-[11px] text-muted-foreground">Align</label>
                      <select
                        className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                        value={String((c as any).align || 'left')}
                        onChange={(e) => setLocal((prev) => prev.map((x, i) => i === idx ? ({ ...x, align: e.target.value as any }) : x))}
                      >
                        {['left','center','right'].map((a) => (<option key={a} value={a}>{a}</option>))}
                      </select>
                    </div>
                  )}
                  {(c.kind === 'kpi' || c.kind === 'chart' || c.kind === 'table') && (
                    <div className="grid grid-cols-[140px,1fr] gap-2 items-center">
                      <label className="text-[11px] text-muted-foreground">Reference widget</label>
                      <div className="flex items-center gap-2">
                        <select
                          className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                          value={String((c as any).refId || '')}
                          onChange={(e) => setLocal((prev) => prev.map((x, i) => i === idx ? ({ ...x, refId: e.target.value }) : x))}
                        >
                          <option value="">(Select)</option>
                          {(choices || []).filter((w) => w.type === c.kind).map((w) => (
                            <option key={w.id} value={w.id}>{w.id} — {w.title}</option>
                          ))}
                        </select>
                        <input
                          className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs flex-1"
                          value={String((c as any).refId || '')}
                          onChange={(e) => setLocal((prev) => prev.map((x, i) => i === idx ? ({ ...x, refId: e.target.value }) : x))}
                          placeholder="or type id"
                        />
                        {onQuickAdd && (
                          <button
                            className="text-[11px] px-2 py-0.5 rounded border bg-[hsl(var(--btn1))] text-black"
                            title={`Create a new ${c.kind} and link it here`}
                            onClick={() => {
                              try {
                                const id = onQuickAdd(c.kind, { addToLayout: false })
                                if (id) setLocal((prev) => prev.map((x, i) => i === idx ? ({ ...x, refId: id }) : x))
                              } catch {}
                            }}
                          >New</button>
                        )}
                      </div>
                      <div className="col-span-2 text-[11px] text-muted-foreground">
                        {(() => {
                          const id = (c as any).refId || ''
                          const preview = (choices || []).find((w) => w.id === id)
                          return preview ? (
                            <span className="inline-flex items-center gap-2">Preview: <span className="px-1.5 py-0.5 rounded bg-[hsl(var(--secondary))]">{preview.title}</span><span className="px-1 py-0.5 rounded border text-[10px]">ID: {preview.id}</span></span>
                          ) : 'This will render the referenced widget inline.'
                        })()}
                      </div>
                      <label className="text-[11px] text-muted-foreground">Label (override)</label>
                      <input
                        className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                        value={String((c as any).label || '')}
                        onChange={(e) => setLocal((prev) => prev.map((x, i) => i === idx ? ({ ...x, label: e.target.value }) : x))}
                        placeholder="Optional label shown above"
                      />
                    </div>
                  )}
                </div>
                <div className="flex flex-col items-end gap-1">
                  <button className="text-[11px] px-2 py-0.5 rounded border hover:bg-muted" onClick={() => move(idx, -1)} disabled={idx === 0}>↑</button>
                  <button className="text-[11px] px-2 py-0.5 rounded border hover:bg-muted" onClick={() => move(idx, +1)} disabled={idx === local.length - 1}>↓</button>
                  <button className="text-[11px] px-2 py-0.5 rounded border hover:bg-muted" onClick={() => remove(idx)}>Remove</button>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>,
    document.body
  )
}
