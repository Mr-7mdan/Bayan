"use client"

import React, { createContext, useContext, useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { RiArrowDownSLine } from '@remixicon/react'

type Ctx = {
  value: string | undefined
  setValue: (v: string) => void
  open: boolean
  setOpen: (v: boolean) => void
  triggerRef: React.RefObject<HTMLElement | null>
  labels: Record<string, string>
  registerLabel: (v: string, label?: string) => void
}
const SelectCtx = createContext<Ctx | null>(null)

export function Select({ value, onValueChangeAction, children }: { value?: string; onValueChangeAction?: (v: string)=>void; children: React.ReactNode }) {
  const [val, setVal] = useState<string | undefined>(value)
  const [open, setOpen] = useState(false)
  const triggerRef = useRef<HTMLElement>(null)
  const [labels, setLabels] = useState<Record<string, string>>({})
  const labelsInitRef = useRef<boolean>(false)
  useEffect(() => { if (value !== undefined) setVal(value) }, [value])
  const setValue = (v: string) => { setVal(v); onValueChangeAction && onValueChangeAction(v) }
  const registerLabel = (v: string, label?: string) => {
    setLabels((prev) => {
      const existed = Object.prototype.hasOwnProperty.call(prev, v)
      const prevVal = prev[v]
      if (label == null) {
        if (!existed) return prev // no change
        const { [v]: _omit, ...rest } = prev
        return rest
      }
      if (existed && prevVal === label) return prev // no change
      return { ...prev, [v]: label }
    })
  }
  // Pre-scan children tree to collect SelectItem labels even when popover is closed
  useEffect(() => {
    if (labelsInitRef.current) return
    const next: Record<string, string> = {}
    const toText = (node: React.ReactNode): string => {
      if (node == null) return ''
      if (typeof node === 'string' || typeof node === 'number') return String(node)
      if (Array.isArray(node)) return node.map(toText).join('')
      if (React.isValidElement(node)) return toText((node as React.ReactElement<any>).props?.children)
      return ''
    }
    const walk = (node: React.ReactNode) => {
      if (node == null) return
      if (Array.isArray(node)) { node.forEach(walk); return }
      if (React.isValidElement(node)) {
        const el = node as React.ReactElement<any>
        if (el.type === SelectItem) {
          const v = el.props?.value as string
          const label = toText(el.props?.children).trim()
          if (v && label) next[v] = label
        }
        if (el.props && 'children' in el.props) walk(el.props.children)
      }
    }
    walk(children)
    const nextKeys = Object.keys(next)
    if (nextKeys.length === 0) return
    setLabels(next)
    labelsInitRef.current = true
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [children])
  const ctx = useMemo(() => ({ value: val, setValue, open, setOpen, triggerRef, labels, registerLabel }), [val, open, labels])
  return <SelectCtx.Provider value={ctx}>{children}</SelectCtx.Provider>
}

export function SelectTrigger({ children, className, disabled, ...rest }: React.ButtonHTMLAttributes<HTMLButtonElement>) {
  const ctx = useContext(SelectCtx)!
  return (
    <button
      type="button"
      {...rest}
      disabled={!!disabled}
      ref={ctx.triggerRef as any}
      onClick={(e) => { if (disabled) { e.preventDefault(); return } ctx.setOpen(!ctx.open) }}
      className={`${className || ''} inline-flex h-8 items-center justify-between gap-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] px-2 text-sm shadow-none ring-0 outline-none focus:ring-0 focus:shadow-none`}
    >
      {children}
      <RiArrowDownSLine className="h-4 w-4 text-muted-foreground" aria-hidden="true" />
    </button>
  )
}

export function SelectValue({ placeholder }: { placeholder?: string }) {
  const ctx = useContext(SelectCtx)!
  const label = ctx.value ? ctx.labels[ctx.value] : undefined
  return <span className="truncate">{label || ctx.value || placeholder || 'Select...'}</span>
}

export function SelectContent({ children, className }: { children: React.ReactNode; className?: string }) {
  const ctx = useContext(SelectCtx)!
  const [rect, setRect] = useState<{ left: number; top: number; width: number } | null>(null)

  useEffect(() => {
    if (!ctx.open) return
    const el = ctx.triggerRef.current
    if (!el) return
    const r = (el as HTMLElement).getBoundingClientRect()
    setRect({ left: r.left, top: r.bottom + 6, width: r.width })
  }, [ctx.open])

  useEffect(() => {
    function onDoc(e: MouseEvent) {
      const target = e.target as Node
      if (!target) return
      const menu = document.getElementById('select-content-portal')
      if (menu && !menu.contains(target) && ctx.triggerRef.current && !ctx.triggerRef.current.contains(target as Node)) {
        ctx.setOpen(false)
      }
    }
    function onEsc(e: KeyboardEvent) { if (e.key === 'Escape') ctx.setOpen(false) }
    if (ctx.open) {
      document.addEventListener('mousedown', onDoc)
      document.addEventListener('keydown', onEsc)
    }
    return () => {
      document.removeEventListener('mousedown', onDoc)
      document.removeEventListener('keydown', onEsc)
    }
  }, [ctx.open])

  if (typeof document === 'undefined' || !ctx.open || !rect) return null
  return createPortal(
    <div id="select-content-portal" className="fixed" style={{ left: rect.left, top: rect.top, width: rect.width, zIndex: 1100 }}>
      <div
        role="listbox"
        className={`rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] shadow-none overflow-auto max-h-[260px] ${className || ''}`}
      >
        {children}
      </div>
    </div>,
    document.body,
  )
}

export function SelectItem({ value, children, onClickAction }: { value: string; children: React.ReactNode; onClickAction?: () => void }) {
  const ctx = useContext(SelectCtx)!
  const selected = ctx.value === value
  // Register label for this value so SelectValue can render it when closed
  useEffect(() => {
    const toText = (node: React.ReactNode): string => {
      if (node == null) return ''
      if (typeof node === 'string' || typeof node === 'number') return String(node)
      if (Array.isArray(node)) return node.map(toText).join('')
      if (React.isValidElement(node)) return toText((node as React.ReactElement<any>).props?.children)
      return ''
    }
    const label = toText(children).trim()
    if (label) ctx.registerLabel(value, label)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [value, children])
  return (
    <div
      role="option"
      aria-selected={selected}
      className={`px-3 py-2 cursor-pointer text-sm border-b border-[hsl(var(--border))] last:border-b-0 hover:bg-[hsl(var(--muted))] ${selected ? 'bg-[hsl(var(--muted))]' : ''}`}
      onClick={() => { ctx.setValue(value); ctx.setOpen(false); onClickAction && onClickAction() }}
    >
      {children}
    </div>
  )
}
