"use client"
import React from 'react'

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
