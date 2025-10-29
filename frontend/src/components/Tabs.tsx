"use client"

import * as React from "react"
import * as RadixTabs from "@radix-ui/react-tabs"
import clsx from "clsx"

export type TabsListVariant = "line" | "solid"

export function Tabs(
  { value, defaultValue, onValueChangeAction, children, className }: { value?: string; defaultValue?: string; onValueChangeAction?: (v: string) => void; children: React.ReactNode; className?: string }
) {
  return (
    <RadixTabs.Root value={value} defaultValue={defaultValue} onValueChange={onValueChangeAction} className={className}>
      {children}
    </RadixTabs.Root>
  )
}

export function TabsList(
  { variant = "line", className, style, children }: { variant?: TabsListVariant; className?: string; style?: React.CSSProperties; children: React.ReactNode }
) {
  const base = clsx(
    "!inline-flex items-center gap-2 rounded-md max-w-full overflow-x-auto overflow-y-hidden whitespace-nowrap px-1 h-9 flex-nowrap no-scrollbar",
    variant === "solid"
      ? "bg-[hsl(var(--secondary))] p-1"
      : "border-b border-[hsl(var(--border))]"
  )
  return (
    <RadixTabs.List className={clsx(base, className)} style={style}>
      {children}
    </RadixTabs.List>
  )
}

export function TabsTrigger(
  { value, children, className, disabled, style }: { value: string; children: React.ReactNode; className?: string; disabled?: boolean; style?: React.CSSProperties }
) {
  const base = clsx(
    "px-3 py-1 text-xs rounded-md mx-0.5 h-7 leading-5 shrink-0",
    "data-[state=active]:bg-card data-[state=active]:text-foreground data-[state=active]:font-semibold",
    "data-[state=active]:shadow-sm data-[state=active]:border data-[state=active]:border-[hsl(var(--border))]",
    // Light: muted; Dark: closer to foreground for legibility
    "text-muted-foreground dark:text-[hsl(var(--foreground)/0.9)] hover:text-foreground hover:bg-[hsl(var(--secondary)/0.55)] transition-colors",
    // Truncate only when not active; active auto-fits to content
    "whitespace-nowrap min-w-[84px]",
    "data-[state=inactive]:max-w-[220px] data-[state=inactive]:overflow-hidden data-[state=inactive]:text-ellipsis",
    "data-[state=active]:max-w-none data-[state=active]:overflow-visible",
    // Keyboard focus
    "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[hsl(var(--ring))]",
    disabled ? "opacity-50 cursor-not-allowed" : "cursor-pointer"
  )
  return (
    <RadixTabs.Trigger value={value} className={clsx(base, className)} disabled={disabled} style={style}>
      {children}
    </RadixTabs.Trigger>
  )
}

export function TabsContent(
  { value, children, className, forceMount }: { value: string; children: React.ReactNode; className?: string; forceMount?: boolean }
) {
  const fm: true | undefined = forceMount ? true : undefined
  return (
    <RadixTabs.Content value={value} className={clsx("data-[state=inactive]:hidden", className)} forceMount={fm}>
      {children}
    </RadixTabs.Content>
  )
}
