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
    // Active tab — branded with the Bayan cyan. We use a soft accent tint
    // background + primary text + a 2px inset bottom border that reads as
    // the active indicator. This works for both `line` and `solid` variants
    // without making tabs look like CTA buttons (which would happen with
    // `bg-primary` solid-fill).
    "data-[state=active]:bg-[hsl(var(--accent)/0.18)]",
    "data-[state=active]:text-[hsl(var(--primary-deep))]",
    "data-[state=active]:font-semibold",
    "data-[state=active]:border data-[state=active]:border-[hsl(var(--primary)/0.28)]",
    "data-[state=active]:shadow-[inset_0_-2px_0_hsl(var(--primary))]",
    "dark:data-[state=active]:bg-[hsl(var(--primary)/0.14)]",
    "dark:data-[state=active]:text-[hsl(var(--primary))]",
    "dark:data-[state=active]:border-[hsl(var(--primary)/0.32)]",
    // Inactive: muted; hover lifts toward foreground
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
