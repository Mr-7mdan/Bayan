"use client"

import * as React from 'react'
import * as AccordionPrimitive from '@radix-ui/react-accordion'
import { RiArrowDownSLine } from '@remixicon/react'

export const Accordion = AccordionPrimitive.Root
export const AccordionItem = AccordionPrimitive.Item

export const AccordionTrigger = React.forwardRef<
  HTMLButtonElement,
  React.ComponentPropsWithoutRef<typeof AccordionPrimitive.Trigger>
>(({ className = '', children, ...props }, ref) => (
  <AccordionPrimitive.Header className="flex">
    <AccordionPrimitive.Trigger
      ref={ref}
      className={[
        'group w-full flex items-center justify-between rounded-none relative z-[60]',
        // Rely on outer scroller padding for alignment
        'px-0 py-1.5 text-xs font-medium',
        // Section headers: use secondary tint in both states (dark-mode aligns via tokens)
        'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary)/0.6)] data-[state=open]:bg-[hsl(var(--secondary)/0.6)] data-[state=open]:hover:bg-[hsl(var(--secondary)/0.6)]',
        // Top border for group outline (bottom border lives on Content)
        'border-t border-[hsl(var(--border))]',
        className,
      ].join(' ')}
      {...props}
    >
      <span className="truncate text-foreground">{children}</span>
      <RiArrowDownSLine className="ml-2 size-4 shrink-0 text-muted-foreground transition-transform group-data-[state=open]:rotate-180" />
    </AccordionPrimitive.Trigger>
  </AccordionPrimitive.Header>
))
AccordionTrigger.displayName = 'AccordionTrigger'

export const AccordionContent = React.forwardRef<
  HTMLDivElement,
  React.ComponentPropsWithoutRef<typeof AccordionPrimitive.Content>
>(({ className = '', children, ...props }, ref) => (
  <AccordionPrimitive.Content
    ref={ref}
    className={[
      'text-foreground relative z-[60] data-[state=closed]:overflow-hidden data-[state=open]:overflow-visible',
      'data-[state=closed]:animate-accordion-up data-[state=open]:animate-accordion-down',
      // Expanded panel background and bottom outline
      'bg-card border-b border-[hsl(var(--border))]',
      // Rely on outer scroller padding for alignment
      'px-0 py-2',
      className,
    ].join(' ')}
    {...props}
  >
    <div className="space-y-3">{children}</div>
  </AccordionPrimitive.Content>
))
AccordionContent.displayName = 'AccordionContent'
