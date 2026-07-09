"use client"

import React from 'react'
import EmptyStateBase from '@/components/feedback/EmptyState'
import Button from './Button'

export interface EmptyStateAction {
  label: string
  onClick: () => void
  icon?: React.ReactNode
}

export interface EmptyStateProps {
  icon?: React.ReactNode
  title: string
  hint?: string
  primary?: EmptyStateAction
  secondary?: EmptyStateAction
}

// Canonical empty state. Composes the presentational feedback/EmptyState (icon +
// title + description layout) and adds token-styled CTAs via the Button primitive.
// Import this from `@/components/ui` going forward; feedback/EmptyState stays the
// low-level, client-safe layout used inside route error/not-found boundaries.
export default function EmptyState({ icon, title, hint, primary, secondary }: EmptyStateProps) {
  const action = (primary || secondary) ? (
    <div className="flex items-center justify-center gap-2">
      {primary && (
        <Button size="sm" variant="primary" icon={primary.icon} onClick={primary.onClick}>{primary.label}</Button>
      )}
      {secondary && (
        <Button size="sm" variant="outline" icon={secondary.icon} onClick={secondary.onClick}>{secondary.label}</Button>
      )}
    </div>
  ) : undefined
  return <EmptyStateBase icon={icon} title={title} description={hint} action={action} />
}
