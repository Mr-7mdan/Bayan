"use client"

import React, { useEffect, useState } from 'react'
import { RiCheckLine, RiErrorWarningLine } from '@remixicon/react'

export type StatusPillState = 'idle' | 'saving' | 'saved' | 'error'

export interface StatusPillProps {
  state: StatusPillState
  /** Called when the user clicks Retry in the error state. */
  onRetry?: () => void
  savingLabel?: string
  savedLabel?: string
  errorLabel?: string
  retryLabel?: string
  /** ms before the "saved" pill fades out. Default 2000. */
  saveFadeMs?: number
  className?: string
}

// Tiny inline autosave indicator for toolbar embedding. saving = spinner,
// saved = check (auto-fades), error = message + Retry callback. Uses the type
// ramp (text-2xs) and motion tokens; status colors from --success/--danger.
export default function StatusPill({
  state,
  onRetry,
  savingLabel = 'Saving…',
  savedLabel = 'Saved',
  errorLabel = 'Save failed',
  retryLabel = 'Retry',
  saveFadeMs = 2000,
  className = '',
}: StatusPillProps) {
  const [faded, setFaded] = useState(false)

  useEffect(() => {
    if (state !== 'saved') { setFaded(false); return }
    const t = setTimeout(() => setFaded(true), saveFadeMs)
    return () => clearTimeout(t)
  }, [state, saveFadeMs])

  if (state === 'idle') return null

  const base = `inline-flex items-center gap-1 text-2xs font-medium ${className}`

  if (state === 'saving') {
    return (
      <span className={`${base} text-muted-foreground`}>
        <span aria-hidden className="inline-block h-3 w-3 rounded-full border-2 border-current border-t-transparent animate-spin" />
        {savingLabel}
      </span>
    )
  }

  if (state === 'saved') {
    return (
      <span
        className={`${base} text-[hsl(var(--success))] transition-opacity [transition-duration:var(--dur-base)] [transition-timing-function:var(--ease-out)] ${faded ? 'opacity-0' : 'opacity-100'}`}
      >
        <RiCheckLine className="h-3.5 w-3.5" />
        {savedLabel}
      </span>
    )
  }

  // error
  return (
    <span className={`${base} text-[hsl(var(--danger))]`}>
      <RiErrorWarningLine className="h-3.5 w-3.5" />
      {errorLabel}
      {onRetry && (
        <button
          type="button"
          onClick={onRetry}
          className="ms-1 rounded px-1 underline underline-offset-2 hover:no-underline"
        >
          {retryLabel}
        </button>
      )}
    </span>
  )
}
