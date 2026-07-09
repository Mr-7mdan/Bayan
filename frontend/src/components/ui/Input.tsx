"use client"

import React, { forwardRef, useId } from 'react'

export type FieldSize = 'sm' | 'md'

export interface InputProps extends Omit<React.InputHTMLAttributes<HTMLInputElement>, 'size'> {
  size?: FieldSize
  label?: string
  hint?: string
  /** When set, paints a danger border + shows the message below. `true` = border only. */
  error?: string | boolean
}

// One height system shared with Select: h-8 (sm) / h-9 (md). Token borders/radius,
// bg-card surface (no opacity hacks). Focus ring inherited from globals.
export const FIELD_SIZES: Record<FieldSize, string> = {
  sm: 'h-8 text-xs px-2.5',
  md: 'h-9 text-sm px-3',
}

export function fieldClasses(size: FieldSize, hasError: boolean, extra = '') {
  return [
    'w-full rounded-md border bg-card text-foreground placeholder:text-muted-foreground',
    'transition-colors [transition-duration:var(--dur-fast)] [transition-timing-function:var(--ease-out)]',
    'disabled:opacity-50 disabled:cursor-not-allowed',
    hasError ? 'border-[hsl(var(--danger))]' : 'border-[hsl(var(--border))]',
    FIELD_SIZES[size],
    extra,
  ].join(' ')
}

const Input = forwardRef<HTMLInputElement, InputProps>(function Input(
  { size = 'md', label, hint, error, id, className = '', ...rest },
  ref,
) {
  const autoId = useId()
  const inputId = id || autoId
  const hasError = !!error
  const errorMsg = typeof error === 'string' ? error : undefined
  return (
    <div className="flex flex-col gap-1">
      {label && (
        <label htmlFor={inputId} className="text-xs font-medium text-foreground">{label}</label>
      )}
      <input
        ref={ref}
        id={inputId}
        aria-invalid={hasError || undefined}
        className={fieldClasses(size, hasError, className)}
        {...rest}
      />
      {errorMsg ? (
        <span className="text-2xs text-[hsl(var(--danger))]">{errorMsg}</span>
      ) : hint ? (
        <span className="text-2xs text-muted-foreground">{hint}</span>
      ) : null}
    </div>
  )
})

export default Input
