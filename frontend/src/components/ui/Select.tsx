"use client"

import React, { forwardRef, useId } from 'react'
import { RiArrowDownSLine } from '@remixicon/react'
import { fieldClasses, type FieldSize } from './Input'

export interface SelectProps extends Omit<React.SelectHTMLAttributes<HTMLSelectElement>, 'size'> {
  size?: FieldSize
  label?: string
  hint?: string
  error?: string | boolean
}

// Native <select> on the same height system as Input. Custom caret via Remix icon
// (logical `end-2` for RTL); native arrow suppressed with appearance-none.
const Select = forwardRef<HTMLSelectElement, SelectProps>(function Select(
  { size = 'md', label, hint, error, id, className = '', children, ...rest },
  ref,
) {
  const autoId = useId()
  const selectId = id || autoId
  const hasError = !!error
  const errorMsg = typeof error === 'string' ? error : undefined
  return (
    <div className="flex flex-col gap-1">
      {label && (
        <label htmlFor={selectId} className="text-xs font-medium text-foreground">{label}</label>
      )}
      <div className="relative">
        <select
          ref={ref}
          id={selectId}
          aria-invalid={hasError || undefined}
          className={fieldClasses(size, hasError, `appearance-none pe-8 ${className}`)}
          {...rest}
        >
          {children}
        </select>
        <RiArrowDownSLine
          aria-hidden
          className="pointer-events-none absolute end-2 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground"
        />
      </div>
      {errorMsg ? (
        <span className="text-2xs text-[hsl(var(--danger))]">{errorMsg}</span>
      ) : hint ? (
        <span className="text-2xs text-muted-foreground">{hint}</span>
      ) : null}
    </div>
  )
})

export default Select
