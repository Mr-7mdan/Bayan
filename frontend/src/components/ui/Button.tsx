"use client"

import React, { forwardRef } from 'react'

export type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'outline' | 'danger'
export type ButtonSize = 'sm' | 'md'

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant
  size?: ButtonSize
  /** Shows a spinner and disables the button. */
  loading?: boolean
  /** Leading icon slot (Remix icon). Hidden while loading (spinner takes its place). */
  icon?: React.ReactNode
}

// Canonical action button. Refines the existing `.btn-primary` brand-cyan look
// using F1 tokens: rounded-md (--radius-md), text-{xs,sm} ramp keys, and a press
// state driven by the motion tokens (--dur-fast / --ease-out). Focus ring comes
// from the global :where(button):focus-visible rule in globals.css.
const VARIANTS: Record<ButtonVariant, string> = {
  primary: 'bg-primary text-primary-foreground border-transparent hover:bg-primary/90',
  secondary: 'bg-card text-foreground border-[hsl(var(--border))] hover:bg-muted',
  outline: 'bg-transparent text-foreground border-[hsl(var(--border))] hover:bg-muted',
  ghost: 'bg-transparent text-foreground border-transparent hover:bg-muted',
  danger: 'bg-[hsl(var(--danger))] text-white border-transparent hover:bg-[hsl(var(--danger)/0.9)]',
}

const SIZES: Record<ButtonSize, string> = {
  sm: 'h-8 px-3 text-xs gap-1.5',
  md: 'h-9 px-4 text-sm gap-2',
}

const Spinner = ({ className = '' }: { className?: string }) => (
  <span
    aria-hidden
    className={`inline-block rounded-full border-2 border-current border-t-transparent animate-spin ${className}`}
  />
)

const Button = forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  { variant = 'primary', size = 'md', loading = false, icon, disabled, className = '', children, ...rest },
  ref,
) {
  const spin = size === 'sm' ? 'h-3.5 w-3.5' : 'h-4 w-4'
  return (
    <button
      ref={ref}
      disabled={disabled || loading}
      aria-busy={loading || undefined}
      className={[
        'inline-flex items-center justify-center rounded-md border font-medium whitespace-nowrap select-none',
        'transition-[transform,background-color,color,border-color] [transition-duration:var(--dur-fast)] [transition-timing-function:var(--ease-out)]',
        'active:scale-[0.98] disabled:opacity-50 disabled:pointer-events-none',
        VARIANTS[variant],
        SIZES[size],
        className,
      ].join(' ')}
      {...rest}
    >
      {loading ? <Spinner className={spin} /> : icon ? <span className="inline-flex shrink-0">{icon}</span> : null}
      {children}
    </button>
  )
})

export default Button
