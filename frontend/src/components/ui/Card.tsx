"use client"

import React from 'react'

export interface CardProps extends React.HTMLAttributes<HTMLDivElement> {
  header?: React.ReactNode
  footer?: React.ReactNode
  /** Adds a hover elevation transition (shadow-card -> shadow-popover). */
  interactive?: boolean
  /** Padding for the body region. Default 'md'. */
  padding?: 'none' | 'sm' | 'md'
}

// Canonical surface: --card token background, --shadow-1 (shadow-card), radius-lg,
// token border. No opacity hacks. Header/footer are divided token borders.
const PAD: Record<NonNullable<CardProps['padding']>, string> = {
  none: '',
  sm: 'p-3',
  md: 'p-4',
}

export default function Card({
  header,
  footer,
  interactive = false,
  padding = 'md',
  className = '',
  children,
  ...rest
}: CardProps) {
  return (
    <div
      className={[
        'rounded-lg border border-[hsl(var(--border))] bg-card text-card-foreground shadow-card',
        interactive
          ? 'transition-shadow [transition-duration:var(--dur-base)] [transition-timing-function:var(--ease-out)] hover:shadow-popover'
          : '',
        className,
      ].join(' ')}
      {...rest}
    >
      {header && (
        <div className="border-b border-[hsl(var(--border))] px-4 py-3 text-sm font-semibold">{header}</div>
      )}
      <div className={PAD[padding]}>{children}</div>
      {footer && (
        <div className="border-t border-[hsl(var(--border))] px-4 py-3">{footer}</div>
      )}
    </div>
  )
}
