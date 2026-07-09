"use client"

import React, { useEffect, useState } from 'react'
import { createPortal } from 'react-dom'
import { RiCloseLine } from '@remixicon/react'
import { useModalFocus } from '@/hooks/useModalFocus'

export type ModalSize = 'sm' | 'md' | 'lg' | 'full'

export interface ModalProps {
  open: boolean
  onClose: () => void
  title?: React.ReactNode
  description?: React.ReactNode
  size?: ModalSize
  footer?: React.ReactNode
  children?: React.ReactNode
  /** Hide the header close button (Esc still closes via useModalFocus). */
  hideClose?: boolean
  className?: string
}

// Portal modal built on the shared useModalFocus hook (Esc + focus trap + restore).
// Unified scrim bg-black/40, shadow-modal (--shadow-3), radius-lg, anim-menu-in entry.
const SIZES: Record<ModalSize, string> = {
  sm: 'w-[440px] max-w-[calc(100vw-2rem)]',
  md: 'w-[560px] max-w-[calc(100vw-2rem)]',
  lg: 'w-[760px] max-w-[calc(100vw-2rem)]',
  full: 'w-[calc(100vw-3rem)] h-[calc(100vh-3rem)]',
}

export default function Modal({
  open,
  onClose,
  title,
  description,
  size = 'md',
  footer,
  children,
  hideClose = false,
  className = '',
}: ModalProps) {
  const panelRef = useModalFocus<HTMLDivElement>(open, onClose)
  const [mounted, setMounted] = useState(false)
  useEffect(() => setMounted(true), [])

  if (!open || !mounted) return null

  return createPortal(
    <div className="fixed inset-0 z-[100] flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/40" onClick={onClose} aria-hidden />
      <div
        ref={panelRef}
        role="dialog"
        aria-modal="true"
        aria-label={typeof title === 'string' ? title : undefined}
        className={[
          'relative z-[1] flex flex-col rounded-lg border border-[hsl(var(--border))] bg-card text-card-foreground shadow-modal anim-menu-in',
          SIZES[size],
          size === 'full' ? '' : 'max-h-[calc(100vh-2rem)]',
          className,
        ].join(' ')}
      >
        {(title || !hideClose) && (
          <div className="flex items-start justify-between gap-3 border-b border-[hsl(var(--border))] px-4 py-3">
            <div className="min-w-0">
              {title && <div className="text-base font-semibold truncate">{title}</div>}
              {description && <div className="mt-0.5 text-xs text-muted-foreground">{description}</div>}
            </div>
            {!hideClose && (
              <button
                type="button"
                onClick={onClose}
                aria-label="Close"
                className="shrink-0 -me-1 -mt-0.5 inline-flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground hover:bg-muted hover:text-foreground transition-colors [transition-duration:var(--dur-fast)]"
              >
                <RiCloseLine className="h-4 w-4" />
              </button>
            )}
          </div>
        )}
        <div className="min-h-0 flex-1 overflow-y-auto px-4 py-4">{children}</div>
        {footer && (
          <div className="flex items-center justify-end gap-2 border-t border-[hsl(var(--border))] px-4 py-3">{footer}</div>
        )}
      </div>
    </div>,
    document.body,
  )
}
