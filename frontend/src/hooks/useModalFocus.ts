"use client"

import { useEffect, useRef } from 'react'

/**
 * Shared a11y helper for hand-rolled createPortal modals that are NOT built on
 * Radix Dialog. Adds the three things those modals lack: focus trap, focus
 * restore to the opener, and Escape-to-close — without touching component logic.
 *
 * Usage:
 *   const panelRef = useModalFocus<HTMLDivElement>(open, onClose)
 *   ...
 *   <div ref={panelRef} role="dialog" aria-modal="true" aria-label="...">
 *
 * Behavior-preserving: only manages focus and listens for Tab/Escape while open.
 * If the modal already handles Escape itself, pass `handleEscape = false`.
 */
const FOCUSABLE =
  'a[href], button:not([disabled]), textarea:not([disabled]), input:not([disabled]), select:not([disabled]), [tabindex]:not([tabindex="-1"])'

export function useModalFocus<T extends HTMLElement = HTMLElement>(
  open: boolean,
  onClose?: () => void,
  handleEscape: boolean = true,
) {
  const panelRef = useRef<T | null>(null)
  const openerRef = useRef<Element | null>(null)

  useEffect(() => {
    if (!open) return
    const panel = panelRef.current
    if (!panel) return

    // Remember what had focus so we can restore it on close.
    openerRef.current = document.activeElement

    // Move focus into the panel (first focusable, else the panel itself).
    const focusables = () => Array.from(panel.querySelectorAll<HTMLElement>(FOCUSABLE)).filter((el) => el.offsetParent !== null || el === document.activeElement)
    const first = focusables()[0]
    if (first) first.focus()
    else { panel.setAttribute('tabindex', '-1'); panel.focus() }

    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && handleEscape) {
        e.stopPropagation()
        onClose?.()
        return
      }
      if (e.key !== 'Tab') return
      const items = focusables()
      if (items.length === 0) { e.preventDefault(); return }
      const firstEl = items[0]
      const lastEl = items[items.length - 1]
      const active = document.activeElement
      if (e.shiftKey) {
        if (active === firstEl || !panel.contains(active)) { e.preventDefault(); lastEl.focus() }
      } else {
        if (active === lastEl || !panel.contains(active)) { e.preventDefault(); firstEl.focus() }
      }
    }

    panel.addEventListener('keydown', onKeyDown)
    return () => {
      panel.removeEventListener('keydown', onKeyDown)
      // Restore focus to the opener (if it is still in the document).
      const opener = openerRef.current as HTMLElement | null
      if (opener && typeof opener.focus === 'function' && document.contains(opener)) {
        opener.focus()
      }
    }
  }, [open, onClose, handleEscape])

  return panelRef
}

export default useModalFocus
