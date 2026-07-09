"use client"

import { useEffect } from 'react'

// iframe-safe: embeds can be ~100px tall, so no EmptyState padding and no
// navigation — just tiny centered text and a retry.
export default function Error({ error, reset }: { error: Error & { digest?: string }; reset: () => void }) {
  useEffect(() => {
    try { window.dispatchEvent(new ErrorEvent('error', { error, message: error.message })) } catch {}
  }, [error])
  return (
    <div className="flex items-center justify-center gap-2 h-full min-h-[60px] p-2 text-xs text-[hsl(var(--muted-foreground))]">
      <span>Widget failed to load</span>
      <button
        className="px-2 py-0.5 rounded border border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))]"
        onClick={reset}
      >
        Retry
      </button>
    </div>
  )
}
