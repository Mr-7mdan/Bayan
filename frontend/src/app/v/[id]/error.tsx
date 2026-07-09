"use client"

import { useEffect } from 'react'
import EmptyState from '@/components/feedback/EmptyState'

export default function Error({ error, reset }: { error: Error & { digest?: string }; reset: () => void }) {
  useEffect(() => {
    // Route error boundaries stop propagation to window.onerror; re-dispatch so
    // ErrorReporterProvider's existing listener reports it (dedupe + bugReportMode).
    try { window.dispatchEvent(new ErrorEvent('error', { error, message: error.message })) } catch {}
  }, [error])
  // Public viewer: no "Go home" link (viewers may be unauthenticated).
  return (
    <div className="min-h-screen flex items-center justify-center bg-[hsl(var(--background))] text-[hsl(var(--foreground))]">
      <EmptyState
        title="Something went wrong"
        description="This dashboard could not be displayed."
        action={
          <button
            className="text-xs px-3 py-1.5 rounded-md border border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))]"
            onClick={reset}
          >
            Try again
          </button>
        }
      />
    </div>
  )
}
