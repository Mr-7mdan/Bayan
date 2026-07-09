import Link from 'next/link'
import EmptyState from '@/components/feedback/EmptyState'

export default function NotFound() {
  return (
    <div className="min-h-screen flex items-center justify-center bg-[hsl(var(--background))] text-[hsl(var(--foreground))]">
      <EmptyState
        title="Page not found"
        description="The page you're looking for doesn't exist or has moved."
        action={
          <Link
            href="/home"
            className="text-xs px-3 py-1.5 rounded-md border border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))]"
          >
            Go home
          </Link>
        }
      />
    </div>
  )
}
