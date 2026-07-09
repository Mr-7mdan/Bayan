import React from 'react'

// Shared empty/error-state presentation. Pure props, client-safe (no "use client"
// needed) so it can render inside route error.tsx / not-found.tsx boundaries.
// ponytail: colors use CSS-var bracket classes because this tailwind config does
// not register `muted` utilities — swap to token classes only if the config gains them.
export default function EmptyState({ icon, title, description, action }: {
  icon?: React.ReactNode
  title: string
  description?: string
  action?: React.ReactNode
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 py-16 text-center">
      {icon && <div className="text-[hsl(var(--muted-foreground)/0.6)]">{icon}</div>}
      <div className="text-sm font-semibold text-[hsl(var(--foreground))]">{title}</div>
      {description && (
        <div className="text-xs text-[hsl(var(--muted-foreground))] max-w-md">{description}</div>
      )}
      {action && <div className="mt-2">{action}</div>}
    </div>
  )
}
