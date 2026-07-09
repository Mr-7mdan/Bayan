"use client"

import React from 'react'

// Shimmer loading placeholders. Base block + composed chart/table/kpi silhouettes.
// The shimmer keyframe is scoped here via styled-jsx (NOT globals.css) and honors
// prefers-reduced-motion. Surface uses the --muted token, no raw hex.
function Shimmer() {
  return (
    <style jsx global>{`
      @keyframes ui-skeleton-shimmer {
        0% { background-position: -200% 0; }
        100% { background-position: 200% 0; }
      }
      .ui-skeleton {
        background-color: hsl(var(--muted));
        background-image: linear-gradient(
          90deg,
          hsl(var(--muted)) 0%,
          hsl(var(--muted-foreground) / 0.12) 50%,
          hsl(var(--muted)) 100%
        );
        background-size: 200% 100%;
        animation: ui-skeleton-shimmer 1.4s ease-in-out infinite;
      }
      @media (prefers-reduced-motion: reduce) {
        .ui-skeleton { animation: none; }
      }
    `}</style>
  )
}

export function Skeleton({ className = '', ...rest }: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <>
      <Shimmer />
      <div className={`ui-skeleton rounded-md ${className}`} {...rest} />
    </>
  )
}

export function SkeletonKpi({ className = '' }: { className?: string }) {
  return (
    <div className={`rounded-lg border border-[hsl(var(--border))] bg-card p-4 ${className}`}>
      <Skeleton className="h-3 w-24" />
      <Skeleton className="mt-3 h-7 w-32" />
      <Skeleton className="mt-2 h-3 w-16" />
    </div>
  )
}

export function SkeletonChart({ className = '' }: { className?: string }) {
  const bars = [55, 80, 40, 95, 65, 75, 50]
  return (
    <div className={`rounded-lg border border-[hsl(var(--border))] bg-card p-4 ${className}`}>
      <Skeleton className="h-3 w-32" />
      <div className="mt-4 flex h-40 items-end gap-2">
        {bars.map((h, i) => (
          <Skeleton key={i} className="flex-1" style={{ height: `${h}%` }} />
        ))}
      </div>
      <Skeleton className="mt-3 h-2.5 w-full" />
    </div>
  )
}

export function SkeletonTable({ rows = 5, cols = 4, className = '' }: { rows?: number; cols?: number; className?: string }) {
  return (
    <div className={`overflow-hidden rounded-lg border border-[hsl(var(--border))] bg-card ${className}`}>
      <div className="flex gap-3 border-b border-[hsl(var(--border))] px-3 py-2.5">
        {Array.from({ length: cols }).map((_, i) => (
          <Skeleton key={i} className="h-3 flex-1" />
        ))}
      </div>
      {Array.from({ length: rows }).map((_, r) => (
        <div key={r} className="flex gap-3 px-3 py-2.5">
          {Array.from({ length: cols }).map((_, c) => (
            <Skeleton key={c} className="h-3 flex-1" />
          ))}
        </div>
      ))}
    </div>
  )
}

export default Skeleton
