"use client"

import ErrorBoundary from '@/components/dev/ErrorBoundary'
import type { WidgetConfig } from '@/types/widgets'

export default function SpacerCard({ options }: { options?: WidgetConfig['options'] }) {
  const autoFit = options?.autoFitCardContent !== false
  const cardFill = options?.cardFill || 'default'
  const bgStyle = cardFill === 'transparent' ? { backgroundColor: 'transparent' } : cardFill === 'custom' ? { backgroundColor: options?.cardCustomColor || '#ffffff' } : undefined
  const cardClass = `${autoFit ? '' : 'h-full'} !border-0 shadow-none rounded-lg ${cardFill === 'transparent' ? 'bg-transparent' : 'bg-card'}`
  return (
    <ErrorBoundary name="SpacerCard">
      <div className={`${cardClass} min-h-[24px]`} style={bgStyle as any} />
    </ErrorBoundary>
  )
}
