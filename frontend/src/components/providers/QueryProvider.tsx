"use client"

import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ReactNode, useEffect, useRef, useState } from 'react'
import { useSearchParams } from 'next/navigation'
import { setQueryAsOf } from '@/lib/api'

export default function QueryProvider({ children }: { children?: ReactNode }) {
  const [client] = useState(() => new QueryClient({
    defaultOptions: {
      queries: {
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
        retry: 1,
        staleTime: 30_000,
        gcTime: 10 * 60_000,
      },
    },
  }))

  // "As of" (?asOf=YYYY-MM-DD): resolve every date preset as if that day were
  // today, to reproduce a dashboard as it stood then. URL-only on purpose —
  // persisting it would silently pin a later visit to old numbers.
  // Set during render, not in an effect: children's effects run before ours,
  // so widgets would otherwise fire their first queries without it.
  const asOf = useSearchParams().get('asOf')
  setQueryAsOf(asOf)

  // Query keys don't include the date, so a change must drop cached results.
  const prev = useRef(asOf)
  useEffect(() => {
    if (prev.current === asOf) return
    prev.current = asOf
    void client.resetQueries()
  }, [asOf, client])

  return <QueryClientProvider client={client}>{children}</QueryClientProvider>
}
