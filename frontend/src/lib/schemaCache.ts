import { Api, type IntrospectResponse } from '@/lib/api'

const KEY = (id: string) => `schema_cache_${id}`
const DEFAULT_TTL_MS = 30 * 60 * 1000 // 30 minutes

export type CachedSchema = { ts: number; data: IntrospectResponse }

export function get(id: string, ttlMs: number = DEFAULT_TTL_MS): IntrospectResponse | null {
  try {
    if (typeof window === 'undefined') return null
    const raw = localStorage.getItem(KEY(id))
    if (!raw) return null
    const parsed = JSON.parse(raw) as CachedSchema
    if (!parsed || typeof parsed.ts !== 'number' || !parsed.data) return null
    if (Date.now() - parsed.ts > ttlMs) return null
    return parsed.data
  } catch {
    return null
  }
}

export function set(id: string, data: IntrospectResponse): void {
  try {
    if (typeof window === 'undefined') return
    const payload: CachedSchema = { ts: Date.now(), data }
    localStorage.setItem(KEY(id), JSON.stringify(payload))
  } catch {}
}

export async function refresh(id: string): Promise<IntrospectResponse> {
  const data = await Api.introspect(id)
  set(id, data)
  return data
}

export function clear(id: string): void {
  try { if (typeof window !== 'undefined') localStorage.removeItem(KEY(id)) } catch {}
}
