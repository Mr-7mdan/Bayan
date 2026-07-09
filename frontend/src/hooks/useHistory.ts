import { useCallback, useRef, useState } from 'react'

/**
 * Bounded undo/redo history over an immutable snapshot type `T`.
 *
 * Snapshots are stored by reference — callers MUST pass immutable snapshots
 * (the builder always replaces layout/config objects, never mutates them), so
 * no deep clone is needed. The stack is capped at `limit` entries; pushing past
 * the cap drops the oldest entry.
 */
export function useHistory<T>(limit = 25) {
  const stackRef = useRef<T[]>([])
  const indexRef = useRef<number>(-1)
  const [ends, setEnds] = useState<{ canUndo: boolean; canRedo: boolean }>({ canUndo: false, canRedo: false })

  const sync = useCallback(() => {
    setEnds({
      canUndo: indexRef.current > 0,
      canRedo: indexRef.current < stackRef.current.length - 1,
    })
  }, [])

  const reset = useCallback((snap: T) => {
    stackRef.current = [snap]
    indexRef.current = 0
    sync()
  }, [sync])

  const push = useCallback((snap: T) => {
    // Dedupe: a debounced config-push can race a just-committed discrete edit.
    // Snapshots share references when nothing changed, so a shallow field compare
    // against the current top is enough to drop the no-op.
    const top = stackRef.current[indexRef.current] as Record<string, unknown> | undefined
    if (top && typeof snap === 'object' && snap) {
      const s = snap as Record<string, unknown>
      const keys = Object.keys(s)
      if (keys.length === Object.keys(top).length && keys.every((k) => top[k] === s[k])) return
    }
    // Drop any redo tail, append the new snapshot, then cap at `limit`.
    const next = stackRef.current.slice(0, indexRef.current + 1)
    next.push(snap)
    while (next.length > limit) next.shift()
    stackRef.current = next
    indexRef.current = next.length - 1
    sync()
  }, [limit, sync])

  const undo = useCallback((): T | null => {
    if (indexRef.current <= 0) return null
    indexRef.current -= 1
    sync()
    return stackRef.current[indexRef.current]
  }, [sync])

  const redo = useCallback((): T | null => {
    if (indexRef.current >= stackRef.current.length - 1) return null
    indexRef.current += 1
    sync()
    return stackRef.current[indexRef.current]
  }, [sync])

  return { push, undo, redo, reset, canUndo: ends.canUndo, canRedo: ends.canRedo }
}
