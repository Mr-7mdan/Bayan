import { describe, it, expect, vi, afterEach } from 'vitest'
import { swallow } from '@/lib/log'

const origEnv = process.env.NODE_ENV

afterEach(() => {
  ;(process.env as any).NODE_ENV = origEnv
  vi.restoreAllMocks()
})

describe('swallow', () => {
  it('warns with context tag outside production', () => {
    ;(process.env as any).NODE_ENV = 'development'
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    swallow(new Error('boom'), 'ctxA')
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn.mock.calls[0][0]).toBe('[swallowed:ctxA]')
  })

  it('stays silent in production', () => {
    ;(process.env as any).NODE_ENV = 'production'
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    swallow(new Error('boom'), 'ctxB')
    expect(warn).not.toHaveBeenCalled()
  })
})
