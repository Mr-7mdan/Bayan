"use client"

import { createContext, useContext, useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import { Api, type UserOut } from '@/lib/api'

export type User = {
  id: string
  name: string
  email: string
  role: 'admin' | 'user'
}

type AuthCtx = {
  user: User | null
  loading: boolean
  login: (email: string, password: string, remember?: boolean) => Promise<void>
  logout: () => Promise<void>
}

const AuthContext = createContext<AuthCtx | null>(null)

export function useAuth(): AuthCtx {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within AuthProvider')
  return ctx
}

export default function AuthProvider({ children }: { children?: ReactNode }) {
  const [user, setUser] = useState<User | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    try {
      const rawLocal = localStorage.getItem('auth_user')
      const rawSession = !rawLocal ? sessionStorage.getItem('auth_user') : null
      const raw = rawLocal || rawSession
      if (raw) setUser(JSON.parse(raw))
    } catch {}
    setLoading(false)
  }, [])

  const login = async (email: string, password: string, remember = true) => {
    const u: UserOut = await Api.login({ email, password })
    const userObj: User = { id: u.id, name: u.name, email: u.email, role: (u.role as any) || 'user' }
    setUser(userObj)
    try {
      const payload = JSON.stringify(userObj)
      if (remember) localStorage.setItem('auth_user', payload)
      else sessionStorage.setItem('auth_user', payload)
    } catch {}
  }

  const logout = async () => {
    setUser(null)
    try {
      localStorage.removeItem('auth_user')
      sessionStorage.removeItem('auth_user')
    } catch {}
  }

  const value = useMemo<AuthCtx>(() => ({ user, loading, login, logout }), [user, loading])
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}
