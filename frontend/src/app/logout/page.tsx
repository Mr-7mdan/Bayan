"use client"

import { useEffect } from 'react'
import { useAuth } from '@/components/providers/AuthProvider'
import { useRouter } from 'next/navigation'

export default function LogoutPage() {
  const { logout } = useAuth()
  const router = useRouter()
  useEffect(() => {
    (async () => {
      await logout()
      router.replace('/login')
    })()
  }, [])
  return null
}
