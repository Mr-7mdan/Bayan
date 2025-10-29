"use client"

import { useEffect, useState } from 'react'
import { useTheme } from '@/components/providers/ThemeProvider'
import { RiMoonLine, RiSunLine } from '@remixicon/react'

export default function ThemeToggle() {
  const { resolved, setTheme } = useTheme()
  const [mounted, setMounted] = useState(false)
  useEffect(() => setMounted(true), [])

  const isDark = mounted && resolved === 'dark'
  const Icon = isDark ? RiMoonLine : RiSunLine

  const handleClick = () => {
    setTheme(isDark ? 'light' : 'dark')
  }

  return (
    <button
      type="button"
      aria-label="Toggle color scheme"
      onClick={handleClick}
      className="inline-flex items-center justify-center h-8 w-8 rounded-lg text-gray-600 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-white/10 transition-colors"
    >
      <Icon className="pointer-events-none w-4 h-4" />
    </button>
  )
}
