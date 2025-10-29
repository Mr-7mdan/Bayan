"use client"

import React from 'react'

export type UISwitchProps = {
  checked?: boolean
  onChangeAction?: (v: boolean) => void
  disabled?: boolean
  className?: string
}

// Lightweight, theme-friendly switch used across contextual menus
export const Switch: React.FC<UISwitchProps> = ({ checked, onChangeAction, disabled, className }) => {
  const [local, setLocal] = React.useState(!!checked)
  React.useEffect(() => { if (checked !== undefined) setLocal(!!checked) }, [checked])

  const toggle = () => {
    if (disabled) return
    const next = !local
    setLocal(next)
    onChangeAction && onChangeAction(next)
  }

  const base = `relative inline-flex items-center h-4 w-8 rounded-full transition-colors duration-200 select-none ${disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}`
  const bg = local ? 'bg-[hsl(var(--primary))]' : 'bg-gray-400/35 dark:bg-gray-500/25'
  const ring = 'ring-1 ring-inset ring-[hsl(var(--border))]'

  return (
    <button type="button" role="switch" aria-checked={local} aria-disabled={disabled}
      className={`${base} ${bg} ${ring} ${className || ''}`} onClick={toggle}>
      <span className={`pointer-events-none inline-block h-3 w-3 rounded-full bg-white shadow transition-transform duration-200 translate-x-1 ${local ? 'translate-x-4' : ''}`} />
    </button>
  )
}

export default Switch
