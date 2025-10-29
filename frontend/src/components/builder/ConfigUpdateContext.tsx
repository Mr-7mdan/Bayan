"use client"

import { createContext, useContext } from 'react'
import type { WidgetConfig } from '@/types/widgets'

export const ConfigUpdateContext = createContext<(cfg: WidgetConfig) => void>(() => {})
export const useConfigUpdate = () => useContext(ConfigUpdateContext)
