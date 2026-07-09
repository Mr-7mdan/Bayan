import { describe, it, expect } from 'vitest'
import {
  parseLegacyPreset,
  isLastNDaysPreset,
  matchQuickPick,
  presetConfigToLabel,
  DEFAULT_PRESET,
  type PresetConfig,
} from '@/lib/datePresets'

describe('parseLegacyPreset', () => {
  it('maps known legacy string keys (case/space-insensitive) to a config', () => {
    expect(parseLegacyPreset('today')).toMatchObject({ period: 'day', offset: 'this' })
    expect(parseLegacyPreset('  This_Week ')).toMatchObject({ period: 'week', offset: 'this' })
  })

  it('returns null for unknown strings and for object input', () => {
    expect(parseLegacyPreset('not_a_preset')).toBeNull()
    // object passthrough is not handled here -> null (backend owns objects)
    expect(parseLegacyPreset(DEFAULT_PRESET)).toBeNull()
  })
})

describe('isLastNDaysPreset', () => {
  it('recognizes last_N_days keys', () => {
    expect(isLastNDaysPreset('last_30_days')).toEqual({ days: 30, label: 'Last 30 Days' })
    expect(isLastNDaysPreset('LAST_7_DAYS')).toEqual({ days: 7, label: 'Last 7 Days' })
  })
  it('returns null for non-matching', () => {
    expect(isLastNDaysPreset('today')).toBeNull()
  })
})

describe('matchQuickPick', () => {
  it('finds the quick pick for a matching config', () => {
    const today: PresetConfig = {
      period: 'day', offset: 'this', as_of: 'today',
      range_mode: 'full', include_weekends: true, apply_holidays: false,
    }
    expect(matchQuickPick(today)?.label).toBe('Today')
  })
  it('returns null for a config with no quick pick', () => {
    const custom: PresetConfig = { ...DEFAULT_PRESET, apply_holidays: true, range_mode: 'to_date' }
    // DEFAULT is "This Week" full; the tweaked one has no exact quick pick.
    expect(matchQuickPick({ ...custom, include_weekends: false, as_of: 'last_working_day', period: 'day', offset: 'last_year_this' })).toBeNull()
  })
})

describe('presetConfigToLabel', () => {
  it('uses the quick-pick label when one matches', () => {
    expect(presetConfigToLabel(DEFAULT_PRESET)).toBe('This Week')
  })
  it('builds a descriptive label for custom configs', () => {
    // offset 'last_year_previous' has no quick pick -> forces the descriptive builder.
    const cfg: PresetConfig = {
      period: 'month', offset: 'last_year_previous', as_of: 'last_working_day',
      range_mode: 'to_date', include_weekends: false, apply_holidays: true,
    }
    const label = presetConfigToLabel(cfg)
    expect(label).toContain('Previous')
    expect(label).toContain('Working')
    expect(label).toContain('Month')
    expect(label).toContain('to Date')
    expect(label).toContain('(excl. holidays)')
  })
})
