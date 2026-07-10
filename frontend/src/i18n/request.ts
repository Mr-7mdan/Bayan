import { getRequestConfig } from 'next-intl/server'
import { cookies } from 'next/headers'
import { messagesByLocale } from '../messages/loader'

export const SUPPORTED_LOCALES = ['en', 'ar'] as const
export type Locale = (typeof SUPPORTED_LOCALES)[number]
export const DEFAULT_LOCALE: Locale = 'en'

// Cookie-based locale resolution (no i18n routing / no URL prefixes).
export default getRequestConfig(async () => {
  const store = await cookies()
  const raw = store.get('NEXT_LOCALE')?.value
  const locale: Locale = SUPPORTED_LOCALES.includes(raw as Locale) ? (raw as Locale) : DEFAULT_LOCALE
  return {
    locale,
    messages: messagesByLocale[locale],
  }
})
