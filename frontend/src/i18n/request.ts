import { getRequestConfig } from 'next-intl/server'
import { cookies, headers } from 'next/headers'
import { messagesByLocale } from '../messages/loader'

export const SUPPORTED_LOCALES = ['en', 'ar'] as const
export type Locale = (typeof SUPPORTED_LOCALES)[number]
export const DEFAULT_LOCALE: Locale = 'en'

// Cookie-based locale resolution (no i18n routing / no URL prefixes), EXCEPT
// public dashboard views (/v/, /render/) which always render in the default
// language so a shared link is consistent regardless of the publisher's own
// locale cookie.
export default getRequestConfig(async () => {
  const pathname = (await headers()).get('x-pathname') || ''
  const forceDefault = /^\/(v|render)\//.test(pathname)
  const raw = forceDefault ? undefined : (await cookies()).get('NEXT_LOCALE')?.value
  const locale: Locale = SUPPORTED_LOCALES.includes(raw as Locale) ? (raw as Locale) : DEFAULT_LOCALE
  return {
    locale,
    messages: messagesByLocale[locale],
  }
})
