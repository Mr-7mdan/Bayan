import { getRequestConfig } from 'next-intl/server'
import { cookies, headers } from 'next/headers'
import { messagesByLocale } from '../messages/loader'

export const SUPPORTED_LOCALES = ['en', 'ar'] as const
export type Locale = (typeof SUPPORTED_LOCALES)[number]
export const DEFAULT_LOCALE: Locale = 'en'

// Cookie-based locale resolution (no i18n routing / no URL prefixes). The app
// ALWAYS defaults to English — Arabic is opt-in via the NEXT_LOCALE cookie only,
// never from the browser's Accept-Language. Public dashboard views (/v/, /render/)
// ignore the cookie entirely so a shared link is language-consistent.
// Wrapped so any failure resolves to English rather than letting next-intl fall
// back to browser-locale detection.
async function resolveLocale(): Promise<Locale> {
  try {
    let pathname = ''
    try { pathname = (await headers()).get('x-pathname') || '' } catch { /* header unavailable */ }
    if (/^\/(v|render)\//.test(pathname)) return DEFAULT_LOCALE
    const raw = (await cookies()).get('NEXT_LOCALE')?.value
    return SUPPORTED_LOCALES.includes(raw as Locale) ? (raw as Locale) : DEFAULT_LOCALE
  } catch {
    return DEFAULT_LOCALE
  }
}

export default getRequestConfig(async () => {
  const locale = await resolveLocale()
  return {
    locale,
    messages: messagesByLocale[locale],
  }
})
