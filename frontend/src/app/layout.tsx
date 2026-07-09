import type { Metadata } from 'next'
import { headers } from 'next/headers'
import type { ReactNode } from 'react'
import { Suspense } from 'react'
import './globals.css'
import { Inter, IBM_Plex_Sans_Arabic } from 'next/font/google'
import { getLocale, getMessages } from 'next-intl/server'
import { NextIntlClientProvider } from 'next-intl'
import QueryProvider from '@/components/providers/QueryProvider'
import AuthProvider from '@/components/providers/AuthProvider'
import BrandingProvider from '@/components/providers/BrandingProvider'
import FiltersProvider from '@/components/providers/FiltersProvider'
import ThemeProvider from '@/components/providers/ThemeProvider'
import ProgressToastProvider from '@/components/providers/ProgressToastProvider'
import EnvironmentProvider from '@/components/providers/EnvironmentProvider'
import ErrorReporterProvider from '@/components/providers/ErrorReporterProvider'

const inter = Inter({ subsets: ['latin'] })
const plexArabic = IBM_Plex_Sans_Arabic({ subsets: ['arabic', 'latin'], weight: ['400', '500', '600', '700'] })

const defaultMetadata: Metadata = {
  title: { default: 'Bayan', template: '%s · Bayan' },
  description: 'Modular reporting dashboards',
  icons: { icon: '/favicon.svg' },
}

export async function generateMetadata(): Promise<Metadata> {
  try {
    const urls: string[] = []
    const base = process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/$/, '')
    if (base) urls.push(`${base}/branding`)
    // Same-origin attempt using incoming host headers (works on Vercel and dev)
    try {
      const h = await headers()
      const proto = h.get('x-forwarded-proto') || 'http'
      const host = h.get('host')
      if (host) urls.push(`${proto}://${host}/api/branding`)
    } catch {}
    // Local fallbacks (dev)
    urls.push('http://127.0.0.1:8000/api/branding')
    urls.push('http://localhost:8000/api/branding')

    let b: any = null
    for (const u of urls) {
      try {
        const res = await fetch(u, { cache: 'no-store' })
        if (res.ok) { b = await res.json(); break }
      } catch { /* try next */ }
    }
    if (!b) return defaultMetadata
    const org = (b?.orgName || '').trim()
    const combined = org ? `${org} · Bayan` : 'Bayan'
    const iconHref = (b?.favicon || '').trim() || '/favicon.svg'
    return {
      title: { default: combined, template: `%s · ${org ? `${org} · ` : ''}Bayan` },
      description: defaultMetadata.description,
      icons: { icon: iconHref },
    }
  } catch {
    return defaultMetadata
  }
}

export default async function RootLayout({ children }: { children: ReactNode }) {
  const locale = await getLocale()
  const messages = await getMessages()
  const dir = locale === 'ar' ? 'rtl' : 'ltr'
  const fontClass = locale === 'ar' ? plexArabic.className : inter.className
  return (
    <html lang={locale} dir={dir} className="h-full">
      <body className={`${fontClass} h-full bg-background text-foreground`}>
        <Suspense fallback={<div className="p-3 text-sm">Loading…</div>}>
          <NextIntlClientProvider locale={locale} messages={messages}>
          <EnvironmentProvider>
            <ThemeProvider>
              <FiltersProvider>
                <BrandingProvider>
                  <QueryProvider>
                    <AuthProvider>
                      <ErrorReporterProvider>
                        <ProgressToastProvider>
                          {children}
                        </ProgressToastProvider>
                      </ErrorReporterProvider>
                    </AuthProvider>
                  </QueryProvider>
                </BrandingProvider>
              </FiltersProvider>
            </ThemeProvider>
          </EnvironmentProvider>
          </NextIntlClientProvider>
        </Suspense>
      </body>
    </html>
  )
}
