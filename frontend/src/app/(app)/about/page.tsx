"use client"

import { useTranslations } from 'next-intl'

export default function AboutPage() {
  const t = useTranslations('pages.about')
  const features = t.raw('features') as string[]
  return (
    <div className="mx-auto max-w-3xl p-6 space-y-6">
      <header>
        <div className="flex items-center gap-4">
          <img src="/bayan-logo.svg" alt="Bayan" className="h-16 w-auto md:h-20 block dark:hidden" />
          <img src="/bayan-logo-dark.svg" alt="Bayan" className="h-16 w-auto md:h-20 hidden dark:block" />
          <div>
            <h1 className="text-2xl font-semibold">{t('title')}</h1>
            <p className="mt-0.5 text-sm text-muted-foreground">{t('subtitle')}</p>
          </div>
        </div>
      </header>

      <section className="rounded-lg border bg-card p-4 space-y-2">
        <h2 className="text-sm font-medium">{t('descriptionHeading')}</h2>
        <p className="text-sm text-muted-foreground">{t('descriptionBody')}</p>
        <ul className="list-disc ps-5 text-sm text-muted-foreground space-y-1">
          {features.map((f, i) => <li key={i}>{f}</li>)}
        </ul>
      </section>

      <section className="rounded-lg border bg-card p-4 space-y-2">
        <h2 className="text-sm font-medium">{t('contactHeading')}</h2>
        <ul className="text-sm space-y-1">
          <li>{t('email')}: <a className="text-blue-600 dark:text-blue-400 hover:underline" href="mailto:Mr-Hamdan@hotmail.com">Mr-Hamdan@hotmail.com</a></li>
          <li>{t('phone')}: <a className="text-blue-600 dark:text-blue-400 hover:underline" href="tel:0598230847">0598-230847</a></li>
          <li>{t('website')}: <a className="text-blue-600 dark:text-blue-400 hover:underline" href="https://www.bayan.ps" target="_blank" rel="noreferrer">https://www.bayan.ps</a></li>
        </ul>
      </section>
    </div>
  )
}
