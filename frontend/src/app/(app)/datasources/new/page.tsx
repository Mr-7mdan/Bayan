import { getTranslations } from 'next-intl/server'

export const dynamic = 'force-dynamic'

export default async function Page() {
  const t = await getTranslations('data')
  return (
    <div className="p-4 text-sm text-muted-foreground">{t('datasources.stubs.new')}</div>
  )
}
