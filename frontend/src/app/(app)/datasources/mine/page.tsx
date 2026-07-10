import { getTranslations } from 'next-intl/server'

export default async function Page() {
  const t = await getTranslations('data')
  return (
    <div className="p-4 text-sm text-muted-foreground">{t('datasources.stubs.mine')}</div>
  )
}
