"use client"

import { Card, Title, Text } from '@tremor/react'
import { useTranslations } from 'next-intl'

export default function MonitoringPage() {
  const t = useTranslations('pages.monitoring')
  return (
    <div className="space-y-4">
      <Card>
        <Title>{t('title')}</Title>
        <Text>{t('body')}</Text>
      </Card>
    </div>
  )
}
