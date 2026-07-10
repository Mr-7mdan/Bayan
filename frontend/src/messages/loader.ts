// Merges the core message bundle (nav/shell/login/common) with per-area
// namespace files so i18n work can be partitioned without JSON merge conflicts.
// Each area file holds flat keys; the loader mounts it under its namespace.
import enCore from './en.json'
import arCore from './ar.json'

import enPages from './en/pages.json'
import enData from './en/data.json'
import enComms from './en/comms.json'
import enBuilder from './en/builder.json'
import enConfigurator from './en/configurator.json'
import enReports from './en/reports.json'

import arPages from './ar/pages.json'
import arData from './ar/data.json'
import arComms from './ar/comms.json'
import arBuilder from './ar/builder.json'
import arConfigurator from './ar/configurator.json'
import arReports from './ar/reports.json'

export const messagesByLocale = {
  en: {
    ...enCore,
    pages: enPages,
    data: enData,
    comms: enComms,
    builder: enBuilder,
    configurator: enConfigurator,
    reports: enReports,
  },
  ar: {
    ...arCore,
    pages: arPages,
    data: arData,
    comms: arComms,
    builder: arBuilder,
    configurator: arConfigurator,
    reports: arReports,
  },
} as const

export type AppLocale = keyof typeof messagesByLocale
