"use client"

import Link from 'next/link'

export default function AboutPage() {
  return (
    <div className="mx-auto max-w-3xl p-6 space-y-6">
      <header>
        <div className="flex items-center gap-4">
          <img src="/bayan.svg" alt="Bayan" className="h-16 w-auto md:h-20" />
          <div>
            <h1 className="text-2xl font-semibold">About Bayan</h1>
            <p className="mt-0.5 text-sm text-muted-foreground">Dashboard & Analytics</p>
          </div>
        </div>
      </header>

      <section className="rounded-lg border bg-card p-4 space-y-2">
        <h2 className="text-sm font-medium">Description</h2>
        <p className="text-sm text-muted-foreground">
          Bayan is your management companion to keep you up‑to‑date on your organization's progress, performance, and KPIs. Build, share, and embed modular dashboards with secure public views, per‑user collections, and flexible themes.
        </p>
        <ul className="list-disc pl-5 text-sm text-muted-foreground space-y-1">
          <li>Build dashboards with stunning visuals, charts, tables, and KPIs.</li>
          <li>Share dashboards with your team, or publish them to the web securely.</li>
          <li>Collaborate with your team to build the dashboard you need.</li>
          <li>Get alerted when thresholds are hit.</li>
          <li>Receive periodic, pre‑configured notifications with charts, data tables, pivot tables, and KPIs.</li>
          <li>Send bulk SMS and emails using your contacts datasets.</li>
          <li>Create charts, KPIs, and tables and embed them in your website.</li>
          <li>Deltas mode for easy comps and analysis.</li>
          <li>AI Powered Assist mode to help you build dashboards.</li>
        </ul>
      </section>

      <section className="rounded-lg border bg-card p-4 space-y-2">
        <h2 className="text-sm font-medium">Contact</h2>
        <ul className="text-sm space-y-1">
          <li>Email: <a className="text-blue-600 dark:text-blue-400 hover:underline" href="mailto:Mr-Hamdan@hotmail.com">Mr-Hamdan@hotmail.com</a></li>
          <li>Phone: <a className="text-blue-600 dark:text-blue-400 hover:underline" href="tel:0598230847">0598-230847</a></li>
          <li>Website: <a className="text-blue-600 dark:text-blue-400 hover:underline" href="https://www.bayan.ps" target="_blank" rel="noreferrer">https://www.bayan.ps</a></li>
        </ul>
      </section>
    </div>
  )
}
