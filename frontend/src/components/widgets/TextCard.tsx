"use client"

import ErrorBoundary from '@/components/dev/ErrorBoundary'
import type { WidgetConfig } from '@/types/widgets'

export default function TextCard({
  title,
  options,
}: {
  title: string
  options?: WidgetConfig['options']
}) {
  const autoFit = options?.autoFitCardContent !== false
  const cardFill = options?.cardFill || 'default'
  const bgStyle = cardFill === 'transparent' ? { backgroundColor: 'transparent' } : cardFill === 'custom' ? { backgroundColor: options?.cardCustomColor || '#ffffff' } : undefined
  const cardClass = `${autoFit ? '' : 'h-full'} !border-0 shadow-none rounded-lg ${cardFill === 'transparent' ? 'bg-transparent' : 'bg-card'}`

  const text = options?.text
  const sanitize = !!text?.sanitizeHtml
  const sanitizeHtml = (raw: string): string => {
    try {
      const parser = new DOMParser()
      const doc = parser.parseFromString(raw, 'text/html')
      const allowedTags = new Set(['DIV','P','SPAN','B','STRONG','I','EM','U','A','BR','H1','H2','H3','H4','H5','H6','UL','OL','LI','BLOCKQUOTE'])
      const allowedStyles = new Set(['color','font-weight','text-align'])
      const isSafeUrl = (url: string) => /^(https?:|mailto:|tel:)/i.test(url)
      const walk = (el: Element) => {
        // Remove script/style tags entirely
        const rm = el.querySelectorAll('script,style')
        rm.forEach((n) => n.remove())
        const recurse = (node: Element) => {
          Array.from(node.children).forEach((child) => {
            const tag = child.tagName
            if (!allowedTags.has(tag)) {
              // unwrap node: replace with its children
              const parent = child.parentElement
              if (parent) {
                while (child.firstChild) parent.insertBefore(child.firstChild, child)
                parent.removeChild(child)
              }
            } else {
              // clean attributes
              const allowedAttrs = new Set(['href','target','rel','style'])
              Array.from((child as Element).attributes).forEach((a) => {
                if (!allowedAttrs.has(a.name.toLowerCase())) (child as Element).removeAttribute(a.name)
              })
              if (child instanceof HTMLAnchorElement) {
                const href = child.getAttribute('href') || ''
                if (!isSafeUrl(href)) child.removeAttribute('href')
                child.setAttribute('target','_blank')
                child.setAttribute('rel','noopener noreferrer')
              }
              const style = (child as HTMLElement).getAttribute('style') || ''
              if (style) {
                const next: string[] = []
                style.split(';').forEach((decl) => {
                  const [kRaw, vRaw] = decl.split(':')
                  if (!kRaw || !vRaw) return
                  const k = kRaw.trim().toLowerCase()
                  const v = vRaw.trim()
                  if (allowedStyles.has(k)) {
                    // lock down values
                    if (k === 'color') { if (/^#[0-9a-fA-F]{3,6}$/.test(v) || /^(rgb|hsl)a?\(/.test(v) || /^[a-zA-Z]+$/.test(v)) next.push(`${k}:${v}`) }
                    else if (k === 'font-weight') { if (/^(400|500|600|700|800|900|bold|normal)$/.test(v)) next.push(`${k}:${v}`) }
                    else if (k === 'text-align') { if (/^(left|center|right|justify)$/.test(v)) next.push(`${k}:${v}`) }
                  }
                })
                if (next.length) (child as HTMLElement).setAttribute('style', next.join(';'))
                else (child as HTMLElement).removeAttribute('style')
              }
              recurse(child as Element)
            }
          })
        }
        recurse(el)
      }
      walk(doc.body)
      return doc.body.innerHTML
    } catch {
      return ''
    }
  }

  return (
    <ErrorBoundary name="TextCard">
      <div className={cardClass} style={bgStyle as any}>
        <div className="space-y-2">
          {/* Optional image */}
          {text?.imageUrl && (
            <div className={`w-full flex ${text.imageAlign === 'right' ? 'justify-end' : text.imageAlign === 'center' ? 'justify-center' : 'justify-start'}`}>
              {/* eslint-disable-next-line @next/next/no-img-element */}
              <img src={text.imageUrl} alt={text.imageAlt || ''} style={{ width: text.imageWidth || 64 }} className="rounded" />
            </div>
          )}
          {/* Rich HTML or Labels */}
          {text?.html ? (
            <div className="prose dark:prose-invert max-w-none" dangerouslySetInnerHTML={{ __html: sanitize ? sanitizeHtml(text.html) : text.html }} />
          ) : (
            <div className="space-y-1">
              {(text?.labels || []).map((lbl) => {
                const align = lbl.align || 'left'
                const classFor = (lbl.style || 'p')
                const cls = [
                  align === 'right' ? 'text-right' : align === 'center' ? 'text-center' : 'text-left',
                  lbl.color ? '' : 'text-foreground',
                  classFor === 'h1' ? 'text-2xl font-semibold' :
                  classFor === 'h2' ? 'text-xl font-semibold' :
                  classFor === 'h3' ? 'text-lg font-semibold' :
                  classFor === 'metric' ? 'text-3xl font-semibold' :
                  classFor === 'label' ? 'text-xs text-muted-foreground' :
                  classFor === 'small' ? 'text-xs' : 'text-sm',
                ].join(' ')
                const style = lbl.color ? { color: lbl.color } : undefined
                return (
                  <div key={lbl.id || lbl.text} className={cls} style={style as any}>{lbl.text}</div>
                )
              })}
            </div>
          )}
        </div>
      </div>
    </ErrorBoundary>
  )
}
