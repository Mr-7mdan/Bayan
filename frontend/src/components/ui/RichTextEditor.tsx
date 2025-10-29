"use client"

import { useEffect, useRef } from 'react'

export default function RichTextEditor({
  value,
  onChange,
  placeholder,
  height = 200,
}: {
  value?: string
  onChange?: (html: string) => void
  placeholder?: string
  height?: number
}) {
  const ref = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (!ref.current) return
    // Avoid overwriting user typing; only update when different
    const curr = ref.current.innerHTML
    if ((value || '') !== curr) {
      ref.current.innerHTML = value || ''
    }
  }, [value])

  function exec(cmd: string, arg?: string) {
    try {
      document.execCommand('styleWithCSS', false, 'true')
    } catch {}
    try {
      document.execCommand(cmd, false, arg)
    } catch {}
    if (ref.current) onChange?.(ref.current.innerHTML)
  }

  function applySpanStyle(style: Record<string, string>) {
    const sel = window.getSelection()
    if (!sel || sel.rangeCount === 0) return
    const range = sel.getRangeAt(0)
    try {
      const span = document.createElement('span')
      Object.entries(style).forEach(([k, v]) => { span.style.setProperty(k, v) })
      const contents = range.extractContents()
      span.appendChild(contents)
      range.insertNode(span)
      sel.removeAllRanges()
      const newRange = document.createRange()
      newRange.selectNodeContents(span)
      sel.addRange(newRange)
    } catch {
      // Fallback: insert HTML
      const temp = document.createElement('div')
      temp.textContent = sel.toString()
      const inner = temp.innerHTML
      const styleStr = Object.entries(style).map(([k, v]) => `${k}:${v}`).join(';')
      document.execCommand('insertHTML', false, `<span style="${styleStr}">${inner}</span>`)
    }
    if (ref.current) onChange?.(ref.current.innerHTML)
  }

  return (
    <div className="rounded-md border">
      <div className="flex flex-wrap items-center gap-1 p-1 border-b bg-transparent">
        <button className="text-[11px] px-2 py-0.5 rounded border hover:bg-muted" type="button" onClick={() => exec('bold')}>B</button>
        <button className="text-[11px] px-2 py-0.5 rounded border hover:bg-muted italic" type="button" onClick={() => exec('italic')}>I</button>
        <button className="text-[11px] px-2 py-0.5 rounded border hover:bg-muted underline" type="button" onClick={() => exec('underline')}>U</button>
        <span className="mx-1 w-px h-4 bg-border" />
        <button className="text-[11px] px-2 py-0.5 rounded border hover:bg-muted" type="button" onClick={() => exec('justifyLeft')}>Left</button>
        <button className="text-[11px] px-2 py-0.5 rounded border hover:bg-muted" type="button" onClick={() => exec('justifyCenter')}>Center</button>
        <button className="text-[11px] px-2 py-0.5 rounded border hover:bg-muted" type="button" onClick={() => exec('justifyRight')}>Right</button>
        <span className="mx-1 w-px h-4 bg-border" />
        <select className="text-[11px] px-1 py-0.5 rounded border bg-[hsl(var(--secondary))]" onChange={(e) => {
          const v = e.target.value
          if (v === 'p') exec('formatBlock', 'P')
          else if (v === 'h1') exec('formatBlock', 'H1')
          else if (v === 'h2') exec('formatBlock', 'H2')
          else if (v === 'h3') exec('formatBlock', 'H3')
          e.currentTarget.selectedIndex = 0
        }}>
          <option>Block</option>
          <option value="p">Paragraph</option>
          <option value="h1">H1</option>
          <option value="h2">H2</option>
          <option value="h3">H3</option>
        </select>
        <select className="text-[11px] px-1 py-0.5 rounded border bg-[hsl(var(--secondary))]" onChange={(e) => {
          const size = e.target.value
          if (size) exec('fontSize', size)
          e.currentTarget.selectedIndex = 0
        }}>
          <option>Size</option>
          <option value="2">Small</option>
          <option value="3">Normal</option>
          <option value="4">Large</option>
          <option value="5">XL</option>
        </select>
        <select className="text-[11px] px-1 py-0.5 rounded border bg-[hsl(var(--secondary))]" onChange={(e) => {
          const w = e.target.value
          if (w) applySpanStyle({ fontWeight: w })
          e.currentTarget.selectedIndex = 0
        }}>
          <option>Weight</option>
          <option value="400">400</option>
          <option value="500">500</option>
          <option value="600">600</option>
          <option value="700">700</option>
        </select>
        <label className="text-[11px] px-1 py-0.5 rounded border bg-[hsl(var(--secondary))] cursor-pointer flex items-center gap-1">
          <span>Color</span>
          <input type="color" className="w-5 h-5 p-0 border-0 bg-transparent" onChange={(e) => exec('foreColor', e.target.value)} />
        </label>
        <button className="text-[11px] px-2 py-0.5 rounded border hover:bg-muted" type="button" onClick={() => {
          const url = prompt('Enter URL')
          if (url) exec('createLink', url)
        }}>Link</button>
        <button className="text-[11px] px-2 py-0.5 rounded border hover:bg-muted" type="button" onClick={() => exec('unlink')}>Unlink</button>
        <span className="mx-1 w-px h-4 bg-border" />
        <button className="text-[11px] px-2 py-0.5 rounded border hover:bg-muted" type="button" onClick={() => exec('removeFormat')}>Clear</button>
      </div>
      <div
        ref={ref}
        className="p-2 min-h-[120px] bg-[hsl(var(--secondary))] outline-none"
        style={{ height }}
        contentEditable
        suppressContentEditableWarning
        onInput={() => { if (ref.current) onChange?.(ref.current.innerHTML) }}
        data-placeholder={placeholder || 'Start typing…'}
      />
    </div>
  )
}
