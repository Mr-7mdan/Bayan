"use client"

// Catches crashes in the root layout / provider stack, where Tailwind and the
// theme/providers may be unavailable. Renders its own <html>/<body> (Next
// requirement) with inline styles only — no token classes, no raw hex.
export default function GlobalError() {
  return (
    <html>
      <body style={{ margin: 0, fontFamily: 'system-ui, sans-serif' }}>
        <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 12, padding: 24, textAlign: 'center' }}>
          <div style={{ fontSize: 16, fontWeight: 600 }}>Something went wrong</div>
          <div style={{ fontSize: 13, opacity: 0.7 }}>The application failed to load.</div>
          <button
            style={{ fontSize: 13, padding: '6px 14px', borderRadius: 8, border: '1px solid currentColor', background: 'transparent', cursor: 'pointer' }}
            onClick={() => location.reload()}
          >
            Reload
          </button>
        </div>
      </body>
    </html>
  )
}
