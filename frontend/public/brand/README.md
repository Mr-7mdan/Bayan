# Bayan · Brand Spec

> Canvas v1 · 3 differentiated logo directions
> Author: regenerated via huashu-design skill workflow

Open `canvas.html` in any browser to compare directions side-by-side
(light + dark, multiple sizes, mock sidebar context).

---

## 🎯 Core asset (one of three to be chosen)

| Direction | Mark file | Lockup file | Concept |
|---|---|---|---|
| **A · Pulse** | `a-pulse/mark-{light,dark}.svg` | `a-pulse/lockup-{light,dark}.svg` | 4 ascending bars in a soft rounded square |
| **B · Glyph** | `b-glyph/mark-{light,dark}.svg` | `b-glyph/lockup-{light,dark}.svg` | The Arabic letter ب abstracted: stroke + dot |
| **C · Wordmark** | — (no separate mark) | `c-wordmark/wordmark-{light,dark}.svg` | "bayan." with cyan accent dot |

### File usage

- `mark-light.svg` — for light backgrounds (light app shell, white emails)
- `mark-dark.svg` — for dark backgrounds (dark sidebar, dark emails)
- `lockup-{variant}.svg` — for headers, OG cards, footer signatures (mark + wordmark)
- All SVGs are flat geometry — no filters, no gradients, no opacity tricks

### Production cutover (once a direction is picked)

Replace these existing files in `frontend/public/`:
- `bayan-logo.svg` → copy of chosen direction's `mark-light.svg`
- `bayan-logo-dark.svg` → copy of chosen direction's `mark-dark.svg`
- Update `Sidebar.tsx` fallback strings if you move filenames around
- Backend `BAYAN_DEFAULTS` in `backend/app/main.py` already points at `/bayan-logo.svg` so no change needed there

---

## 🎨 Brand palette (preserved from existing)

| Role | HEX | Notes |
|---|---|---|
| Primary | `#29B5E8` | Bayan cyan; brand signature |
| Cyan-light | `#7DD3F3` | Chart accent only |
| Cyan-dark | `#0598CC` | Chart accent only |
| Ink | `#011419` | Wordmark on light |
| Background | `#FAFBFC` | App canvas, light |
| Background dark | `#020617` | App canvas, dark |

The cyan ramp belongs to the chart palette. **The logo itself uses a single
solid cyan** — using the gradient on the mark blends it visually with the
charts, and a logo should never compete with the data inside the app.

---

## 🅰 Typography (wordmark)

- **Display / wordmark**: Inter Tight 600, letter-spacing −0.04em
- **Body** (existing): Inter
- **Mono / data**: JetBrains Mono / `ui-monospace`

For production SVG, run wordmark text through outline-to-paths
(`fonttools subset` or Inkscape "Object → Object to Path") so it
renders identically without depending on Inter Tight being installed.
The current SVGs use a system fallback chain ending at `system-ui` —
acceptable for previews, not for press-kit-quality assets.

---

## 🚫 Anti-slop rules (what we cut from the previous logo)

| Cut | Why |
|---|---|
| Drop shadow filter | Adds noise; doesn't survive favicon shrink |
| Multi-stop gradients | Generic SaaS-tech tell; flattens to mush at 16 px |
| Five concepts in one mark (bars + line + pie + frame + ticks) | A logo says one thing; ours said five |
| Fill-opacity 0.15 / 0.2 / 0.3 / 0.6 / 0.9 / 0.95 | Blurry hierarchy; mark should be crisp at any size |
| White rectangle "dashboard frame" backdrop | Container-inside-container — visual stutter |

---

## 🧭 Decision aid

Pick **A · Pulse** if:
- You want the safest migration path (closest to the existing identity)
- The brand is "professional, geometric, data-first" and that's enough

Pick **B · Glyph** if:
- You want the most distinctive identity in the analytics space
- The Arabic etymology of the name (Bayan = clarity) matters to your story
- You value scale-to-favicon legibility most

Pick **C · Wordmark** if:
- You're repositioning Bayan toward a typographic / dev-tool aesthetic
- You're OK with no app-icon (or willing to design a "b·" sub-mark for favicons later)

**Mix** is allowed: e.g. Direction A's mark + Direction C's wordmark for the
header lockup. They're built on the same color and the same letter-spacing
discipline, so they compose cleanly.
