# Alpha Theme Variants

Each `.css` file in this folder is an opt-in "alpha" theme. Themes are gated
behind the **Alpha themes** toggle in the Environment page; with the toggle off
they are inert. With the toggle on, a user picks one light variant and one
dark variant.

## How a variant is selected

`ThemeProvider` (`frontend/src/components/providers/ThemeProvider.tsx`) writes
two attributes to `<html>`:

- **Dark:** `<html class="dark" data-variant="…">` for `bluish` (default),
  `blackish`, `core-interface`, `platform-architecture`.
- **Light:** `<html data-light-variant="…">` for `nova-estate`. When the
  light variant is `default` the attribute is *removed*, not set to
  `"default"`, so existing selectors that don't know about variants keep
  cascading from `:root`.

## Selector contract for a variant CSS file

Every alpha variant must scope **all** its rules behind its own attribute
selector. Never write rules at `:root` or `.dark` directly — that would leak
into the production palette.

```css
/* core-interface.css — dark */
.dark[data-variant="core-interface"] {
  --primary: 188 92% 53%;          /* hsl conversion of #22D3EE */
  --background: 240 7% 4%;          /* #0A0A0C */
  /* …rest of the token overrides… */
}

/* nova-estate.css — light */
:root[data-light-variant="nova-estate"]:not(.dark) {
  --primary: 173 80% 40%;           /* #14B8A6 */
  /* …rest… */
}
```

## Which tokens to override

Every variant must redefine the following tokens. Anything you leave
undeclared falls back to the production value, which means a partially-styled
mixed-palette UI. Override **all of these** even if your value matches the
default:

```
--background           --foreground
--muted                --muted-foreground
--surface-0..3
--card                 --card-foreground
--popover              --popover-foreground
--border               --input               --ring
--primary              --primary-foreground  --primary-deep
--secondary            --secondary-foreground
--accent               --accent-foreground
--topbar-bg            --topbar-fg
--header-accent
--success --warning --danger
--chart-1 .. --chart-5
```

Optional — if your variant's design system calls for it:

```
--radius-card          (e.g. 24px for nova-estate, 16px for core-interface,
                        2px for platform-architecture)
--font-display
--font-body
```

These extras are not consumed by existing components yet; they're a
forward-looking hook so a future redesign pass can pick them up.

## Component-level overrides

Some components use hardcoded Tailwind colors (e.g. `text-blue-600`,
`bg-emerald-500`) that won't pick up token changes. If your variant calls
for those to change, append the rules at the end of your variant's CSS file
under the same scoped selector:

```css
.dark[data-variant="core-interface"] .sidebar-item-active-dark {
  /* override the cyan inset border with the Core Interface electric cyan */
  box-shadow: inset 2px 0 0 hsl(var(--primary));
}
```

## Motion

Variants with `motion-level: minimal` (e.g. Platform-Architecture) should add:

```css
.dark[data-variant="platform-architecture"] * {
  /* clamp ambient transitions to the design's 150ms */
  transition-duration: 150ms !important;
}
```

…sparingly, since broad `*` selectors hurt repaint cost.

## Hooking up a new variant

1. Add a new file `frontend/src/app/themes/<slug>.css`.
2. Import it from `frontend/src/app/globals.css` *after* the `@tailwind`
   directives so it can override utility-layer rules:

   ```css
   @import './themes/<slug>.css';
   ```

3. Add the slug to the union type in `ThemeProvider.tsx`
   (`DarkVariantAlpha` or extend `LightVariant`) and to the
   `ALPHA_DARK_VARIANTS` / `ALPHA_LIGHT_VARIANTS` arrays so the master
   alpha-toggle knows about it.

4. Wire it into the Environment page's "Alpha themes" picker
   (`frontend/src/app/(app)/admin/environment/AdminEnvironmentClient.tsx`).
