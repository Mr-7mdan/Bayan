---
version: "alpha"
name: "Platform Architecture"
description: "Platform Architecture Pricing Section is designed for comparing plans and supporting conversion decisions. Key features include plan comparison blocks and conversion-oriented actions. It is suitable for subscription pricing pages and plan comparison experiences."
colors:
  primary: "#0E2C2A"
  secondary: "#0A2220"
  tertiary: "#0F3054"
  neutral: "#2C2B34"
  background: "#0E2C2A"
  surface: "#0A2220"
  text-primary: "#F3F4F6"
  text-secondary: "#54B6AB"
  border: "#18534C"
  accent: "#0E2C2A"
typography:
  display-lg:
    fontFamily: "Inter"
    fontSize: "48px"
    fontWeight: 400
    lineHeight: "48px"
    letterSpacing: "-0.025em"
  body-md:
    fontFamily: "Inter"
    fontSize: "14px"
    fontWeight: 400
    lineHeight: "22.75px"
spacing:
  base: "12px"
  sm: "12px"
  md: "16px"
  lg: "20px"
  xl: "24px"
  gap: "16px"
  card-padding: "40px"
  section-padding: "40px"
---

## Overview

- **Composition cues:**
  - Layout: Flex
  - Content Width: Bounded
  - Framing: Framed
  - Grid: Minimal

## Colors

The color system uses dark mode with #0E2C2A as the main accent and #2C2B34 as the neutral foundation.

- **Primary (#0E2C2A):** Main accent and emphasis color.
- **Secondary (#0A2220):** Supporting accent for secondary emphasis.
- **Tertiary (#0F3054):** Reserved accent for supporting contrast moments.
- **Neutral (#2C2B34):** Neutral foundation for backgrounds, surfaces, and supporting chrome.

- **Usage:** Background: #0E2C2A; Surface: #0A2220; Text Primary: #F3F4F6; Text Secondary: #54B6AB; Border: #18534C; Accent: #0E2C2A

## Typography

Typography relies on Inter across display, body, and utility text.

- **Display (`display-lg`):** Inter, 48px, weight 400, line-height 48px, letter-spacing -0.025em.
- **Body (`body-md`):** Inter, 14px, weight 400, line-height 22.75px.

## Layout

Layout follows a flex composition with reusable spacing tokens. Preserve the flex, bounded structural frame before changing ornament or component styling. Use 12px as the base rhythm and let larger gaps step up from that cadence instead of introducing unrelated spacing values.

Treat the page as a flex / bounded composition, and keep that framing stable when adding or remixing sections.

- **Layout type:** Flex
- **Content width:** Bounded
- **Base unit:** 12px
- **Scale:** 12px, 16px, 20px, 24px, 32px, 40px, 64px, 96px
- **Section padding:** 40px, 112px
- **Card padding:** 40px
- **Gaps:** 16px, 40px, 96px

## Elevation & Depth

Depth is communicated through outlined, border contrast, and reusable shadow or blur treatments. Keep those recipes consistent across hero panels, cards, and controls so the page reads as one material system.

Surfaces should read as outlined first, with borders, shadows, and blur only reinforcing that material choice.

- **Surface style:** Outlined
- **Borders:** 1px #18534C; 1px #444251; 1px #1E5496; 1px #A0D4CD

## Shapes

Shapes rely on a tight radius system anchored by 2px and scaled across cards, buttons, and supporting surfaces. Icon geometry should stay compatible with that soft-to-controlled silhouette.

Use the radius family intentionally: larger surfaces can open up, but controls and badges should stay within the same rounded DNA instead of inventing sharper or pill-only exceptions.

- **Corner radii:** 2px

## Do's and Don'ts

Use these constraints to keep future generations aligned with the current system instead of drifting into adjacent styles.

### Do
- Do use the primary palette as the main accent for emphasis and action states.
- Do keep spacing aligned to the detected 12px rhythm.
- Do reuse the Outlined surface treatment consistently across cards and controls.
- Do keep corner radii within the detected 2px family.

### Don't
- Don't introduce extra accent colors outside the core palette roles unless the page needs a new semantic state.
- Don't exceed the detected minimal motion intensity without a deliberate reason.

## Motion

Motion stays restrained and interface-led across text, layout, and scroll transitions. Timing clusters around 150ms. Easing favors ease and cubic-bezier(0.4.

**Motion Level:** minimal

**Durations:** 150ms

**Easings:** ease, cubic-bezier(0.4, 0, 0.2, 1)
