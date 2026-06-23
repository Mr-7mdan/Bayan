---
version: "alpha"
name: "Core Interface"
description: "Core Interface Dashboard Section is designed for demonstrating application workflows and interface hierarchy. Key features include clear information density, modular panels, and interface rhythm. It is suitable for product showcases, admin panels, and analytics experiences."
colors:
  primary: "#22D3EE"
  secondary: "#06B6D4"
  tertiary: "#2548F6"
  neutral: "#0A0A0C"
  background: "#0A0A0C"
  surface: "#22D3EE"
  text-primary: "#737373"
  text-secondary: "#FFFFFF"
  border: "#2A2A30"
  accent: "#22D3EE"
typography:
  display-lg:
    fontFamily: "Inter"
    fontSize: "48px"
    fontWeight: 300
    lineHeight: "48px"
    letterSpacing: "-0.025em"
  body-md:
    fontFamily: "Inter"
    fontSize: "14px"
    fontWeight: 300
    lineHeight: "20px"
  label-md:
    fontFamily: "Inter"
    fontSize: "12px"
    fontWeight: 300
    lineHeight: "16px"
    letterSpacing: "0.3px"
rounded:
  full: "9999px"
spacing:
  base: "4px"
  sm: "2px"
  md: "4px"
  lg: "6px"
  xl: "8px"
  gap: "8px"
  card-padding: "14px"
  section-padding: "24px"
components:
  button-primary:
    textColor: "{colors.text-secondary}"
    typography: "{typography.label-md}"
    rounded: "{rounded.full}"
    padding: "6px"
  button-link:
    textColor: "#A3A3A3"
    typography: "{typography.label-md}"
    rounded: "{rounded.full}"
    padding: "6px"
  card:
    rounded: "16px"
    padding: "20px"
---

## Overview

- **Composition cues:**
  - Layout: Grid
  - Content Width: Bounded
  - Framing: Open
  - Grid: Strong

## Colors

The color system uses dark mode with #22D3EE as the main accent and #0A0A0C as the neutral foundation.

- **Primary (#22D3EE):** Main accent and emphasis color.
- **Secondary (#06B6D4):** Supporting accent for secondary emphasis.
- **Tertiary (#2548F6):** Reserved accent for supporting contrast moments.
- **Neutral (#0A0A0C):** Neutral foundation for backgrounds, surfaces, and supporting chrome.

- **Usage:** Background: #0A0A0C; Surface: #22D3EE; Text Primary: #737373; Text Secondary: #FFFFFF; Border: #2A2A30; Accent: #22D3EE

- **Gradients:** bg-gradient-to-b from-[#1C1C20] to-[#121215], bg-gradient-to-r from-cyan-600 to-cyan-400, bg-gradient-to-b from-[#2A2A30] to-[#1C1C20], bg-gradient-to-r from-[#2A2A30] to-transparent

## Typography

Typography relies on Inter across display, body, and utility text.

- **Display (`display-lg`):** Inter, 48px, weight 300, line-height 48px, letter-spacing -0.025em.
- **Body (`body-md`):** Inter, 14px, weight 300, line-height 20px.
- **Labels (`label-md`):** Inter, 12px, weight 300, line-height 16px, letter-spacing 0.3px.

## Layout

Layout follows a grid composition with reusable spacing tokens. Preserve the grid, bounded structural frame before changing ornament or component styling. Use 4px as the base rhythm and let larger gaps step up from that cadence instead of introducing unrelated spacing values.

Treat the page as a grid / bounded composition, and keep that framing stable when adding or remixing sections.

- **Layout type:** Grid
- **Content width:** Bounded
- **Base unit:** 4px
- **Scale:** 2px, 4px, 6px, 8px, 14px, 16px, 20px, 24px
- **Section padding:** 24px, 40px, 48px
- **Card padding:** 14px, 20px, 24px, 40px
- **Gaps:** 8px, 12px, 14px, 16px

## Elevation & Depth

Depth is communicated through elevated, border contrast, and reusable shadow or blur treatments. Keep those recipes consistent across hero panels, cards, and controls so the page reads as one material system.

Surfaces should read as elevated first, with borders, shadows, and blur only reinforcing that material choice.

- **Surface style:** Elevated
- **Borders:** 1px #2A2A30
- **Shadows:** rgb(255, 255, 255) 0px 0px 0px 0px, rgb(42, 42, 48) 0px 0px 0px 1px, rgba(255, 255, 255, 0.06) 0px 1px 0px 0px inset, rgba(0, 0, 0, 0.5) 0px 12px 24px 0px; rgb(255, 255, 255) 0px 0px 0px 0px, rgb(58, 58, 64) 0px 0px 0px 1px, rgba(0, 0, 0, 0.6) 0px 2px 4px 0px inset, rgba(255, 255, 255, 0.05) 0px 1px 0px 0px; rgb(255, 255, 255) 0px 0px 0px 0px, rgb(58, 58, 64) 0px 0px 0px 1px, rgba(0, 0, 0, 0.8) 0px 1px 2px 0px inset

### Techniques
- **Gradient border shell:** Use a thin gradient border shell around the main card. Wrap the surface in an outer shell with 40px padding and a 32px radius. Drive the shell with linear-gradient(rgb(24, 24, 28), rgb(10, 10, 12)) so the edge reads like premium depth instead of a flat stroke. Keep the actual stroke understated so the gradient shell remains the hero edge treatment. Inset the real content surface inside the wrapper with a slightly smaller radius so the gradient only appears as a hairline frame.

## Shapes

Shapes rely on a tight radius system anchored by 4px and scaled across cards, buttons, and supporting surfaces. Icon geometry should stay compatible with that soft-to-controlled silhouette.

Use the radius family intentionally: larger surfaces can open up, but controls and badges should stay within the same rounded DNA instead of inventing sharper or pill-only exceptions.

- **Corner radii:** 4px, 6px, 8px, 12px, 16px, 9999px
- **Icon treatment:** Linear
- **Icon sets:** Solar

## Components

Anchor interactions to the detected button styles. Reuse the existing card surface recipe for content blocks.

### Buttons
- **Primary:** text #FFFFFF, radius 9999px, padding 6px, border 0px solid rgb(229, 231, 235).
- **Links:** text #A3A3A3, radius 9999px, padding 6px, border 0px solid rgb(229, 231, 235).

### Cards and Surfaces
- **Card surface:** border 0px solid rgb(229, 231, 235), radius 16px, padding 20px, shadow rgb(255, 255, 255) 0px 0px 0px 0px, rgb(42, 42, 48) 0px 0px 0px 1px, rgba(255, 255, 255, 0.06) 0px 1px 0px 0px inset, rgba(0, 0, 0, 0.5) 0px 12px 24px 0px.
- **Card surface:** border 0px solid rgb(229, 231, 235), radius 16px, padding 24px, shadow rgb(255, 255, 255) 0px 0px 0px 0px, rgb(42, 42, 48) 0px 0px 0px 1px, rgba(255, 255, 255, 0.06) 0px 1px 0px 0px inset, rgba(0, 0, 0, 0.5) 0px 12px 24px 0px.
- **Card surface:** border 0px solid rgb(229, 231, 235), radius 12px, padding 14px, shadow rgb(255, 255, 255) 0px 0px 0px 0px, rgb(42, 42, 48) 0px 0px 0px 1px, rgba(255, 255, 255, 0.05) 0px 1px 0px 0px inset, rgba(0, 0, 0, 0.4) 0px 6px 12px 0px.

### Iconography
- **Treatment:** Linear.
- **Sets:** Solar.

## Do's and Don'ts

Use these constraints to keep future generations aligned with the current system instead of drifting into adjacent styles.

### Do
- Do use the primary palette as the main accent for emphasis and action states.
- Do keep spacing aligned to the detected 4px rhythm.
- Do reuse the Elevated surface treatment consistently across cards and controls.
- Do keep corner radii within the detected 4px, 6px, 8px, 12px, 16px, 9999px family.

### Don't
- Don't introduce extra accent colors outside the core palette roles unless the page needs a new semantic state.
- Don't mix unrelated shadow or blur recipes that break the current depth system.
- Don't exceed the detected moderate motion intensity without a deliberate reason.

## Motion

Motion feels controlled and interface-led across text, layout, and section transitions. Timing clusters around 150ms and 2000ms. Easing favors ease and cubic-bezier(0.4. Hover behavior focuses on text and stroke changes.

**Motion Level:** moderate

**Durations:** 150ms, 2000ms

**Easings:** ease, cubic-bezier(0.4, 0, 1), 0.2, 0.6

**Hover Patterns:** text, stroke, color, shadow

## WebGL

Reconstruct the graphics as a full-bleed background field using custom shaders. The effect should read as technical, meditative, and atmospheric: dot-matrix particle field with light gray and sparse spacing. Build it from dot particles + soft depth fade so the effect reads clearly. Animate it as slow breathing pulse. Interaction can react to the pointer, but only as a subtle drift. Preserve dom fallback.

**Id:** webgl

**Label:** WebGL

**Stack:** WebGL

**Insights:**
  - **Scene:**
    - **Value:** Full-bleed background field
  - **Effect:**
    - **Value:** Dot-matrix particle field
  - **Primitives:**
    - **Value:** Dot particles + soft depth fade
  - **Motion:**
    - **Value:** Slow breathing pulse
  - **Interaction:**
    - **Value:** Pointer-reactive drift
  - **Render:**
    - **Value:** custom shaders

**Techniques:** Dot matrix, Breathing pulse, Pointer parallax, Shader gradients, Noise fields

**Code Evidence:**
  - **HTML reference:**
    - **Language:** html
    - **Snippet:**
      ```html
      <!-- Dither Background Shader Canvas (Moved inside shell and rendered faint) -->
      <canvas id="dither-bg" class="absolute inset-0 w-full h-full z-0 pointer-events-none opacity-[0.12] mix-blend-screen"></canvas>

      <!-- Inner Secure Content -->
      ```
  - **JS reference:**
    - **Language:** js
    - **Snippet:**
      ```
      attribute vec2 position;
              void main() {
                  gl_Position = vec4(position, 0.0, 1.0);
              }
          `;
          const vs = gl.createShader(gl.VERTEX_SHADER);
      …
      ```
  - **Draw call:**
    - **Language:** js
    - **Snippet:**
      ```
      -1.0,  1.0,  
           1.0,  1.0
      ]);

      const vbo = gl.createBuffer();
      gl.bindBuffer(gl.ARRAY_BUFFER, vbo);
      gl.bufferData(gl.ARRAY_BUFFER, vertices, gl.STATIC_DRAW);
      …
      ```
