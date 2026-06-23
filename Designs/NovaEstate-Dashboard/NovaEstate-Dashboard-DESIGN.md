---
version: "alpha"
name: "NovaEstate Dashboard"
description: "Novaestate Analytics Dashboard Section is designed for demonstrating application workflows and interface hierarchy. Key features include clear information density, modular panels, and interface rhythm. It is suitable for product showcases, admin panels, and analytics experiences."
colors:
  primary: "#14B8A6"
  secondary: "#DBEAFE"
  tertiary: "#F4FCE3"
  neutral: "#FFFFFF"
  background: "#FFFFFF"
  surface: "#F8FAFC"
  text-primary: "#0F172A"
  text-secondary: "#94A3B8"
  border: "#F1F5F9"
  accent: "#14B8A6"
typography:
  display-lg:
    fontFamily: "System Font"
    fontSize: "48px"
    fontWeight: 600
    lineHeight: "48px"
    letterSpacing: "-0.025em"
  body-md:
    fontFamily: "System Font"
    fontSize: "12px"
    fontWeight: 400
    lineHeight: "16px"
  label-md:
    fontFamily: "System Font"
    fontSize: "16px"
    fontWeight: 400
    lineHeight: "24px"
rounded:
  full: "9999px"
spacing:
  base: "4px"
  sm: "1px"
  md: "2px"
  lg: "4px"
  xl: "6px"
  gap: "4px"
  card-padding: "9px"
  section-padding: "24px"
components:
  button-primary:
    backgroundColor: "#FDF2F8"
    textColor: "#DB2777"
    typography: "{typography.label-md}"
    rounded: "{rounded.full}"
    padding: "8px"
  button-link:
    textColor: "#64748B"
    typography: "{typography.label-md}"
    rounded: "{rounded.full}"
    padding: "8px"
  card:
    backgroundColor: "{colors.neutral}"
    rounded: "24px"
    padding: "24px"
---

## Overview

- **Composition cues:**
  - Layout: Grid
  - Content Width: Full Bleed
  - Framing: Glassy
  - Grid: Strong

## Colors

The color system uses light mode with #14B8A6 as the main accent and #FFFFFF as the neutral foundation.

- **Primary (#14B8A6):** Main accent and emphasis color.
- **Secondary (#DBEAFE):** Supporting accent for secondary emphasis.
- **Tertiary (#F4FCE3):** Reserved accent for supporting contrast moments.
- **Neutral (#FFFFFF):** Neutral foundation for backgrounds, surfaces, and supporting chrome.

- **Usage:** Background: #FFFFFF; Surface: #F8FAFC; Text Primary: #0F172A; Text Secondary: #94A3B8; Border: #F1F5F9; Accent: #14B8A6

- **Gradients:** bg-gradient-to-br from-white/40 to-transparent via-white/10, bg-gradient-to-r from-teal-900/80 to-transparent via-teal-800/40

## Typography

Typography relies on System Font across display, body, and utility text.

- **Display (`display-lg`):** System Font, 48px, weight 600, line-height 48px, letter-spacing -0.025em.
- **Body (`body-md`):** System Font, 12px, weight 400, line-height 16px.
- **Labels (`label-md`):** System Font, 16px, weight 400, line-height 24px.

## Layout

Layout follows a grid composition with reusable spacing tokens. Preserve the grid, full bleed structural frame before changing ornament or component styling. Use 4px as the base rhythm and let larger gaps step up from that cadence instead of introducing unrelated spacing values.

Treat the page as a grid / full bleed composition, and keep that framing stable when adding or remixing sections.

- **Layout type:** Grid
- **Content width:** Full Bleed
- **Base unit:** 4px
- **Scale:** 1px, 2px, 4px, 6px, 8px, 12px, 16px, 20px
- **Section padding:** 24px, 32px, 40px
- **Card padding:** 9px, 16px, 20px, 24px
- **Gaps:** 4px, 6px, 8px, 12px

## Elevation & Depth

Depth is communicated through glass, border contrast, and reusable shadow or blur treatments. Keep those recipes consistent across hero panels, cards, and controls so the page reads as one material system.

Surfaces should read as glass first, with borders, shadows, and blur only reinforcing that material choice.

- **Surface style:** Glass
- **Borders:** 1px #F1F5F9; 0.5px #CBD5E1; 1px #E2E8F0; 1px #F8FAFC
- **Shadows:** rgba(0, 0, 0, 0) 0px 0px 0px 0px, rgba(0, 0, 0, 0) 0px 0px 0px 0px, rgba(0, 0, 0, 0.05) 0px 1px 2px 0px; rgba(0, 0, 0, 0) 0px 0px 0px 0px, rgba(0, 0, 0, 0) 0px 0px 0px 0px, rgba(0, 0, 0, 0.1) 0px 20px 25px -5px, rgba(0, 0, 0, 0.1) 0px 8px 10px -6px; rgba(0, 0, 0, 0) 0px 0px 0px 0px, rgba(0, 0, 0, 0) 0px 0px 0px 0px, rgba(0, 0, 0, 0.15) 0px 30px 60px -15px
- **Blur:** 12px, 4px

### Techniques
- **Gradient border shell:** Use a thin gradient border shell around the main card. Wrap the surface in an outer shell with 1px padding and a 24px radius. Drive the shell with linear-gradient(to right bottom, rgba(255, 255, 255, 0.4), rgba(255, 255, 255, 0.1), rgba(0, 0, 0, 0)) so the edge reads like premium depth instead of a flat stroke. Keep the actual stroke understated so the gradient shell remains the hero edge treatment. Inset the real content surface inside the wrapper with a slightly smaller radius so the gradient only appears as a hairline frame.

## Shapes

Shapes rely on a tight radius system anchored by 6px and scaled across cards, buttons, and supporting surfaces. Icon geometry should stay compatible with that soft-to-controlled silhouette.

Use the radius family intentionally: larger surfaces can open up, but controls and badges should stay within the same rounded DNA instead of inventing sharper or pill-only exceptions.

- **Corner radii:** 6px, 16px, 24px, 32px, 9999px
- **Icon treatment:** Linear
- **Icon sets:** Solar

## Components

Anchor interactions to the detected button styles. Reuse the existing card surface recipe for content blocks.

### Buttons
- **Primary:** background #FDF2F8, text #DB2777, radius 9999px, padding 8px, border 0px solid rgb(229, 231, 235).
- **Links:** text #64748B, radius 9999px, padding 8px, border 0px solid rgb(229, 231, 235).

### Cards and Surfaces
- **Card surface:** background #FFFFFF, border 1px solid rgb(241, 245, 249), radius 24px, padding 24px, shadow rgba(0, 0, 0, 0) 0px 0px 0px 0px, rgba(0, 0, 0, 0) 0px 0px 0px 0px, rgba(0, 0, 0, 0.05) 0px 1px 2px 0px.
- **Card surface:** background rgba(255, 255, 255, 0.8), border 0px solid rgb(229, 231, 235), radius 23px, padding 20px, shadow none.
- **Card surface:** background #FFFFFF, border 1px solid rgb(241, 245, 249), radius 24px, padding 16px, shadow rgba(0, 0, 0, 0) 0px 0px 0px 0px, rgba(0, 0, 0, 0) 0px 0px 0px 0px, rgba(0, 0, 0, 0.05) 0px 1px 2px 0px.

### Iconography
- **Treatment:** Linear.
- **Sets:** Solar.

## Do's and Don'ts

Use these constraints to keep future generations aligned with the current system instead of drifting into adjacent styles.

### Do
- Do use the primary palette as the main accent for emphasis and action states.
- Do keep spacing aligned to the detected 4px rhythm.
- Do reuse the Glass surface treatment consistently across cards and controls.
- Do keep corner radii within the detected 6px, 16px, 24px, 32px, 9999px family.

### Don't
- Don't introduce extra accent colors outside the core palette roles unless the page needs a new semantic state.
- Don't mix unrelated shadow or blur recipes that break the current depth system.
- Don't exceed the detected moderate motion intensity without a deliberate reason.

## Motion

Motion feels controlled and interface-led across text, layout, and section transitions. Timing clusters around 150ms and 700ms. Easing favors ease and cubic-bezier(0.4. Hover behavior focuses on text and color changes. Scroll choreography uses GSAP ScrollTrigger for section reveals and pacing.

**Motion Level:** moderate

**Durations:** 150ms, 700ms, 1000ms

**Easings:** ease, cubic-bezier(0.4, 0, 0.2, 1)

**Hover Patterns:** text, color, shadow

**Scroll Patterns:** gsap-scrolltrigger

## WebGL

Reconstruct the graphics as a full-bleed background field using webgl, renderer, alpha, antialias, dpr clamp, custom shaders. The effect should read as technical, meditative, and atmospheric: fine line lattice with gray and sparse spacing. Build it from line trails + sparse anchors so the effect reads clearly. Animate it as slow breathing pulse. Interaction can react to the pointer, but only as a subtle drift. Preserve dom fallback.

**Id:** webgl

**Label:** WebGL

**Stack:** ThreeJS, WebGL

**Insights:**
  - **Scene:**
    - **Value:** Full-bleed background field
  - **Effect:**
    - **Value:** Fine line lattice
  - **Primitives:**
    - **Value:** Line trails + sparse anchors
  - **Motion:**
    - **Value:** Slow breathing pulse
  - **Interaction:**
    - **Value:** Pointer-reactive drift
  - **Render:**
    - **Value:** WebGL, Renderer, alpha, antialias, DPR clamp, custom shaders

**Techniques:** Perspective grid, Line lattice, Breathing pulse, Pointer parallax, Shader gradients

**Code Evidence:**
  - **HTML reference:**
    - **Language:** html
    - **Snippet:**
      ```html
      <!-- WebGL Background -->
      <canvas id="webgl-canvas"></canvas>

      <!-- Main Application Wrapper -->
      ```
  - **JS reference:**
    - **Language:** html
    - **Snippet:**
      ```html
      <script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
      ```

## ThreeJS

Reconstruct the Three.js layer as a full-bleed background field with layered spatial depth that feels volumetric and technical. Use alpha, antialias, dpr clamp renderer settings, orthographic projection, plane geometry, shadermaterial materials, and ambient + key + rim lighting. Motion should read as slow orbital drift, with poster frame + dom fallback.

**Id:** threejs

**Label:** ThreeJS

**Stack:** ThreeJS, WebGL

**Insights:**
  - **Scene:**
    - **Value:** Full-bleed background field with layered spatial depth
  - **Render:**
    - **Value:** alpha, antialias, DPR clamp
  - **Camera:**
    - **Value:** Orthographic projection
  - **Lighting:**
    - **Value:** ambient + key + rim
  - **Materials:**
    - **Value:** ShaderMaterial
  - **Geometry:**
    - **Value:** plane
  - **Motion:**
    - **Value:** Slow orbital drift

**Techniques:** Shader materials, Timeline beats, alpha, antialias, DPR clamp, Poster frame + DOM fallback

**Code Evidence:**
  - **HTML reference:**
    - **Language:** html
    - **Snippet:**
      ```html
      <!-- WebGL Background -->
      <canvas id="webgl-canvas"></canvas>

      <!-- Main Application Wrapper -->
      ```
  - **JS reference:**
    - **Language:** html
    - **Snippet:**
      ```html
      <script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
      ```
