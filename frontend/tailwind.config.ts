// @ts-nocheck
import type { Config } from 'tailwindcss'
import animate from 'tailwindcss-animate'

const config: Config = {
  darkMode: ['class'],
  content: [
    './src/**/*.{ts,tsx}',
    './node_modules/@tremor/**/*.{js,ts,jsx,tsx,mjs}',
    './node_modules/@tremor/react/**/*.{js,ts,jsx,tsx,mjs}',
  ],
  safelist: [
    // Tremor positive/negative fills
    'fill-blue-600','dark:fill-blue-500',
    'fill-indigo-600','dark:fill-indigo-500',
    'fill-rose-600','dark:fill-rose-500',
    'fill-amber-600','dark:fill-amber-500',
    'fill-violet-600','dark:fill-violet-500',
    'fill-emerald-600','dark:fill-emerald-500',
    'fill-gray-600','dark:fill-gray-500',
    // Strokes for line/area
    'stroke-blue-500','stroke-indigo-500','stroke-rose-500','stroke-amber-500','stroke-violet-500','stroke-emerald-500','stroke-gray-500',
    // Broad patterns for Tremor charts
    { pattern: /^(fill|stroke)-(indigo|rose|amber|emerald|violet|blue|gray|cyan|pink|lime|fuchsia)-(400|500|600)$/ },
    { pattern: /^(text|bg)-(indigo|rose|amber|emerald|violet|blue|gray)-(50|100|200|300|400|500|600)$/ },
    { pattern: /^(fill|stroke)-(indigo|rose|amber|emerald|violet|blue|gray|cyan|pink|lime|fuchsia)-(400|500|600)$/, variants: ['dark'] },
  ],
  theme: {
    container: { center: true, padding: '1rem', screens: { '2xl': '1400px' } },
    extend: {
      colors: {
        border: 'hsl(var(--border))',
        input: 'hsl(var(--input))',
        ring: 'hsl(var(--ring))',
        background: 'hsl(var(--background))',
        foreground: 'hsl(var(--foreground))',
        primary: {
          DEFAULT: 'hsl(var(--primary))',
          foreground: 'hsl(var(--primary-foreground))',
          deep: 'hsl(var(--primary-deep))',
        },
        secondary: {
          DEFAULT: 'hsl(var(--secondary))',
          foreground: 'hsl(var(--secondary-foreground))',
        },
        accent: {
          DEFAULT: 'hsl(var(--accent))',
          foreground: 'hsl(var(--accent-foreground))',
        },
        card: {
          DEFAULT: 'hsl(var(--card))',
          foreground: 'hsl(var(--card-foreground))',
        },
        chart: {
          1: 'hsl(var(--chart-1))',
          2: 'hsl(var(--chart-2))',
          3: 'hsl(var(--chart-3))',
          4: 'hsl(var(--chart-4))',
          5: 'hsl(var(--chart-5))',
        },
      },
      fontSize: {
        '2xs': ['var(--font-2xs)', { lineHeight: '1rem' }],      // 11px
        xs: ['var(--font-xs)', { lineHeight: '1rem' }],          // 12px
        sm: ['var(--font-sm)', { lineHeight: '1.25rem' }],       // 13px
        base: ['var(--font-base)', { lineHeight: '1.375rem' }],  // 14px
        lg: ['var(--font-lg)', { lineHeight: '1.5rem' }],        // 16px
        xl: ['var(--font-xl)', { lineHeight: '1.75rem' }],       // 20px
        '2xl': ['var(--font-2xl)', { lineHeight: '2rem' }],      // 24px
      },
      borderRadius: {
        lg: 'var(--radius-lg)',
        md: 'var(--radius-md)',
        sm: 'var(--radius-sm)',
      },
      boxShadow: {
        card: 'var(--shadow-1)',
        popover: 'var(--shadow-2)',
        modal: 'var(--shadow-3)',
      },
    },
  },
  plugins: [animate],
}

export default config
