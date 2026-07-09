import { defineConfig } from 'vitest/config'
import path from 'path'

// Spec 25: Vitest + Testing Library foundation. Pure-util tests today; jsdom env
// and RTL are wired so component tests can land without reconfiguring.
export default defineConfig({
  // tsconfig.json sets jsx: 'preserve' for Next.js; Vite 8 (rolldown/oxc) inherits
  // that and leaves JSX untransformed for tests. Force the automatic runtime here
  // so .tsx component tests compile without a Next build.
  oxc: {
    jsx: { runtime: 'automatic', importSource: 'react' },
  },
  test: {
    environment: 'jsdom',
    globals: true,
    include: ['src/**/*.test.{ts,tsx}'],
  },
  resolve: {
    alias: { '@': path.resolve(__dirname, 'src') },
  },
})
