import { defineConfig } from 'vitest/config'
import path from 'path'

// Spec 25: Vitest + Testing Library foundation. Pure-util tests today; jsdom env
// and RTL are wired so component tests can land without reconfiguring.
export default defineConfig({
  test: {
    environment: 'jsdom',
    globals: true,
    include: ['src/**/*.test.{ts,tsx}'],
  },
  resolve: {
    alias: { '@': path.resolve(__dirname, 'src') },
  },
})
