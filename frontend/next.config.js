const createNextIntlPlugin = require('next-intl/plugin')
const withNextIntl = createNextIntlPlugin('./src/i18n/request.ts')

/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // Standalone server bundle for the Docker image (spec 23)
  output: 'standalone',
  // Use stable option location for typed routes (moved out of experimental)
  typedRoutes: true,
  // Silence workspace root inference warning (point to monorepo root if applicable)
  outputFileTracingRoot: __dirname,
  // Tree-shake barrel imports from icon/UI kits so unused icons/components stay out of chunks
  experimental: {
    optimizePackageImports: ['@tremor/react', '@remixicon/react', 'lucide-react'],
  },
  typescript: {
    // Dangerously allow production builds to successfully complete even if
    // your project has type errors.
    ignoreBuildErrors: true,
  },
  // Note: allowedDevOrigins was removed as it's no longer supported in Next.js 15+
  // Note: eslint config moved to .eslintrc or next lint CLI options
  async headers() {
    return [
      {
        source: '/:path*',
        headers: [
          {
            key: 'Content-Security-Policy',
            value: "script-src 'self' 'unsafe-inline' 'unsafe-eval'; object-src 'none';",
          },
        ],
      },
    ]
  },
  async rewrites() {
    const backend = (process.env.NEXT_PUBLIC_API_BASE_URL || '').replace(/\/$/, '')
    if (!backend) return []
    return [
      {
        source: '/api/:path*',
        destination: `${backend}/:path*`,
      },
    ]
  },
};

/** @type {import('next').NextConfig} */

module.exports = withNextIntl(nextConfig);