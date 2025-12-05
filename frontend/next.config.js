/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // Use stable option location for typed routes (moved out of experimental)
  typedRoutes: true,
  // Silence workspace root inference warning (point to monorepo root if applicable)
  outputFileTracingRoot: __dirname,
  typescript: {
    // Dangerously allow production builds to successfully complete even if
    // your project has type errors.
    ignoreBuildErrors: true,
  },
  // Note: allowedDevOrigins was removed as it's no longer supported in Next.js 15+
  // Note: eslint config moved to .eslintrc or next lint CLI options
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

module.exports = nextConfig;