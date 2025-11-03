/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // Use stable option location for typed routes (moved out of experimental)
  typedRoutes: true,
  // Silence workspace root inference warning (point to monorepo root if applicable)
  outputFileTracingRoot: __dirname,
  experimental: {
    allowedDevOrigins: [
      'http://localhost:3000',
      'http://127.0.0.1:3000',
      'http://172.16.4.120:3000',
      'http://192.168.31.43:3000',
    ],
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
  eslint: {
    ignoreDuringBuilds: true
  }
};

/** @type {import('next').NextConfig} */

module.exports = nextConfig;