import type { NextRequest } from 'next/server'
import { NextResponse } from 'next/server'

// Public routes reachable while logged out. The API verifies auth
// authoritatively; this is only a presence check on the session cookie.
const PUBLIC = [/^\/login/, /^\/logout/, /^\/reset-password/, /^\/v\//, /^\/render\//, /^\/themes/, /^\/demos/]

export function middleware(req: NextRequest) {
  const { pathname } = req.nextUrl
  if (pathname === '/') {
    const url = req.nextUrl.clone()
    url.pathname = '/home'
    return NextResponse.redirect(url)
  }
  if (PUBLIC.some((r) => r.test(pathname))) return NextResponse.next()
  if (!req.cookies.get('bayan_session')) {
    const url = req.nextUrl.clone()
    url.pathname = '/login'
    url.searchParams.set('next', pathname)
    return NextResponse.redirect(url)
  }
  return NextResponse.next()
}

export const config = {
  matcher: ['/((?!_next|api|favicon.ico|.*\\..*).*)'],
}
