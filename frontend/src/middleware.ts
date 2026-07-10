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
  // Expose the pathname to the i18n request config (which otherwise can't see it)
  // so public dashboard views can force the default language.
  const headers = new Headers(req.headers)
  headers.set('x-pathname', pathname)
  const pass = () => NextResponse.next({ request: { headers } })
  if (PUBLIC.some((r) => r.test(pathname))) return pass()
  if (!req.cookies.get('bayan_session')) {
    const url = req.nextUrl.clone()
    url.pathname = '/login'
    url.searchParams.set('next', pathname)
    return NextResponse.redirect(url)
  }
  return pass()
}

export const config = {
  matcher: ['/((?!_next|api|favicon.ico|.*\\..*).*)'],
}
