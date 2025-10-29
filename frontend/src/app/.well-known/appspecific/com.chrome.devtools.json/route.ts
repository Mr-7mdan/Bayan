import { NextResponse } from 'next/server'

export async function GET() {
  // Serve an empty JSON to silence Chrome DevTools probe
  return new NextResponse('{}', {
    status: 200,
    headers: { 'content-type': 'application/json; charset=utf-8' },
  })
}
