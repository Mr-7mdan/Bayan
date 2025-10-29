"use client"

import React from 'react'

export default class ErrorBoundary extends React.Component<{
  name?: string
  children?: React.ReactNode
}, { hasError: boolean; msg?: string }> {
  constructor(props: any) {
    super(props)
    this.state = { hasError: false }
  }
  static getDerivedStateFromError(error: any) {
    return { hasError: true, msg: (error?.message || String(error)) }
  }
  componentDidCatch(error: any, info: any) {
    try {
      // eslint-disable-next-line no-console
      console.error(`[ErrorBoundary${this.props.name ? ": "+this.props.name : ''}]`, error, info?.componentStack)
    } catch {}
  }
  render() {
    if (this.state.hasError) {
      return (
        <div className="text-xs text-red-600">
          Error in {this.props.name || 'component'}: {this.state.msg}
        </div>
      )
    }
    return this.props.children as any
  }
}
