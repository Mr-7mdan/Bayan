declare module 'react-grid-layout' {
  import * as React from 'react'
  const GridLayout: React.ComponentType<Record<string, unknown>>
  export default GridLayout
  export const Responsive: React.ComponentType<Record<string, unknown>>
  export function WidthProvider<P>(c: React.ComponentType<P>): React.ComponentType<P>
}
