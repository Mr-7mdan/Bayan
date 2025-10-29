"use client"

import React, { useMemo } from 'react'
import ReactFlow, { Background, Controls, MiniMap, Node, Edge } from 'reactflow'
import 'reactflow/dist/style.css'
import type { IntrospectResponse } from '@/lib/api'

export default function SchemaGraph({ schema, height = 560 }: { schema: IntrospectResponse; height?: number }) {
  const { nodes, edges } = useMemo(() => {
    const ns: Node[] = []
    const es: Edge[] = []
    const allTables: Array<{ key: string; schema: string; table: string; columns: Array<{ name: string; type?: string | null }> }> = []
    for (const s of schema.schemas || []) {
      for (const t of s.tables || []) {
        allTables.push({ key: `${s.name}.${t.name}`, schema: s.name, table: t.name, columns: t.columns || [] })
      }
    }
    const cols = 4
    const xGap = 320
    const yGap = 240
    allTables.forEach((t, i) => {
      const x = (i % cols) * xGap
      const y = Math.floor(i / cols) * yGap
      ns.push({
        id: t.key,
        position: { x, y },
        data: {
          label: (
            <div className="rounded-md border bg-[hsl(var(--card))] text-[hsl(var(--foreground))]">
              <div className="px-3 py-2 border-b flex items-center justify-between">
                <div className="font-semibold text-sm">{t.table}</div>
                <div className="text-xs opacity-70">{t.schema}</div>
              </div>
              <div className="p-2 text-xs max-h-44 overflow-auto">
                <table className="w-full">
                  <tbody>
                    {t.columns.map((c) => (
                      <tr key={c.name} className="border-t">
                        <td className="py-0.5 font-mono pr-2">{c.name}</td>
                        <td className="py-0.5 text-[11px] text-muted-foreground">{c.type || '-'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ),
        },
        style: { width: 280 },
        draggable: true,
        selectable: true,
        className: 'elev-bottom',
        type: 'default',
      })
    })
    // No edges for now; will add when FK metadata is available
    return { nodes: ns, edges: es }
  }, [schema])

  return (
    <div style={{ height }} className="rounded-md border">
      <ReactFlow nodes={nodes} edges={edges} fitView>
        <MiniMap pannable zoomable />
        <Controls />
        <Background />
      </ReactFlow>
    </div>
  )
}
