"use client"

import { useState, useEffect, useMemo } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import { RiCloseLine, RiArrowRightLine, RiCheckLine, RiAlertLine } from '@remixicon/react'
import { Api } from '@/lib/api'

type DatasourceInfo = {
  id: string
  name: string
  type: string
}

type DatasourceMappingItem = {
  sourceDatasourceId: string
  sourceDatasourceName?: string
  targetDatasourceId: string
  tables: string[] // Tables used by this datasource
}

type TableMappingItem = {
  sourceTable: string
  targetTable: string
  sourceDatasourceId: string
}

type Props = {
  open: boolean
  onClose: () => void
  onConfirm: (datasourceIdMap: Record<string, string>, tableNameMap: Record<string, string>) => void
  importData: {
    dashboards: any[]
    datasources: any[]
  }
  userId: string
}

export default function ImportMappingDialog({ open, onClose, onConfirm, importData, userId }: Props) {
  const [loading, setLoading] = useState(false)
  const [availableDatasources, setAvailableDatasources] = useState<DatasourceInfo[]>([])
  const [datasourceMappings, setDatasourceMappings] = useState<DatasourceMappingItem[]>([])
  const [tableMappings, setTableMappings] = useState<TableMappingItem[]>([])
  const [availableTables, setAvailableTables] = useState<Record<string, string[]>>({}) // datasourceId -> tables[]
  const [loadingTables, setLoadingTables] = useState<Record<string, boolean>>({})

  // Extract datasource IDs and table names from import data
  useEffect(() => {
    if (!open || !importData.dashboards.length) return

    const sourceDatasourceIds = new Set<string>()
    const tablesByDatasource = new Map<string, Set<string>>()

    const walk = (node: any, currentDsId?: string) => {
      if (node && typeof node === 'object') {
        if (Array.isArray(node)) {
          node.forEach(item => walk(item, currentDsId))
        } else {
          // Track datasource ID
          const dsId = node.datasourceId || currentDsId
          if (dsId && typeof dsId === 'string') {
            sourceDatasourceIds.add(dsId)
            if (!tablesByDatasource.has(dsId)) {
              tablesByDatasource.set(dsId, new Set())
            }
          }

          // Track table names for this datasource
          if (dsId) {
            const tables = tablesByDatasource.get(dsId)!
            if (node.source && typeof node.source === 'string') tables.add(node.source)
            if (node.table && typeof node.table === 'string') tables.add(node.table)
            if (node.tableName && typeof node.tableName === 'string') tables.add(node.tableName)
          }

          // Recurse
          Object.values(node).forEach(val => walk(val, dsId || currentDsId))
        }
      }
    }

    importData.dashboards.forEach(d => walk(d.definition || d))

    // Create initial datasource mappings
    const dsMap: DatasourceMappingItem[] = Array.from(sourceDatasourceIds).map(srcId => {
      // Try to find matching datasource name from importData.datasources
      const srcDs = importData.datasources?.find((ds: any) => ds.id === srcId)
      return {
        sourceDatasourceId: srcId,
        sourceDatasourceName: srcDs?.name || srcId,
        targetDatasourceId: '', // User needs to select
        tables: Array.from(tablesByDatasource.get(srcId) || [])
      }
    })

    setDatasourceMappings(dsMap)

    // Create initial table mappings (all unmapped)
    const tblMap: TableMappingItem[] = []
    dsMap.forEach(dsMapping => {
      dsMapping.tables.forEach(table => {
        tblMap.push({
          sourceTable: table,
          targetTable: table, // Default to same name
          sourceDatasourceId: dsMapping.sourceDatasourceId
        })
      })
    })
    setTableMappings(tblMap)
  }, [open, importData])

  // Load available datasources in target system (all datasources, not filtered by user)
  useEffect(() => {
    if (!open) return
    ;(async () => {
      try {
        setLoading(true)
        // Pass userId as actorId (second param) to get all datasources the user can access
        const list = await Api.listDatasources(undefined, userId)
        setAvailableDatasources(list || [])
      } catch (e) {
        console.error('Failed to load datasources:', e)
      } finally {
        setLoading(false)
      }
    })()
  }, [open, userId])

  // Load available tables when target datasource is selected
  const loadTablesForDatasource = async (datasourceId: string) => {
    if (availableTables[datasourceId] || loadingTables[datasourceId]) return

    setLoadingTables(prev => ({ ...prev, [datasourceId]: true }))
    try {
      const result = await Api.tablesOnly(datasourceId)
      const tables: string[] = []
      
      // Filter out system/role schemas (pg_catalog, information_schema, etc.)
      const isSystemSchema = (schemaName: string) => {
        const lower = schemaName.toLowerCase()
        return lower.startsWith('pg_') || 
               lower === 'information_schema' || 
               lower === 'sys' || 
               lower === 'mysql' ||
               lower === 'performance_schema'
      }
      
      result.schemas?.forEach(schema => {
        // Skip system schemas
        if (schema.name && isSystemSchema(schema.name)) return
        
        schema.tables?.forEach(table => {
          tables.push(schema.name ? `${schema.name}.${table}` : table)
        })
      })
      setAvailableTables(prev => ({ ...prev, [datasourceId]: tables }))
    } catch (e) {
      console.error(`Failed to load tables for datasource ${datasourceId}:`, e)
      setAvailableTables(prev => ({ ...prev, [datasourceId]: [] }))
    } finally {
      setLoadingTables(prev => ({ ...prev, [datasourceId]: false }))
    }
  }

  const handleDatasourceChange = (sourceDsId: string, targetDsId: string) => {
    setDatasourceMappings(prev =>
      prev.map(item =>
        item.sourceDatasourceId === sourceDsId
          ? { ...item, targetDatasourceId: targetDsId }
          : item
      )
    )
    if (targetDsId) {
      loadTablesForDatasource(targetDsId)
    }
  }

  const handleTableChange = (sourceTable: string, sourceDsId: string, targetTable: string) => {
    setTableMappings(prev =>
      prev.map(item =>
        item.sourceTable === sourceTable && item.sourceDatasourceId === sourceDsId
          ? { ...item, targetTable }
          : item
      )
    )
  }

  const handleConfirm = () => {
    // Build datasource ID map
    const dsIdMap: Record<string, string> = {}
    datasourceMappings.forEach(item => {
      if (item.targetDatasourceId) {
        dsIdMap[item.sourceDatasourceId] = item.targetDatasourceId
      }
    })

    // Build table name map
    const tableNameMap: Record<string, string> = {}
    tableMappings.forEach(item => {
      if (item.targetTable && item.targetTable !== item.sourceTable) {
        tableNameMap[item.sourceTable] = item.targetTable
      }
    })

    onConfirm(dsIdMap, tableNameMap)
  }

  const canConfirm = useMemo(() => {
    // All datasources must have a target selected
    return datasourceMappings.every(item => item.targetDatasourceId)
  }, [datasourceMappings])

  // Group table mappings by source datasource
  const tablesBySourceDs = useMemo(() => {
    const grouped = new Map<string, TableMappingItem[]>()
    tableMappings.forEach(item => {
      if (!grouped.has(item.sourceDatasourceId)) {
        grouped.set(item.sourceDatasourceId, [])
      }
      grouped.get(item.sourceDatasourceId)!.push(item)
    })
    return grouped
  }, [tableMappings])

  return (
    <Dialog.Root open={open} onOpenChange={(isOpen) => !isOpen && onClose()}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 bg-black/50 z-[100]" />
        <Dialog.Content className="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-[101] w-[90vw] max-w-4xl max-h-[85vh] overflow-hidden bg-[hsl(var(--card))] rounded-xl border border-[hsl(var(--border))] shadow-xl">
          <div className="flex items-center justify-between px-6 py-4 border-b border-[hsl(var(--border))]">
            <div>
              <Dialog.Title className="text-lg font-semibold text-[hsl(var(--foreground))]">
                Configure Import Mappings
              </Dialog.Title>
              <Dialog.Description className="text-sm text-[hsl(var(--muted-foreground))] mt-1">
                Map datasources and table names from the import file to your target system
              </Dialog.Description>
            </div>
            <Dialog.Close asChild>
              <button className="p-2 rounded-md hover:bg-[hsl(var(--muted))] text-[hsl(var(--muted-foreground))]">
                <RiCloseLine className="w-5 h-5" />
              </button>
            </Dialog.Close>
          </div>

          <div className="overflow-y-auto max-h-[calc(85vh-140px)] px-6 py-4">
            {loading ? (
              <div className="text-center py-8 text-[hsl(var(--muted-foreground))]">Loading datasources...</div>
            ) : (
              <div className="space-y-6">
                {datasourceMappings.map((dsMapping) => {
                  const targetDs = availableDatasources.find(ds => ds.id === dsMapping.targetDatasourceId)
                  const targetTables = dsMapping.targetDatasourceId ? availableTables[dsMapping.targetDatasourceId] : []
                  const isLoadingTables = dsMapping.targetDatasourceId ? loadingTables[dsMapping.targetDatasourceId] : false
                  const tables = tablesBySourceDs.get(dsMapping.sourceDatasourceId) || []

                  return (
                    <div key={dsMapping.sourceDatasourceId} className="border border-[hsl(var(--border))] rounded-lg p-4">
                      {/* Datasource Mapping */}
                      <div className="mb-4">
                        <label className="text-sm font-medium text-[hsl(var(--foreground))] mb-2 block">
                          Datasource Mapping
                        </label>
                        <div className="flex items-center gap-3">
                          <div className="flex-1 px-3 py-2 rounded-md bg-[hsl(var(--muted))] text-sm">
                            <span className="font-medium">Source:</span> {dsMapping.sourceDatasourceName || dsMapping.sourceDatasourceId}
                          </div>
                          <RiArrowRightLine className="w-5 h-5 text-[hsl(var(--muted-foreground))] flex-shrink-0" />
                          <select
                            value={dsMapping.targetDatasourceId}
                            onChange={(e) => handleDatasourceChange(dsMapping.sourceDatasourceId, e.target.value)}
                            className="flex-1 px-3 py-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] text-sm focus:outline-none focus:ring-2 focus:ring-[hsl(var(--ring))]"
                          >
                            <option value="">Select target datasource...</option>
                            {availableDatasources.map(ds => (
                              <option key={ds.id} value={ds.id}>
                                {ds.name} ({ds.type})
                              </option>
                            ))}
                          </select>
                        </div>
                      </div>

                      {/* Table Mappings */}
                      {tables.length > 0 && (
                        <div>
                          <label className="text-sm font-medium text-[hsl(var(--foreground))] mb-2 block">
                            Table Name Mappings ({tables.length})
                          </label>
                          <div className="space-y-2 max-h-60 overflow-y-auto">
                            {tables.map((tableMapping) => (
                              <div key={`${tableMapping.sourceDatasourceId}-${tableMapping.sourceTable}`} className="flex items-center gap-2 text-sm">
                                <div className="flex-1 px-2 py-1.5 rounded bg-[hsl(var(--muted))] font-mono text-xs truncate">
                                  {tableMapping.sourceTable}
                                </div>
                                <RiArrowRightLine className="w-4 h-4 text-[hsl(var(--muted-foreground))] flex-shrink-0" />
                                {isLoadingTables ? (
                                  <div className="flex-1 px-2 py-1.5 text-xs text-[hsl(var(--muted-foreground))]">Loading tables...</div>
                                ) : !dsMapping.targetDatasourceId ? (
                                  <div className="flex-1 px-2 py-1.5 text-xs text-[hsl(var(--muted-foreground))]">Select a datasource first</div>
                                ) : targetTables && targetTables.length > 0 ? (
                                  <select
                                    value={tableMapping.targetTable}
                                    onChange={(e) => handleTableChange(tableMapping.sourceTable, tableMapping.sourceDatasourceId, e.target.value)}
                                    className="flex-1 px-2 py-1.5 rounded border border-[hsl(var(--border))] bg-[hsl(var(--background))] font-mono text-xs focus:outline-none focus:ring-1 focus:ring-[hsl(var(--ring))]"
                                  >
                                    {/* If current selection is not in the list, add it as first option */}
                                    {tableMapping.targetTable && !targetTables.includes(tableMapping.targetTable) && (
                                      <option value={tableMapping.targetTable}>
                                        {tableMapping.targetTable} (custom)
                                      </option>
                                    )}
                                    {targetTables.map(table => (
                                      <option key={table} value={table}>
                                        {table}
                                      </option>
                                    ))}
                                  </select>
                                ) : (
                                  <div className="flex-1 px-2 py-1.5 text-xs text-amber-600">No tables found in target datasource</div>
                                )}
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
            )}
          </div>

          <div className="flex items-center justify-between px-6 py-4 border-t border-[hsl(var(--border))]">
            <div className="flex items-center gap-2 text-sm">
              {!canConfirm && (
                <>
                  <RiAlertLine className="w-4 h-4 text-amber-500" />
                  <span className="text-[hsl(var(--muted-foreground))]">Please select a target datasource for each source</span>
                </>
              )}
            </div>
            <div className="flex items-center gap-3">
              <button
                onClick={onClose}
                className="px-4 py-2 rounded-md text-sm font-medium text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted))] transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleConfirm}
                disabled={!canConfirm}
                className="px-4 py-2 rounded-md text-sm font-medium bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors flex items-center gap-2"
              >
                <RiCheckLine className="w-4 h-4" />
                Confirm & Import
              </button>
            </div>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
