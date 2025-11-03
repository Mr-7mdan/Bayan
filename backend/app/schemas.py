from __future__ import annotations

from pydantic import BaseModel, Field
from pydantic.config import ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime


class HealthResponse(BaseModel):
    status: str = "ok"
    app: str
    env: str


class TestConnectionRequest(BaseModel):
    dsn: Optional[str] = Field(
        default=None, description="SQLAlchemy DSN, e.g. postgres://... If omitted, tests local DuckDB."
    )


class TestConnectionResponse(BaseModel):
    ok: bool
    error: Optional[str] = None


# --- Detect DB Server ---
class DetectRequest(BaseModel):
    dsn: Optional[str] = Field(default=None, description="Optional full DSN to use for detection")
    host: Optional[str] = Field(default=None)
    port: Optional[int] = Field(default=None)
    user: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    db: Optional[str] = Field(default=None)
    driver: Optional[str] = Field(default=None)
    timeout: Optional[int] = Field(default=3, description="Socket/connect timeout in seconds")


class DetectResponse(BaseModel):
    ok: bool = True
    detected: Optional[str] = Field(default=None, description="One of postgres|mysql|mssql|oracle|unknown")
    method: Optional[str] = Field(default=None, description="dsn|version_query|handshake|port_hint")
    versionString: Optional[str] = None
    candidates: Optional[List[str]] = None
    error: Optional[str] = None


# --- Datasources ---
class DatasourceCreate(BaseModel):
    name: str
    type: str
    connectionUri: Optional[str] = Field(default=None)
    options: Optional[Dict[str, Any]] = Field(default=None)
    userId: Optional[str] = Field(default=None)


class DatasourceOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    type: str
    createdAt: datetime = Field(alias="created_at", serialization_alias="createdAt")
    active: Optional[bool] = None


class DatasourceDetailOut(DatasourceOut):
    connectionUri: Optional[str] = None
    options: Optional[Dict[str, Any]] = None


# Allows partial updates for edit dialog
class DatasourceUpdate(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    connectionUri: Optional[str] = None
    options: Optional[Dict[str, Any]] = None
    active: Optional[bool] = None


class ColumnInfo(BaseModel):
    name: str
    type: Optional[str] = None


class TableInfo(BaseModel):
    name: str
    columns: List[ColumnInfo]


class SchemaInfo(BaseModel):
    name: str
    tables: List[TableInfo]


class IntrospectResponse(BaseModel):
    schemas: List[SchemaInfo]


# --- Query ---
class QueryRequest(BaseModel):
    sql: str = Field(description="Raw SQL to execute against the target datasource")
    datasourceId: Optional[str] = Field(default=None, description="If omitted, uses local DuckDB")
    limit: Optional[int] = Field(default=1000)
    params: Optional[Dict[str, Any]] = Field(default=None, description="Bound parameters for the SQL (e.g., startDate, endDate)")
    offset: Optional[int] = Field(default=0)
    includeTotal: Optional[bool] = Field(default=False, description="If true, also return total row count of the inner query")
    requestId: Optional[str] = None
    # Preference: route to local DuckDB when the referenced table exists locally
    preferLocalDuck: Optional[bool] = Field(default=None)
    preferLocalTable: Optional[str] = Field(default=None, description="Optional table name hint for local DuckDB preference")


class QueryResponse(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    elapsedMs: Optional[int] = None
    totalRows: Optional[int] = None


# --- Distinct ---
class DistinctRequest(BaseModel):
    source: str
    field: str
    where: Optional[Dict[str, Any]] = None
    datasourceId: Optional[str] = None


class DistinctResponse(BaseModel):
    values: List[Any]


# --- Pivot (server-side aggregation for pivot grid) ---
class PivotRequest(BaseModel):
    source: str
    rows: List[str] = []
    cols: List[str] = []
    valueField: Optional[str] = Field(default=None, description="Measure field to aggregate; omit for COUNT(*)")
    aggregator: Optional[str] = Field(default="count", description="count|sum|avg|min|max|distinct")
    where: Optional[Dict[str, Any]] = None
    datasourceId: Optional[str] = None
    limit: Optional[int] = Field(default=None, description="Optional cap on number of group rows returned")
    widgetId: Optional[str] = None
    requestId: Optional[str] = None


# --- Dashboards ---
class DashboardDefinition(BaseModel):
    layout: List[Dict[str, Any]] = []
    widgets: Dict[str, Dict[str, Any]] = {}
    # Optional per-dashboard settings (e.g., public page options)
    options: Optional[Dict[str, Any]] = None


class DashboardSaveRequest(BaseModel):
    id: Optional[str] = None
    name: str
    userId: Optional[str] = None
    definition: DashboardDefinition


class DashboardOut(BaseModel):
    id: str
    name: str
    userId: Optional[str] = None
    createdAt: datetime = Field(alias="created_at", serialization_alias="createdAt")
    definition: DashboardDefinition


class PublishOut(BaseModel):
    publicId: str
    protected: bool = False


class EmbedTokenOut(BaseModel):
    token: str
    exp: int


class SetPublishTokenRequest(BaseModel):
    token: Optional[str] = Field(default=None, description="Set to a non-empty string to protect link; omit or empty to remove protection")


class SidebarCountsResponse(BaseModel):
    dashboardCount: int
    datasourceCount: int
    sharedCount: int
    collectionCount: int


class DashboardListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    userId: Optional[str] = None
    createdAt: datetime = Field(alias="created_at", serialization_alias="createdAt")
    updatedAt: Optional[datetime] = Field(default=None, alias="updated_at", serialization_alias="updatedAt")
    published: bool = False
    publicId: Optional[str] = None
    widgetsCount: int = 0
    tablesCount: int = 0
    datasourceCount: int = 0


class AddToCollectionRequest(BaseModel):
    userId: str
    dashboardId: str
    collectionName: Optional[str] = None
    # Optional: when used to share with a specific user, attach the sharer and dashboard name for a notification
    sharedBy: Optional[str] = None
    dashboardName: Optional[str] = None
    permission: Optional[str] = Field(default=None, description="ro|rw")


class AddToCollectionResponse(BaseModel):
    collectionId: str
    collectionName: str
    added: bool = Field(description="True when the dashboard was newly added")
    totalItems: int = Field(description="Number of dashboards in this collection")
    collectionsCount: int = Field(description="Number of collections owned by the user")
    collectionItemsCount: int = Field(description="Total dashboards across all collections for the user")


class NotificationOut(BaseModel):
    id: str
    message: str
    createdAt: datetime = Field(alias="created_at", serialization_alias="createdAt")


class CollectionItemOut(BaseModel):
    collectionId: str
    dashboardId: str
    name: str
    ownerId: Optional[str] = None
    ownerName: Optional[str] = None
    permission: Optional[str] = None  # 'ro' | 'rw'
    addedAt: datetime = Field(alias="added_at", serialization_alias="addedAt")
    published: bool = False
    publicId: Optional[str] = None


# --- Contacts ---
class ContactIn(BaseModel):
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    tags: Optional[List[str]] = None
    userId: Optional[str] = None


class ContactOut(BaseModel):
    id: str
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    tags: List[str] = []
    active: bool = True
    createdAt: datetime = Field(alias="created_at", serialization_alias="createdAt")


class ContactsListResponse(BaseModel):
    items: List[ContactOut]
    total: int
    page: int
    pageSize: int


class ImportContactsRequest(BaseModel):
    items: List[ContactIn]


class ImportContactsResponse(BaseModel):
    imported: int
    total: int


class BulkEmailPayload(BaseModel):
    ids: Optional[List[str]] = None
    emails: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    subject: str
    html: str
    rateLimitPerMinute: Optional[int] = None
    queue: Optional[bool] = None
    notifyEmail: Optional[str] = None


class BulkSmsPayload(BaseModel):
    ids: Optional[List[str]] = None
    numbers: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    message: str
    rateLimitPerMinute: Optional[int] = None
    queue: Optional[bool] = None
    notifyEmail: Optional[str] = None


# --- QuerySpec (skeleton for Ibis integration) ---
class QuerySpec(BaseModel):
    source: str
    select: Optional[List[str]] = None
    where: Optional[Dict[str, Any]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    # Optional chart semantics for server-side aggregation
    x: Optional[str] = None
    y: Optional[str] = None
    agg: Optional[str] = Field(default=None, description="none|count|distinct|avg|sum|min|max")
    groupBy: Optional[str] = Field(default=None, description="none|day|week|month|quarter|year")
    measure: Optional[str] = Field(default=None, description="Custom SQL expression for value")
    # Optional multi-series; kept flexible to avoid over-constraining client payload
    series: Optional[list[dict[str, Any]]] = None
    legend: Optional[str] = None
    # Ranking hints for aggregated queries
    orderBy: Optional[str] = Field(default=None, description="x|value")
    order: Optional[str] = Field(default=None, description="asc|desc")
    # Week start convention for groupBy=week (default ISO Monday)
    weekStart: Optional[str] = Field(default=None, description="mon|sun")


class QuerySpecRequest(BaseModel):
    spec: QuerySpec
    datasourceId: Optional[str] = None
    limit: Optional[int] = 1000
    offset: Optional[int] = 0
    includeTotal: Optional[bool] = False
    widgetId: Optional[str] = None
    # Preference: route execution to local DuckDB when the base source exists locally
    preferLocalDuck: Optional[bool] = None


class BrandingOut(BaseModel):
    fonts: dict
    palette: dict
    orgName: Optional[str] = None
    logoLight: Optional[str] = None
    logoDark: Optional[str] = None
    favicon: Optional[str] = None

class BrandingUpdateIn(BaseModel):
    orgName: Optional[str] = None
    logoLight: Optional[str] = None
    logoDark: Optional[str] = None
    favicon: Optional[str] = None


# --- Favorites ---
class FavoriteOut(BaseModel):
    userId: str
    dashboardId: str
    name: Optional[str] = None
    updatedAt: Optional[datetime] = None


class AddFavoriteRequest(BaseModel):
    dashboardId: str


# --- Auth / Users ---
class UserOut(BaseModel):
    id: str
    name: str
    email: str
    role: str
    
class UserRowOut(BaseModel):
    id: str
    name: str
    email: str
    role: str
    active: bool = True
    createdAt: datetime = Field(alias="created_at", serialization_alias="createdAt")


class SignupRequest(BaseModel):
    name: str
    email: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class ChangePasswordRequest(BaseModel):
    userId: str
    oldPassword: str
    newPassword: str


class ResetPasswordRequest(BaseModel):
    email: str
    newPassword: str


class AdminCreateUserRequest(BaseModel):
    name: str
    email: str
    password: str
    role: str = "user"


class SetActiveRequest(BaseModel):
    active: bool


class AdminSetPasswordRequest(BaseModel):
    newPassword: str


# --- Sync tasks & local stats ---
class SyncTaskCreate(BaseModel):
    datasourceId: Optional[str] = None
    sourceSchema: str | None = None
    sourceTable: str
    destTableName: str
    mode: str  # 'sequence' | 'snapshot'
    pkColumns: list[str] | None = None
    selectColumns: list[str] | None = None
    sequenceColumn: str | None = None
    batchSize: int | None = 10000
    scheduleCron: str | None = None
    enabled: bool = True


class SyncTaskOut(BaseModel):
    id: str
    datasourceId: str
    sourceSchema: str | None = None
    sourceTable: str
    destTableName: str
    mode: str
    pkColumns: list[str] = []
    selectColumns: list[str] = []
    sequenceColumn: str | None = None
    batchSize: int | None = 10000
    scheduleCron: str | None = None
    enabled: bool = True
    groupKey: str
    createdAt: datetime
    # latest state snapshot
    lastRunAt: datetime | None = None
    lastRowCount: int | None = None
    inProgress: bool = False
    error: str | None = None
    progressCurrent: int | None = None
    progressTotal: int | None = None
    progressPhase: str | None = None


class LocalTableStat(BaseModel):
    table: str
    rowCount: int | None = None
    lastSyncAt: datetime | None = None
    datasourceId: str | None = None
    sourceSchema: str | None = None
    sourceTable: str | None = None


class LocalStatsResponse(BaseModel):
    enginePath: str
    fileSize: int
    tables: list[LocalTableStat]


class SyncRunOut(BaseModel):
    id: str
    taskId: str
    datasourceId: str
    mode: str
    startedAt: datetime
    finishedAt: datetime | None = None
    rowCount: int | None = None
    error: str | None = None


# --- Export / Import: Datasources ---
class DatasourceExportItem(BaseModel):
    id: str
    name: str
    type: str
    connectionUri: str | None = None
    options: Dict[str, Any] | None = None
    userId: str | None = None
    active: bool | None = True
    createdAt: datetime
    # Optional sync tasks (settings only)
    syncTasks: list["SyncTaskExportItem"] | None = None


class DatasourceImportItem(BaseModel):
    id: str | None = None
    name: str
    type: str
    connectionUri: str | None = None
    options: Dict[str, Any] | None = None
    userId: str | None = None
    active: bool | None = True
    syncTasks: list["SyncTaskImportItem"] | None = None


class DatasourceImportRequest(BaseModel):
    items: list[DatasourceImportItem]


class DatasourceImportResponse(BaseModel):
    created: int
    updated: int
    items: list[DatasourceOut]
    idMap: Dict[str, str] | None = None


# --- Export / Import: Dashboards ---
class DashboardExportItem(BaseModel):
    id: str
    name: str
    userId: str | None = None
    definition: DashboardDefinition
    createdAt: datetime
    updatedAt: datetime | None = None


class DashboardExportResponse(BaseModel):
    dashboards: list[DashboardExportItem]
    datasources: list[DatasourceExportItem] | None = None


class DashboardImportItem(BaseModel):
    name: str
    userId: str | None = None
    definition: DashboardDefinition
    id: str | None = None


class DashboardImportRequest(BaseModel):
    dashboards: list[DashboardImportItem]
    datasourceIdMap: Dict[str, str] | None = None
    datasources: list[DatasourceImportItem] | None = None


class DashboardImportResponse(BaseModel):
    imported: int
    items: list[DashboardOut]


# --- Sync tasks export/import ---
class SyncTaskExportItem(BaseModel):
    id: str
    datasourceId: str
    sourceSchema: str | None = None
    sourceTable: str
    destTableName: str
    mode: str
    pkColumns: list[str] = []
    selectColumns: list[str] = []
    sequenceColumn: str | None = None
    batchSize: int | None = 10000
    scheduleCron: str | None = None
    enabled: bool = True
    groupKey: str
    createdAt: datetime


class SyncTaskImportItem(BaseModel):
    id: str | None = None
    sourceSchema: str | None = None
    sourceTable: str
    destTableName: str
    mode: str
    pkColumns: list[str] | None = None
    selectColumns: list[str] | None = None
    sequenceColumn: str | None = None
    batchSize: int | None = 10000
    scheduleCron: str | None = None
    enabled: bool = True


# --- Datasource-level Transforms DSL ---
class Condition(BaseModel):
    op: str  # 'eq'|'ne'|'gt'|'gte'|'lt'|'lte'|'in'|'like'|'regex'
    left: str
    right: Any | None = None


class Scope(BaseModel):
    level: str  # 'datasource'|'table'|'widget'
    widgetId: Optional[str] = None
    table: Optional[str] = None


class CustomColumn(BaseModel):
    name: str
    expr: str
    type: Optional[str] = None  # 'string'|'number'|'date'|'boolean'
    scope: Optional[Scope] = None


class TransformCase(BaseModel):
    type: str = "case"
    target: str
    cases: List[Dict[str, Any]]  # { when: Condition, then: Value }
    else_: Any | None = Field(default=None, alias="else")
    model_config = ConfigDict(populate_by_name=True)
    scope: Optional[Scope] = None


class TransformReplace(BaseModel):
    type: str = "replace"
    target: str
    search: Any
    replace: Any
    scope: Optional[Scope] = None


class TransformTranslate(BaseModel):
    type: str = "translate"
    target: str
    search: str
    replace: str
    scope: Optional[Scope] = None


class TransformNullHandling(BaseModel):
    type: str = "nullHandling"
    target: str
    mode: str  # 'coalesce'|'isnull'|'ifnull'
    value: Any
    scope: Optional[Scope] = None


class TransformComputed(BaseModel):
    type: str = "computed"
    name: str
    expr: str
    valueType: Optional[str] = None
    scope: Optional[Scope] = None


class TransformUnpivot(BaseModel):
    type: str = "unpivot"
    sourceColumns: List[str]
    keyColumn: str
    valueColumn: str
    mode: Optional[str] = None  # 'unpivot'|'union'
    omitZeroNull: Optional[bool] = None
    scope: Optional[Scope] = None


Transform = TransformCase | TransformReplace | TransformTranslate | TransformNullHandling | TransformComputed | TransformUnpivot


class JoinSpec(BaseModel):
    joinType: str  # 'left'|'inner'|'right'
    targetTable: str
    sourceKey: str
    targetKey: str
    columns: Optional[List[Dict[str, Optional[str]]]] = None
    aggregate: Optional[Dict[str, Any]] = None
    filter: Optional[Condition] = None


class SortSpec(BaseModel):
    by: str
    direction: str  # 'asc'|'desc'
    semantic: Optional[str] = None


class TopNSpec(BaseModel):
    n: int
    by: str
    direction: str  # 'asc'|'desc'
    scope: Optional[str] = None  # 'pre-agg'|'post-agg'


class DatasourceTransforms(BaseModel):
    customColumns: List[CustomColumn] = []
    transforms: List[Transform] = []
    joins: List[JoinSpec] = []
    defaults: Optional[Dict[str, Any]] = None  # { sort?: SortSpec, limitTopN?: TopNSpec }


class TransformsPreviewRequest(DatasourceTransforms):
    source: Optional[str] = None
    select: Optional[List[str]] = None
    limit: Optional[int] = 100
    context: Optional[Dict[str, Any]] = None  # e.g., { widgetId }


class PreviewResponse(BaseModel):
    sql: Optional[str] = None
    columns: Optional[List[str]] = None
    rows: Optional[List[List[Any]]] = None
    warnings: Optional[List[str]] = None
