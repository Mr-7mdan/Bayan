from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from datetime import datetime
from sqlalchemy.orm import Session
import json

from ..models import (
    SessionLocal,
    save_dashboard,
    load_dashboard,
    Dashboard,
    ShareLink,
    publish_dashboard_link,
    unpublish_dashboard_links,
    get_share_link_by_dashboard,
    get_share_link_by_public,
    set_share_link_token,
    verify_share_link_token,
    delete_dashboard,
    get_share_permission,
    list_share_permissions_by_dashboard,
    remove_share_permission,
    remove_dashboard_from_all_collections_of_user,
    list_embed_tokens_by_dashboard,
    create_embed_token_row,
    revoke_embed_token,
    EmbedToken,
    User,
)
from ..schemas import (
    DashboardSaveRequest,
    DashboardOut,
    PublishOut,
    SetPublishTokenRequest,
    DashboardListItem,
    DashboardExportItem,
    DashboardExportResponse,
    DashboardImportItem,
    EmbedTokenOut,
    DashboardImportRequest,
    DashboardImportResponse,
    DatasourceExportItem,
    DatasourceImportItem,
)
from ..config import settings
from ..models import Datasource, SyncTask
from ..security import decrypt_text, encrypt_text, sign_embed_token, verify_embed_token

router = APIRouter(prefix="/dashboards", tags=["dashboards"])

_EMBED_RL_WINDOW_SEC = 60
_EMBED_RL_MAX_REQUESTS = 120
_embed_rl_counters: dict[str, list[float]] = {}

def _rate_limit_ok(key: str, now: float) -> bool:
    try:
        arr = _embed_rl_counters.get(key) or []
        cutoff = now - _EMBED_RL_WINDOW_SEC
        arr = [t for t in arr if t >= cutoff]
        if len(arr) >= _EMBED_RL_MAX_REQUESTS:
            _embed_rl_counters[key] = arr
            return False
        arr.append(now)
        _embed_rl_counters[key] = arr
        return True
    except Exception:
        return True


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _is_admin(db: Session, actor_id: str | None) -> bool:
    if not actor_id:
        return False
    aid = str(actor_id).strip()
    # Allow the configured snapshot actor to bypass permission checks (server-side snapshot rendering)
    try:
        if aid and (aid == settings.snapshot_actor_id):
            return True
    except Exception:
        pass
    u = db.query(User).filter(User.id == aid).first()
    return bool(u and (u.role or "user").lower() == "admin")


def _collect_datasource_ids_from_definition(defn: dict) -> set[str]:
    ids: set[str] = set()
    def walk(node):
        if isinstance(node, dict):
            for k, v in node.items():
                if k == "datasourceId" and isinstance(v, str):
                    try:
                        ids.add(v)
                    except Exception:
                        pass
                walk(v)
        elif isinstance(node, list):
            for it in node:
                walk(it)
    walk(defn or {})
    return ids


def _rewrite_datasource_ids(defn: dict, id_map: dict[str, str], table_map: dict[str, str] | None = None) -> dict:
    """Rewrite datasourceId and optionally table/source names in dashboard definition."""
    def walk(node):
        if isinstance(node, dict):
            out = {}
            for k, v in node.items():
                if k == "datasourceId" and isinstance(v, str) and v in id_map:
                    out[k] = id_map[v]
                elif table_map and k in ("source", "table", "tableName") and isinstance(v, str) and v in table_map:
                    out[k] = table_map[v]
                else:
                    out[k] = walk(v)
            return out
        elif isinstance(node, list):
            return [walk(it) for it in node]
        else:
            return node
    return walk(defn or {})


@router.get("", response_model=list[DashboardListItem])
def list_dashboards(
    userId: str | None = Query(default=None),
    published: bool | None = Query(default=None),
    actorId: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    # If a userId is provided, return only that user's dashboards.
    # If not provided, return only dev_user sample (for empty state/demo), unless admin actor is provided, in which case list all.
    if _is_admin(db, actorId) and (userId is None or (str(userId).strip().lower() in {"", "undefined", "null"})):
        q = db.query(Dashboard)
    elif userId is None or (str(userId).strip().lower() in {"", "undefined", "null"}):
        q = db.query(Dashboard).filter(Dashboard.user_id == "dev_user")
    else:
        uid = str(userId).strip()
        q = db.query(Dashboard).filter(Dashboard.user_id == uid)
    # Published filter via existence in share_links
    if published is True:
        q = q.join(ShareLink, ShareLink.dashboard_id == Dashboard.id)
    elif published is False:
        q = q.outerjoin(ShareLink, ShareLink.dashboard_id == Dashboard.id).filter(ShareLink.id.is_(None))

    items: list[DashboardListItem] = []
    for d in q.order_by(Dashboard.created_at.desc()).all():
        try:
            defn = json.loads(d.definition_json or "{}")
        except Exception:
            defn = {}
        widgets = defn.get("widgets") or {}
        widgets_count = len(widgets)
        tables_count = 0
        datasources_set: set[str] = set()
        for cfg in widgets.values():
            try:
                if cfg.get("type") == "table":
                    tables_count += 1
                dsid = cfg.get("datasourceId")
                if dsid:
                    datasources_set.add(str(dsid))
            except AttributeError:
                continue
        # If widgets exist but there is no explicit datasource id, default to 1 (local datasource)
        ds_count = len(datasources_set) if datasources_set else (1 if widgets_count > 0 else 0)
        sl = get_share_link_by_dashboard(db, d.id)
        items.append(
            DashboardListItem(
                id=d.id,
                name=d.name,
                userId=d.user_id,
                created_at=d.created_at,
                updated_at=d.updated_at,
                published=bool(sl),
                publicId=(sl.public_id if sl else None),
                widgetsCount=widgets_count,
                tablesCount=tables_count,
                datasourceCount=ds_count,
            )
        )
    return items


@router.post("", response_model=DashboardOut)
def save_dash(payload: DashboardSaveRequest, actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    try:
        # Enforce permissions on update: owner or explicit 'rw' permission
        if payload.id:
            d0 = load_dashboard(db, payload.id)
            if not d0:
                raise ValueError("Dashboard not found")
            actor = (payload.userId or actorId or "dev_user")
            if _is_admin(db, actor):
                actor = d0.user_id or actor  # bypass checks
            if d0.user_id and d0.user_id != actor:
                perm = get_share_permission(db, d0.id, actor)
                if perm != "rw":
                    raise HTTPException(status_code=403, detail="No write permission for this dashboard")
        d: Dashboard = save_dashboard(
            db,
            user_id=payload.userId,
            name=payload.name,
            definition=payload.definition.model_dump(),
            dash_id=payload.id,
        )
        return DashboardOut(
            id=d.id,
            name=d.name,
            userId=d.user_id,
            created_at=d.created_at,
            definition=json.loads(d.definition_json or "{}"),
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{dash_id}", response_model=DashboardOut)
def get_dash(dash_id: str, actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    d = load_dashboard(db, dash_id)
    if not d:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    # Enforce read permissions: owner, admin, or shared (ro/rw)
    if not _is_admin(db, actorId):
        actor = (actorId or "").strip()
        if d.user_id and d.user_id != actor:
            perm = get_share_permission(db, d.id, actor)
            if perm not in ("ro", "rw"):
                raise HTTPException(status_code=403, detail="Forbidden")
    # Parse and sanitize definition to avoid schema validation 500s on legacy/invalid shapes
    try:
        raw = json.loads(d.definition_json or "{}")
    except Exception:
        raw = {}
    if not isinstance(raw, dict):
        raw = {}
    layout = raw.get("layout")
    if not isinstance(layout, list):
        layout = []
    widgets = raw.get("widgets")
    if not isinstance(widgets, dict):
        widgets = {}
    options = raw.get("options") if isinstance(raw.get("options"), dict) else None
    # Drop non-dict widget configs to prevent schema validation errors
    try:
        if isinstance(widgets, dict):
            widgets = {str(k): (v if isinstance(v, dict) else {}) for k, v in widgets.items()}
        else:
            widgets = {}
    except Exception:
        widgets = {}
    defn = {"layout": layout, "widgets": widgets}
    if options is not None:
        defn["options"] = options
    # Validate against schema; fallback to empty if invalid
    from ..schemas import DashboardDefinition as _DashDef
    try:
        defn_model = _DashDef.model_validate(defn)
    except Exception:
        defn_model = _DashDef()
    return DashboardOut(id=d.id, name=d.name, userId=d.user_id, created_at=d.created_at, definition=defn_model)


# --- Embed tokens management ---
class EmbedTokenRowOut(BaseModel):
    id: str
    token: str
    exp: int
    createdAt: datetime = Field(alias="created_at", serialization_alias="createdAt")
    revokedAt: datetime | None = Field(default=None, alias="revoked_at", serialization_alias="revokedAt")


@router.get("/{dash_id}/embed-tokens", response_model=list[EmbedTokenRowOut])
def list_embed_tokens(dash_id: str, actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    d = load_dashboard(db, dash_id)
    if not d:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    # owner or admin
    if not _is_admin(db, actorId):
        actor = (actorId or "").strip()
        if d.user_id and d.user_id != actor:
            raise HTTPException(status_code=403, detail="Forbidden")
    rows = list_embed_tokens_by_dashboard(db, dash_id) or []
    out = []
    for r in rows:
        out.append(EmbedTokenRowOut(id=r.id, token=r.token, exp=int(r.exp), created_at=r.created_at, revoked_at=r.revoked_at))
    return out


@router.delete("/{dash_id}/embed-tokens/{token_id}")
def delete_embed_token(dash_id: str, token_id: str, actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    d = load_dashboard(db, dash_id)
    if not d:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    # owner or admin
    if not _is_admin(db, actorId):
        actor = (actorId or "").strip()
        if d.user_id and d.user_id != actor:
            raise HTTPException(status_code=403, detail="Forbidden")
    deleted = revoke_embed_token(db, token_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Token not found")
    return {"deleted": deleted}


# --- Shares management ---
class ShareEntryOut(BaseModel):
    userId: str
    permission: str
    createdAt: datetime = Field(alias="created_at", serialization_alias="createdAt")
    userName: str | None = None
    email: str | None = None


@router.get("/{dash_id}/shares", response_model=list[ShareEntryOut])
def list_shares(dash_id: str, actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    d = load_dashboard(db, dash_id)
    if not d:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    # owner or admin
    if not _is_admin(db, actorId):
        actor = (actorId or "").strip()
        if d.user_id and d.user_id != actor:
            raise HTTPException(status_code=403, detail="Forbidden")
    perms = list_share_permissions_by_dashboard(db, dash_id)
    out: list[ShareEntryOut] = []
    for p in perms:
        u = db.query(User).filter(User.id == p.user_id).first()
        out.append(ShareEntryOut(userId=p.user_id, permission=p.permission, created_at=p.created_at, userName=(u.name if u else None), email=(u.email if u else None)))
    return out


@router.delete("/{dash_id}/shares/{user_id}")
def delete_share(dash_id: str, user_id: str, actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    d = load_dashboard(db, dash_id)
    if not d:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    # owner or admin
    if not _is_admin(db, actorId):
        actor = (actorId or "").strip()
        if d.user_id and d.user_id != actor:
            raise HTTPException(status_code=403, detail="Forbidden")
    removed = remove_share_permission(db, dash_id, user_id)
    # Also remove from all of the target user's collections (cleanup UX)
    try:
        remove_dashboard_from_all_collections_of_user(db, user_id, dash_id)
    except Exception:
        pass
    if not removed:
        raise HTTPException(status_code=404, detail="Share not found")
    return {"deleted": removed}


@router.post("/public/{public_id}/embed-token")
def create_embed_token(public_id: str, ttl: int = Query(default=3600), actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    sl = get_share_link_by_public(db, public_id)
    if not sl:
        raise HTTPException(status_code=404, detail="Not found")
    d = load_dashboard(db, sl.dashboard_id)
    if not d:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    # Require owner or admin to issue tokens
    if not (_is_admin(db, actorId) or (d.user_id and d.user_id == (actorId or "").strip())):
        raise HTTPException(status_code=403, detail="Forbidden")
    token, exp = sign_embed_token(public_id, ttl)
    # Persist the token for revocation/listing
    create_embed_token_row(db, d.id, public_id, token, exp)
    return EmbedTokenOut(token=token, exp=exp)


@router.delete("/{dash_id}")
def delete_dash(dash_id: str, userId: str | None = Query(default=None), actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    # Permission: owner or 'rw'
    d0 = load_dashboard(db, dash_id)
    if not d0:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    actor = (userId or actorId or "dev_user")
    if _is_admin(db, actor):
        actor = d0.user_id or actor  # bypass checks
    if d0.user_id and d0.user_id != actor:
        perm = get_share_permission(db, d0.id, actor)
        if perm != "rw":
            raise HTTPException(status_code=403, detail="No write permission for this dashboard")
    deleted = delete_dashboard(db, dash_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    return {"deleted": deleted}


@router.post("/{dash_id}/publish", response_model=PublishOut)
def publish_dash(dash_id: str, userId: str | None = Query(default=None), actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    d = load_dashboard(db, dash_id)
    if not d:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    actor = (userId or actorId or "dev_user")
    if _is_admin(db, actor):
        actor = d.user_id or actor
    if d.user_id and d.user_id != actor:
        perm = get_share_permission(db, d.id, actor)
        if perm != "rw":
            raise HTTPException(status_code=403, detail="No write permission for this dashboard")
    sl = publish_dashboard_link(db, dash_id)
    return PublishOut(publicId=sl.public_id, protected=bool(sl.token_hash))


@router.post("/{dash_id}/unpublish")
def unpublish_dash(dash_id: str, userId: str | None = Query(default=None), actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    d = load_dashboard(db, dash_id)
    if not d:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    actor = (userId or actorId or "dev_user")
    if _is_admin(db, actor):
        actor = d.user_id or actor
    if d.user_id and d.user_id != actor:
        perm = get_share_permission(db, d.id, actor)
        if perm != "rw":
            raise HTTPException(status_code=403, detail="No write permission for this dashboard")
    count = unpublish_dashboard_links(db, dash_id)
    return {"unpublished": count}


@router.get("/{dash_id}/publish", response_model=PublishOut)
def get_publish_status(dash_id: str, db: Session = Depends(get_db)):
    d = load_dashboard(db, dash_id)
    if not d:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    sl = get_share_link_by_dashboard(db, dash_id)
    if not sl:
        raise HTTPException(status_code=404, detail="Not published")
    return PublishOut(publicId=sl.public_id, protected=bool(sl.token_hash))


@router.post("/{dash_id}/publish/token", response_model=PublishOut)
def set_publish_token(dash_id: str, payload: SetPublishTokenRequest, userId: str | None = Query(default=None), actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    d = load_dashboard(db, dash_id)
    if not d:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    actor = (userId or actorId or "dev_user")
    if _is_admin(db, actor):
        actor = d.user_id or actor
    if d.user_id and d.user_id != actor:
        perm = get_share_permission(db, d.id, actor)
        if perm != "rw":
            raise HTTPException(status_code=403, detail="No write permission for this dashboard")
    sl = publish_dashboard_link(db, dash_id)
    try:
        set_share_link_token(db, sl, (payload.token or None), settings.secret_key)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return PublishOut(publicId=sl.public_id, protected=bool(sl.token_hash))


@router.get("/public/{public_id}", response_model=DashboardOut)
def get_public(request: Request, public_id: str, token: str | None = Query(default=None), et: str | None = Query(default=None), db: Session = Depends(get_db)):
    # Basic in-memory rate limiting per (public_id, client_ip)
    try:
        ip = (request.client.host if request and request.client else "-")
        key = f"{public_id}:{ip}"
        import time
        if not _rate_limit_ok(key, time.time()):
            raise HTTPException(status_code=429, detail="Too Many Requests")
    except HTTPException:
        raise
    except Exception:
        pass
    sl = get_share_link_by_public(db, public_id)
    if not sl:
        raise HTTPException(status_code=404, detail="Not found")
    # Authorization: allow if (a) link not protected, (b) correct share token, or (c) valid server-signed embed token
    if sl.token_hash:
        ok = False
        if token and verify_share_link_token(sl, token, settings.secret_key):
            ok = True
        elif et and verify_embed_token(et, public_id):
            # Also ensure embed token not revoked in DB
            row = (
                db.query(EmbedToken)
                .filter(EmbedToken.public_id == public_id, EmbedToken.token == et, EmbedToken.revoked_at.is_(None))
                .first()
            )
            ok = bool(row)
        if not ok:
            raise HTTPException(status_code=401, detail="Unauthorized")
    d = load_dashboard(db, sl.dashboard_id)
    if not d:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    # Sanitize and validate definition to avoid schema 500s on legacy/invalid shapes
    try:
        raw = json.loads(d.definition_json or "{}")
    except Exception:
        raw = {}
    if not isinstance(raw, dict):
        raw = {}
    layout = raw.get("layout")
    if not isinstance(layout, list):
        layout = []
    widgets = raw.get("widgets")
    if not isinstance(widgets, dict):
        widgets = {}
    options = raw.get("options") if isinstance(raw.get("options"), dict) else None
    try:
        if isinstance(widgets, dict):
            widgets = {str(k): (v if isinstance(v, dict) else {}) for k, v in widgets.items()}
        else:
            widgets = {}
    except Exception:
        widgets = {}
    defn = {"layout": layout, "widgets": widgets}
    if options is not None:
        defn["options"] = options
    from ..schemas import DashboardDefinition as _DashDef
    try:
        defn_model = _DashDef.model_validate(defn)
    except Exception:
        defn_model = _DashDef()
    return DashboardOut(id=d.id, name=d.name, userId=d.user_id, created_at=d.created_at, definition=defn_model)


# --- Export / Import ---
def _ds_to_export_item(ds: Datasource) -> DatasourceExportItem:
    conn = None
    if ds.connection_encrypted:
        try:
            conn = decrypt_text(ds.connection_encrypted)
        except Exception:
            conn = None
    try:
        opts = json.loads(ds.options_json or "{}")
    except Exception:
        opts = {}
    return DatasourceExportItem(
        id=ds.id,
        name=ds.name,
        type=ds.type,
        connectionUri=conn,
        options=opts,
        userId=ds.user_id,
        active=bool(getattr(ds, "active", True)),
        createdAt=ds.created_at,
    )


@router.get("/export", response_model=DashboardExportResponse)
def export_dashboards(userId: str | None = Query(default=None), ids: list[str] | None = Query(default=None), includeDatasources: bool = Query(default=True), includeSyncTasks: bool = Query(default=True), actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    # Permission: admin can export any; otherwise restrict to provided userId==actor or fallback to actor
    if _is_admin(db, actorId):
        q = db.query(Dashboard)
        if userId and str(userId).strip().lower() not in {"", "undefined", "null"}:
            q = q.filter(Dashboard.user_id == str(userId).strip())
    else:
        actor = (actorId or "").strip()
        if not actor:
            raise HTTPException(status_code=403, detail="Forbidden")
        q = db.query(Dashboard).filter(Dashboard.user_id == actor)
    if ids:
        q = q.filter(Dashboard.id.in_(ids))
    rows = q.order_by(Dashboard.created_at.desc()).all()
    d_items: list[DashboardExportItem] = []
    ds_ids: set[str] = set()
    for d in rows:
        try:
            defn = json.loads(d.definition_json or "{}")
        except Exception:
            defn = {}
        d_items.append(DashboardExportItem(id=d.id, name=d.name, userId=d.user_id, definition=defn, createdAt=d.created_at, updatedAt=d.updated_at))
        if includeDatasources:
            ds_ids |= _collect_datasource_ids_from_definition(defn)
    ds_items: list[DatasourceExportItem] = []
    if includeDatasources and ds_ids:
        ds_q = db.query(Datasource).filter(Datasource.id.in_(list(ds_ids)))
        # Non-admin: restrict to own
        if not _is_admin(db, actorId):
            actor = (actorId or "").strip()
            ds_q = ds_q.filter(Datasource.user_id == actor)
        for ds in ds_q.all():
            item = _ds_to_export_item(ds)
            if includeSyncTasks:
                tasks = db.query(SyncTask).filter(SyncTask.datasource_id == ds.id).order_by(SyncTask.created_at.asc()).all()
                st_items: list[dict] = []
                for t in tasks:
                    st_items.append({
                        "id": t.id,
                        "datasourceId": t.datasource_id,
                        "sourceSchema": t.source_schema,
                        "sourceTable": t.source_table,
                        "destTableName": t.dest_table_name,
                        "mode": t.mode,
                        "pkColumns": t.pk_columns,
                        "selectColumns": t.select_columns,
                        "sequenceColumn": t.sequence_column,
                        "batchSize": t.batch_size,
                        "scheduleCron": t.schedule_cron,
                        "enabled": t.enabled,
                        "groupKey": t.group_key,
                        "createdAt": t.created_at,
                    })
                item.syncTasks = st_items  # type: ignore[attr-defined]
            ds_items.append(item)
    return DashboardExportResponse(dashboards=d_items, datasources=(ds_items or None))


@router.get("/{dash_id}/export", response_model=DashboardExportResponse)
def export_dashboard(dash_id: str, includeDatasources: bool = Query(default=True), includeSyncTasks: bool = Query(default=True), actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    d = load_dashboard(db, dash_id)
    if not d:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    # owner or admin
    if not _is_admin(db, actorId):
        actor = (actorId or "").strip()
        if d.user_id and d.user_id != actor:
            raise HTTPException(status_code=403, detail="Forbidden")
    try:
        defn = json.loads(d.definition_json or "{}")
    except Exception:
        defn = {}
    d_item = DashboardExportItem(id=d.id, name=d.name, userId=d.user_id, definition=defn, createdAt=d.created_at, updatedAt=d.updated_at)
    ds_items: list[DatasourceExportItem] = []
    if includeDatasources:
        ids = _collect_datasource_ids_from_definition(defn)
        if ids:
            ds_q = db.query(Datasource).filter(Datasource.id.in_(list(ids)))
            if not _is_admin(db, actorId):
                actor = (actorId or "").strip()
                ds_q = ds_q.filter(Datasource.user_id == actor)
            for ds in ds_q.all():
                item = _ds_to_export_item(ds)
                if includeSyncTasks:
                    tasks = db.query(SyncTask).filter(SyncTask.datasource_id == ds.id).order_by(SyncTask.created_at.asc()).all()
                    st_items: list[dict] = []
                    for t in tasks:
                        st_items.append({
                            "id": t.id,
                            "datasourceId": t.datasource_id,
                            "sourceSchema": t.source_schema,
                            "sourceTable": t.source_table,
                            "destTableName": t.dest_table_name,
                            "mode": t.mode,
                            "pkColumns": t.pk_columns,
                            "selectColumns": t.select_columns,
                            "sequenceColumn": t.sequence_column,
                            "batchSize": t.batch_size,
                            "scheduleCron": t.schedule_cron,
                            "enabled": t.enabled,
                            "groupKey": t.group_key,
                            "createdAt": t.created_at,
                        })
                    item.syncTasks = st_items  # type: ignore[attr-defined]
                ds_items.append(item)
    return DashboardExportResponse(dashboards=[d_item], datasources=(ds_items or None))


@router.post("/import", response_model=DashboardImportResponse)
def import_dashboards(payload: DashboardImportRequest, actorId: str | None = Query(default=None), db: Session = Depends(get_db)):
    if not payload or not isinstance(payload.dashboards, list):
        raise HTTPException(status_code=400, detail="dashboards array is required")
    # Build datasource id map if provided
    id_map: dict[str, str] = {}
    if isinstance(payload.datasourceIdMap, dict):
        id_map.update({str(k): str(v) for k, v in payload.datasourceIdMap.items() if k and v})
    # Build table name map if provided
    table_map: dict[str, str] = {}
    if isinstance(payload.tableNameMap, dict):
        table_map.update({str(k): str(v) for k, v in payload.tableNameMap.items() if k and v})
    # Optionally import datasources first (cannot auto-build id_map without old IDs; rely on caller-provided map)
    # Force all imported dashboards to be owned by actorId (for both admin and non-admin users)
    created = 0
    out: list[DashboardOut] = []
    for it in payload.dashboards:
        # Force owner to actorId for all users
        actor = (actorId or "").strip()
        if not actor:
            raise HTTPException(status_code=403, detail="actorId is required for import")
        owner = actor
        # Rewrite datasource IDs and table names if mappings are provided
        defn = it.definition.model_dump()
        if id_map or table_map:
            defn = _rewrite_datasource_ids(defn, id_map, table_map if table_map else None)
        # Upsert by id or (name, owner)
        d: Dashboard | None = None
        if it.id:
            d = load_dashboard(db, it.id)
        if d and d.user_id and d.user_id != owner and not _is_admin(db, actorId):
            # Do not allow overriding other owners; create new instead
            d = None
        if d:
            d.name = it.name
            d.definition_json = json.dumps(defn)
            db.add(d)
            db.commit()
            db.refresh(d)
        else:
            d = save_dashboard(db, user_id=owner, name=it.name, definition=defn)
            created += 1
        out.append(DashboardOut(id=d.id, name=d.name, userId=d.user_id, created_at=d.created_at, definition=json.loads(d.definition_json or "{}")))
    return DashboardImportResponse(imported=len(out), items=out)
