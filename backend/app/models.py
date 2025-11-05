from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import uuid4
import hashlib

from sqlalchemy import Boolean, DateTime, Integer, String, Text, create_engine, func, text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from sqlalchemy.pool import NullPool

from .config import settings


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    __table_args__ = (UniqueConstraint("email", name="uq_users_email"),)

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    email: Mapped[str] = mapped_column(String, nullable=False)
    password_hash: Mapped[str] = mapped_column(String, nullable=False)
    role: Mapped[str] = mapped_column(String, nullable=False, default="user")  # 'admin' | 'user'
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


# Contacts directory
class Contact(Base):
    __tablename__ = "contacts"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    email: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    tags_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    @property
    def tags(self) -> list[str]:
        try:
            return json.loads(self.tags_json or "[]")
        except Exception:
            return []

    @tags.setter
    def tags(self, value: list[str] | None) -> None:
        self.tags_json = json.dumps(value or [])

class Datasource(Base):
    __tablename__ = "datasources"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    connection_encrypted: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    options_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


# Per-user datasource shares (read-only or read-write)
class DatasourceShare(Base):
    __tablename__ = "datasource_shares"
    __table_args__ = (UniqueConstraint("datasource_id", "user_id", name="uq_ds_share"),)

    id: Mapped[str] = mapped_column(String, primary_key=True)
    datasource_id: Mapped[str] = mapped_column(String, nullable=False)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    permission: Mapped[str] = mapped_column(String, nullable=False, default="ro")  # 'ro' or 'rw'
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


# Per-user dashboard permissions (read-only or read-write)
class SharePermission(Base):
    __tablename__ = "share_permissions"
    __table_args__ = (UniqueConstraint("dashboard_id", "user_id", name="uq_perm_dash_user"),)

    id: Mapped[str] = mapped_column(String, primary_key=True)
    dashboard_id: Mapped[str] = mapped_column(String, nullable=False)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    permission: Mapped[str] = mapped_column(String, nullable=False)  # 'ro' or 'rw'
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    @property
    def options(self) -> dict:
        return json.loads(self.options_json or "{}")

    @options.setter
    def options(self, value: dict | None) -> None:
        self.options_json = json.dumps(value or {})


class Dashboard(Base):
    __tablename__ = "dashboards"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    definition_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, onupdate=func.now(), nullable=True)


class ShareLink(Base):
    __tablename__ = "share_links"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    dashboard_id: Mapped[str] = mapped_column(String, nullable=False)
    public_id: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    token_hash: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


# Persisted embed tokens for widgets embedding
class EmbedToken(Base):
    __tablename__ = "embed_tokens"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    dashboard_id: Mapped[str] = mapped_column(String, nullable=False)
    public_id: Mapped[str] = mapped_column(String, nullable=False)
    # Full signed token string as issued to clients
    token: Mapped[str] = mapped_column(Text, nullable=False)
    # Expiry (epoch seconds)
    exp: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    revoked_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


# Lightweight notifications for cross-user shares
class UserNotification(Base):
    __tablename__ = "user_notifications"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

class Collection(Base):
    __tablename__ = "collections"
    __table_args__ = (UniqueConstraint("user_id", "name", name="uq_collections_user_name"),)

    id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class CollectionItem(Base):
    __tablename__ = "collection_items"
    __table_args__ = (UniqueConstraint("collection_id", "dashboard_id", name="uq_collection_item"),)

    id: Mapped[str] = mapped_column(String, primary_key=True)
    collection_id: Mapped[str] = mapped_column(String, nullable=False)
    dashboard_id: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


# --- Engine & Session (SQLite metadata DB) ---
_DATA_DIR = Path(settings.metadata_db_path).resolve().parent
_DATA_DIR.mkdir(parents=True, exist_ok=True)

engine_meta = create_engine(
    f"sqlite+pysqlite:///{settings.metadata_db_path}",
    future=True,
    connect_args={"check_same_thread": False},
    pool_pre_ping=True,
    poolclass=NullPool,
)
SessionLocal = sessionmaker(bind=engine_meta, autoflush=False, autocommit=False)


def init_db() -> None:
    Base.metadata.create_all(bind=engine_meta)
    # Lightweight migration: ensure token_hash exists on share_links
    with engine_meta.connect() as conn:
        try:
            info = conn.execute(text("PRAGMA table_info(share_links)")).fetchall()
            cols = {row[1] for row in info}
            if "token_hash" not in cols:
                conn.execute(text("ALTER TABLE share_links ADD COLUMN token_hash TEXT"))
            # ensure notifications table exists
            conn.execute(text("CREATE TABLE IF NOT EXISTS user_notifications (id TEXT PRIMARY KEY, user_id TEXT NOT NULL, message TEXT NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)"))
            # ensure share_permissions table exists
            conn.execute(text("CREATE TABLE IF NOT EXISTS share_permissions (id TEXT PRIMARY KEY, dashboard_id TEXT NOT NULL, user_id TEXT NOT NULL, permission TEXT NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)"))
            # add unique index if not exists
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_perm_dash_user ON share_permissions(dashboard_id, user_id)"))
            # ensure datasource_shares table exists and has required columns
            conn.execute(text("CREATE TABLE IF NOT EXISTS datasource_shares (id TEXT PRIMARY KEY, datasource_id TEXT NOT NULL, user_id TEXT NOT NULL, permission TEXT NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)"))
            # backfill missing columns if table was created without 'permission'
            info_shares = conn.execute(text("PRAGMA table_info(datasource_shares)")).fetchall()
            cols_sh = {row[1] for row in info_shares}
            if "permission" not in cols_sh:
                conn.execute(text("ALTER TABLE datasource_shares ADD COLUMN permission TEXT NOT NULL DEFAULT 'ro'"))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_ds_share ON datasource_shares(datasource_id, user_id)"))
            # ensure 'active' column exists on users
            info_users = conn.execute(text("PRAGMA table_info(users)")).fetchall()
            cols_users = {row[1] for row in info_users}
            if "active" not in cols_users:
                conn.execute(text("ALTER TABLE users ADD COLUMN active INTEGER DEFAULT 1"))
            # ensure 'active' column exists on datasources
            info_ds = conn.execute(text("PRAGMA table_info(datasources)")).fetchall()
            cols_ds = {row[1] for row in info_ds}
            if "active" not in cols_ds:
                conn.execute(text("ALTER TABLE datasources ADD COLUMN active INTEGER DEFAULT 1"))
            # ensure progress columns exist on sync_states
            info_sync = conn.execute(text("PRAGMA table_info(sync_states)")).fetchall()
            cols_sync = {row[1] for row in info_sync}
            if "progress_current" not in cols_sync:
                conn.execute(text("ALTER TABLE sync_states ADD COLUMN progress_current INTEGER"))
            if "progress_total" not in cols_sync:
                conn.execute(text("ALTER TABLE sync_states ADD COLUMN progress_total INTEGER"))
            if "last_duck_path" not in cols_sync:
                conn.execute(text("ALTER TABLE sync_states ADD COLUMN last_duck_path TEXT"))
            if "cancel_requested" not in cols_sync:
                conn.execute(text("ALTER TABLE sync_states ADD COLUMN cancel_requested INTEGER DEFAULT 0"))
            if "progress_phase" not in cols_sync:
                conn.execute(text("ALTER TABLE sync_states ADD COLUMN progress_phase TEXT"))
            # ensure sync_runs table exists
            conn.execute(text(
                """
                CREATE TABLE IF NOT EXISTS sync_runs (
                  id TEXT PRIMARY KEY,
                  task_id TEXT NOT NULL,
                  datasource_id TEXT NOT NULL,
                  mode TEXT NOT NULL,
                  started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                  finished_at DATETIME NULL,
                  row_count INTEGER NULL,
                  error TEXT NULL
                )
                """
            ))
            # ensure select_columns_json exists on sync_tasks
            info_tasks = conn.execute(text("PRAGMA table_info(sync_tasks)")).fetchall()
            cols_tasks = {row[1] for row in info_tasks}
            if "select_columns_json" not in cols_tasks:
                conn.execute(text("ALTER TABLE sync_tasks ADD COLUMN select_columns_json TEXT"))
            # ensure base_template_html and logo_url exist on email_config
            info_email = conn.execute(text("PRAGMA table_info(email_config)")).fetchall()
            cols_email = {row[1] for row in info_email}
            if "base_template_html" not in cols_email:
                conn.execute(text("ALTER TABLE email_config ADD COLUMN base_template_html TEXT"))
            if "logo_url" not in cols_email:
                conn.execute(text("ALTER TABLE email_config ADD COLUMN logo_url TEXT"))
        except Exception:
            pass
        # ensure base_url exists on ai_config
        try:
            info_ai = conn.execute(text("PRAGMA table_info(ai_config)")).fetchall()
            cols_ai = {row[1] for row in info_ai}
            if "base_url" not in cols_ai:
                conn.execute(text("ALTER TABLE ai_config ADD COLUMN base_url TEXT"))
        except Exception:
            pass


# --- Helpers ---
@dataclass
class NewDatasourceInput:
    name: str
    type: str
    connection_encrypted: Optional[str]
    options: Optional[dict]
    user_id: Optional[str] = None


def create_datasource(db, payload: NewDatasourceInput) -> Datasource:
    ds = Datasource(
        id=str(uuid4()),
        user_id=payload.user_id,
        name=payload.name,
        type=payload.type,
        connection_encrypted=payload.connection_encrypted,
        options_json=json.dumps(payload.options or {}),
    )
    db.add(ds)
    db.commit()
    db.refresh(ds)
    return ds


def save_dashboard(db, user_id: Optional[str], name: str, definition: dict, dash_id: Optional[str] = None) -> Dashboard:
    if dash_id:
        d: Dashboard | None = db.get(Dashboard, dash_id)
        if not d:
            raise ValueError("Dashboard not found")
        d.name = name
        d.definition_json = json.dumps(definition)
    else:
        d = Dashboard(id=str(uuid4()), user_id=user_id, name=name, definition_json=json.dumps(definition))
        db.add(d)
    db.commit()
    db.refresh(d)
    return d


def load_dashboard(db, dash_id: str) -> Optional[Dashboard]:
    return db.get(Dashboard, dash_id)


DEFAULT_COLLECTION_NAME = "Favorites"


def ensure_collection(db, user_id: str, name: Optional[str] = None) -> Collection:
    if not user_id:
        raise ValueError("user_id is required to ensure a collection")
    coll_name = (name or DEFAULT_COLLECTION_NAME).strip() or DEFAULT_COLLECTION_NAME
    coll = (
        db.query(Collection)
        .filter(Collection.user_id == user_id, Collection.name == coll_name)
        .first()
    )
    if coll:
        return coll
    coll = Collection(id=str(uuid4()), user_id=user_id, name=coll_name)
    db.add(coll)
    db.commit()
    db.refresh(coll)
    return coll


def get_collection_by_id(db, collection_id: str, user_id: Optional[str] = None) -> Optional[Collection]:
    if not collection_id:
        return None
    query = db.query(Collection).filter(Collection.id == collection_id)
    if user_id is not None:
        query = query.filter(Collection.user_id == user_id)
    return query.first()


def add_dashboard_to_collection(db, collection: Collection, dashboard_id: str) -> tuple[CollectionItem, bool]:
    if not dashboard_id:
        raise ValueError("dashboard_id is required")
    existing = (
        db.query(CollectionItem)
        .filter(
            CollectionItem.collection_id == collection.id,
            CollectionItem.dashboard_id == dashboard_id,
        )
        .first()
    )
    if existing:
        return existing, False
    item = CollectionItem(
        id=str(uuid4()), collection_id=collection.id, dashboard_id=dashboard_id
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item, True


def count_dashboards_for_user(db, user_id: str) -> int:
    if not user_id:
        return 0
    return (
        db.query(func.count(Dashboard.id))
        .filter(Dashboard.user_id == user_id)
        .scalar()
        or 0
    )


def count_datasources_for_user(db, user_id: str) -> int:
    if not user_id:
        return 0
    return (
        db.query(func.count(Datasource.id))
        .filter(Datasource.user_id == user_id)
        .scalar()
        or 0
    )


def count_dashboards_for_user_inclusive(db, user_id: str) -> int:
    # Include 'dev_user' to support local-first demo data created prior to auth
    ids = [v for v in {user_id or 'dev_user', 'dev_user'} if v]
    return (
        db.query(func.count(Dashboard.id))
        .filter(Dashboard.user_id.in_(ids))
        .scalar()
        or 0
    )


def count_datasources_for_user_inclusive(db, user_id: str) -> int:
    ids = [v for v in {user_id or 'dev_user', 'dev_user'} if v]
    return (
        db.query(func.count(Datasource.id))
        .filter(Datasource.user_id.in_(ids))
        .scalar()
        or 0
    )


def count_collections_for_user(db, user_id: str) -> int:
    if not user_id:
        return 0
    return (
        db.query(func.count(Collection.id))
        .filter(Collection.user_id == user_id)
        .scalar()
        or 0
    )


def count_collection_items_for_user(db, user_id: str) -> int:
    if not user_id:
        return 0
    return (
        db.query(func.count(CollectionItem.id))
        .join(Collection, CollectionItem.collection_id == Collection.id)
        .filter(Collection.user_id == user_id)
        .scalar()
        or 0
    )


def count_dashboards_shared_with_user(db, user_id: str) -> int:
    if not user_id:
        return 0
    # Shared dashboards via share links (public dashboards). For now, treat all published dashboards from other users as shared.
    return (
        db.query(func.count(ShareLink.id))
        .join(Dashboard, ShareLink.dashboard_id == Dashboard.id)
        .filter(Dashboard.user_id != user_id)
        .scalar()
        or 0
    )


def count_items_in_collection(db, collection_id: str) -> int:
    if not collection_id:
        return 0
    return (
        db.query(func.count(CollectionItem.id))
        .filter(CollectionItem.collection_id == collection_id)
        .scalar()
        or 0
    )


def remove_dashboard_from_collection(db, collection_id: str, dashboard_id: str) -> int:
    if not collection_id or not dashboard_id:
        return 0
    deleted = (
        db.query(CollectionItem)
        .filter(
            CollectionItem.collection_id == collection_id,
            CollectionItem.dashboard_id == dashboard_id,
        )
        .delete()
    )
    if deleted:
        db.commit()
    return deleted or 0


# --- Share links helpers ---
def _random_public_id() -> str:
    return uuid4().hex[:10]


def get_share_link_by_dashboard(db, dash_id: str) -> Optional[ShareLink]:
    return db.query(ShareLink).filter(ShareLink.dashboard_id == dash_id).first()


def get_share_link_by_public(db, public_id: str) -> Optional[ShareLink]:
    return db.query(ShareLink).filter(ShareLink.public_id == public_id).first()


def publish_dashboard_link(db, dash_id: str) -> ShareLink:
    sl = get_share_link_by_dashboard(db, dash_id)
    if sl:
        return sl
    sl = ShareLink(id=str(uuid4()), dashboard_id=dash_id, public_id=_random_public_id())
    db.add(sl)
    db.commit()
    db.refresh(sl)
    return sl


def unpublish_dashboard_links(db, dash_id: str) -> int:
    q = db.query(ShareLink).filter(ShareLink.dashboard_id == dash_id)
    count = q.count()
    q.delete()
    db.commit()
    return count


def set_share_link_token(db, sl: ShareLink, token: Optional[str], secret_key: str) -> ShareLink:
    if token:
        new_hash = _hash_token(token, secret_key)
        # Enforce uniqueness across all share links (system-wide)
        exists = (
            db.query(ShareLink)
            .filter(ShareLink.token_hash == new_hash, ShareLink.id != sl.id)
            .first()
        )
        if exists:
            raise ValueError("Token already exists; please generate a different one")
        sl.token_hash = new_hash
    else:
        sl.token_hash = None
    db.add(sl)
    db.commit()
    db.refresh(sl)
    return sl


def verify_share_link_token(sl: ShareLink, token: Optional[str], secret_key: str) -> bool:
    if not sl.token_hash:
        return True  # not protected
    if not token:
        return False
    return sl.token_hash == _hash_token(token, secret_key)


# --- Embed tokens helpers ---
def create_embed_token_row(db, dashboard_id: str, public_id: str, token: str, exp: int) -> EmbedToken:
    et = EmbedToken(id=str(uuid4()), dashboard_id=dashboard_id, public_id=public_id, token=token, exp=int(exp))
    db.add(et)
    db.commit()
    db.refresh(et)
    return et


def list_embed_tokens_by_dashboard(db, dashboard_id: str) -> list[EmbedToken]:
    return (
        db.query(EmbedToken)
        .filter(EmbedToken.dashboard_id == dashboard_id)
        .order_by(EmbedToken.created_at.desc())
        .all()
    )


def revoke_embed_token(db, token_id: str) -> int:
    now = datetime.utcnow()
    row = db.query(EmbedToken).filter(EmbedToken.id == token_id).first()
    if not row:
        return 0
    row.revoked_at = now
    db.add(row)
    db.commit()
    return 1


def delete_dashboard(db, dash_id: str) -> int:
    """Delete a dashboard and any related share links and collection items.
    Returns number of dashboards deleted (0 or 1).
    """
    if not dash_id:
        return 0
    # Delete share links
    db.query(ShareLink).where(ShareLink.dashboard_id == dash_id).delete()
    # Delete embed tokens
    db.query(EmbedToken).where(EmbedToken.dashboard_id == dash_id).delete()
    # Delete collection items
    db.query(CollectionItem).where(CollectionItem.dashboard_id == dash_id).delete()
    # Delete dashboard
    deleted = db.query(Dashboard).where(Dashboard.id == dash_id).delete()
    if deleted:
        db.commit()
    return deleted or 0


def _hash_token(token: str, secret_key: str) -> str:
    h = hashlib.sha256()
    h.update((secret_key + ":" + token).encode("utf-8"))
    return h.hexdigest()


# --- Share listing/removal helpers ---
def list_share_permissions_by_dashboard(db, dashboard_id: str) -> list[SharePermission]:
    return (
        db.query(SharePermission)
        .filter(SharePermission.dashboard_id == dashboard_id)
        .order_by(SharePermission.created_at.desc())
        .all()
    )


def remove_share_permission(db, dashboard_id: str, user_id: str) -> int:
    deleted = (
        db.query(SharePermission)
        .filter(SharePermission.dashboard_id == dashboard_id, SharePermission.user_id == user_id)
        .delete()
    )
    if deleted:
        db.commit()
    return deleted or 0


def remove_dashboard_from_all_collections_of_user(db, user_id: str, dashboard_id: str) -> int:
    # Remove dashboard from every collection owned by the user
    if not user_id or not dashboard_id:
        return 0
    # Find collections for user
    colls = db.query(Collection).filter(Collection.user_id == user_id).all()
    if not colls:
        return 0
    coll_ids = [c.id for c in colls]
    deleted = (
        db.query(CollectionItem)
        .filter(CollectionItem.collection_id.in_(coll_ids), CollectionItem.dashboard_id == dashboard_id)
        .delete(synchronize_session=False)
    )
    if deleted:
        db.commit()
    return deleted or 0


# --- Notifications helpers ---
def add_notification(db, user_id: str, message: str) -> UserNotification:
    n = UserNotification(id=str(uuid4()), user_id=user_id, message=message)
    db.add(n)
    db.commit()
    db.refresh(n)
    return n


def pop_notifications(db, user_id: str) -> list[UserNotification]:
    items: list[UserNotification] = (
        db.query(UserNotification).filter(UserNotification.user_id == user_id).order_by(UserNotification.created_at.asc()).all()
    )
    if items:
        ids = [it.id for it in items]
        db.query(UserNotification).filter(UserNotification.id.in_(ids)).delete(synchronize_session=False)
        db.commit()
    return items


# --- Permissions helpers ---
def grant_share_permission(db, dashboard_id: str, user_id: str, permission: str) -> SharePermission:
    from sqlalchemy.exc import IntegrityError
    permission = (permission or "ro").lower()
    if permission not in {"ro", "rw"}:
        permission = "ro"
    # Upsert behavior
    existing = (
        db.query(SharePermission)
        .filter(SharePermission.dashboard_id == dashboard_id, SharePermission.user_id == user_id)
        .first()
    )
    if existing:
        existing.permission = permission
        db.add(existing)
        db.commit()
        db.refresh(existing)
        return existing
    sp = SharePermission(id=str(uuid4()), dashboard_id=dashboard_id, user_id=user_id, permission=permission)
    db.add(sp)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        # Retry as update in case of race
        return grant_share_permission(db, dashboard_id, user_id, permission)
    db.refresh(sp)
    return sp


def get_share_permission(db, dashboard_id: str, user_id: str) -> str | None:
    sp = (
        db.query(SharePermission)
        .filter(SharePermission.dashboard_id == dashboard_id, SharePermission.user_id == user_id)
        .first()
    )
    return (sp.permission if sp else None)


# --- Sync models (tasks, state, locks) ---
class SyncTask(Base):
    __tablename__ = "sync_tasks"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    datasource_id: Mapped[str] = mapped_column(String, nullable=False)
    source_schema: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    source_table: Mapped[str] = mapped_column(String, nullable=False)
    # Destination table name in DuckDB (materialized)
    dest_table_name: Mapped[str] = mapped_column(String, nullable=False)
    # 'sequence' | 'snapshot' (extendable)
    mode: Mapped[str] = mapped_column(String, nullable=False)
    # JSON-encoded array of primary key column names
    pk_columns_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # JSON-encoded array of selected columns to copy from source (optional, default: all columns)
    select_columns_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # For sequence mode
    sequence_column: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    batch_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, default=10000)
    schedule_cron: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # Tasks that materialize the same target share a group key to avoid overlap
    group_key: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    @property
    def pk_columns(self) -> list[str]:
        try:
            return json.loads(self.pk_columns_json or "[]")
        except Exception:
            return []

    @pk_columns.setter
    def pk_columns(self, value: list[str] | None) -> None:
        self.pk_columns_json = json.dumps(value or [])

    @property
    def select_columns(self) -> list[str]:
        try:
            return json.loads(self.select_columns_json or "[]")
        except Exception:
            return []

    @select_columns.setter
    def select_columns(self, value: list[str] | None) -> None:
        self.select_columns_json = json.dumps(value or [])


class SyncState(Base):
    __tablename__ = "sync_states"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    task_id: Mapped[str] = mapped_column(String, nullable=False)
    last_sequence_value: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_row_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    in_progress: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # Optional live progress for long-running jobs (e.g., snapshots)
    progress_current: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    progress_total: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    # The DuckDB file path used during the last successful run (for accurate local stats)
    last_duck_path: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    # Cancellation flag requested by user
    cancel_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # Optional phase: 'fetch' | 'insert'
    progress_phase: Mapped[Optional[str]] = mapped_column(String, nullable=True)


class SyncLock(Base):
    __tablename__ = "sync_locks"

    # Lock per group_key to avoid overlapping runs for the same destination
    group_key: Mapped[str] = mapped_column(String, primary_key=True)
    locked_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class SyncRun(Base):
    __tablename__ = "sync_runs"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    task_id: Mapped[str] = mapped_column(String, nullable=False)
    datasource_id: Mapped[str] = mapped_column(String, nullable=False)
    mode: Mapped[str] = mapped_column(String, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


# Alerts & Notifications
class AlertRule(Base):
    __tablename__ = "alert_rules"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    kind: Mapped[str] = mapped_column(String, nullable=False, default="alert")  # 'alert' | 'notification'
    widget_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    dashboard_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    config_json: Mapped[str] = mapped_column(Text, nullable=False)
    last_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, onupdate=func.now(), nullable=True)


class EmailConfig(Base):
    __tablename__ = "email_config"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    host: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    port: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    username: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    password_encrypted: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    from_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    from_email: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    use_tls: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    base_template_html: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    logo_url: Mapped[Optional[str]] = mapped_column(String, nullable=True)


class SmsConfigHadara(Base):
    __tablename__ = "sms_config_hadara"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    api_key_encrypted: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    default_sender: Mapped[Optional[str]] = mapped_column(String, nullable=True)

class AiConfig(Base):
    __tablename__ = "ai_config"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    provider: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    model: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    api_key_encrypted: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    base_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


# Per-alert run history
class AlertRun(Base):
    __tablename__ = "alert_runs"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    alert_id: Mapped[str] = mapped_column(String, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
