from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..models import (
    SessionLocal,
    ensure_collection,
    add_dashboard_to_collection,
    count_dashboards_for_user_inclusive,
    count_datasources_for_user_inclusive,
    count_dashboards_for_user,
    count_datasources_for_user,
    count_collection_items_for_user,
    count_collections_for_user,
    count_items_in_collection,
    remove_dashboard_from_collection,
    get_collection_by_id,
    add_notification,
    pop_notifications,
    grant_share_permission,
    Collection,
    CollectionItem,
    Dashboard,
    get_share_permission,
    get_share_link_by_dashboard,
    User,
)
from ..schemas import (
    SidebarCountsResponse,
    AddToCollectionRequest,
    AddToCollectionResponse,
    NotificationOut,
    CollectionItemOut,
    FavoriteOut,
    AddFavoriteRequest,
    SignupRequest,
    LoginRequest,
    ChangePasswordRequest,
    ResetPasswordRequest,
    UserOut,
    UserRowOut,
    AdminCreateUserRequest,
    SetActiveRequest,
    AdminSetPasswordRequest,
)
from uuid import uuid4
from ..security import hash_password, verify_password

router = APIRouter(prefix="/users", tags=["users"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _require_admin(db: Session, actor_id: str) -> None:
    uid = (actor_id or "").strip()
    u = db.query(User).filter(User.id == uid).first()
    if not u or (u.role or "user").lower() != "admin":
        raise HTTPException(status_code=403, detail="Admin required")


@router.get("/{user_id}/counts", response_model=SidebarCountsResponse)
def get_sidebar_counts(user_id: str, db: Session = Depends(get_db)) -> SidebarCountsResponse:
    uid = (user_id or "").strip()
    if uid.lower() in {"", "undefined", "null"}:
        uid = "dev_user"
    # Strict per-user counts to respect userId
    dashboards = count_dashboards_for_user(db, uid)
    datasources = count_datasources_for_user(db, uid)
    # Treat "Shared With Me" as the number of items the user has added into their collection(s)
    shared = count_collection_items_for_user(db, uid)
    collections = count_collections_for_user(db, uid)
    return SidebarCountsResponse(
        dashboardCount=dashboards,
        datasourceCount=datasources,
        sharedCount=shared,
        collectionCount=collections,
    )


@router.post("/{user_id}/collections", response_model=AddToCollectionResponse)
def add_dashboard_collection(
    user_id: str,
    payload: AddToCollectionRequest,
    db: Session = Depends(get_db),
) -> AddToCollectionResponse:
    if payload.userId and payload.userId != user_id:
        raise HTTPException(status_code=400, detail="Body userId mismatch with path")
    dashboard_id = (payload.dashboardId or "").strip()
    if not dashboard_id:
        raise HTTPException(status_code=400, detail="dashboardId is required")
    try:
        collection = ensure_collection(db, user_id, payload.collectionName)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    item, added = add_dashboard_to_collection(db, collection, dashboard_id)
    # Always grant/refresh the permission mapping if provided
    if payload.permission:
        try:
            grant_share_permission(db, dashboard_id, user_id, payload.permission)
        except Exception:
            pass
    # If this request represents a cross-user share, enqueue a notification
    if added and payload.sharedBy and payload.dashboardName:
        try:
            add_notification(db, user_id, f"{payload.sharedBy} has shared \"{payload.dashboardName}\" with you.")
        except Exception:
            # swallow notification errors to avoid breaking core sharing flow
            pass
    total_items = count_items_in_collection(db, collection.id)
    collections_count = count_collections_for_user(db, user_id)
    collection_items_count = count_collection_items_for_user(db, user_id)
    return AddToCollectionResponse(
        collectionId=collection.id,
        collectionName=collection.name,
        added=added,
        totalItems=total_items,
        collectionsCount=collections_count,
        collectionItemsCount=collection_items_count,
    )


@router.get("/{user_id}/notifications", response_model=list[NotificationOut])
def get_notifications(user_id: str, db: Session = Depends(get_db)):
    # Return and clear notifications (pop semantics)
    items = pop_notifications(db, user_id)
    return [NotificationOut(id=n.id, message=n.message, created_at=n.created_at) for n in items]


@router.get("/{user_id}/collections/items", response_model=list[CollectionItemOut])
def list_collection_items(user_id: str, db: Session = Depends(get_db)):
    # List all dashboards added to any of the user's collections, with permission and publish info
    colls = db.query(Collection).filter(Collection.user_id == user_id).all()
    if not colls:
        return []
    coll_ids = [c.id for c in colls]
    from ..models import User as _User
    rows = (
        db.query(CollectionItem, Dashboard, _User)
        .join(Dashboard, CollectionItem.dashboard_id == Dashboard.id)
        .outerjoin(_User, _User.id == Dashboard.user_id)
        .filter(CollectionItem.collection_id.in_(coll_ids))
        .order_by(CollectionItem.created_at.desc())
        .all()
    )
    out: list[CollectionItemOut] = []
    for ci, d, u in rows:
        sl = get_share_link_by_dashboard(db, d.id)
        perm = get_share_permission(db, d.id, user_id) or "ro"
        out.append(
            CollectionItemOut(
                collectionId=ci.collection_id,
                dashboardId=d.id,
                name=d.name,
                ownerId=d.user_id,
                ownerName=(u.name if u else None),
                permission=perm,
                added_at=ci.created_at,
                published=bool(sl),
                publicId=(sl.public_id if sl else None),
            )
        )
    return out


@router.delete("/{user_id}/collections/{collection_id}/{dashboard_id}", response_model=AddToCollectionResponse)
def remove_dashboard_collection(
    user_id: str,
    collection_id: str,
    dashboard_id: str,
    db: Session = Depends(get_db),
) -> AddToCollectionResponse:
    collection = get_collection_by_id(db, collection_id, user_id)
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found")
    removed = remove_dashboard_from_collection(db, collection_id, dashboard_id)
    if not removed:
        raise HTTPException(status_code=404, detail="Dashboard not in collection")
    total_items = count_items_in_collection(db, collection_id)
    collections_count = count_collections_for_user(db, user_id)
    collection_items_count = count_collection_items_for_user(db, user_id)
    return AddToCollectionResponse(
        collectionId=collection_id,
        collectionName=collection.name,
        added=False,
        totalItems=total_items,
        collectionsCount=collections_count,
        collectionItemsCount=collection_items_count,
    )


# --- Auth endpoints ---
@router.post("/signup", response_model=UserOut)
def signup(payload: SignupRequest, db: Session = Depends(get_db)) -> UserOut:
    email = (payload.email or "").strip().lower()
    name = (payload.name or "").strip() or email.split("@")[0]
    role = "user"
    existing = db.query(User).filter(User.email == email).first()
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    u = User(id=str(uuid4()), name=name, email=email, password_hash=hash_password(payload.password), role=role)
    db.add(u)
    db.commit()
    db.refresh(u)
    return UserOut(id=u.id, name=u.name, email=u.email, role=u.role)


@router.post("/login", response_model=UserOut)
def login(payload: LoginRequest, db: Session = Depends(get_db)) -> UserOut:
    email = (payload.email or "").strip().lower()
    u = db.query(User).filter(User.email == email).first()
    if not u or not verify_password(payload.password, u.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return UserOut(id=u.id, name=u.name, email=u.email, role=u.role)


@router.post("/change-password", response_model=dict)
def change_password(payload: ChangePasswordRequest, db: Session = Depends(get_db)):
    uid = (payload.userId or "").strip()
    u = db.get(User, uid)
    if not u:
        raise HTTPException(status_code=404, detail="User not found")
    if not verify_password(payload.oldPassword, u.password_hash):
        raise HTTPException(status_code=401, detail="Old password is incorrect")
    u.password_hash = hash_password(payload.newPassword)
    db.add(u)
    db.commit()
    return {"ok": True}


@router.post("/reset-password", response_model=dict)
def reset_password(payload: ResetPasswordRequest, db: Session = Depends(get_db)):
    email = (payload.email or "").strip().lower()
    u = db.query(User).filter(User.email == email).first()
    if not u:
        raise HTTPException(status_code=404, detail="User not found")
    u.password_hash = hash_password(payload.newPassword)
    db.add(u)
    db.commit()
    return {"ok": True}


@router.post("/bootstrap-admin", response_model=UserOut)
def bootstrap_admin(payload: SignupRequest, db: Session = Depends(get_db)) -> UserOut:
    """Create the very first admin user if none exists.
    Subsequent calls are blocked once an admin exists.
    """
    has_admin = db.query(User).filter((User.role == "admin")).first()
    if has_admin:
        raise HTTPException(status_code=403, detail="Admin already exists")
    email = (payload.email or "").strip().lower()
    name = (payload.name or "").strip() or email.split("@")[0]
    existing = db.query(User).filter(User.email == email).first()
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    u = User(id=str(uuid4()), name=name, email=email, password_hash=hash_password(payload.password), role="admin", active=True)
    db.add(u)
    db.commit()
    db.refresh(u)
    return UserOut(id=u.id, name=u.name, email=u.email, role=u.role)


# --- Admin: users management ---
@router.get("/admin/list", response_model=list[UserRowOut])
def admin_list_users(actorId: str, db: Session = Depends(get_db)):
    _require_admin(db, actorId)
    rows = db.query(User).order_by(User.created_at.desc()).all()
    out: list[UserRowOut] = []
    for u in rows:
        out.append(UserRowOut(id=u.id, name=u.name, email=u.email, role=u.role, active=bool(u.active), created_at=u.created_at))
    return out


@router.post("/admin", response_model=UserOut)
def admin_create_user(payload: AdminCreateUserRequest, actorId: str, db: Session = Depends(get_db)):
    _require_admin(db, actorId)
    email = (payload.email or "").strip().lower()
    name = (payload.name or "").strip() or email.split("@")[0]
    role = (payload.role or "user").lower()
    if role not in {"admin", "user"}:
        role = "user"
    existing = db.query(User).filter(User.email == email).first()
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    u = User(id=str(uuid4()), name=name, email=email, password_hash=hash_password(payload.password), role=role, active=True)
    db.add(u)
    db.commit()
    db.refresh(u)
    return UserOut(id=u.id, name=u.name, email=u.email, role=u.role)


@router.post("/admin/{target_id}/set-active", response_model=dict)
def admin_set_active(target_id: str, payload: SetActiveRequest, actorId: str, db: Session = Depends(get_db)):
    _require_admin(db, actorId)
    u = db.get(User, target_id)
    if not u:
        raise HTTPException(status_code=404, detail="User not found")
    u.active = bool(payload.active)
    db.add(u)
    db.commit()
    return {"ok": True}


@router.post("/admin/{target_id}/set-password", response_model=dict)
def admin_set_password(target_id: str, payload: AdminSetPasswordRequest, actorId: str, db: Session = Depends(get_db)):
    _require_admin(db, actorId)
    u = db.get(User, target_id)
    if not u:
        raise HTTPException(status_code=404, detail="User not found")
    u.password_hash = hash_password(payload.newPassword)
    db.add(u)
    db.commit()
    return {"ok": True}


# --- Favorites (stored as the default "Favorites" collection) ---
@router.get("/{user_id}/favorites", response_model=list[FavoriteOut])
def list_favorites(user_id: str, db: Session = Depends(get_db)):
    # Ensure the default Favorites collection exists
    coll = ensure_collection(db, user_id)
    # Join items with dashboards for names and timestamps
    rows = (
        db.query(CollectionItem, Dashboard)
        .join(Dashboard, CollectionItem.dashboard_id == Dashboard.id)
        .filter(CollectionItem.collection_id == coll.id)
        .order_by(CollectionItem.created_at.desc())
        .all()
    )
    out: list[FavoriteOut] = []
    for ci, d in rows:
        out.append(
            FavoriteOut(
                userId=user_id,
                dashboardId=d.id,
                name=d.name,
                updatedAt=d.updated_at or d.created_at,
            )
        )
    return out


@router.post("/{user_id}/favorites", response_model=dict)
def add_favorite(user_id: str, payload: AddFavoriteRequest, db: Session = Depends(get_db)):
    dash_id = (payload.dashboardId or "").strip()
    if not dash_id:
        raise HTTPException(status_code=400, detail="dashboardId is required")
    coll = ensure_collection(db, user_id)
    _item, _added = add_dashboard_to_collection(db, coll, dash_id)
    return {"ok": True}


@router.delete("/{user_id}/favorites/{dashboard_id}", response_model=dict)
def remove_favorite(user_id: str, dashboard_id: str, db: Session = Depends(get_db)):
    coll = ensure_collection(db, user_id)
    removed = remove_dashboard_from_collection(db, coll.id, dashboard_id)
    return {"ok": bool(removed)}
