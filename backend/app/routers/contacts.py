from __future__ import annotations

from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query
from datetime import datetime, timedelta
from apscheduler.triggers.date import DateTrigger
from sqlalchemy.orm import Session
import re

from ..models import SessionLocal, Contact
from ..schemas import (
    ContactIn,
    ContactOut,
    ContactsListResponse,
    ImportContactsRequest,
    ImportContactsResponse,
    BulkEmailPayload,
    BulkSmsPayload,
)
from ..alerts_service import send_email, send_sms_hadara
from ..scheduler import ensure_scheduler_started

router = APIRouter(prefix="/contacts", tags=["contacts"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _to_out(m: Contact) -> ContactOut:
    return ContactOut(
        id=m.id,
        name=m.name,
        email=m.email,
        phone=m.phone,
        tags=m.tags,
        active=bool(getattr(m, "active", True)),
        created_at=getattr(m, "created_at", None),
    )


@router.get("", response_model=ContactsListResponse)
async def list_contacts(
    search: Optional[str] = Query(default=None),
    tags: Optional[str] = Query(default=None, description="Comma-separated"),
    active: Optional[bool] = Query(default=None),
    page: int = Query(default=1),
    pageSize: int = Query(default=20),
    db: Session = Depends(get_db),
) -> ContactsListResponse:
    page = max(1, int(page or 1))
    pageSize = max(1, min(200, int(pageSize or 20)))
    q = db.query(Contact)
    if active is not None:
        q = q.filter(Contact.active == bool(active))
    if search:
        s = f"%{search.strip()}%"
        q = q.filter(
            (Contact.name.ilike(s))
            | (Contact.email.ilike(s))
            | (Contact.phone.ilike(s))
            | ((Contact.tags_json != None) & (Contact.tags_json.ilike(s)))  # noqa: E711
        )
    if tags:
        for tag in [t.strip() for t in tags.split(',') if t.strip()]:
            q = q.filter((Contact.tags_json != None) & (Contact.tags_json.ilike(f"%\"{tag}\"%")))  # noqa: E711
    total = q.count()
    items = q.order_by(Contact.created_at.desc()).offset((page-1)*pageSize).limit(pageSize).all()
    return ContactsListResponse(items=[_to_out(it) for it in items], total=total, page=page, pageSize=pageSize)


@router.post("", response_model=ContactOut)
async def create_contact(payload: ContactIn, db: Session = Depends(get_db)) -> ContactOut:
    if not (payload.name or '').strip():
        raise HTTPException(status_code=400, detail="name is required")
    c = Contact(
        id=str(uuid4()),
        user_id=payload.userId,
        name=payload.name.strip(),
        email=(payload.email or None),
        phone=(payload.phone or None),
        active=True,
    )
    try:
        c.tags = payload.tags or []
    except Exception:
        c.tags = []
    db.add(c); db.commit(); db.refresh(c)
    return _to_out(c)


@router.put("/{contact_id}", response_model=ContactOut)
async def update_contact(contact_id: str, payload: ContactIn, db: Session = Depends(get_db)) -> ContactOut:
    c = db.get(Contact, contact_id)
    if not c:
        raise HTTPException(status_code=404, detail="Not found")
    if payload.name is not None:
        c.name = payload.name
    if payload.email is not None:
        c.email = payload.email
    if payload.phone is not None:
        c.phone = payload.phone
    if payload.tags is not None:
        c.tags = payload.tags
    db.add(c); db.commit(); db.refresh(c)
    return _to_out(c)


@router.post("/{contact_id}/deactivate")
async def deactivate_contact(contact_id: str, active: bool = Query(default=False), db: Session = Depends(get_db)) -> dict:
    c = db.get(Contact, contact_id)
    if not c:
        raise HTTPException(status_code=404, detail="Not found")
    c.active = bool(active)
    db.add(c); db.commit()
    return {"ok": True, "active": c.active}


@router.delete("/{contact_id}")
async def delete_contact(contact_id: str, db: Session = Depends(get_db)) -> dict:
    c = db.get(Contact, contact_id)
    if not c:
        return {"deleted": 0}
    db.delete(c); db.commit()
    return {"deleted": 1}


@router.post("/import", response_model=ImportContactsResponse)
async def import_contacts(payload: ImportContactsRequest, db: Session = Depends(get_db)) -> ImportContactsResponse:
    count = 0
    for it in (payload.items or []):
        try:
            c = Contact(id=str(uuid4()), user_id=it.userId, name=it.name, email=it.email, phone=it.phone, active=True)
            try: c.tags = it.tags or []
            except Exception: c.tags = []
            db.add(c); count += 1
        except Exception:
            continue
    db.commit()
    return ImportContactsResponse(imported=count, total=len(payload.items or []))


@router.get("/export")
async def export_contacts(ids: Optional[str] = Query(default=None), db: Session = Depends(get_db)) -> dict:
    q = db.query(Contact)
    if ids:
        id_list = [i.strip() for i in ids.split(',') if i.strip()]
        q = q.filter(Contact.id.in_(id_list))
    items = q.order_by(Contact.created_at.desc()).all()
    return {"items": [(_to_out(it).model_dump()) for it in items]}


@router.post("/send-email")
async def send_bulk_email(payload: BulkEmailPayload, db: Session = Depends(get_db)) -> dict:
    # Collect candidates from ids, direct emails, and tags
    candidates: list[str] = []
    if payload.ids:
        for cid in payload.ids:
            c = db.get(Contact, cid)
            if c and c.active and (c.email or '').strip():
                candidates.append(c.email.strip())
    for e in (payload.emails or []):
        s = (e or '').strip()
        if s:
            candidates.append(s)
    for tg in (payload.tags or []):
        t = (tg or '').strip()
        if not t:
            continue
        q = db.query(Contact).filter(Contact.active == True, (Contact.tags_json != None) & (Contact.tags_json.ilike(f'%"{t}"%')))  # noqa: E711
        for c in q.all():
            s = (c.email or '').strip()
            if s:
                candidates.append(s)
    # Deduplicate AFTER tag expansion using case-insensitive key
    dedup: dict[str, str] = {}
    for s in candidates:
        key = s.lower()
        if key not in dedup:
            dedup[key] = s
    to_list = sorted(list(dedup.values()))
    if not to_list:
        raise HTTPException(status_code=400, detail="No recipients")
    if not (payload.subject or '').strip():
        raise HTTPException(status_code=400, detail="subject is required")
    if not (payload.html or '').strip():
        raise HTTPException(status_code=400, detail="html is required")
    # Validate emails (basic)
    email_re = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
    bad = [e for e in to_list if not email_re.match(e)]
    if bad:
        raise HTTPException(status_code=400, detail=f"Invalid email(s): {', '.join(bad[:3])}{'…' if len(bad)>3 else ''}")
    # If queueing or rate limit specified, schedule chunked jobs per minute
    rl = None
    try:
        rl = int(payload.rateLimitPerMinute) if payload.rateLimitPerMinute is not None else None
        if rl is not None and rl <= 0:
            rl = None
    except Exception:
        rl = None
    if payload.queue or (rl is not None):
        sched = ensure_scheduler_started()
        chunk_size = rl if (rl is not None) else len(to_list)
        if chunk_size <= 0:
            chunk_size = len(to_list)
        chunks = [to_list[i:i+chunk_size] for i in range(0, len(to_list), chunk_size)]
        for idx, chunk in enumerate(chunks):
            run_at = datetime.utcnow() + timedelta(seconds=(60 * idx if rl else 0))
            try:
                # schedule a one-off job to send this chunk
                sched.add_job(
                    func=_send_email_chunk_job,
                    trigger=DateTrigger(run_date=run_at),
                    kwargs={"subject": payload.subject, "html": payload.html, "to": chunk},
                    id=f"contacts-email:{uuid4().hex}",
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=60,
                )
            except Exception:
                # continue scheduling remaining chunks
                continue
        return {"ok": True, "count": len(to_list), "queued": True}
    # Immediate single-shot send
    ok, err = send_email(db, subject=payload.subject, to=to_list, html=payload.html)
    if not ok:
        raise HTTPException(status_code=500, detail=err or "Failed to send email")
    return {"ok": True, "count": len(to_list)}


@router.post("/send-sms")
async def send_bulk_sms(payload: BulkSmsPayload, db: Session = Depends(get_db)) -> dict:
    # Collect candidates from ids, direct numbers, and tags
    candidates: list[str] = []
    if payload.ids:
        for cid in payload.ids:
            c = db.get(Contact, cid)
            if c and c.active and (c.phone or '').strip():
                candidates.append(c.phone.strip())
    for p in (payload.numbers or []):
        s = (p or '').strip()
        if s:
            candidates.append(s)
    for tg in (payload.tags or []):
        t = (tg or '').strip()
        if not t:
            continue
        q = db.query(Contact).filter(Contact.active == True, (Contact.tags_json != None) & (Contact.tags_json.ilike(f'%"{t}"%')))  # noqa: E711
        for c in q.all():
            s = (c.phone or '').strip()
            if s:
                candidates.append(s)
    # Deduplicate AFTER tag expansion using formatting-insensitive key
    def _norm_phone(x: str) -> str:
        s = (x or '').strip()
        if not s:
            return ''
        if s.startswith('+'):
            return '+' + re.sub(r'[\s\-.()]+', '', s[1:])
        return re.sub(r'[\s\-.()]+', '', s)
    dedup: dict[str, str] = {}
    for s in candidates:
        key = _norm_phone(s)
        if key and key not in dedup:
            dedup[key] = s
    to_list = sorted(list(dedup.values()))
    if not to_list:
        raise HTTPException(status_code=400, detail="No recipients")
    if not (payload.message or '').strip():
        raise HTTPException(status_code=400, detail="message is required")
    # Validate phone numbers (very permissive)
    phone_re = re.compile(r"^[+]?[0-9\-().\s]{6,}$")
    bad = [n for n in to_list if not phone_re.match(n)]
    if bad:
        raise HTTPException(status_code=400, detail=f"Invalid phone number(s): {', '.join(bad[:3])}{'…' if len(bad)>3 else ''}")
    rl = None
    try:
        rl = int(payload.rateLimitPerMinute) if payload.rateLimitPerMinute is not None else None
        if rl is not None and rl <= 0:
            rl = None
    except Exception:
        rl = None
    if payload.queue or (rl is not None):
        sched = ensure_scheduler_started()
        chunk_size = rl if (rl is not None) else len(to_list)
        if chunk_size <= 0:
            chunk_size = len(to_list)
        chunks = [to_list[i:i+chunk_size] for i in range(0, len(to_list), chunk_size)]
        for idx, chunk in enumerate(chunks):
            run_at = datetime.utcnow() + timedelta(seconds=(60 * idx if rl else 0))
            try:
                sched.add_job(
                    func=_send_sms_chunk_job,
                    trigger=DateTrigger(run_date=run_at),
                    kwargs={"numbers": chunk, "message": payload.message},
                    id=f"contacts-sms:{uuid4().hex}",
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=60,
                )
            except Exception:
                continue
        return {"ok": True, "count": len(to_list), "queued": True}
    ok, err = send_sms_hadara(db, to_numbers=to_list, message=payload.message)
    if not ok:
        raise HTTPException(status_code=500, detail=err or "Failed to send SMS")
    return {"ok": True, "count": len(to_list)}


def _send_email_chunk_job(*, subject: str, html: str, to: list[str]) -> None:
    db = SessionLocal()
    try:
        send_email(db, subject=subject, to=to, html=html)
    finally:
        try:
            db.close()
        except Exception:
            pass


def _send_sms_chunk_job(*, numbers: list[str], message: str) -> None:
    db = SessionLocal()
    try:
        send_sms_hadara(db, to_numbers=numbers, message=message)
    finally:
        try:
            db.close()
        except Exception:
            pass
