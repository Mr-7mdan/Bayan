"""Holiday calendar CRUD API endpoints."""
from __future__ import annotations

import csv
import io
from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..models import HolidayRule, SessionLocal

router = APIRouter(prefix="/holidays", tags=["holidays"])


def _get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── Pydantic schemas ─────────────────────────────────────────────────────

class HolidayRuleCreate(BaseModel):
    name: str
    rule_type: str  # "specific" | "recurring"
    specific_date: Optional[str] = None
    recurrence_expr: Optional[str] = None


class HolidayRuleResponse(BaseModel):
    id: str
    name: str
    rule_type: str
    specific_date: Optional[str] = None
    recurrence_expr: Optional[str] = None

    model_config = {"from_attributes": True}


# ── Endpoints ─────────────────────────────────────────────────────────────

@router.get("", response_model=list[HolidayRuleResponse])
def list_holidays(db: Session = Depends(_get_db)):
    return db.query(HolidayRule).order_by(HolidayRule.name).all()


@router.post("", response_model=HolidayRuleResponse)
def create_holiday(body: HolidayRuleCreate, db: Session = Depends(_get_db)):
    rule = HolidayRule(id=str(uuid4()), **body.model_dump())
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return rule


@router.put("/{rule_id}", response_model=HolidayRuleResponse)
def update_holiday(rule_id: str, body: HolidayRuleCreate, db: Session = Depends(_get_db)):
    rule = db.query(HolidayRule).filter(HolidayRule.id == rule_id).first()
    if not rule:
        raise HTTPException(404, "Holiday rule not found")
    for k, v in body.model_dump().items():
        setattr(rule, k, v)
    db.commit()
    db.refresh(rule)
    return rule


@router.delete("/{rule_id}")
def delete_holiday(rule_id: str, db: Session = Depends(_get_db)):
    rule = db.query(HolidayRule).filter(HolidayRule.id == rule_id).first()
    if not rule:
        raise HTTPException(404, "Holiday rule not found")
    db.delete(rule)
    db.commit()
    return {"ok": True}


@router.post("/upload")
async def upload_holidays(file: UploadFile = File(...), db: Session = Depends(_get_db)):
    """Upload CSV with columns: name, rule_type, specific_date, recurrence_expr"""
    content = await file.read()
    text = content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    created = []
    for row in reader:
        rule = HolidayRule(
            id=str(uuid4()),
            name=row.get("name", ""),
            rule_type=row.get("rule_type", "specific"),
            specific_date=row.get("specific_date"),
            recurrence_expr=row.get("recurrence_expr"),
        )
        db.add(rule)
        created.append(rule.id)
    db.commit()
    return {"created": len(created), "ids": created}
