from __future__ import annotations

import json
from typing import Any, Optional
from uuid import uuid4
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from decimal import Decimal
import base64
import re as _re
import html as _html
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..models import SessionLocal, AlertRule, EmailConfig, SmsConfigHadara, AlertRun, Dashboard
from ..security import encrypt_text, decrypt_text
from ..alerts_service import run_rule, send_email, send_sms_hadara, evaluate_threshold, _render_kpi_html  # type: ignore
from ..alerts_service import _render_table_html, _apply_base_template, _apply_xpick_to_where  # type: ignore
from ..alerts_service import _build_kpi_svg, _build_chart_svg_placeholder, _to_svg_data_uri, _fmt_num  # type: ignore
from ..routers.query import run_query_spec, run_pivot, period_totals  # reuse query execution
from ..config import settings
from ..schemas import QuerySpecRequest, PivotRequest
from .snapshot import snapshot_embed_png  # headless widget snapshot

router = APIRouter(prefix="/alerts", tags=["alerts"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- Schemas ---
class AlertConfig(BaseModel):
    datasourceId: Optional[str] = None
    triggers: list[dict] = Field(default_factory=list)
    actions: list[dict] = Field(default_factory=list)
    render: Optional[dict] = None
    template: Optional[str] = None
    triggersGroup: Optional[dict] = None


class AlertCreate(BaseModel):
    name: str
    kind: str = Field(default="alert")  # 'alert' | 'notification'
    widgetId: Optional[str] = None
    dashboardId: Optional[str] = None
    enabled: bool = True
    config: AlertConfig


class AlertOut(BaseModel):
    id: str
    name: str
    kind: str
    widgetId: Optional[str] = None
    dashboardId: Optional[str] = None
    enabled: bool
    config: AlertConfig
    lastRunAt: Optional[datetime] = None
    lastStatus: Optional[str] = None

    @staticmethod
    def from_model(m: AlertRule) -> "AlertOut":
        try:
            cfg = json.loads(m.config_json or "{}")
        except Exception:
            cfg = {}
        return AlertOut(
            id=m.id,
            name=m.name,
            kind=m.kind,
            widgetId=m.widget_id,
            dashboardId=m.dashboard_id,
            enabled=m.enabled,
            config=AlertConfig.model_validate(cfg or {}),
            lastRunAt=m.last_run_at,
            lastStatus=m.last_status,
        )

class AlertRunOut(BaseModel):
    id: str
    alertId: str
    startedAt: datetime
    finishedAt: Optional[datetime] = None
    status: Optional[str] = None
    message: Optional[str] = None

    @staticmethod
    def from_model(m: AlertRun) -> "AlertRunOut":
        return AlertRunOut(
            id=m.id,
            alertId=m.alert_id,
            startedAt=m.started_at,
            finishedAt=m.finished_at,
            status=m.status,
            message=m.message,
        )


class EmailConfigPayload(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = Field(default=587)
    username: Optional[str] = None
    password: Optional[str] = None  # plain; will be encrypted
    fromName: Optional[str] = None
    fromEmail: Optional[str] = None
    useTls: bool = True
    baseTemplateHtml: Optional[str] = None
    logoUrl: Optional[str] = None


class SmsConfigPayload(BaseModel):
    apiKey: Optional[str] = None
    defaultSender: Optional[str] = None


class TestEmailPayload(BaseModel):
    to: list[str]
    subject: str = "Test Email"
    html: str = "<div>Test</div>"


class TestSmsPayload(BaseModel):
    to: list[str]
    message: str


# --- CRUD ---
@router.get("")
async def list_alerts(db: Session = Depends(get_db)) -> list[AlertOut]:
    items = db.query(AlertRule).order_by(AlertRule.created_at.desc()).all()
    return [AlertOut.from_model(it) for it in items]


@router.post("")
async def create_alert(payload: AlertCreate, db: Session = Depends(get_db)) -> AlertOut:
    a = AlertRule(
        id=str(uuid4()),
        name=payload.name,
        kind=payload.kind,
        widget_id=payload.widgetId,
        dashboard_id=payload.dashboardId,
        enabled=bool(payload.enabled),
        config_json=json.dumps(payload.config.model_dump() or {}),
    )
    db.add(a)
    db.commit()
    db.refresh(a)
    return AlertOut.from_model(a)


@router.get("/{alert_id}")
async def get_alert(alert_id: str, db: Session = Depends(get_db)) -> AlertOut:
    a = db.get(AlertRule, alert_id)
    if not a:
        raise HTTPException(status_code=404, detail="Not found")
    return AlertOut.from_model(a)


@router.put("/{alert_id}")
async def update_alert(alert_id: str, payload: AlertCreate, db: Session = Depends(get_db)) -> AlertOut:
    a = db.get(AlertRule, alert_id)
    if not a:
        raise HTTPException(status_code=404, detail="Not found")
    a.name = payload.name
    a.kind = payload.kind
    a.widget_id = payload.widgetId
    a.dashboard_id = payload.dashboardId
    a.enabled = bool(payload.enabled)
    a.config_json = json.dumps(payload.config.model_dump() or {})
    db.add(a)
    db.commit()
    db.refresh(a)
    return AlertOut.from_model(a)


@router.delete("/{alert_id}")
async def delete_alert(alert_id: str, db: Session = Depends(get_db)) -> dict:
    a = db.get(AlertRule, alert_id)
    if not a:
        return {"deleted": 0}
    db.delete(a)
    db.commit()
    return {"deleted": 1}


@router.post("/{alert_id}/run")
async def run_alert_now(alert_id: str, db: Session = Depends(get_db)) -> dict:
    a = db.get(AlertRule, alert_id)
    if not a:
        raise HTTPException(status_code=404, detail="Not found")
    # Log start
    run_id = str(uuid4())
    ar = AlertRun(id=run_id, alert_id=a.id)
    db.add(ar); db.commit()
    try:
        ok, msg = run_rule(db, a, force_time_ok=True)
        a.last_run_at = datetime.utcnow()
        a.last_status = (msg or ("ok" if ok else "failed"))
        # Update run row
        ar.finished_at = datetime.utcnow()
        ar.status = ("ok" if ok else "failed")
        ar.message = msg or ("ok" if ok else "failed")
        db.add(a); db.add(ar); db.commit()
        return {"ok": ok, "message": msg or ("ok" if ok else "failed")}
    except Exception as e:
        ar.finished_at = datetime.utcnow()
        ar.status = "failed"
        ar.message = str(e)
        db.add(ar); db.commit()
        raise

@router.get("/{alert_id}/runs")
async def list_alert_runs(alert_id: str, limit: int = 50, db: Session = Depends(get_db)) -> list[AlertRunOut]:
    limit = max(1, min(int(limit or 50), 200))
    rows = (
        db.query(AlertRun)
        .filter(AlertRun.alert_id == alert_id)
        .order_by(AlertRun.started_at.desc())
        .limit(limit)
        .all()
    )
    return [AlertRunOut.from_model(r) for r in rows]


class EvaluatePayload(BaseModel):
    name: str = Field(default="Notification")
    dashboardId: Optional[str] = None
    config: AlertConfig


class EvaluateResponse(BaseModel):
    html: str
    kpi: Optional[float] = None


class EvaluateV2Response(BaseModel):
    emailHtml: str
    smsText: Optional[str] = None
    kpi: Optional[float] = None
    context: dict[str, Any] = Field(default_factory=dict)
    humanSummary: Optional[str] = None


@router.post("/evaluate")
async def evaluate_alert(payload: EvaluatePayload, actorId: str | None = Query(default=None), db: Session = Depends(get_db)) -> EvaluateResponse:
    cfg = payload.config.model_dump() if hasattr(payload.config, 'model_dump') else (payload.config or {})
    triggers = cfg.get("triggers") or []
    render = cfg.get("render") or {}
    render_mode_is_kpi = False
    kpi_label = "KPI"
    ds_id = cfg.get("datasourceId")

    # Compute KPI from first threshold trigger if present
    kpi_value: Optional[float] = None
    t0: Optional[dict[str, Any]] = None
    for t in triggers:
        if str(t.get("type") or "").lower() == "threshold":
            t0 = t
            try:
                _ok, k = evaluate_threshold(db, trigger=t, datasource_id=ds_id)
                kpi_value = k
            except Exception:
                kpi_value = None
            break

    # Ensure KPI has a value before building content: compute fallback if evaluate_threshold didn't provide one
    if kpi_value is None:
        try:
            src = (t0.get('source') if t0 else None) or cfg.get('source') or ''
            if src:
                where_fb = dict(((t0.get('where') if t0 else None) or {}))
                x_field_fb = (t0.get('xField') if t0 else None)
                x_mode_fb = (t0.get('xMode') if t0 else None)
                x_val_fb = (t0.get('xValue') if t0 else None)
                if x_field_fb:
                    xf_raw = str(x_field_fb)
                    import re as _re
                    m_dp = _re.match(r"^(.*)\s*\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$", xf_raw, flags=_re.IGNORECASE)
                    x_base = (m_dp.group(1).strip() if m_dp else xf_raw)
                    if x_mode_fb == 'custom' and x_val_fb not in (None, ''):
                        where_fb[x_base] = [x_val_fb]
                    elif x_mode_fb == 'range':
                        xr = ((t0.get('xRange') if t0 else None) or {})
                        if xr.get('from'): where_fb[f"{x_base}__gte"] = xr.get('from')
                        if xr.get('to'): where_fb[f"{x_base}__lte"] = xr.get('to')
                    elif x_mode_fb == 'token':
                        from datetime import date, timedelta
                        today = date.today()
                        tok = str(((t0.get('xToken') if t0 else None) or ''))
                        def _iso(d: date) -> str: return d.isoformat()
                        if tok == 'today':
                            s = today; e = today + timedelta(days=1)
                            where_fb[f"{x_base}__gte"] = _iso(s)
                            where_fb[f"{x_base}__lt"] = _iso(e)
                        elif tok == 'yesterday':
                            s = today - timedelta(days=1); e = today
                            where_fb[f"{x_base}__gte"] = _iso(s)
                            where_fb[f"{x_base}__lt"] = _iso(e)
                        elif tok == 'this_month':
                            s = today.replace(day=1)
                            nm = (today.month + 1) if today.month < 12 else 1
                            ny = today.year + 1 if nm == 1 else today.year
                            from datetime import date as _date
                            e = _date(ny, nm, 1)
                            where_fb[f"{x_base}__gte"] = _iso(s)
                            where_fb[f"{x_base}__lt"] = _iso(e)
                spec_fb: dict = { 'source': src, 'agg': ((t0.get('aggregator') if t0 else None) or 'count'), 'where': where_fb or None }
                measure_fb = (t0.get('measure') if t0 else None) or (t0.get('y') if t0 else None)
                if (spec_fb['agg'] != 'count') and measure_fb:
                    spec_fb['y'] = measure_fb; spec_fb['measure'] = measure_fb
                req_fb = QuerySpecRequest(spec=spec_fb, datasourceId=ds_id, limit=10000, offset=0, includeTotal=False)
                res_fb = run_query_spec(req_fb, db)
                v_fb = 0.0
                rows_fb = res_fb.rows or []
                if rows_fb:
                    if isinstance(rows_fb[0], list):
                        # Prefer 'value' alias if present in columns
                        try:
                            cols_fb = [str(c).lower() for c in (res_fb.columns or [])]
                            vi = cols_fb.index('value') if 'value' in cols_fb else -1
                        except Exception:
                            vi = -1
                        if vi >= 0:
                            for r in rows_fb:
                                try:
                                    cell = r[vi]
                                    if isinstance(cell, (int, float)): v_fb += float(cell)
                                except Exception:
                                    pass
                        else:
                            for r in rows_fb:
                                for cell in r:
                                    if isinstance(cell, (int, float)):
                                        v_fb += float(cell)
                    elif isinstance(rows_fb[0], dict):
                        for r in rows_fb:
                            # Prefer 'value' key if present
                            if 'value' in r and isinstance(r['value'], (int, float)):
                                v_fb += float(r['value'])
                            else:
                                for _, cell in r.items():
                                    if isinstance(cell, (int, float)):
                                        v_fb += float(cell)
                kpi_value = float(v_fb)
        except Exception:
            pass

    parts: list[str] = [f"<div style='font-family:Inter,Arial,sans-serif;font-size:13px'>Rule: {payload.name}</div>"]
    mode = str(render.get("mode") or "kpi")
    if mode == "table":
        try:
            spec = render.get("querySpec") or {}
            req = QuerySpecRequest(spec=spec, datasourceId=ds_id, limit=spec.get("limit") or 1000, offset=0, includeTotal=False)
            res = run_query_spec(req, db)
            parts.append(_render_table_html(res))
        except Exception:
            parts.append("<div>Failed to render table.</div>")
    elif mode == "kpi":
        kpi_label = render.get("label") or "KPI"
        render_mode_is_kpi = True
        # Do not auto-append a KPI card; only placeholders should render if used in the template
    else:
        parts.append("<div>Chart rendering not yet available.</div>")

    # Optional template (append) with placeholders
    raw_tpl = cfg.get("template")
    if raw_tpl is not None:
        try:
            # Build context tokens for replacement
            thr_raw = None
            thr_low = None
            thr_high = None
            op = None
            agg = None
            measure = None
            x_field = None
            x_val = None
            if t0:
                op = t0.get("operator")
                val = t0.get("value")
                if isinstance(val, list) and len(val) >= 2:
                    thr_low, thr_high = val[0], val[1]
                    thr_raw = thr_low
                else:
                    thr_raw = val
                agg = (t0.get("aggregator") or "").lower()
                measure = t0.get("measure") or t0.get("y")
                x_field = t0.get("xField")
                x_val = t0.get("xValue")
            filters_obj = (t0.get("where") if t0 else None) or {}
            filters_h = "; ".join([f"{k}={('|'.join(map(str,v)) if isinstance(v,list) else v)}" for k,v in filters_obj.items()])
            ctx: dict[str, str] = {
                "kpi": "" if kpi_value is None else str(kpi_value),
                "operator": "" if op is None else str(op),
                "threshold": "" if thr_raw is None else str(thr_raw),
                "threshold_low": "" if thr_low is None else str(thr_low),
                "threshold_high": "" if thr_high is None else str(thr_high),
                "agg": "" if agg is None else str(agg),
                "measure": "" if measure is None else str(measure),
                "xField": "" if x_field is None else str(x_field),
                "xValue": "" if x_val is None else str(x_val),
                "filters": filters_h,
                "filters_json": json.dumps(filters_obj or {}),
                "source": str(cfg.get("source") or (t0.get("source") if t0 else "") or ""),
                "datasourceId": str(ds_id or ""),
            }
            out = str(raw_tpl)
            for k, v in ctx.items():
                try:
                    out = out.replace("{{" + k + "}}", v)
                except Exception:
                    pass
            parts.append(f"<div>{out}</div>")
        except Exception:
            pass
    html = "\n".join(parts)
    return EvaluateResponse(html=html, kpi=kpi_value)


@router.post("/evaluate-v2")
async def evaluate_alert_v2(payload: EvaluatePayload, actorId: str | None = Query(default=None), db: Session = Depends(get_db)) -> EvaluateV2Response:
    cfg = payload.config.model_dump() if hasattr(payload.config, 'model_dump') else (payload.config or {})
    triggers = cfg.get("triggers") or []
    render = cfg.get("render") or {}
    ds_id = cfg.get("datasourceId")

    # Threshold trigger context
    kpi_value: Optional[float] = None
    t0: Optional[dict[str, Any]] = None
    # Prefer triggersGroup.threshold for preview (it carries calcMode)
    try:
        tg0 = cfg.get('triggersGroup') if isinstance(cfg.get('triggersGroup'), dict) else None
        thr_g = (tg0 or {}).get('threshold') if isinstance((tg0 or {}).get('threshold'), dict) else None
    except Exception:
        tg0 = None
        thr_g = None
    for t in triggers:
        if str(t.get("type") or "").lower() == "threshold":
            t0 = t
            break
    thr_base = thr_g if isinstance(thr_g, dict) else t0
    calc_mode = 'query'
    dbg_kpi_from = ""
    dbg_kpi_where_s = None
    if thr_base:
        calc_mode = str((thr_base.get('calcMode') or 'query')).lower()
        if calc_mode == 'pivot':
            try:
                source = str((thr_base.get('source') or cfg.get('source') or '')).strip()
                agg = str(thr_base.get('aggregator') or 'count').lower()
                measure = thr_base.get('measure') or thr_base.get('y')
                where0 = thr_base.get('where') or None
                x_field = thr_base.get('xField') or None
                # Apply x pick to where
                where2, x_value2 = _apply_xpick_to_where(where0, x_field=x_field, trigger=thr_base, datasource_id=ds_id, source=source, db=db)
                if x_field is not None and (x_value2 is not None) and (str(x_value2).strip() not in ('*','All')):
                    w = dict(where2 or {})
                    w[str(x_field)] = x_value2
                    where2 = w
                # As-of semantics for 'today' in KPI pivot calc: only set upper bound; period_totals will set start=1900-01-01
                try:
                    xm_ = str((thr_base.get('xMode') or thr_base.get('xPick') or '')).lower()
                    tok_ = str((thr_base.get('xToken') or '') or (thr_base.get('xPick') or '')).lower()
                except Exception:
                    xm_, tok_ = '', ''
                # Treat xPick token strings as token mode
                try:
                    if (xm_ not in ('token', 'range', 'custom', 'special')) and (tok_ in ('today','yesterday','this_month')):
                        xm_ = 'token'
                except Exception:
                    pass
                if x_field and (xm_ == 'token') and (tok_ == 'today'):
                    try:
                        from datetime import date as _date, timedelta as _td
                        today = _date.today(); end_excl = (today + _td(days=1)).isoformat()
                        w2 = dict(where2 or {})
                        try:
                            w2.pop(f"{x_field}__gte", None); w2.pop(f"{x_field}__gt", None)
                        except Exception:
                            pass
                        w2[f"{x_field}__lt"] = end_excl
                        where2 = w2
                    except Exception:
                        pass
                v_sum = 0.0
                if x_field:
                    # Use period_totals(total) for KPI when an X field is defined, to respect the date window precisely
                    try:
                        payload_tot_kpi = {
                            'source': source,
                            'datasourceId': ds_id,
                            'y': (str(measure) if ((agg or 'count') != 'count' and measure) else None),
                            'measure': None,
                            'agg': str(agg or 'count'),
                            'dateField': str(x_field),
                            # If where2 contains __gte/__lt they will be ignored here; period_totals uses explicit start/end
                            'start': None,
                            'end': None,
                            'where': dict(where2 or {}),
                            'legend': None,
                        }
                        # Extract start/end from where2 if present; otherwise derive for token cases using xMode/token
                        s_iso = None; e_iso = None
                        try:
                            # Prefer explicit bounds in where2
                            s_iso = payload_tot_kpi['where'].pop(f"{x_field}__gte", None) or payload_tot_kpi['where'].pop(f"{x_field}__gt", None)
                            e_iso = payload_tot_kpi['where'].pop(f"{x_field}__lt", None) or payload_tot_kpi['where'].pop(f"{x_field}__lte", None)
                        except Exception:
                            s_iso = None; e_iso = None
                        if (s_iso is None and e_iso is None):
                            try:
                                xm_ = str((thr_base.get('xMode') or thr_base.get('xPick') or '')).lower()
                                tok_ = str((thr_base.get('xToken') or thr_base.get('xPick') or '')).lower()
                            except Exception:
                                xm_, tok_ = '', ''
                            # Treat xPick token strings as token mode
                            try:
                                if (xm_ not in ('token','range','custom','special')) and (tok_ in ('today','yesterday','this_month')):
                                    xm_ = 'token'
                            except Exception:
                                pass
                            if xm_ == 'token' and tok_ == 'today':
                                from datetime import date as _date, timedelta as _td
                                today = _date.today()
                                # cumulative as-of today
                                s_iso = '1900-01-01'
                                e_iso = (today + _td(days=1)).isoformat()
                        elif (s_iso is None and e_iso is not None):
                            # Only end bound present: treat as as-of cumulative
                            s_iso = '1900-01-01'
                        elif (s_iso is None and e_iso is None):
                            try:
                                xm_ = str((thr_base.get('xMode') or thr_base.get('xPick') or '')).lower()
                                tok_ = str((thr_base.get('xToken') or '') or (thr_base.get('xPick') or '')).lower()
                            except Exception:
                                xm_, tok_ = '', ''
                            if xm_ == 'token' and tok_ == 'today':
                                from datetime import date as _date, timedelta as _td
                                today = _date.today()
                                s_iso = '1900-01-01'
                                e_iso = (today + _td(days=1)).isoformat()
                        payload_tot_kpi['start'] = s_iso
                        payload_tot_kpi['end'] = e_iso
                        # If either start or end missing, fall back to pivot total
                        if payload_tot_kpi['start'] and payload_tot_kpi['end']:
                            res_tot_kpi = period_totals(payload_tot_kpi, db)
                            v_sum = float((res_tot_kpi or {}).get('total') or 0)
                        else:
                            raise Exception('incomplete bounds for period_totals')
                    except Exception:
                        # Fallback to a minimal pivot total
                        try:
                            payload_p = PivotRequest(
                                source=source,
                                rows=[],
                                cols=[],
                                valueField=(None if agg == 'count' else (str(measure) if measure else None)),
                                aggregator=agg, where=where2, datasourceId=ds_id, limit=1, widgetId=None
                            )
                            res = run_pivot(payload_p, db)
                            rows = res.rows or []
                            if rows and isinstance(rows[0], (list, tuple)):
                                r0 = rows[0]
                                if len(r0) > 0:
                                    v = r0[-1]
                                    from decimal import Decimal as _Dec
                                    def _coerce_num(x):
                                        try:
                                            if x is None:
                                                return 0.0
                                            if isinstance(x, (int, float)):
                                                return float(x)
                                            if isinstance(x, _Dec):
                                                return float(x)
                                            s = str(x).replace(',', '').strip()
                                            return float(s) if s else 0.0
                                        except Exception:
                                            return 0.0
                                    v_sum = _coerce_num(v)
                        except Exception:
                            v_sum = 0.0
                else:
                    # No X field: keep the original pivot total
                    try:
                        payload_p = PivotRequest(
                            source=source,
                            rows=[],
                            cols=[],
                            valueField=(None if agg == 'count' else (str(measure) if measure else None)),
                            aggregator=agg, where=where2, datasourceId=ds_id, limit=1, widgetId=None
                        )
                        res = run_pivot(payload_p, db)
                        rows = res.rows or []
                        if rows and isinstance(rows[0], (list, tuple)):
                            r0 = rows[0]
                            if len(r0) > 0:
                                v = r0[-1]
                                from decimal import Decimal as _Dec
                                def _coerce_num(x):
                                    try:
                                        if x is None:
                                            return 0.0
                                        if isinstance(x, (int, float)):
                                            return float(x)
                                        if isinstance(x, _Dec):
                                            return float(x)
                                        s = str(x).replace(',', '').strip()
                                        return float(s) if s else 0.0
                                    except Exception:
                                        return 0.0
                                v_sum = _coerce_num(v)
                    except Exception:
                        v_sum = 0.0
                kpi_value = v_sum
                dbg_kpi_from = "pivot"
                try:
                    dbg_kpi_bounds_s = None
                    if 'payload_tot_kpi' in locals():
                        dbg_kpi_bounds_s = json.dumps({ 'start': payload_tot_kpi.get('start'), 'end': payload_tot_kpi.get('end') })
                except Exception:
                    dbg_kpi_bounds_s = None
            except Exception:
                kpi_value = None
        else:
            try:
                _ok, k = evaluate_threshold(db, trigger=thr_base, datasource_id=ds_id)
                kpi_value = k
                dbg_kpi_from = "query"
            except Exception:
                kpi_value = None

    # Helper: tolerant spec runner that prunes unknown filter columns (e.g., when transforms reference fields not present on a given source)
    def _run_spec_prune(spec: dict, *, limit: int = 10000):
        try:
            req = QuerySpecRequest(spec=spec, datasourceId=ds_id, limit=limit, offset=0, includeTotal=False)
            return run_query_spec(req, db)
        except Exception as e:
            msg = str(e or '')
            try:
                m = _re.search(r"Invalid column name '([^']+)'", msg, flags=_re.IGNORECASE)
                if not m:
                    raise
                bad = (m.group(1) or '').strip()
                where0 = dict(spec.get('where') or {})
                def _norm(n: str) -> str:
                    return (n or '').strip().strip('[]').strip('`').strip('"').split('.')[-1]
                removed = False
                for k in list(where0.keys()):
                    base = k.split('__', 1)[0]
                    if _norm(base) == _norm(bad):
                        where0.pop(k, None)
                        removed = True
                if removed:
                    spec2 = dict(spec)
                    spec2['where'] = (where0 or None)
                    req2 = QuerySpecRequest(spec=spec2, datasourceId=ds_id, limit=limit, offset=0, includeTotal=False)
                    return run_query_spec(req2, db)
                raise
            except Exception:
                raise

    # If render provides a QuerySpec and mode is KPI, prefer computing KPI from it for preview accuracy
    try:
        render_spec = render.get("querySpec") if isinstance(render.get("querySpec"), dict) else None
    except Exception:
        render_spec = None

    if str((render.get("mode") or "").lower()) == "kpi" and render_spec and calc_mode != 'pivot':
        try:
            # Merge X window from threshold trigger into render_spec.where for consistent preview
            spec_in = dict(render_spec)
            try:
                x_field_rs = None
                base_tr = thr_base if 'thr_base' in locals() else None
                if base_tr and isinstance(base_tr, dict):
                    x_field_rs = base_tr.get('xField') or None
                w_in = spec_in.get('where') if isinstance(spec_in.get('where'), dict) else None
                w2, xv2 = _apply_xpick_to_where(w_in, x_field=x_field_rs, trigger=(base_tr or {}), datasource_id=ds_id, source=str(spec_in.get('source') or ''), db=db)
                if x_field_rs is not None and (xv2 is not None) and (str(xv2).strip() not in ('*','All')):
                    wtmp = dict(w2 or {})
                    wtmp[str(x_field_rs)] = xv2
                    w2 = wtmp
                if w2 is not None:
                    spec_in['where'] = w2
            except Exception:
                pass
            res_r = _run_spec_prune(spec_in, limit=10000)
            v_sum = 0.0
            rows_r = res_r.rows or []
            cols_r = [str(c).lower() for c in (res_r.columns or [])]
            vi = cols_r.index('value') if 'value' in cols_r else -1
            if rows_r:
                if isinstance(rows_r[0], (list, tuple)):
                    for r in rows_r:
                        if vi >= 0 and len(r) > vi and isinstance(r[vi], (int, float)):
                            v_sum += float(r[vi]); continue
                        for cell in r:
                            if isinstance(cell, (int, float)):
                                v_sum += float(cell); break
                elif isinstance(rows_r[0], dict):
                    for r in rows_r:
                        if 'value' in r and isinstance(r['value'], (int, float)):
                            v_sum += float(r['value']); continue
                        for _, cell in r.items():
                            if isinstance(cell, (int, float)):
                                v_sum += float(cell); break
            kpi_value = float(v_sum)
        except Exception:
            pass

    # Build content HTML block placeholder; do not pre-append raw tables here. We prefer TABLE_IMG/HTML later.
    parts: list[str] = []
    render_mode_is_kpi = False
    kpi_label = "KPI"
    mode = str(render.get("mode") or "kpi")
    if mode == "kpi":
        kpi_label = render.get("label") or "KPI"
        render_mode_is_kpi = True

    # Templates (email insert, sms text) with placeholders
    email_insert = str(cfg.get("template") or "")
    template_present = bool(email_insert.strip())
    sms_text = None
    try:
        sms_action = next((a for a in (cfg.get("actions") or []) if str(a.get("type") or "").lower() == "sms"), None)
        if sms_action and sms_action.get("message"):
            sms_text = str(sms_action.get("message"))
    except Exception:
        sms_text = None

    # Pull fields from threshold trigger, with fallback to triggersGroup.threshold
    tg = cfg.get('triggersGroup') if isinstance(cfg.get('triggersGroup'), dict) else None
    thr2 = (tg or {}).get('threshold') if isinstance((tg or {}).get('threshold'), dict) else None
    # Build context
    thr_raw = None; thr_low = None; thr_high = None; op = None; agg = None; measure = None; x_field = None; x_val = None; x_mode = None
    if t0 or thr2:
        base = t0 or thr2 or {}
        op = str(base.get("operator") or "").strip()
        val = base.get("value")
        if isinstance(val, list) and len(val) >= 2:
            thr_low, thr_high = val[0], val[1]
            thr_raw = thr_low
        else:
            thr_raw = val
        agg = (base.get("aggregator") or "").lower()
        measure = base.get("measure") or base.get("y")
        x_field = base.get("xField")
        x_val = base.get("xValue")
        # Normalize x mode/pick: support both xMode/xToken and xPick
        x_mode = base.get("xMode") or None
        _xpick_in = base.get("xPick")
        # Normalize: treat token-like values as token mode
        xp_src = str((_xpick_in if _xpick_in is not None else (x_mode or ''))).lower()
        if xp_src in ("today", "yesterday", "this_month"):
            x_mode = "token"
        elif xp_src in ("range", "custom", "special"):
            x_mode = xp_src
    filters_obj = ((t0.get("where") if t0 else None) or (thr2.get("where") if thr2 else None)) or {}
    filters_h = "; ".join([f"{k}={('|'.join(map(str,v)) if isinstance(v,list) else v)}" for k,v in filters_obj.items()])
    # xPick token
    x_pick = None
    try:
        if x_mode == 'token':
            x_pick = (t0.get('xToken') if t0 else None) or (thr2.get('xToken') if thr2 else None) or (_xpick_in if 'xp' in locals() else _xpick_in)
        elif x_mode == 'special':
            x_pick = t0.get('xSpecial')
        elif x_mode == 'range':
            x_pick = 'range'
        elif x_mode == 'custom':
            x_pick = 'custom'
    except Exception:
        x_pick = None
    # Legend default: if a legend filter is present, use its value; otherwise default to 'All' when a legendField is configured
    try:
        legend_field_ctx = (t0.get('legendField') if t0 else None) or (thr2.get('legendField') if thr2 else None)
    except Exception:
        legend_field_ctx = None
    legend_default = ''
    try:
        if legend_field_ctx and (legend_field_ctx in filters_obj):
            vv = filters_obj.get(legend_field_ctx)
            if isinstance(vv, list) and len(vv) > 0:
                legend_default = '' if vv[0] is None else str(vv[0])
            elif vv is not None:
                legend_default = str(vv)
        # Do NOT default to 'All' when legend_field exists but no filter is set
    except Exception:
        legend_default = ''

    # Resolve a human-friendly x value for tokens/range
    from datetime import date, timedelta
    def resolve_x_value() -> str:
        try:
            if x_mode == 'custom':
                return '' if (x_val is None or x_val == '') else str(x_val)
            if x_mode == 'token':
                tok = ((t0.get('xToken') if t0 else None) or (thr2.get('xToken') if thr2 else None) or (x_pick or ''))
                today = date.today()
                if tok == 'today':
                    return today.isoformat()
                if tok == 'yesterday':
                    return (today - timedelta(days=1)).isoformat()
                if tok == 'this_month':
                    return today.isoformat()[:7]
                return str(tok)
            if x_mode == 'range':
                xr = (t0.get('xRange') if t0 else None) or {}
                return f"{xr.get('from') or ''}..{xr.get('to') or ''}"
            if x_mode == 'special':
                return str((t0.get('xSpecial') if t0 else '') or '')
            return ''
        except Exception:
            return ''
    x_value_resolved = resolve_x_value()

    # Build filter values-only list and HTML chips
    def _only_values(obj: dict[str, Any]) -> list[str]:
        out: list[str] = []
        try:
            for _, v in (obj or {}).items():
                if isinstance(v, list):
                    for it in v:
                        if it is None: continue
                        out.append(str(it))
                elif v is not None:
                    out.append(str(v))
        except Exception:
            pass
        return out
    _vals = _only_values(filters_obj)
    _vals_html = " ".join([f"<span style='display:inline-block;border:1px solid #e5e7eb;border-radius:999px;padding:2px 8px;margin:1px 2px;font-size:11px'>{v}</span>" for v in _vals])

    # Pretty x value for common tokens
    def _fmt_x_value_pretty(x_mode_val: Any, resolved: str) -> str:
        try:
            r = str(resolved or '')
            if not r:
                return ''
            # Range like a..b
            if '..' in r:
                a, b = r.split('..', 1)
                return f"{_fmt_x_value_pretty(None, a)} .. { _fmt_x_value_pretty(None, b)}"
            # yyyy-mm-dd
            if len(r) >= 10 and r[4] == '-' and r[7] == '-':
                return f"{r[8:10]}/{r[5:7]}/{r[0:4]}"
            # yyyy-mm
            if len(r) >= 7 and r[4] == '-' and (len(r)==7 or r[7] in (' ', 'T', '-')):
                m = int(r[5:7])
                months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
                mm = months[m-1] if 1 <= m <= 12 else r[5:7]
                return f"{mm}-{r[0:4]}"
            return r
        except Exception:
            return str(resolved or '')

    # Widget reference info for context/preview
    try:
        wref = render.get('widgetRef') or {}
        wid = str(wref.get('widgetId') or '')
        did = str(wref.get('dashboardId') or '')
    except Exception:
        wid = ''
        did = ''

    context: dict[str, Any] = {
        "kpi": "" if kpi_value is None else str(kpi_value),
        "kpi_fmt": "" if kpi_value is None else _fmt_num(kpi_value, 0),
        "operator": "" if op is None else str(op),
        "threshold": "" if thr_raw is None else str(thr_raw),
        "threshold_low": "" if thr_low is None else str(thr_low),
        "threshold_high": "" if thr_high is None else str(thr_high),
        "agg": "" if agg is None else str(agg),
        "measure": "" if measure is None else str(measure),
        "xField": "" if x_field is None else str(x_field),
        "xValue": "" if x_val is None else str(x_val),
        "xPick": "" if x_pick is None else str(x_pick),
        # Prefer legend naming; keep category key empty for backward compatibility
        "legend": str(legend_default or ''),
        "category": "",
        "xValueResolved": x_value_resolved,
        "xValuePretty": _fmt_x_value_pretty(x_pick, x_value_resolved),
        "filters": filters_h,
        "filters_json": json.dumps(filters_obj or {}),
        "filters_values": " | ".join(_vals),
        "filters_values_html": _vals_html,
        "source": str(cfg.get("source") or (t0.get("source") if t0 else "") or ""),
        "datasourceId": str(ds_id or ""),
        "legendField": str(((t0.get('legendField') if t0 else None) or (thr2.get('legendField') if thr2 else None) or '') or ''),
        "widgetId": wid,
        "dashboardId": did,
        "renderMode": mode,
    }
    # Entry marker to confirm evaluate_alert_v2 path is executing
    try:
        context["dbg_preview_marker"] = "v4-entry"
    except Exception:
        pass
    # Debug: help troubleshoot preview mismatches
    try:
        if dbg_kpi_from:
            context["dbg_kpi_source"] = dbg_kpi_from
        context["dbg_calcMode"] = str(calc_mode)
        if dbg_kpi_where_s:
            context["dbg_kpi_where"] = dbg_kpi_where_s
        try:
            if 'dbg_kpi_bounds_s' in locals() and dbg_kpi_bounds_s:
                context["dbg_kpi_bounds"] = dbg_kpi_bounds_s
        except Exception:
            pass
    except Exception as e:
        try:
            context['dbg_scan_error'] = str(e)
        except Exception:
            pass

    # Add widget placeholders for template inserts (preview uses data URIs)
    try:
        # Prefer real widget snapshot when a widgetRef is provided
        try:
            wref = (render.get("widgetRef") or {}) if isinstance(render, dict) else {}
            _wid = (wref.get("widgetId") if isinstance(wref, dict) else None) or None
            _did = (wref.get("dashboardId") if isinstance(wref, dict) else None) or (payload.dashboardId or None)
            if _wid:
                # Heuristic sizes; emails will downscale to container width
                snap_w = int((render.get("width") if isinstance(render, dict) else None) or 1000)
                snap_h = int((render.get("height") if isinstance(render, dict) else None) or (280 if render_mode_is_kpi else 360))
                png = await snapshot_embed_png(
                    dashboard_id=_did,
                    public_id=None,
                    token=None,
                    widget_id=_wid,
                    datasource_id=ds_id,
                    width=snap_w,
                    height=snap_h,
                    theme=str((render.get("theme") if isinstance(render, dict) else None) or "dark"),
                    actor_id=(actorId or settings.snapshot_actor_id),
                    wait_ms=6000,
                )
                b64 = base64.b64encode(png).decode("ascii")
                tag = f"<img alt='Widget' src='data:image/png;base64,{b64}' style='max-width:100%;height:auto'/>"
                context["KPI_IMG"] = tag
                context["CHART_IMG"] = tag
                context["TABLE_IMG"] = tag
        except Exception:
            # Fallback to placeholders if snapshot fails or Playwright not installed
            try:
                val = kpi_value if (kpi_value is not None) else 0
                svg = _build_kpi_svg(val, kpi_label)
                context["KPI_IMG"] = f"<img alt='KPI' src='{_to_svg_data_uri(svg)}' style='max-width:100%;height:auto'/>"
            except Exception:
                pass
            try:
                svg_c = _build_chart_svg_placeholder()
                context["CHART_IMG"] = f"<img alt='Chart' src='{_to_svg_data_uri(svg_c)}' style='max-width:100%;height:auto'/>"
            except Exception:
                pass
        # If no custom template is provided, append the image into parts for preview convenience
        try:
            if not template_present:
                if mode == "kpi" and context.get("KPI_IMG"):
                    parts.append(str(context.get("KPI_IMG")))
                elif mode == "chart" and context.get("CHART_IMG"):
                    parts.append(str(context.get("CHART_IMG")))
                elif mode == "table":
                    if context.get("TABLE_IMG"):
                        parts.append(str(context.get("TABLE_IMG")))
                    elif context.get("TABLE_HTML"):
                        parts.append(str(context.get("TABLE_HTML")))
        except Exception:
            pass

        # Provide TABLE_HTML only if it hasn't been set
        try:
            if not context.get("TABLE_HTML"):
                # 1) Prefer server-side pivot HTML when widgetRef points to a Pivot Table widget
                try:
                    wref = (render.get("widgetRef") or {}) if isinstance(render, dict) else {}
                    wid = (wref.get("widgetId") if isinstance(wref, dict) else None) or None
                    did = (wref.get("dashboardId") if isinstance(wref, dict) else None) or (payload.dashboardId or None)
                    widget_cfg = None
                    if wid and did:
                        try:
                            drow = db.get(Dashboard, did)
                            if drow and drow.definition_json:
                                import json as _json
                                definition = _json.loads(drow.definition_json or "{}")
                                widget_cfg = ((definition or {}).get('widgets') or {}).get(str(wid))
                        except Exception:
                            widget_cfg = None
                    if isinstance(widget_cfg, dict) and ((widget_cfg.get('type') or '') == 'table'):
                        # Detect pivot table mode from options
                        opts = (widget_cfg.get('options') or {})
                        tbl = (opts.get('table') or {})
                        pcfg_probe = (tbl.get('pivotConfig') or {})
                        pv_probe = ((widget_cfg.get('pivot') or {}).get('values') or [])
                        is_pivot = ((tbl.get('tableType') or 'data') == 'pivot') or bool(pcfg_probe.get('rows') or pcfg_probe.get('cols') or pv_probe)
                        if is_pivot:
                            # Build pivot dims and value from widget config
                            pcfg = (tbl.get('pivotConfig') or {})
                            row_dims = list((pcfg.get('rows') or []))
                            col_dims = list((pcfg.get('cols') or []))
                            pv = ((widget_cfg.get('pivot') or {}).get('values') or [])
                            # First value chip drives measure and agg
                            value_field = None
                            agg_raw = None
                            label = None
                            try:
                                chip = (pv[0] if len(pv) > 0 else {}) or {}
                                value_field = chip.get('field') or chip.get('measureId') or None
                                agg_raw = chip.get('agg') or 'sum'
                                label = chip.get('label') or (value_field or 'Value')
                            except Exception:
                                value_field = None; agg_raw = 'sum'; label = 'Value'
                            agg = str(agg_raw or 'sum').lower()
                            if 'distinct' in agg: agg = 'distinct'
                            elif agg.startswith('avg'): agg = 'avg'
                            elif agg not in {'sum','avg','min','max','distinct','count'}: agg = 'count'
                            # Execute server-side pivot
                            from ..schemas import PivotRequest
                            qspec_src = ((widget_cfg.get('querySpec') or {}) or {}).get('source')
                            if not qspec_src:
                                try:
                                    qspec_src = ((render.get('querySpec') or {}) or {}).get('source') if isinstance(render, dict) else None
                                except Exception:
                                    qspec_src = None
                            if not qspec_src:
                                try:
                                    qspec_src = (cfg.get('source') if isinstance(cfg, dict) else None) or None
                                except Exception:
                                    qspec_src = None
                            if not qspec_src:
                                # cannot run server pivot without a base source
                                raise Exception('No querySpec.source for server pivot')
                            payload_p = PivotRequest(
                                source=qspec_src,
                                rows=row_dims,
                                cols=col_dims,
                                valueField=(None if agg=='count' and not value_field else (value_field or None)),
                                aggregator=agg,
                                where=(filters_obj or {}),
                                datasourceId=ds_id,
                                limit=int((tbl.get('pivotMaxRows') or 20000)),
                                widgetId=str(wid),
                            )
                            res_p = run_pivot(payload_p, db)
                            cols = list(res_p.columns or [])
                            data = list(res_p.rows or [])
                            # Prune rows with null/empty on any dimension to match UI behavior
                            try:
                                rdn = len(row_dims)
                                cdn = len(col_dims)
                                def _ok_dim(v: object) -> bool:
                                    try:
                                        if v is None: return False
                                        s = str(v).strip()
                                        return s != ''
                                    except Exception:
                                        return False
                                data = [r for r in data if isinstance(r, (list, tuple)) and all(_ok_dim(r[i]) for i in range(0, rdn)) and all(_ok_dim(r[rdn + j]) for j in range(0, cdn))]
                            except Exception:
                                pass
                            # Simple HTML for common case: no column dims → two columns (row key, value) + total
                            def _esc(x: object) -> str:
                                try:
                                    s = str(x if x is not None else '')
                                    return s.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('"','&quot;').replace("'",'&#39;')
                                except Exception:
                                    return ''
                            if data and len(col_dims) == 0:
                                # Determine value column index
                                try:
                                    # last column is value
                                    vi = len(cols) - 1
                                except Exception:
                                    vi = -1
                                # Build rows keyed by row_dims joined label
                                total = 0.0
                                rows_html = []
                                for r in data:
                                    try:
                                        if not isinstance(r, (list, tuple)): continue
                                        name = " / ".join(_esc(r[i]) for i in range(0, len(row_dims))) if row_dims else ''
                                        v = r[vi] if (vi >= 0 and vi < len(r)) else None
                                        try:
                                            fv = float(v) if v is not None else 0.0
                                        except Exception:
                                            fv = 0.0
                                        total += fv
                                        rows_html.append(f"<tr><th style='padding:6px 8px;border:1px solid #233143;color:#D1D5DB;background:#0F172A;text-align:left'>{name}</th><td style='padding:6px 8px;border:1px solid #233143;color:#E5E7EB;background:#0B1220;text-align:right'>{fv:,.0f}</td></tr>")
                                    except Exception:
                                        pass
                                total_row = f"<tr><th style='padding:6px 8px;border:1px solid #233143;color:#FDE68A;background:#0F172A;text-align:left'>Total</th><td style='padding:6px 8px;border:1px solid #233143;color:#FDE68A;background:#0B1220;text-align:right'>{total:,.0f}</td></tr>"
                                head = f"<tr><th style='padding:6px 8px;border:1px solid #233143;color:#9CA3AF;background:#111827;text-align:left'>{_esc(row_dims[0] if row_dims else 'Item')}</th><th style='padding:6px 8px;border:1px solid #233143;color:#9CA3AF;background:#111827;text-align:right'>{_esc(label or 'Value')}</th></tr>"
                                html = """<table style='border-collapse:collapse;width:100%;font-family:Inter,Arial,sans-serif;font-size:13px;background:#0B0F15;color:#E5E7EB'>
<thead>""" + head + "</thead><tbody>" + ("".join(rows_html)) + total_row + "</tbody></table>"
                                context["TABLE_HTML"] = html
                            elif data:
                                # Generalized matrix for multi-level column dimensions
                                rdn = len(row_dims)
                                cdn = len(col_dims)
                                try:
                                    vi = len(cols) - 1
                                except Exception:
                                    vi = -1
                                # Value format preferences (pivotStyle overrides)
                                pv_style = (tbl.get('pivotStyle') or {})
                                fmt_mode = (pv_style.get('valueFormat') or 'number')
                                pfx = (pv_style.get('valuePrefix') or '')
                                sfx = (pv_style.get('valueSuffix') or '')
                                show_row_totals = (pcfg.get('rowTotals') is not False)
                                show_col_totals = (pcfg.get('colTotals') is not False)

                                def _fmt(n: float) -> str:
                                    try:
                                        x = float(n) if n is not None else 0.0
                                    except Exception:
                                        x = 0.0
                                    m = str(fmt_mode or 'number')
                                    def wrap(s: str) -> str:
                                        a = (pfx or '')
                                        b = (sfx or '')
                                        if a:
                                            s = (a + ('' if a.endswith(' ') else ' ') + s).strip()
                                        if b:
                                            s = (s + ('' if b.startswith(' ') else ' ') + b).strip()
                                        return s
                                    ab = abs(x)
                                    if m == 'abbrev':
                                        if ab >= 1_000_000_000: return wrap(f"{x/1_000_000_000:,.2f}B")
                                        if ab >= 1_000_000: return wrap(f"{x/1_000_000:,.2f}M")
                                        if ab >= 1_000: return wrap(f"{x/1_000:,.2f}K")
                                        return wrap(f"{x:,.2f}")
                                    if m == 'short':
                                        return wrap(f"{x:,.1f}")
                                    if m == 'twoDecimals':
                                        return wrap(f"{x:,.2f}")
                                    if m == 'oneDecimal':
                                        return wrap(f"{x:,.1f}")
                                    if m == 'wholeNumber':
                                        return wrap(f"{round(x):,}")
                                    if m == 'thousands':
                                        return wrap(f"{x:,.0f}")
                                    if m == 'millions':
                                        return wrap(f"{x/1_000_000:,.2f}")
                                    if m == 'billions':
                                        return wrap(f"{x/1_000_000_000:,.2f}")
                                    if m == 'percent':
                                        return wrap(f"{x*100:,.1f}%")
                                    return wrap(f"{x:,.0f}")

                                # Build value map and distinct ordered row/column keys
                                row_leaves: list[tuple] = []
                                _r_seen = set()
                                col_root: dict = {}
                                order_by_level: list[list[str]] = [[] for _ in range(cdn)]
                                seen_by_level: list[set[str]] = [set() for _ in range(cdn)]
                                valmap: dict[tuple, float] = {}
                                for r in data:
                                    if not isinstance(r, (list, tuple)): continue
                                    rk = tuple(r[i] for i in range(0, rdn)) if rdn > 0 else tuple()
                                    ck = tuple(r[rdn + j] for j in range(0, cdn)) if cdn > 0 else tuple()
                                    v = r[vi] if (vi >= 0 and vi < len(r)) else 0
                                    try:
                                        vv = float(v) if v is not None else 0.0
                                    except Exception:
                                        vv = 0.0
                                    valmap[(rk, ck)] = vv
                                    if rk not in _r_seen:
                                        _r_seen.add(rk); row_leaves.append(rk)
                                    # Build column tree
                                    node = col_root
                                    for lvl in range(cdn):
                                        lb = str(ck[lvl] if lvl < len(ck) else '')
                                        if lb not in node:
                                            node[lb] = {}
                                            if lb not in seen_by_level[lvl]:
                                                seen_by_level[lvl].add(lb)
                                                order_by_level[lvl].append(lb)
                                        node = node[lb]

                                # Compute leaf counts and collect leaf keys (DFS honoring order_by_level)
                                leaf_counts: dict[tuple, int] = {}
                                col_leaves: list[tuple] = []
                                def _count(n: dict, depth: int, path: tuple) -> int:
                                    if depth >= cdn:
                                        return 1
                                    s = 0
                                    labels = order_by_level[depth]
                                    for lb in labels:
                                        if lb in n:
                                            s += _count(n[lb], depth + 1, path + (lb,))
                                    leaf_counts[path] = max(1, s)
                                    return max(1, s)
                                _count(col_root, 0, tuple())
                                def _collect(n: dict, depth: int, path: tuple):
                                    if depth >= cdn:
                                        col_leaves.append(path)
                                        return
                                    labels = order_by_level[depth]
                                    for lb in labels:
                                        if lb in n:
                                            _collect(n[lb], depth + 1, path + (lb,))
                                _collect(col_root, 0, tuple())

                                # Row header spans by prefix
                                prefix_counts: dict[tuple, int] = {}
                                for rk in row_leaves:
                                    for i in range(1, rdn + 1):
                                        pf = rk[:i]
                                        prefix_counts[pf] = prefix_counts.get(pf, 0) + 1

                                # Compose HTML table
                                TH_HEAD = "padding:6px 8px;border:1px solid #233143;color:#9CA3AF;background:#111827;text-align:left"
                                TH_ROW = "padding:6px 8px;border:1px solid #233143;color:#D1D5DB;background:#0F172A;text-align:left"
                                TD_CELL = "padding:6px 8px;border:1px solid #233143;color:#E5E7EB;background:#0B1220;text-align:right"
                                TD_TOTAL = "padding:6px 8px;border:1px solid #233143;color:#FDE68A;background:#0B1220;text-align:right;font-weight:600"
                                table_open = "<table style='border-collapse:collapse;width:100%;font-family:Inter,Arial,sans-serif;font-size:13px;background:#0B0F15;color:#E5E7EB'>"

                                # Header builder
                                thead_parts: list[str] = []
                                if cdn > 0:
                                    # First header row: top-left spacer over row headers, then level 0 labels; include Total header if col totals
                                    left_span = max(1, rdn)
                                    first = [f"<th style='{TH_HEAD}' colspan='{left_span}' rowspan='{cdn}'></th>"] if left_span > 0 else []
                                    row0: list[str] = first
                                    for lb in order_by_level[0]:
                                        if lb in col_root:
                                            cs = leaf_counts.get((lb,), 1)
                                            row0.append(f"<th style='{TH_HEAD}' colspan='{cs}'>{_esc(lb)}</th>")
                                    if show_col_totals:
                                        row0.append(f"<th style='{TH_HEAD}' rowspan='{cdn}'>Total</th>")
                                    thead_parts.append("<tr>" + "".join(row0) + "</tr>")
                                    # Subsequent header rows (levels 1..cdn-1)
                                    def _emit_level(n: dict, depth: int, path: tuple):
                                        if depth >= cdn:
                                            return
                                        cells: list[str] = []
                                        labels = order_by_level[depth]
                                        for lb in labels:
                                            if lb in n:
                                                cs = leaf_counts.get(path + (lb,), 1)
                                                cells.append(f"<th style='{TH_HEAD}' colspan='{cs}'>{_esc(lb)}</th>")
                                        if cells:
                                            thead_parts.append("<tr>" + "".join(cells) + "</tr>")
                                        # Recurse into children to build next row
                                        next_nodes: list[dict] = []
                                        for lb in labels:
                                            if lb in n:
                                                next_nodes.append(n[lb])
                                        # merge all children into a single virtual level for traversal order
                                        merged: dict = {}
                                        for i2, lb in enumerate(labels):
                                            if lb in n:
                                                # keep insertion order by extending dict
                                                for k2, v2 in n[lb].items():
                                                    if k2 not in merged:
                                                        merged[k2] = v2
                                        if depth + 1 < cdn:
                                            _emit_level(merged, depth + 1, path)
                                    if cdn > 1:
                                        _emit_level(col_root, 1, tuple())
                                else:
                                    # No column dimensions; not expected in this branch but safe-guard
                                    left_span = max(1, rdn)
                                    thead_parts.append(f"<tr><th style='{TH_HEAD}' colspan='{left_span}'></th><th style='{TH_HEAD}'>Value</th></tr>")

                                # Body rows
                                tbody_parts: list[str] = []
                                seen_prefix: set[tuple] = set()
                                col_totals: list[float] = [0.0 for _ in range(len(col_leaves))]
                                grand_total = 0.0
                                for rk in row_leaves:
                                    tds: list[str] = []
                                    # Row headers with rowSpan merges
                                    for d in range(0, rdn):
                                        pf = rk[: d + 1]
                                        if pf not in seen_prefix:
                                            seen_prefix.add(pf)
                                            rs = prefix_counts.get(pf, 1)
                                            tds.append(f"<th style='{TH_ROW}' rowspan='{rs}'>{_esc(rk[d])}</th>")
                                    # Data cells
                                    row_sum = 0.0
                                    for j, ck in enumerate(col_leaves):
                                        v = float(valmap.get((rk, ck), 0.0) or 0.0)
                                        row_sum += v
                                        col_totals[j] += v
                                        tds.append(f"<td style='{TD_CELL}'>{_fmt(v)}</td>")
                                    if show_row_totals:
                                        tds.append(f"<td style='{TD_TOTAL}'>{_fmt(row_sum)}</td>")
                                    grand_total += row_sum
                                    tbody_parts.append("<tr>" + "".join(tds) + "</tr>")

                                # Column totals row
                                if show_col_totals:
                                    tr: list[str] = []
                                    left_span = max(1, rdn)
                                    tr.append(f"<th style='{TH_HEAD}' colspan='{left_span}'>Total</th>")
                                    for j in range(len(col_leaves)):
                                        tr.append(f"<td style='{TD_TOTAL}'>{_fmt(col_totals[j])}</td>")
                                    if show_row_totals:
                                        tr.append(f"<td style='{TD_TOTAL}'>{_fmt(grand_total)}</td>")
                                    tbody_parts.append("<tr>" + "".join(tr) + "</tr>")

                                html = table_open + "<thead>" + "".join(thead_parts) + "</thead><tbody>" + "".join(tbody_parts) + "</tbody></table>"
                                context["TABLE_HTML"] = html
                        else:
                            # No data returned
                            context["TABLE_HTML"] = "<div>No data for pivot.</div>"
                        
                    else:
                        # Not a pivot-configured Table; do not fallback to raw data table
                        context["TABLE_HTML"] = "<div>Widget is not configured as a pivot table.</div>"
                except Exception as e:
                    try:
                        context["TABLE_HTML"] = "<div>Failed to render pivot: " + _html.escape(str(e)) + "</div>"
                        context["dbg_pivot_error"] = str(e)
                    except Exception:
                        context["TABLE_HTML"] = "<div>Failed to render pivot.</div>"
        except Exception:
            try:
                if not context.get("TABLE_HTML"):
                    context["TABLE_HTML"] = "<div>Failed to render table.</div>"
            except Exception:
                pass
    except Exception:
        pass

    def fill_tokens(s: str) -> str:
        out = s
        for k, v in context.items():
            try:
                out = out.replace("{{" + k + "}}", str(v))
                try:
                    out = _re.sub(r"\{\{\s*" + _re.escape(str(k)) + r"\s*\}\}", str(v), out, flags=_re.IGNORECASE)
                except Exception:
                    pass
            except Exception:
                pass
        return out

    # Decide if we will multi-render per legend (no legend filter/value picked)
    # Support multi-legend and extra row fields (treated as part of composite legend for preview composition)
    want_multi = False
    legend_fields: list[str] = []
    row_fields: list[str] = []
    try:
        # Collect legend fields from trigger or group threshold
        _lf1 = (t0.get('legendFields') if t0 else None) or []
        _lf2 = None
        try:
            tg2 = cfg.get('triggersGroup') if isinstance(cfg.get('triggersGroup'), dict) else None
            thr2_l = (tg2 or {}).get('threshold') if isinstance((tg2 or {}).get('threshold'), dict) else None
            _lf2 = (thr2_l or {}).get('legendFields') if isinstance((thr2_l or {}).get('legendFields'), list) else None
        except Exception:
            _lf2 = None
        # Prefer group legend fields if present; otherwise use trigger legend fields
        if isinstance(_lf2, list) and len(_lf2) > 0:
            legend_fields = [str(x) for x in _lf2 if x is not None and str(x) != '']
        elif isinstance(_lf1, list):
            legend_fields = [str(x) for x in _lf1 if x is not None and str(x) != '']
        # Fallback to single legendField
        try:
            _single_leg = (t0.get('legendField') if t0 else None)
            if (not legend_fields) and _single_leg:
                if isinstance(_single_leg, list):
                    legend_fields.extend([str(x) for x in _single_leg if x is not None and str(x) != ''])
                else:
                    legend_fields.append(str(_single_leg))
            if (not legend_fields) and (thr2_l or {}):
                _single2 = (thr2_l or {}).get('legendField')
                if _single2:
                    if isinstance(_single2, list):
                        legend_fields.extend([str(x) for x in _single2 if x is not None and str(x) != ''])
                    else:
                        legend_fields.append(str(_single2))
        except Exception:
            pass
        # Extra row fields (treated as part of composite legend when composing multi)
        _rf1 = (t0.get('rowFields') if t0 else None) or []
        _rf2 = (thr2_l or {}).get('rowFields') if isinstance((thr2_l or {}).get('rowFields'), list) else None
        if isinstance(_rf2, list) and len(_rf2) > 0:
            row_fields = [str(x) for x in _rf2 if x is not None and str(x) != '']
        elif isinstance(_rf1, list):
            row_fields = [str(x) for x in _rf1 if x is not None and str(x) != '']
        # Composite legend keys used for grouping in preview
        # Deduplicate legend/row while preserving order
        def _dedup(seq: list[str]) -> list[str]:
            _s = set(); out = []
            for it in seq:
                if it not in _s:
                    _s.add(it); out.append(it)
            return out
        legend_fields = _dedup(legend_fields)
        row_fields = _dedup(row_fields)
        _legend_for_multi = legend_fields[:]  # keep original for debug
        legend_fields_eff = _dedup((legend_fields + row_fields) if (legend_fields or row_fields) else [])
        # Decide multi: enabled when we have at least one grouping field and template exists, and none of the legend fields are filtered
        if email_insert and legend_fields_eff:
            lf_set = set(legend_fields_eff)
            has_leg = any(((k in lf_set) or any(str(k).startswith(f"{f}__") for f in lf_set)) for k in (filters_obj or {}).keys())
            want_multi = not has_leg
        else:
            want_multi = False
    except Exception:
        want_multi = False

    # Debug: expose template/multi flags
    try:
        context['dbg_template_present'] = bool(template_present)
        context['dbg_template_len'] = len(email_insert or '')
        context['dbg_want_multi'] = bool(want_multi)
        if 'legend_fields' in locals() and legend_fields:
            # Deduplicate while preserving order
            _seen = set(); _uniq = []
            for f in legend_fields:
                if f not in _seen:
                    _seen.add(f); _uniq.append(f)
            context['dbg_legend_for_multi'] = ", ".join([str(x) for x in _uniq])
            context['legendFields'] = _uniq
    except Exception:
        pass

    # Do not force legend/category to 'All' – leave legend blank unless a legend filter is present

    content_html = "\n".join(parts)
    composed_multi_email = False

    # If we are going to multi-render, clear top-level KPI placeholders so they don't appear outside cards
    if want_multi:
        try:
            context["KPI_IMG"] = ""
        except Exception:
            pass

    # Keep any pre-built content; do not clear parts/content_html even if a custom template is present

    # Wrap with base email template for preview (may be overridden later after inserts are composed)
    try:
        from ..models import EmailConfig
        cfg_email = db.query(EmailConfig).first()
        if cfg_email:
            email_html = _apply_base_template(cfg_email, payload.name or "Notification", content_html)
        else:
            email_html = content_html
    except Exception:
        email_html = content_html

    sms_out = fill_tokens(sms_text or (email_insert or payload.name))

    # Fallback KPI if evaluate_threshold didn't provide one
    if kpi_value is None:
        try:
            # Build a lightweight spec from t0
            src = (t0.get('source') if t0 else None) or cfg.get('source') or ''
            if src:
                where_fb = dict(filters_obj)
                if x_field:
                    if (t0 or {}).get('xMode') == 'custom' and x_val not in (None, ''):
                        where_fb[x_field] = [x_val]
                    elif (t0 or {}).get('xMode') == 'range':
                        xr = (t0 or {}).get('xRange') or {}
                        if xr.get('from'): where_fb[f"{x_field}__gte"] = xr.get('from')
                        if xr.get('to'): where_fb[f"{x_field}__lte"] = xr.get('to')
                    elif (t0 or {}).get('xMode') == 'token':
                        # Apply same windowing as threshold evaluation for consistency
                        tok = str((t0.get('xToken') if t0 else '') or '').lower()
                        from datetime import date as _date, timedelta as _td
                        today = _date.today()
                        if tok == 'today':
                            s = today
                            e = today + _td(days=1)
                            where_fb[f"{x_field}__gte"] = s.isoformat()
                            where_fb[f"{x_field}__lt"] = e.isoformat()
                        elif tok == 'yesterday':
                            e = today
                            s = today - _td(days=1)
                            where_fb[f"{x_field}__gte"] = s.isoformat()
                            where_fb[f"{x_field}__lt"] = e.isoformat()
                        elif tok == 'this_month':
                            s = _date(today.year, today.month, 1)
                            nm = (today.month + 1) if today.month < 12 else 1
                            ny = today.year + 1 if nm == 1 else today.year
                            e = _date(ny, nm, 1)
                            where_fb[f"{x_field}__gte"] = s.isoformat()
                            where_fb[f"{x_field}__lt"] = e.isoformat()
                spec_fb: dict = { 'source': src, 'agg': (agg or 'count'), 'where': where_fb or None }
                if (agg or 'count') != 'count' and (measure or ''):
                    spec_fb['y'] = measure; spec_fb['measure'] = measure
                req_fb = QuerySpecRequest(spec=spec_fb, datasourceId=ds_id, limit=10000, offset=0, includeTotal=False)
                res_fb = run_query_spec(req_fb, db)
                v_fb = 0.0
                rows_fb = res_fb.rows or []
                if rows_fb:
                    if isinstance(rows_fb[0], list):
                        for r in rows_fb:
                            for cell in r:
                                if isinstance(cell, (int, float)):
                                    v_fb += float(cell)
                    elif isinstance(rows_fb[0], dict):
                        for r in rows_fb:
                            for _, cell in r.items():
                                if isinstance(cell, (int, float)):
                                    v_fb += float(cell)
                kpi_value = float(v_fb)
                context['kpi'] = str(kpi_value)
                try:
                    context['kpi_fmt'] = _fmt_num(kpi_value, 0)
                    if render_mode_is_kpi:
                        svg = _build_kpi_svg((kpi_value if (kpi_value is not None) else 0), kpi_label)
                        context["KPI_IMG"] = f"<img alt='KPI' src='{_to_svg_data_uri(svg)}' style='max-width:100%;height:auto'/>"
                except Exception:
                    pass
        except Exception:
            pass

    # If legend fields (single or multiple) present, attempt to determine per-legend aggregates for preview
    try:
        legend_field = (t0.get('legendField') if t0 else None) or (thr2.get('legendField') if thr2 else None)
        use_multi_base = False
        try:
            use_multi_base = bool(legend_field) or (('legend_fields_eff' in locals()) and bool(legend_fields_eff))
        except Exception:
            use_multi_base = bool(legend_field)
        if use_multi_base:
            src = (t0.get('source') if t0 else None) or cfg.get('source') or ''
            if src:
                try:
                    context['dbg_scan_begin'] = True
                except Exception:
                    pass
                # Pre-aggregation WHERE: include x token window to ensure data for the selected period exists
                where_fb = dict(filters_obj)
                try:
                    base_pick = t0 or thr2 or {}
                    if x_field and str(x_mode or '') == 'token':
                        tok = str(((base_pick.get('xToken') or None) or x_pick or '')).lower()
                        from datetime import date as _date, timedelta as _td
                        today = _date.today()
                        if tok == 'today':
                            # As-of cumulative pre-filter: only upper bound < tomorrow
                            e = today + _td(days=1)
                            try:
                                where_fb.pop(f"{x_field}__gte", None); where_fb.pop(f"{x_field}__gt", None)
                            except Exception:
                                pass
                            where_fb[f"{x_field}__lt"] = e.isoformat()
                        elif tok == 'yesterday':
                            e = today; s = today - _td(days=1)
                            where_fb[f"{x_field}__gte"] = s.isoformat()
                            where_fb[f"{x_field}__lt"] = e.isoformat()
                        elif tok == 'this_month':
                            s = _date(today.year, today.month, 1)
                            nm = (today.month + 1) if today.month < 12 else 1
                            ny = today.year + 1 if nm == 1 else today.year
                            e = _date(ny, nm, 1)
                            where_fb[f"{x_field}__gte"] = s.isoformat()
                            where_fb[f"{x_field}__lt"] = e.isoformat()
                    elif x_field and str(x_mode or '') == 'range':
                        xr = (base_pick.get('xRange') or {})
                        if xr.get('from'): where_fb[f"{x_field}__gte"] = xr.get('from')
                        if xr.get('to'): where_fb[f"{x_field}__lte"] = xr.get('to')
                    elif x_field and str(x_mode or '') == 'custom' and (x_val not in (None, '')):
                        if str(x_val).strip() not in ('*','All'):
                            where_fb[str(x_field)] = [x_val]
                except Exception:
                    pass

                # Common selection helpers
                pick_token = str(x_pick or '').strip()
                sel_key = str(x_value_resolved or '').strip()
                xm_raw = str(x_mode or '').lower()
                tok_like = pick_token in ('today','yesterday','this_month') or xm_raw in ('today','yesterday','this_month')
                xm_eff = ('token' if tok_like else xm_raw)
                apply_x_match = not (xm_eff in ('token','range')) and not (xm_eff == 'custom' and str(x_val or '').strip() in ('*','All'))
                def x_matches_common(xv: Any) -> bool:
                    try:
                        if not sel_key:
                            return True
                        s = str(xv or '')
                        if pick_token in {'today', 'yesterday'} and len(sel_key) >= 10:
                            return s.startswith(sel_key[:10])
                        if pick_token == 'this_month' and len(sel_key) >= 7:
                            return s.startswith(sel_key[:7])
                        if xm_eff == 'custom' and (x_val not in (None, '')):
                            return s == str(x_val)
                        return s == sel_key
                    except Exception:
                        return True
                def passes(op_val: str | None, x: float, lo: float | None, hi: float | None) -> bool:
                    try:
                        if op_val == 'between' and lo is not None and hi is not None:
                            return (x >= min(lo, hi)) and (x <= max(lo, hi))
                        if op_val == '>': return x > (lo if lo is not None else 0)
                        if op_val == '>=': return x >= (lo if lo is not None else 0)
                        if op_val == '<': return x < (lo if lo is not None else 0)
                        if op_val == '<=': return x <= (lo if lo is not None else 0)
                        if op_val == '==': return (lo is not None) and (x == lo)
                        return True
                    except Exception:
                        return False

                best_cat = None
                best_val: float | None = None
                multi_html_parts: list[str] = []
                groups_for_multi: dict[str, dict] = {}
                kpi_sum_matches: float = 0.0
                scan_total: int = 0
                scan_match_count: int = 0
                scan_samples: list[str] = []
                # Normalize threshold numbers for comparisons
                try:
                    thr_lo_f = float(thr_low) if thr_low is not None else (float(thr_raw) if thr_raw is not None else None)
                except Exception:
                    thr_lo_f = None
                try:
                    thr_hi_f = float(thr_high) if thr_high is not None else None
                except Exception:
                    thr_hi_f = None

                # First, if an X field is a date and we have a concrete window (token/range/custom concrete), try period_totals which groups by legend reliably.
                used_period_totals = False
                if x_field:
                    s_iso = None; e_iso = None
                    from datetime import date as _date, timedelta as _td
                    today = _date.today()
                    xm_raw2 = str((t0 or {}).get('xMode') or '').lower()
                    tok2 = str(((t0.get('xToken') if t0 else None) or (x_pick or '') or xm_raw2 or '')).lower()
                    xm_eff2 = 'token' if (tok2 in ('today','yesterday','this_month') or xm_raw2 in ('today','yesterday','this_month')) else xm_raw2
                    if xm_eff2 == 'token':
                        tok = tok2
                        if tok == 'today':
                            # As-of cumulative totals: start from a very early date
                            s_iso = '1900-01-01'
                            e_iso = (today + _td(days=1)).isoformat()
                        elif tok == 'yesterday':
                            e = today; s = today - _td(days=1)
                            s_iso = s.isoformat(); e_iso = e.isoformat()
                        elif tok == 'this_month':
                            s = _date(today.year, today.month, 1)
                            nm = (today.month + 1) if today.month < 12 else 1
                            ny = today.year + 1 if nm == 1 else today.year
                            e = _date(ny, nm, 1)
                            s_iso = s.isoformat(); e_iso = e.isoformat()
                    elif xm_eff2 == 'range':
                        xr = (t0.get('xRange') if t0 else None) or {}
                        a = str(xr.get('from') or '').strip()
                        b = str(xr.get('to') or '').strip()
                        if a:
                            s_iso = a
                        if b:
                            # Make end exclusive by +1 day when looks like a date
                            try:
                                yy, mm, dd = [int(x) for x in b[:10].split('-')]
                                e_iso = (_date(yy, mm, dd) + _td(days=1)).isoformat()
                            except Exception:
                                e_iso = b
                    elif xm_eff2 == 'custom' and (x_val not in (None, '', '*', 'All')):
                        a = str(x_val).strip()
                        try:
                            yy, mm, dd = [int(x) for x in a[:10].split('-')]
                            s_iso = _date(yy, mm, dd).isoformat()
                            e_iso = (_date(yy, mm, dd) + _td(days=1)).isoformat()
                        except Exception:
                            s_iso = a; e_iso = a
                    # If we resolved a concrete start/end, aggregate with period_totals per legend
                    if x_field and s_iso and e_iso and ((('legend_fields_eff' in locals()) and legend_fields_eff) or legend_field):
                        try:
                            context['dbg_period_totals'] = True
                        except Exception:
                            pass
                        payload_tot = {
                            'source': src,
                            'datasourceId': ds_id,
                            'y': (str(measure) if ((agg or 'count') != 'count' and measure) else None),
                            'measure': None,
                            'agg': str(agg or 'count'),
                            'dateField': str(x_field),
                            'start': s_iso,
                            'end': e_iso,
                            'where': dict(where_fb or {}),
                            'legend': (legend_fields_eff if ('legend_fields_eff' in locals() and legend_fields_eff) else [str(legend_field)]),
                        }
                        try:
                            res_tot = period_totals(payload_tot, db)
                            totals_map = (res_tot or {}).get('totals') or {}
                        except Exception:
                            totals_map = {}
                        # Debug: period_totals
                        try:
                            context['dbg_used_period_totals'] = True
                            context['dbg_totals_len'] = (len(totals_map) if isinstance(totals_map, dict) else 0)
                            if isinstance(totals_map, dict):
                                ks = list(totals_map.keys())
                                context['dbg_totals_keys_sample'] = ', '.join([str(x) for x in ks[:3]])
                        except Exception:
                            pass
                        if isinstance(totals_map, dict) and totals_map:
                            used_period_totals = True
                            # Collect parent-child groups when multiple legend fields present
                            _has_multi_legend = (('legend_fields_eff' in locals()) and legend_fields_eff and (len(legend_fields_eff) > 1))
                            for cat, v in totals_map.items():
                                try:
                                    vnum = float(v)
                                except Exception:
                                    continue
                                try:
                                    scan_total += 1
                                except Exception:
                                    pass
                                if passes(op, vnum, thr_lo_f, thr_hi_f):
                                    try:
                                        scan_match_count += 1
                                        if len(scan_samples) < 5:
                                            scan_samples.append(f"{str(cat)}={vnum}")
                                    except Exception:
                                        pass
                                    if (best_val is None) or (vnum > best_val):
                                        best_val = vnum; best_cat = cat
                                    try: kpi_sum_matches += float(vnum)
                                    except Exception: pass
                                    if want_multi and email_insert:
                                        if _has_multi_legend:
                                            # Accumulate into groups_for_multi: parent legend + child labels
                                            try:
                                                s = '' if cat is None else str(cat)
                                                parent_val = s
                                                child_label = ''
                                                if '•' in s:
                                                    parts = [p.strip() for p in s.split('•')]
                                                    if parts:
                                                        parent_val = parts[0]
                                                        child_label = ' • '.join([p for p in parts[1:] if p]).strip(' •')
                                                if str(parent_val).strip() != '':
                                                    g = groups_for_multi.get(parent_val) or {'sum': 0.0, 'children': {}}
                                                    g['sum'] = float(g.get('sum') or 0.0) + float(vnum)
                                                    if child_label:
                                                        ch = g['children']
                                                        ch[child_label] = float(ch.get(child_label) or 0.0) + float(vnum)
                                                    groups_for_multi[parent_val] = g
                                            except Exception:
                                                pass
                                        else:
                                            # Single legend: append per-cat card immediately
                                            prev_leg = context.get('legend'); prev_kpi = context.get('kpi'); prev_kpif = context.get('kpi_fmt') if 'kpi_fmt' in context else None
                                            try:
                                                context['legend'] = '' if cat is None else str(cat)
                                                context['kpi'] = '' if vnum is None else str(vnum)
                                                context['kpi_fmt'] = '' if vnum is None else _fmt_num(vnum, 0)
                                                multi_html_parts.append(f"<div class='card'>{fill_tokens(email_insert)}</div>")
                                            except Exception:
                                                pass
                                            finally:
                                                if prev_leg is None:
                                                    try: del context['legend']
                                                    except Exception: pass
                                                else:
                                                    context['legend'] = prev_leg
                                                if prev_kpi is None:
                                                    try: del context['kpi']
                                                    except Exception: pass
                                                else:
                                                    context['kpi'] = prev_kpi
                                                if prev_kpif is None:
                                                    try: del context['kpi_fmt']
                                                    except Exception: pass
                                                else:
                                                    context['kpi_fmt'] = prev_kpif
                            # Fallback: pick top legend even if no matches passed threshold
                            if (best_cat is None) and totals_map:
                                try:
                                    _top_cat, _top_val = max(((str(k), float(v or 0)) for k, v in totals_map.items()), key=lambda kv: kv[1])
                                    best_cat = _top_cat
                                    best_val = _top_val
                                    try:
                                        context['dbg_top_legend'] = _top_cat
                                        context['dbg_top_value'] = _top_val
                                    except Exception:
                                        pass
                                except Exception:
                                    pass
                            # Finalize context legend/KPI from matches
                            if best_cat is not None:
                                context['legend'] = str(best_cat)
                                _kpi_matches = float(kpi_sum_matches)
                                context['kpi'] = str(_kpi_matches)
                                try:
                                    context['kpi_fmt'] = _fmt_num(_kpi_matches, 0)
                                    if render_mode_is_kpi:
                                        svg = _build_kpi_svg((_kpi_matches if (_kpi_matches is not None) else 0), kpi_label)
                                        context["KPI_IMG"] = f"<img alt='KPI' src='{_to_svg_data_uri(svg)}' style='max-width:100%;height:auto'/>"
                                except Exception:
                                    pass
                            else:
                                try:
                                    context['dbg_best_cat_none'] = True
                                except Exception:
                                    pass
                            # Debug counters for preview Context
                            try:
                                context['dbg_perLegend_total'] = scan_total
                                context['dbg_perLegend_matches'] = scan_match_count
                                if scan_samples:
                                    context['dbg_perLegend_samples'] = ", ".join(scan_samples)
                            except Exception:
                                pass
                            # Grouped multi-render for period_totals: prefer parent/child grouping when multi legend exists
                            if want_multi and (('legend_fields_eff' in locals()) and legend_fields_eff and (len(legend_fields_eff) > 1)) and groups_for_multi:
                                try:
                                    context['dbg_preview_path'] = 'period_totals_group'
                                    context['dbg_groups_parent_count'] = len(groups_for_multi)
                                    _smp = []
                                    for i, (pk, data) in enumerate(groups_for_multi.items()):
                                        if i >= 3: break
                                        ch = data.get('children') or {}
                                        _smp.append(f"{str(pk)}: sum={data.get('sum')}, children={len(ch)}")
                                    context['dbg_groups_sample'] = "; ".join(_smp)
                                except Exception:
                                    pass
                                parts_out: list[str] = []
                                parents_ord = sorted(groups_for_multi.items(), key=lambda kv: float((kv[1].get('sum') or 0.0)), reverse=True)
                                for parent_val, data in parents_ord:
                                    prev_leg = context.get('legend'); prev_kpi = context.get('kpi'); prev_kpif = context.get('kpi_fmt') if 'kpi_fmt' in context else None
                                    try:
                                        pv_sum = float(data.get('sum') or 0.0)
                                        context['legend'] = '' if parent_val is None else str(parent_val)
                                        context['kpi'] = str(pv_sum)
                                        context['kpi_fmt'] = _fmt_num(pv_sum, 0)
                                        child_map = data.get('children') or {}
                                        try:
                                            child_items = sorted(((str(k), float(v)) for k, v in child_map.items() if str(k).strip() != ''), key=lambda kv: kv[1], reverse=True)
                                        except Exception:
                                            child_items = []
                                        body = fill_tokens(email_insert)
                                        if child_items:
                                            lis = "".join([f"<li><span class='label'>{_html.escape(k)}</span> <span class='val'>{_fmt_num(v, 0)}</span></li>" for k, v in child_items])
                                            extra = f"<ul class='child-list' style='margin:6px 0 0 0;padding-left:16px'>{lis}</ul>"
                                        else:
                                            extra = ""
                                        parts_out.append(f"<div class='card'>{body}{extra}</div>")
                                    except Exception:
                                        pass
                                    finally:
                                        if prev_leg is None:
                                            try: del context['legend']
                                            except Exception: pass
                                        else:
                                            context['legend'] = prev_leg
                                        if prev_kpi is None:
                                            try: del context['kpi']
                                            except Exception: pass
                                        else:
                                            context['kpi'] = prev_kpi
                                        if prev_kpif is None:
                                            try: del context['kpi_fmt']
                                            except Exception: pass
                                        else:
                                            context['kpi_fmt'] = prev_kpif
                                if parts_out:
                                    combined = content_html + "\n" + "\n".join(parts_out)
                                    try:
                                        from ..models import EmailConfig
                                        _cfg_email = db.query(EmailConfig).first()
                                        if _cfg_email:
                                            email_html = _apply_base_template(_cfg_email, payload.name or "Notification", combined)
                                        else:
                                            email_html = combined
                                    except Exception:
                                        email_html = combined
                                    composed_multi_email = True
                                    try:
                                        context['dbg_composed_multi'] = True
                                        context['dbg_multi_count'] = len(parts_out)
                                    except Exception:
                                        pass
                            # Compose multi-render if we have items (single legend)
                            elif want_multi and multi_html_parts:
                                combined = content_html + "\n" + "\n".join(multi_html_parts)
                                try:
                                    from ..models import EmailConfig
                                    _cfg_email = db.query(EmailConfig).first()
                                    if _cfg_email:
                                        email_html = _apply_base_template(_cfg_email, payload.name or "Notification", combined)
                                    else:
                                        email_html = combined
                                except Exception:
                                    email_html = combined
                                composed_multi_email = True
                                try:
                                    context['dbg_composed_multi'] = True
                                    context['dbg_multi_count'] = len(multi_html_parts)
                                except Exception:
                                    pass
                            elif want_multi and email_insert and not multi_html_parts:
                                # No per-legend matches: still show the single insert
                                try:
                                    single = f"<div>{fill_tokens(email_insert)}</div>"
                                    combined = content_html + "\n" + single
                                    from ..models import EmailConfig
                                    _cfg_email = db.query(EmailConfig).first()
                                    if _cfg_email:
                                        email_html = _apply_base_template(_cfg_email, payload.name or "Notification", combined)
                                    else:
                                        email_html = combined
                                except Exception:
                                    pass
                                else:
                                    composed_multi_email = True
                                    try:
                                        context['dbg_composed_multi'] = False
                                        context['dbg_multi_count'] = 0
                                        context['dbg_multi_reason'] = 'no_items'
                                    except Exception:
                                        pass

            # If multiple legend fields are present, prefer pivot path to construct parent-child group cards
            if (('legend_fields_eff' in locals()) and isinstance(legend_fields_eff, list) and len(legend_fields_eff) > 1):
                used_period_totals = False
            if not used_period_totals and calc_mode == 'pivot':
                # Pivot-based per-legend scan
                # For token/range windows, aggregate per-legend across the window (no per-X rows)
                xm2_raw = str((t0 or {}).get('xMode') or '').lower()
                try:
                    tok2 = str((x_pick or '')).lower()
                    xm2_eff = 'token' if (tok2 in ('today','yesterday','this_month') or xm2_raw in ('today','yesterday','this_month')) else xm2_raw
                except Exception:
                    xm2_eff = xm2_raw
                include_x = (x_field is not None) and (xm2_eff not in ('token','range'))
                rows_dims: list[str] = (([str(f) for f in legend_fields_eff] if ('legend_fields_eff' in locals() and legend_fields_eff) else ([str(legend_field)] if legend_field else [])) + ([str(x_field)] if include_x else []))
                payload_p = PivotRequest(
                    source=src, rows=rows_dims, cols=[],
                    valueField=(None if (agg or 'count') == 'count' else (str(measure) if measure else None)),
                    aggregator=str(agg or 'count'), where=(where_fb or None), datasourceId=ds_id, limit=20000, widgetId=None
                )
                res_p = run_pivot(payload_p, db)
                rows_p = list(res_p.rows or [])
                cat_dims = (len(legend_fields_eff) if ('legend_fields_eff' in locals() and legend_fields_eff) else (1 if legend_field else 0))
                cat_idx = 0
                x_idx = (cat_dims if include_x else -1)
                val_idx = len(rows_dims)
                # Debug: pivot fallback rows
                try:
                    context['dbg_pivot_include_x'] = bool(include_x)
                    context['dbg_pivot_rows'] = len(rows_p)
                    context['dbg_preview_path'] = 'pivot_group'
                except Exception:
                    pass
                # Grouping structures for parent-child rendering
                groups: dict[str, dict] = {}
                parent_idx = 0
                child_start_idx = 1
                if rows_p:
                    for r in rows_p:
                        try:
                            if not isinstance(r, (list, tuple)):
                                continue
                            v = r[val_idx] if len(r) > val_idx else None
                            try:
                                v_f = float(v) if v is not None else None
                            except Exception:
                                v_f = None
                            if v_f is None:
                                continue
                            # Build composite category from legend dims and split parent/child
                            parent_val = ''
                            child_label = ''
                            if cat_dims > 0:
                                parent_val = '' if (parent_idx >= len(r) or r[parent_idx] is None) else str(r[parent_idx])
                                if cat_dims > 1:
                                    parts = []
                                    for i in range(child_start_idx, cat_dims):
                                        parts.append('' if (i >= len(r) or r[i] is None) else str(r[i]))
                                    child_label = ' • '.join([p for p in parts if p]).strip(' •')
                            else:
                                parent_val = (r[cat_idx] if len(r) > cat_idx else None) or ''
                            if (str(parent_val).strip() == ''):
                                continue
                            xv = (r[x_idx] if (x_idx >= 0 and len(r) > x_idx) else None)
                            if apply_x_match and (not x_matches_common(xv)):
                                continue
                            total_ok = passes(op, float(v_f), thr_lo_f, thr_hi_f)
                            try:
                                scan_total += 1
                            except Exception:
                                pass
                            if not total_ok:
                                continue
                            try:
                                scan_match_count += 1
                                if len(scan_samples) < 5:
                                    lbl = parent_val if child_label == '' else f"{parent_val} • {child_label}"
                                    scan_samples.append(f"{lbl}={v_f}")
                            except Exception:
                                pass
                            if (best_val is None) or (float(v_f) > best_val):
                                best_val = float(v_f); best_cat = parent_val if parent_val != '' else None
                            try:
                                kpi_sum_matches += float(v_f)
                            except Exception:
                                pass
                            # Accumulate into parent group
                            if want_multi and email_insert:
                                try:
                                    g = groups.get(parent_val) or {'sum': 0.0, 'children': {}}
                                    g['sum'] = float(g.get('sum') or 0.0) + float(v_f)
                                    if child_label:
                                        ch = g['children']; ch[child_label] = float(ch.get(child_label) or 0.0) + float(v_f)
                                    groups[parent_val] = g
                                except Exception:
                                    pass
                        except Exception:
                            pass
                    # Fallback: if still no best_cat, pick the max row by value
                    if best_cat is None:
                        try:
                            _best = None
                            for r in rows_p:
                                if not isinstance(r, (list, tuple)):
                                    continue
                                v = r[val_idx] if len(r) > val_idx else None
                                try:
                                    v_f = float(v) if v is not None else None
                                except Exception:
                                    v_f = None
                                if v_f is None:
                                    continue
                                _cat = r[cat_idx] if len(r) > cat_idx else None
                                if (_best is None) or (v_f > _best[1]):
                                    _best = (str(_cat), float(v_f))
                            if _best is not None:
                                best_cat, best_val = _best
                                try:
                                    context['dbg_top_legend'] = best_cat
                                    context['dbg_top_value'] = best_val
                                except Exception:
                                    pass
                        except Exception:
                            pass
                # Fallback: if no rows when including X, retry with [legend] only
                if (not rows_p) and x_field:
                    try:
                        payload_l = PivotRequest(
                            source=src, rows=[str(legend_field)], cols=[],
                            valueField=(None if (agg or 'count') == 'count' else (str(measure) if measure else None)),
                            aggregator=str(agg or 'count'), where=(where_fb or None), datasourceId=ds_id, limit=20000, widgetId=None
                        )
                        res_l = run_pivot(payload_l, db)
                        rows_l = list(res_l.rows or [])
                        cat_idx2 = 0
                        val_idx2 = 1
                        for r in rows_l:
                            try:
                                if not isinstance(r, (list, tuple)):
                                    continue
                                v = r[val_idx2] if (val_idx2 < len(r)) else None
                                if isinstance(v, (int, float)):
                                    try:
                                        scan_total += 1
                                    except Exception:
                                        pass
                                    cv = r[cat_idx2] if (cat_idx2 < len(r)) else None
                                    if passes(op, float(v), thr_lo_f, thr_hi_f):
                                        try:
                                            scan_match_count += 1
                                            if len(scan_samples) < 5:
                                                scan_samples.append(f"{str(cv)}={float(v)}")
                                        except Exception:
                                            pass
                                        if (best_val is None) or (float(v) > best_val):
                                            best_val = float(v); best_cat = cv
                                        try: kpi_sum_matches += float(v)
                                        except Exception: pass
                                        if want_multi and email_insert:
                                            prev_leg = context.get('legend'); prev_kpi = context.get('kpi'); prev_kpif = context.get('kpi_fmt') if 'kpi_fmt' in context else None
                                            try:
                                                context['legend'] = '' if cv is None else str(cv)
                                                context['kpi'] = '' if v is None else str(v)
                                                context['kpi_fmt'] = '' if v is None else _fmt_num(v, 0)
                                                multi_html_parts.append(f"<div class='card'>{fill_tokens(email_insert)}</div>")
                                            except Exception:
                                                pass
                                            finally:
                                                if prev_leg is None:
                                                    try: del context['legend']
                                                    except Exception: pass
                                                else:
                                                    context['legend'] = prev_leg
                                                if prev_kpi is None:
                                                    try: del context['kpi']
                                                    except Exception: pass
                                                else:
                                                    context['kpi'] = prev_kpi
                                                if prev_kpif is None:
                                                    try: del context['kpi_fmt']
                                                    except Exception: pass
                                                else:
                                                    context['kpi_fmt'] = prev_kpif
                            except Exception:
                                pass
                    except Exception:
                        pass
                elif (not used_period_totals) and (not (('legend_fields_eff' in locals()) and legend_fields_eff and len(legend_fields_eff) > 1)):
                    # QuerySpec-based per-legend scan (existing path)
                    # If single legendField is empty but we have multiple legend fields, pick the first for /query/spec
                    _legend_for_query = legend_field
                    try:
                        if (not _legend_for_query) and ('legend_fields_eff' in locals()) and legend_fields_eff:
                            _legend_for_query = str(legend_fields_eff[0])
                    except Exception:
                        pass
                    spec_cat: dict = { 'source': src, 'agg': (agg or 'count'), 'legend': _legend_for_query, 'where': (where_fb or None) }
                    # Ensure an X dimension exists to avoid 'total' fallback in /query/spec; default to legend
                    spec_cat['x'] = x_field or legend_field
                    if (agg or 'count') != 'count' and (measure or ''):
                        spec_cat['y'] = measure; spec_cat['measure'] = measure
                    res_cat = _run_spec_prune(spec_cat, limit=10000)
                    cols: list[str] = list(res_cat.columns or [])  # type: ignore
                    rows_cat = list(res_cat.rows or [])  # type: ignore
                    if cols and rows_cat:
                        # Identify legend and numeric columns
                        cols_l = [str(c).lower() for c in cols]
                        # Legend column: prefer 'legend' alias; fallback to provided legend_field
                        leg_idx = None
                        try:
                            if 'legend' in cols_l:
                                leg_idx = cols_l.index('legend')
                            elif legend_field and legend_field in cols:
                                leg_idx = cols.index(legend_field)
                        except Exception:
                            leg_idx = None
                        # Numeric 'value' column preferred
                        vi = None
                        try:
                            if 'value' in cols_l:
                                vi = cols_l.index('value')
                        except Exception:
                            vi = None
                        # X column index for selection
                        xi = None
                        try:
                            if 'x' in cols_l:
                                xi = cols_l.index('x')
                            elif x_field and x_field in cols:
                                xi = cols.index(x_field)
                        except Exception:
                            xi = None
                        for r in rows_cat:
                            x_ok = True
                            cat = None
                            val_num = None
                            if isinstance(r, (list, tuple)):
                                try:
                                    if xi is not None and xi < len(r):
                                        x_ok = (x_matches_common(r[xi]) if apply_x_match else True)
                                except Exception:
                                    x_ok = True
                                try:
                                    if leg_idx is not None and leg_idx < len(r):
                                        cat = r[leg_idx]
                                except Exception:
                                    cat = None
                                if vi is not None and vi < len(r):
                                    try:
                                        val_num = float(r[vi])
                                    except Exception:
                                        try:
                                            from decimal import Decimal
                                            val_num = float(Decimal(r[vi]))
                                        except Exception:
                                            val_num = None
                                else:
                                    for cell in r:
                                        try:
                                            val_num = float(cell)
                                            break
                                        except Exception:
                                            try:
                                                from decimal import Decimal
                                                val_num = float(Decimal(cell))
                                            except Exception:
                                                val_num = None
                            elif isinstance(r, dict):
                                try:
                                    if xi is not None and xi >= 0 and xi < len(cols):
                                        key_x = cols[xi]
                                        x_ok = (x_matches_common(r.get(key_x)) if apply_x_match else True)
                                except Exception:
                                    x_ok = True
                                try:
                                    if leg_idx is not None and leg_idx >= 0 and leg_idx < len(cols):
                                        key_l = cols[leg_idx]
                                        cat = r.get(key_l)
                                    else:
                                        cat = r.get('legend', r.get(legend_field))
                                except Exception:
                                    cat = r.get('legend', r.get(legend_field))
                                if vi is not None and vi >= 0 and vi < len(cols):
                                    try:
                                        key_v = cols[vi]
                                        v0 = r.get(key_v)
                                        val_num = float(v0) if v0 is not None else None
                                    except Exception:
                                        val_num = None
                                if val_num is None:
                                    for v in r.values():
                                        try:
                                            val_num = float(v)
                                            break
                                        except Exception:
                                            val_num = None
                            else:
                                continue
                            if not x_ok: continue
                            if val_num is None: continue
                            # Apply threshold check
                            is_match = passes(op, val_num, thr_lo_f, thr_hi_f)
                            try:
                                scan_total += 1
                            except Exception:
                                pass
                            if is_match:
                                try:
                                    scan_match_count += 1
                                    if len(scan_samples) < 5:
                                        scan_samples.append(f"{'' if cat is None else str(cat)}={val_num}")
                                except Exception:
                                    pass
                                if (best_val is None) or (val_num > best_val):
                                    best_val = val_num; best_cat = cat
                                try: kpi_sum_matches += float(val_num)
                                except Exception: pass
                                if want_multi and email_insert:
                                    prev_leg = context.get('legend'); prev_kpi = context.get('kpi'); prev_kpif = context.get('kpi_fmt') if 'kpi_fmt' in context else None
                                    try:
                                        context['legend'] = '' if cat is None else str(cat)
                                        context['kpi'] = '' if val_num is None else str(val_num)
                                        context['kpi_fmt'] = '' if val_num is None else _fmt_num(val_num, 0)
                                        multi_html_parts.append(f"<div class='card'>{fill_tokens(email_insert)}</div>")
                                    except Exception:
                                        pass
                                    finally:
                                        if prev_leg is None:
                                            try: del context['legend']
                                            except Exception: pass
                                        else:
                                            context['legend'] = prev_leg
                                        if prev_kpi is None:
                                            try: del context['kpi']
                                            except Exception: pass
                                        else:
                                            context['kpi'] = prev_kpi
                                        if prev_kpif is None:
                                            try: del context['kpi_fmt']
                                            except Exception: pass
                                        else:
                                            context['kpi_fmt'] = prev_kpif

                if best_cat is not None:
                    context['legend'] = str(best_cat)
                    # KPI from datapoint (sum over matching legends)
                    _kpi_matches = float(kpi_sum_matches)
                    context['kpi'] = str(_kpi_matches)
                    try:
                        context['kpi_fmt'] = _fmt_num(_kpi_matches, 0)
                        if render_mode_is_kpi:
                            svg = _build_kpi_svg((_kpi_matches if (_kpi_matches is not None) else 0), kpi_label)
                            context["KPI_IMG"] = f"<img alt='KPI' src='{_to_svg_data_uri(svg)}' style='max-width:100%;height:auto'/>"
                    except Exception:
                        pass
                else:
                    try:
                        context['dbg_best_cat_none'] = True
                    except Exception:
                        pass
                # Compose grouped multi cards per parent
                if want_multi and email_insert and groups:
                    try:
                        # Order parents by descending sum
                        parents_ord = sorted(groups.items(), key=lambda kv: float(kv[1].get('sum') or 0.0), reverse=True)
                        for parent_val, data in parents_ord:
                            prev_leg = context.get('legend'); prev_kpi = context.get('kpi'); prev_kpif = context.get('kpi_fmt') if 'kpi_fmt' in context else None
                            try:
                                pv_sum = float(data.get('sum') or 0.0)
                                context['legend'] = '' if parent_val is None else str(parent_val)
                                context['kpi'] = str(pv_sum)
                                context['kpi_fmt'] = _fmt_num(pv_sum, 0)
                                # Build child list (unique child labels only)
                                child_map = data.get('children') or {}
                                child_items = []
                                try:
                                    # Sort children by value desc
                                    child_items = sorted(((str(k), float(v)) for k, v in child_map.items() if str(k).strip() != ''), key=lambda kv: kv[1], reverse=True)
                                except Exception:
                                    child_items = []
                                body = fill_tokens(email_insert)
                                if child_items:
                                    lis = "".join([f"<li><span class='label'>{_html.escape(k)}</span> <span class='val'>{_fmt_num(v, 0)}</span></li>" for k, v in child_items])
                                    extra = f"<ul class='child-list' style='margin:6px 0 0 0;padding-left:16px'>{lis}</ul>"
                                else:
                                    extra = ""
                                multi_html_parts.append(f"<div class='card'>{body}{extra}</div>")
                            except Exception:
                                pass
                            finally:
                                if prev_leg is None:
                                    try: del context['legend']
                                    except Exception: pass
                                else:
                                    context['legend'] = prev_leg
                                if prev_kpi is None:
                                    try: del context['kpi']
                                    except Exception: pass
                                else:
                                    context['kpi'] = prev_kpi
                                if prev_kpif is None:
                                    try: del context['kpi_fmt']
                                    except Exception: pass
                                else:
                                    context['kpi_fmt'] = prev_kpif
                    except Exception:
                        pass
                # Group diagnostics
                try:
                    if groups:
                        context['dbg_groups_parent_count'] = len(groups)
                        try:
                            _smp = []
                            for i, (pk, data) in enumerate(groups.items()):
                                if i >= 3: break
                                ch = data.get('children') or {}
                                _smp.append(f"{str(pk)}: sum={data.get('sum')}, children={len(ch)}")
                            context['dbg_groups_sample'] = "; ".join(_smp)
                        except Exception:
                            pass
                except Exception:
                    pass
                # Debug counters for preview Context
                try:
                    context['dbg_perLegend_total'] = scan_total
                    context['dbg_perLegend_matches'] = scan_match_count
                    if scan_samples:
                        context['dbg_perLegend_samples'] = ", ".join(scan_samples)
                except Exception:
                    pass
                # If we collected multi parts, override email_html to include them appended to the UPDATED KPI content block
                if want_multi and multi_html_parts:
                    combined = content_html + "\n" + "\n".join(multi_html_parts)
                    try:
                        from ..models import EmailConfig
                        _cfg_email = db.query(EmailConfig).first()
                        if _cfg_email:
                            email_html = _apply_base_template(_cfg_email, payload.name or "Notification", combined)
                        else:
                            email_html = combined
                    except Exception:
                        email_html = combined
                    composed_multi_email = True
                    try:
                        context['dbg_composed_multi'] = True
                        context['dbg_multi_count'] = len(multi_html_parts)
                    except Exception:
                        pass
                elif want_multi and email_insert and not multi_html_parts:
                    # Last-resort: build per-legend cards using legend dims only
                    try:
                        rows_dims_lr: list[str] = []
                        if 'legend_fields_eff' in locals() and legend_fields_eff:
                            rows_dims_lr = [str(f) for f in legend_fields_eff]
                        elif legend_field:
                            rows_dims_lr = [str(legend_field)]
                        if rows_dims_lr:
                            payload_lr = PivotRequest(
                                source=src,
                                rows=rows_dims_lr,
                                cols=[],
                                valueField=(None if (str(agg or 'count') == 'count') else (str(measure) if measure else None)),
                                aggregator=str(agg or 'count'),
                                where=(where_fb or None),
                                datasourceId=ds_id,
                                limit=20000,
                                widgetId=None,
                            )
                            res_lr = run_pivot(payload_lr, db)
                            rows_lr = list(res_lr.rows or [])
                            try:
                                context['dbg_pivot_rows'] = len(rows_lr)
                                context['dbg_preview_path'] = 'pivot_last_resort_group'
                            except Exception:
                                pass
                            # Group rows by parent legend (first legend dim) and aggregate children from remaining dims
                            val_idx_lr = len(rows_dims_lr)
                            cat_dims_lr = len(rows_dims_lr)
                            groups_lr: dict[str, dict] = {}
                            for r in [rr for rr in rows_lr if isinstance(rr, (list, tuple))]:
                                try:
                                    v = r[val_idx_lr] if len(r) > val_idx_lr else None
                                    try:
                                        v_f = float(v) if v is not None else None
                                    except Exception:
                                        v_f = None
                                    if v_f is None:
                                        continue
                                    parent_val = '' if (cat_dims_lr < 1 or r[0] is None) else str(r[0])
                                    if (str(parent_val).strip() == ''):
                                        # Skip entries without a parent legend
                                        continue
                                    child_label = ''
                                    if cat_dims_lr > 1:
                                        parts = []
                                        for i in range(1, cat_dims_lr):
                                            parts.append('' if (i >= len(r) or r[i] is None) else str(r[i]))
                                        child_label = ' • '.join([p for p in parts if p]).strip(' •')
                                    g = groups_lr.get(parent_val) or {'sum': 0.0, 'children': {}}
                                    g['sum'] = float(g.get('sum') or 0.0) + float(v_f)
                                    if child_label:
                                        ch = g['children']
                                        ch[child_label] = float(ch.get(child_label) or 0.0) + float(v_f)
                                    groups_lr[parent_val] = g
                                except Exception:
                                    pass

                            # Emit one card per parent with child list
                            count_cards = 0
                            if groups_lr:
                                try:
                                    context['dbg_groups_parent_count'] = len(groups_lr)
                                    _smp = []
                                    for i, (pk, data) in enumerate(groups_lr.items()):
                                        if i >= 3: break
                                        ch = data.get('children') or {}
                                        _smp.append(f"{str(pk)}: sum={data.get('sum')}, children={len(ch)}")
                                    context['dbg_groups_sample'] = "; ".join(_smp)
                                except Exception:
                                    pass
                                parents_ord = sorted(groups_lr.items(), key=lambda kv: float(kv[1].get('sum') or 0.0), reverse=True)[:24]
                                for parent_val, data in parents_ord:
                                    prev_leg = context.get('legend'); prev_kpi = context.get('kpi'); prev_kpif = context.get('kpi_fmt') if 'kpi_fmt' in context else None
                                    try:
                                        pv_sum = float(data.get('sum') or 0.0)
                                        context['legend'] = '' if parent_val is None else str(parent_val)
                                        context['kpi'] = str(pv_sum)
                                        context['kpi_fmt'] = _fmt_num(pv_sum, 0)
                                        # Build child list (unique child labels only)
                                        child_map = data.get('children') or {}
                                        try:
                                            child_items = sorted(((str(k), float(v)) for k, v in child_map.items() if str(k).strip() != ''), key=lambda kv: kv[1], reverse=True)
                                        except Exception:
                                            child_items = []
                                        body = fill_tokens(email_insert)
                                        if child_items:
                                            lis = "".join([f"<li><span class='label'>{_html.escape(k)}</span> <span class='val'>{_fmt_num(v, 0)}</span></li>" for k, v in child_items])
                                            extra = f"<ul class='child-list' style='margin:6px 0 0 0;padding-left:16px'>{lis}</ul>"
                                        else:
                                            extra = ""
                                        multi_html_parts.append(f"<div class='card'>{body}{extra}</div>")
                                        count_cards += 1
                                    except Exception:
                                        pass
                                    finally:
                                        if prev_leg is None:
                                            try: del context['legend']
                                            except Exception: pass
                                        else:
                                            context['legend'] = prev_leg
                                        if prev_kpi is None:
                                            try: del context['kpi']
                                            except Exception: pass
                                        else:
                                            context['kpi'] = prev_kpi
                                        if prev_kpif is None:
                                            try: del context['kpi_fmt']
                                            except Exception: pass
                                        else:
                                            context['kpi_fmt'] = prev_kpif
                            if count_cards > 0:
                                combined = content_html + "\n" + "\n".join(multi_html_parts)
                                from ..models import EmailConfig
                                _cfg_email = db.query(EmailConfig).first()
                                if _cfg_email:
                                    email_html = _apply_base_template(_cfg_email, payload.name or "Notification", combined)
                                else:
                                    email_html = combined
                                composed_multi_email = True
                                try:
                                    context['dbg_composed_multi'] = True
                                    context['dbg_multi_count'] = count_cards
                                    context['dbg_multi_reason'] = 'last_resort_group'
                                except Exception:
                                    pass
                    except Exception:
                        # Ignore and fall through to single insert
                        pass
                    if not composed_multi_email:
                        try:
                            single = f"<div>{fill_tokens(email_insert)}</div>"
                            combined = content_html + "\n" + single
                            from ..models import EmailConfig
                            _cfg_email = db.query(EmailConfig).first()
                            if _cfg_email:
                                email_html = _apply_base_template(_cfg_email, payload.name or "Notification", combined)
                            else:
                                email_html = combined
                        except Exception:
                            pass
                        else:
                            composed_multi_email = True
                            try:
                                context['dbg_composed_multi'] = False
                                context['dbg_multi_count'] = 0
                                context['dbg_multi_reason'] = 'no_items'
                            except Exception:
                                pass
                elif (not want_multi) and email_insert:
                    # Single-render: fill tokens AFTER KPI/context are finalized
                    try:
                        single = f"<div>{fill_tokens(email_insert)}</div>"
                        combined = content_html + "\n" + single
                        from ..models import EmailConfig
                        _cfg_email = db.query(EmailConfig).first()
                        if _cfg_email:
                            email_html = _apply_base_template(_cfg_email, payload.name or "Notification", combined)
                        else:
                            email_html = combined
                    except Exception:
                        pass
    except Exception:
        pass

    # Never auto-add KPI after computations; leave content_html as-is
    try:
        pass
    except Exception:
        pass

    # If multi-render was not composed above, try to compose grouped cards now; otherwise handle single-render
    try:
        if email_insert and (not composed_multi_email):
            # Try late composition for grouped multi if we have groups or parts
            if want_multi and (groups_for_multi or multi_html_parts):
                try:
                    parts_out: list[str] = []
                    if groups_for_multi:
                        parents_ord = sorted(groups_for_multi.items(), key=lambda kv: float((kv[1].get('sum') or 0.0)), reverse=True)
                        for parent_val, data in parents_ord:
                            prev_leg = context.get('legend'); prev_kpi = context.get('kpi'); prev_kpif = context.get('kpi_fmt') if 'kpi_fmt' in context else None
                            try:
                                pv_sum = float(data.get('sum') or 0.0)
                                context['legend'] = '' if parent_val is None else str(parent_val)
                                context['kpi'] = str(pv_sum)
                                context['kpi_fmt'] = _fmt_num(pv_sum, 0)
                                child_map = data.get('children') or {}
                                child_items = []
                                try:
                                    child_items = sorted(((str(k), float(v)) for k, v in child_map.items() if str(k).strip() != ''), key=lambda kv: kv[1], reverse=True)
                                except Exception:
                                    child_items = []
                                body = fill_tokens(email_insert)
                                if child_items:
                                    lis = "".join([f"<li><span class='label'>{_html.escape(k)}</span> <span class='val'>{_fmt_num(v, 0)}</span></li>" for k, v in child_items])
                                    extra = f"<ul class='child-list' style='margin:6px 0 0 0;padding-left:16px'>{lis}</ul>"
                                else:
                                    extra = ""
                                parts_out.append(f"<div class='card'>{body}{extra}</div>")
                            except Exception:
                                pass
                            finally:
                                if prev_leg is None:
                                    try: del context['legend']
                                    except Exception: pass
                                else:
                                    context['legend'] = prev_leg
                                if prev_kpi is None:
                                    try: del context['kpi']
                                    except Exception: pass
                                else:
                                    context['kpi'] = prev_kpi
                                if prev_kpif is None:
                                    try: del context['kpi_fmt']
                                    except Exception: pass
                                else:
                                    context['kpi_fmt'] = prev_kpif
                    # If earlier parts exist, append them too
                    if multi_html_parts and not parts_out:
                        parts_out = multi_html_parts
                    if parts_out:
                        combined = content_html + "\n" + "\n".join(parts_out)
                        from ..models import EmailConfig
                        _cfg_email2 = db.query(EmailConfig).first()
                        if _cfg_email2:
                            email_html = _apply_base_template(_cfg_email2, payload.name or "Notification", combined)
                        else:
                            email_html = combined
                        composed_multi_email = True
                        try:
                            context['dbg_composed_multi'] = True
                            context['dbg_multi_count'] = len(parts_out)
                        except Exception:
                            pass
                    else:
                        # Fall through to single insert
                        single = f"<div>{fill_tokens(email_insert)}</div>"
                        combined = content_html + "\n" + single
                        from ..models import EmailConfig
                        _cfg_email2 = db.query(EmailConfig).first()
                        if _cfg_email2:
                            email_html = _apply_base_template(_cfg_email2, payload.name or "Notification", combined)
                        else:
                            email_html = combined
                except Exception:
                    # On any error, just do single insert
                    single = f"<div>{fill_tokens(email_insert)}</div>"
                    combined = content_html + "\n" + single
                    from ..models import EmailConfig
                    _cfg_email2 = db.query(EmailConfig).first()
                    if _cfg_email2:
                        email_html = _apply_base_template(_cfg_email2, payload.name or "Notification", combined)
                    else:
                        email_html = combined
            else:
                single = f"<div>{fill_tokens(email_insert)}</div>"
                combined = content_html + "\n" + single
                from ..models import EmailConfig
                _cfg_email2 = db.query(EmailConfig).first()
                if _cfg_email2:
                    email_html = _apply_base_template(_cfg_email2, payload.name or "Notification", combined)
                else:
                    email_html = combined
        elif (not want_multi):
            from ..models import EmailConfig
            _cfg_email2 = db.query(EmailConfig).first()
            if _cfg_email2:
                email_html = _apply_base_template(_cfg_email2, payload.name or "Notification", content_html)
            else:
                email_html = content_html
    except Exception:
        pass

    # Replace tokens in final email_html as a last step (so tokens in base template also render)
    try:
        email_html = fill_tokens(email_html)
    except Exception:
        pass

    # Human summary
    summary = None
    try:
        if t0:
            thr_s = f"{agg}({measure or '*'}) on {context['source'] or 'source'}"
            if x_field:
                thr_s += f" for {x_field}={'*' if not x_val else x_val}"
            if op:
                if isinstance(t0.get('value'), list) and len(t0.get('value'))>=2:
                    thr_s += f" {op} [{thr_low}, {thr_high}]"
                else:
                    thr_s += f" {op} {thr_raw}"
            if filters_h:
                thr_s += f" (Filters: {filters_h})"
            summary = thr_s
    except Exception:
        summary = None

    return EvaluateV2Response(emailHtml=email_html, smsText=sms_out, kpi=kpi_value, context=context, humanSummary=summary)


# --- Provider configs ---
@router.get("/config/email")
async def get_email_config(db: Session = Depends(get_db)) -> EmailConfigPayload:
    c = db.query(EmailConfig).first()
    if not c:
        return EmailConfigPayload()
    return EmailConfigPayload(
        host=c.host,
        port=c.port or 587,
        username=c.username,
        # password not returned
        fromName=c.from_name,
        fromEmail=c.from_email,
        useTls=bool(c.use_tls),
        baseTemplateHtml=c.base_template_html,
        logoUrl=c.logo_url,
    )


@router.put("/config/email")
async def put_email_config(payload: EmailConfigPayload, db: Session = Depends(get_db)) -> dict:
    c = db.query(EmailConfig).first()
    if not c:
        c = EmailConfig(id="default")
    c.host = payload.host or c.host
    c.port = payload.port or c.port or 587
    c.username = payload.username or c.username
    if payload.password:
        c.password_encrypted = encrypt_text(payload.password)
    c.from_name = payload.fromName or c.from_name
    c.from_email = payload.fromEmail or c.from_email
    c.use_tls = bool(payload.useTls)
    if payload.baseTemplateHtml is not None:
        c.base_template_html = payload.baseTemplateHtml
    if payload.logoUrl is not None:
        c.logo_url = payload.logoUrl
    db.add(c)
    db.commit()
    return {"ok": True}


@router.get("/config/sms/hadara")
async def get_sms_config_hadara(db: Session = Depends(get_db)) -> SmsConfigPayload:
    c = db.query(SmsConfigHadara).first()
    if not c:
        return SmsConfigPayload()
    return SmsConfigPayload(apiKey="***", defaultSender=c.default_sender)


@router.put("/config/sms/hadara")
async def put_sms_config_hadara(payload: SmsConfigPayload, db: Session = Depends(get_db)) -> dict:
    c = db.query(SmsConfigHadara).first()
    if not c:
        c = SmsConfigHadara(id="hadara")
    if payload.apiKey:
        c.api_key_encrypted = encrypt_text(payload.apiKey)
    if payload.defaultSender is not None:
        c.default_sender = payload.defaultSender
    db.add(c)
    db.commit()
    return {"ok": True}


# --- Tests ---
@router.post("/test-email")
async def test_email(payload: TestEmailPayload, db: Session = Depends(get_db)) -> dict:
    ok, err = send_email(db, subject=payload.subject, to=payload.to, html=payload.html)
    if not ok:
        raise HTTPException(status_code=400, detail=err or "Failed to send")
    return {"ok": True}


@router.post("/test-sms")
async def test_sms(payload: TestSmsPayload, db: Session = Depends(get_db)) -> dict:
    ok, err = send_sms_hadara(db, to_numbers=payload.to, message=payload.message)
    if not ok:
        raise HTTPException(status_code=400, detail=err or "Failed to send")
    return {"ok": True}
