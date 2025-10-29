from __future__ import annotations

def _fmt_num(value: Any, decimals: int = 0) -> str:
    try:
        x = float(value)
        fmt = f"{{:,.{decimals}f}}"
        s = fmt.format(x)
        # Strip decimals if decimals==0 but a trailing .0 appears
        if decimals == 0:
            if s.endswith('.0'):
                s = s[:-2]
        return s
    except Exception:
        try:
            return str(value if value is not None else '')
        except Exception:
            return ''

import json
import logging
import time
import smtplib
from email.message import EmailMessage
from typing import Any, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import urlopen
import base64
from uuid import uuid4

from sqlalchemy.orm import Session
from .metrics import counter_inc

from .models import AlertRule, EmailConfig, SmsConfigHadara, Dashboard
from .security import decrypt_text
from .routers.query import run_query_spec
from .schemas import QuerySpecRequest, QueryResponse
from datetime import datetime, timedelta
from .config import settings

# --- Rendering helpers ---

def _html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&#39;")
    )


def _render_table_html(res: QueryResponse) -> str:
    cols = list(res.columns or [])
    rows = list(res.rows or [])
    head = "".join([f"<th style='border:1px solid #d0d5dd;background:#f3f4f6;padding:6px;text-align:left;'>{_html_escape(str(c))}</th>" for c in cols])
    body = []
    for idx, r in enumerate(rows):
        tds = []
        for i, c in enumerate(cols):
            v = r[i] if i < len(r) else None
            tds.append(f"<td style='border:1px solid #d0d5dd;padding:6px;'>{_html_escape('' if v is None else str(v))}</td>")
        bg = "#f9fafb" if (idx % 2 == 1) else "#ffffff"
        body.append(f"<tr style='background:{bg}'>{''.join(tds)}</tr>")
    return f"<table style='border-collapse:collapse;font-family:Inter,Arial,sans-serif;font-size:13px'><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def _render_kpi_html(value: Any, label: Optional[str] = None) -> str:
    lab = _html_escape(label or "KPI")
    val = _html_escape(_fmt_num(value, decimals=0))
    return f"""
    <div style='font-family:Inter,Arial,sans-serif'>
      <div style='font-size:12px;color:#6b7280;margin-bottom:4px'>{lab}</div>
      <div style='font-size:24px;font-weight:600'>{val}</div>
    </div>
    """.strip()


# --- Simple SVG placeholders (embedded or attached) ---
def _build_kpi_svg(value: Any, label: Optional[str] = None, *, width: int = 600, height: int = 300) -> bytes:
    lab = _html_escape(label or "KPI")
    val = _html_escape(_fmt_num(value, decimals=0))
    svg = f"""
<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>
  <defs>
    <style>
      .lab{{font: 14px Inter, Arial, sans-serif; fill: #6b7280}}
      .val{{font: 36px Inter, Arial, sans-serif; font-weight: 600; fill: #111827}}
      .card{{fill:#ffffff;stroke:#e5e7eb;stroke-width:1}}
    </style>
  </defs>
  <rect class='card' x='0.5' y='0.5' width='{width-1}' height='{height-1}' rx='10' ry='10' />
  <text class='lab' x='{width/2}' y='{height/2 - 10}' text-anchor='middle'>{lab}</text>
  <text class='val' x='{width/2}' y='{height/2 + 26}' text-anchor='middle'>{val}</text>
  <text x='{width-10}' y='{height-10}' text-anchor='end' style='font:10px Inter,Arial,sans-serif; fill:#9ca3af'>KPI Placeholder</text>
  Sorry, your email client does not support inline SVG.
</svg>
""".strip()
    return svg.encode("utf-8")


def _build_chart_svg_placeholder(text: str = "Chart Placeholder", *, width: int = 600, height: int = 300) -> bytes:
    t = _html_escape(text or "Chart Placeholder")
    svg = f"""
<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>
  <defs>
    <style>
      .cap{{font: 14px Inter, Arial, sans-serif; fill: #6b7280}}
      .card{{fill:#ffffff;stroke:#e5e7eb;stroke-width:1}}
    </style>
  </defs>
  <rect class='card' x='0.5' y='0.5' width='{width-1}' height='{height-1}' rx='10' ry='10' />
  <g transform='translate(24,34)'>
    <rect x='0' y='0' width='{width-48}' height='{height-72}' fill='#f3f4f6' stroke='#e5e7eb'/>
    <text class='cap' x='{(width-48)/2}' y='{(height-72)/2}' text-anchor='middle'>{t}</text>
  </g>
  <text x='{width-10}' y='{height-10}' text-anchor='end' style='font:10px Inter,Arial,sans-serif; fill:#9ca3af'>Chart Placeholder</text>
  Sorry, your email client does not support inline SVG.
</svg>
""".strip()
    return svg.encode("utf-8")


def _to_svg_data_uri(svg_bytes: bytes) -> str:
    try:
        b64 = base64.b64encode(svg_bytes).decode("ascii")
        return f"data:image/svg+xml;base64,{b64}"
    except Exception:
        return ""


logger = logging.getLogger(__name__)


def _fetch_snapshot_png_via_http(*, dashboard_id: Optional[str], public_id: Optional[str], token: Optional[str], widget_id: str, datasource_id: Optional[str], width: int, height: int, theme: str, actor_id: Optional[str], retries: int = 2, backoff_sec: float = 0.8) -> Optional[bytes]:
    base = (settings.backend_base_url or "http://localhost:8000/api").rstrip("/")
    try:
        qs: dict[str, str] = {
            "widgetId": str(widget_id),
            "w": str(int(width)),
            "h": str(int(height)),
            "theme": str(theme or "dark"),
            "waitMs": str(4000),
        }
        if datasource_id:
            qs["datasourceId"] = str(datasource_id)
        if dashboard_id:
            qs["dashboardId"] = str(dashboard_id)
        elif public_id:
            qs["publicId"] = str(public_id)
            if token:
                qs["token"] = str(token)
        if actor_id:
            qs["actorId"] = str(actor_id)
        url = f"{base}/snapshot/widget?{urlencode(qs)}"
        last_err: Exception | None = None
        for attempt in range(0, max(0, int(retries)) + 1):
            try:
                with urlopen(url, timeout=30) as resp:  # nosec B310
                    return resp.read()
            except Exception as e:  # pragma: no cover
                last_err = e
                try:
                    logger.warning("snapshot http attempt %s failed for %s: %s", attempt + 1, url, getattr(e, "message", str(e)))
                except Exception:
                    pass
                if attempt < int(retries):
                    try:
                        time.sleep(max(0.0, float(backoff_sec) * (attempt + 1)))
                    except Exception:
                        pass
                else:
                    break
    except Exception:  # pragma: no cover
        return None
    return None


# --- Email / SMS senders ---

def send_email(db: Session, *, subject: str, to: list[str], html: str, replacements: Optional[dict[str, str]] = None, inline_images: Optional[list[tuple[str, bytes, str, str]]] = None) -> Tuple[bool, Optional[str]]:
    cfg: EmailConfig | None = db.query(EmailConfig).first()
    if not cfg or not cfg.host or not cfg.username or not cfg.password_encrypted:
        return False, "Email is not configured"
    password = decrypt_text(cfg.password_encrypted or "") or ""
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        # Office 365 typically requires the envelope and header From to match the authenticated username
        from_email = (cfg.username or "").strip()
        if cfg.from_name:
            msg["From"] = f"{cfg.from_name} <{from_email}>"
        else:
            msg["From"] = from_email
        msg["To"] = ", ".join(to)
        msg.set_content("This email contains HTML content. Please view in an HTML-capable client.")
        final_html = _apply_base_template(cfg, subject, html)
        # Apply extra placeholders after wrapping
        if replacements:
            for k, v in replacements.items():
                try:
                    final_html = final_html.replace("{{" + k + "}}", v)
                except Exception:
                    pass
        # Inline logo via CID (supports data URI and HTTP/HTTPS)
        inline_images_buf: list[tuple[str, bytes, str, str]] = []
        try:
            lu = (cfg.logo_url or "").strip()
            if lu.startswith("data:") and ";base64," in lu:
                try:
                    head, b64 = lu.split(",", 1)
                    mime = head[5:].split(";", 1)[0] if head.startswith("data:") else "image/png"
                    data = base64.b64decode(b64)
                    logo_cid = f"logo_{uuid4().hex[:8]}"
                    # replace occurrences of the data URI in HTML with cid
                    final_html = final_html.replace(lu, f"cid:{logo_cid}")
                    inline_images_buf.append((logo_cid, data, mime, "logo"))
                except Exception:
                    pass
            elif lu.lower().startswith("http://") or lu.lower().startswith("https://"):
                try:
                    with urlopen(lu, timeout=15) as resp:  # nosec B310
                        data = resp.read()
                        ct = (resp.headers.get("content-type") or "image/png").split(";")[0]
                    logo_cid = f"logo_{uuid4().hex[:8]}"
                    final_html = final_html.replace(lu, f"cid:{logo_cid}")
                    inline_images_buf.append((logo_cid, data, ct, "logo"))
                except Exception:
                    pass
        except Exception:
            pass
        msg.add_alternative(final_html, subtype="html")
        # Attach inline images (CID) if provided
        try:
            all_images = []
            if inline_images_buf:
                all_images.extend(inline_images_buf)
            if inline_images:
                all_images.extend(inline_images)
            if all_images:
                html_part = msg.get_body(preferencelist=("html",))
                target = html_part if html_part is not None else msg
                for cid, data, mime, filename in all_images:
                    try:
                        maintype, subtype = (mime.split("/", 1) + ["octet-stream"])[:2]
                    except Exception:
                        maintype, subtype = "application", "octet-stream"
                    target.add_related(data, maintype=maintype, subtype=subtype, cid=cid, filename=filename)
        except Exception:
            pass
        server = smtplib.SMTP(cfg.host, int(cfg.port or 587), timeout=30)
        try:
            if cfg.use_tls:
                server.starttls()
            server.login(cfg.username, password)
            server.send_message(msg)
        finally:
            try:
                server.quit()
            except Exception:
                pass
        try:
            counter_inc("notifications_email_sent_total")
        except Exception:
            pass
        return True, None
    except Exception as e:
        try:
            counter_inc("notifications_email_failed_total")
        except Exception:
            pass
        return False, str(e)


def _default_base_template(logo_url: Optional[str]) -> str:
    lu = (logo_url or "").strip()
    logo = f"<img src='{lu}' alt='Logo' style='max-height:40px;display:block'/>" if lu else ""
    return f"""
<!doctype html>
<html>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>{{subject}}</title>
  <style>
    body{{margin:0;padding:0;background:#f7f7f8;color:#111827;font-family:Inter,Arial,sans-serif;}}
    .wrap{{width:100%;padding:24px 0;}}
    .container{{max-width:640px;margin:0 auto;background:#ffffff;border:1px solid #e5e7eb;border-radius:12px;box-shadow:0 1px 2px rgba(0,0,0,0.04);overflow:hidden;}}
    .header{{padding:16px 20px;border-bottom:1px solid #e5e7eb;background:#fafafa;display:flex;align-items:center;gap:12px;}}
    .brand{{font-size:14px;font-weight:600;color:#111827;}}
    .content{{padding:20px;}}
    .footer{{padding:16px 20px;border-top:1px solid #e5e7eb;color:#6b7280;font-size:12px;background:#fafafa;}}
    table{{border-collapse:collapse;width:100%}}
    th,td{{border:1px solid #e5e7eb;padding:6px;text-align:left;}}
    thead th{{background:#f3f4f6;}}
    tbody tr:nth-child(even){{background:#f9fafb;}}
  </style>
  <link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap' rel='stylesheet'>
  <style> @media (prefers-color-scheme: dark) {{ body{{background:#0b0f15;color:#e5e7eb}} .container{{background:#0f1720;border-color:#1f2937}} .header{{background:#0f1720;border-color:#1f2937}} .footer{{background:#0f1720;border-color:#1f2937;color:#9ca3af}} }} </style>
  <style> img{{border:0;}} a{{color:#2563eb;text-decoration:none}} </style>
  <style> .logo{{display:flex;align-items:center;gap:12px}} </style>
  <style> .subject{{font-size:14px;font-weight:600}} </style>
  <style> /* table style duplicated for some clients */ table{{border-collapse:collapse;width:100%}} th,td{{border:1px solid #e5e7eb;padding:6px}} th{{background:#f3f4f6}} tbody tr:nth-child(even){{background:#f9fafb}} </style>
  <style> .card{{border:1px solid #e5e7eb;border-radius:10px;padding:16px}} </style>
</head>
<body>
  <div class='wrap'>
    <div class='container'>
      <div class='header'>
        <div class='logo'>{logo}</div>
        <div class='brand'>{{subject}}</div>
      </div>
      <div class='content'>
        {{content}}
      </div>
      <div class='footer'>
        © {{year}}
      </div>
    </div>
  </div>
</body>
</html>
""".strip()


def _apply_base_template(cfg: EmailConfig, subject: str, body_html: str) -> str:
    tpl = (cfg.base_template_html or "").strip() or _default_base_template(cfg.logo_url)
    out = tpl.replace("{{content}}", body_html)
    out = out.replace("{{subject}}", subject or "")
    out = out.replace("{{logoUrl}}", (cfg.logo_url or ""))
    out = out.replace("{{year}}", str(datetime.utcnow().year))
    return out


def send_sms_hadara(db: Session, *, to_numbers: list[str], message: str) -> Tuple[bool, Optional[str]]:
    cfg: SmsConfigHadara | None = db.query(SmsConfigHadara).first()
    if not cfg or not cfg.api_key_encrypted:
        return False, "SMS provider is not configured"
    api_key = decrypt_text(cfg.api_key_encrypted or "") or ""
    base = "http://smsservice.hadara.ps:4545/SMS.ashx/bulkservice/sessionvalue/sendmessage/"
    try:
        for p in to_numbers:
            qs = urlencode({
                "apikey": api_key,
                "to": p,
                "msg": message,
            })
            with urlopen(f"{base}?{qs}", timeout=15) as resp:  # nosec B310
                _ = resp.read()
        try:
            counter_inc("notifications_sms_sent_total", amount=float(len(to_numbers or [])))
        except Exception:
            pass
        return True, None
    except Exception as e:
        try:
            counter_inc("notifications_sms_failed_total", amount=float(len(to_numbers or [])))
        except Exception:
            pass
        return False, str(e)


# --- KPI/Threshold evaluation ---

def compute_kpi(db: Session, *, datasource_id: Optional[str], source: str, agg: str, measure: Optional[str], where: Optional[dict], x_field: Optional[str] = None, x_value: Optional[Any] = None) -> float:
    spec: dict[str, Any] = {
        "source": source,
        "agg": agg or "count",
    }
    if measure:
        spec["measure"] = measure
    if where:
        spec["where"] = where
    if x_field and (x_value is not None):
        w = dict(where or {})
        w[str(x_field)] = x_value
        spec["where"] = w
    req = QuerySpecRequest(spec=spec, datasourceId=datasource_id, limit=1, offset=0, includeTotal=False)
    res = run_query_spec(req, db)
    # Prefer 'value' alias; otherwise pick the first numeric cell
    try:
        rows = res.rows or []
        if not rows:
            return 0.0
        # array rows
        if isinstance(rows[0], (list, tuple)):
            cols = [str(c).lower() for c in (res.columns or [])]
            try:
                vi = cols.index('value') if 'value' in cols else -1
            except Exception:
                vi = -1
            from decimal import Decimal as _Dec
            def _coerce_num(x: Any) -> float:
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
            if vi >= 0:
                v = rows[0][vi] if len(rows[0]) > vi else None
                return _coerce_num(v)
            # fallback: first numeric-looking cell in the row
            for cell in rows[0]:
                n = _coerce_num(cell)
                if n != 0.0 or (isinstance(cell, (int, float))):
                    return n
            return 0.0
        # object rows
        if isinstance(rows[0], dict):
            r0 = rows[0]
            from decimal import Decimal as _Dec
            def _coerce_num2(x: Any) -> float:
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
            if 'value' in r0:
                return _coerce_num2(r0['value'])
            for _, cell in r0.items():
                n = _coerce_num2(cell)
                if n != 0.0 or (isinstance(cell, (int, float))):
                    return n
            return 0.0
    except Exception:
        return 0.0
    return 0.0


def _resolve_special_x_value(db: Session, *, datasource_id: Optional[str], source: str, x_field: str, where: Optional[dict], kind: str) -> Optional[Any]:
    # Use agg min/max to resolve first/last/min/max
    k = kind.lower()
    agg = "min" if k in ("min", "first") else "max"
    spec: dict[str, Any] = {"source": source, "agg": agg, "measure": x_field}
    if where:
        spec["where"] = where
    req = QuerySpecRequest(spec=spec, datasourceId=datasource_id, limit=1, offset=0, includeTotal=False)
    res = run_query_spec(req, db)
    try:
        if res.rows and res.rows[0]:
            return res.rows[0][0]
    except Exception:
        return None
    return None


def _apply_xpick_to_where(where: Optional[dict], *, x_field: Optional[str], trigger: dict, datasource_id: Optional[str], source: str, db: Session) -> Tuple[Optional[dict], Optional[Any]]:
    # Returns (where', x_value') to be used for compute_kpi
    if not x_field:
        return (where, None)
    # Accept both xMode and xPick (UI may send xPick)
    mode = str((trigger.get("xMode") or trigger.get("xPick") or "")).lower()
    if mode == "custom":
        return (where, trigger.get("xValue"))
    w = dict(where or {})
    if mode == "range":
        xr = trigger.get("xRange") or {}
        if xr.get("from") is not None and str(xr.get("from")).strip() != "":
            w[f"{x_field}__gte"] = xr.get("from")
        if xr.get("to") is not None and str(xr.get("to")).strip() != "":
            w[f"{x_field}__lte"] = xr.get("to")
        return (w, None)
    if mode == "token":
        # Accept both xToken and xPick for token choices
        tok = str((trigger.get("xToken") or trigger.get("xPick") or "")).lower()
        now = datetime.utcnow()
        # Use date-only strings to be compatible with DATE columns
        if tok == "today":
            # As-of cumulative: only apply upper bound < tomorrow; remove any lower bound
            end = datetime(now.year, now.month, now.day) + timedelta(days=1)
            try:
                w.pop(f"{x_field}__gte", None); w.pop(f"{x_field}__gt", None)
            except Exception:
                pass
            w[f"{x_field}__lt"] = end.date().isoformat()
        elif tok == "yesterday":
            end = datetime(now.year, now.month, now.day)
            start = end - timedelta(days=1)
            w[f"{x_field}__gte"] = start.date().isoformat()
            w[f"{x_field}__lt"] = end.date().isoformat()
        elif tok == "this_month":
            start = datetime(now.year, now.month, 1)
            if now.month == 12:
                end = datetime(now.year + 1, 1, 1)
            else:
                end = datetime(now.year, now.month + 1, 1)
            w[f"{x_field}__gte"] = start.date().isoformat()
            w[f"{x_field}__lt"] = end.date().isoformat()
        return (w, None)
    if mode == "special":
        sp = str(trigger.get("xSpecial") or "").lower()
        try:
            val = _resolve_special_x_value(db, datasource_id=datasource_id, source=source, x_field=x_field, where=where, kind=sp)
            return (where, val)
        except Exception:
            return (where, None)
    # default fallback
    xv = trigger.get("xValue")
    return (where, xv)


def evaluate_threshold(db: Session, *, trigger: dict, datasource_id: Optional[str]) -> Tuple[bool, float]:
    # trigger: { source, aggregator, measure?, where?, xField?, xValue?, xMode?, operator, value }
    agg = str(trigger.get("aggregator") or "count").lower()
    measure = trigger.get("measure") or trigger.get("y")
    source = str(trigger.get("source") or "").strip()
    where = trigger.get("where") or None
    x_field = trigger.get("xField") or None
    # Resolve x pick into where/x_value
    where2, x_value2 = _apply_xpick_to_where(where, x_field=x_field, trigger=trigger, datasource_id=datasource_id, source=source, db=db)
    operator = str(trigger.get("operator") or "").strip()
    value = trigger.get("value")
    kpi = compute_kpi(db, datasource_id=datasource_id, source=source, agg=agg, measure=measure, where=where2, x_field=x_field, x_value=x_value2)
    ok = False
    try:
        if operator == "<":
            ok = kpi < float(value)
        elif operator == "<=":
            ok = kpi <= float(value)
        elif operator == ">":
            ok = kpi > float(value)
        elif operator == ">=":
            ok = kpi >= float(value)
        elif operator in ("=", "=="):
            ok = kpi == float(value)
        elif operator == "between":
            lo, hi = (value or [None, None])
            ok = (lo is not None and hi is not None and float(lo) <= kpi <= float(hi))
    except Exception:
        ok = False
    return ok, kpi


# --- Rule runner ---

def _fmt_run_at(dt: datetime) -> str:
    try:
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return dt.isoformat() + "Z"


def _apply_placeholders(s: str, repl: dict[str, str]) -> str:
    out = s
    for k, v in repl.items():
        try:
            out = out.replace("{{" + k + "}}", v)
        except Exception:
            pass
        try:
            import re as _re
            # Also tolerate whitespace inside braces: {{ key }}
            out = _re.sub(r"\{\{\s*" + _re.escape(k) + r"\s*\}\}", str(v), out)
        except Exception:
            pass
    return out


def _now_matches_time(cond: dict) -> bool:
    try:
        now = datetime.utcnow()
        hhmm = str(cond.get("time") or "00:00")
        hh, mm = (int(hhmm.split(":")[0] or 0), int(hhmm.split(":")[1] or 0))
        # time window tolerance: same hour:minute
        if now.hour != hh or now.minute != mm:
            return False
        sched = cond.get("schedule") or {"kind": "daily"}
        kind = str(sched.get("kind") or "daily").lower()
        if kind == "weekly":
            dows = sched.get("dows") or []
            return now.weekday() in set(int(x) for x in dows)
        if kind == "monthly":
            doms = sched.get("doms") or []
            return now.day in set(int(x) for x in doms)
        return True
    except Exception:
        return True


def run_rule(db: Session, rule: AlertRule, *, force_time_ok: bool = False) -> Tuple[bool, str]:
    try:
        cfg = json.loads(rule.config_json or "{}")
    except Exception:
        return False, "Invalid config"
    triggers = cfg.get("triggers") or []
    actions = cfg.get("actions") or []
    ds_id = cfg.get("datasourceId")
    render = cfg.get("render") or {}

    # V2: triggersGroup support
    tg = cfg.get("triggersGroup") or None
    if isinstance(tg, dict):
        time_ok = False
        thr_ok = False
        # Time condition
        tcond = tg.get("time") or {}
        if tcond and tcond.get("enabled"):
            time_ok = _now_matches_time(tcond)
        else:
            time_ok = True  # if not enabled, ignore
        # Manual run: bypass time window if forced
        if force_time_ok:
            try:
                time_ok = True
            except Exception:
                time_ok = True
        # Threshold condition
        thr = tg.get("threshold") or {}
        if thr and thr.get("enabled"):
            try:
                thr_ok, kpi_value = evaluate_threshold(db, trigger=thr, datasource_id=ds_id)
            except Exception:
                thr_ok = False
        else:
            thr_ok = True
            kpi_value = None
        logic = str(tg.get("logic") or "AND").upper()
        fired = (time_ok and thr_ok) if logic == "AND" else (time_ok or thr_ok)
        if not fired:
            return True, "No trigger fired"
        # Continue to assemble and send as in v1 using computed kpi_value and render
        html_parts = [f"<div style='font-family:Inter,Arial,sans-serif;font-size:13px'>Rule: {_html_escape(rule.name)}</div>"]
        if render.get("mode") == "kpi":
            val = (kpi_value if (kpi_value is not None) else 0)
            label = render.get("label") or "KPI"
            html_parts.append(_render_kpi_html(val, label=label))
        elif render.get("mode") == "table":
            try:
                spec = render.get("querySpec") or {}
                req = QuerySpecRequest(spec=spec, datasourceId=ds_id, limit=spec.get("limit") or 1000, offset=0, includeTotal=False)
                res = run_query_spec(req, db)
                html_parts.append(_render_table_html(res))
            except Exception:
                html_parts.append("<div>Failed to render table.</div>")
        html = "\n".join(html_parts)
        # Dispatch
        errs: list[str] = []
        # Prepare inline images and replacements for template insert
        inline_images: list[tuple[str, bytes, str, str]] = []
        replacements_extra: dict[str, str] = {}
        try:
            # Preferred: real widget snapshot via headless embed if widgetRef present
            try:
                wref = (render or {}).get("widgetRef") or {}
                wid = (wref or {}).get("widgetId")
                did = (wref or {}).get("dashboardId") or getattr(rule, "dashboard_id", None)
                if wid:
                    w = int((render or {}).get("width") or 1000)
                    h = int((render or {}).get("height") or (280 if str((render or {}).get("mode") or "kpi") == "kpi" else 360))
                    th = str((render or {}).get("theme") or "dark")
                    # Prefer dashboard owner's user id for snapshot actor to enforce correct permissions
                    actor_for_snapshot = None
                    try:
                        if did:
                            drow = db.get(Dashboard, did)
                            if drow and getattr(drow, 'user_id', None):
                                actor_for_snapshot = drow.user_id
                    except Exception:
                        actor_for_snapshot = None
                    if not actor_for_snapshot:
                        actor_for_snapshot = getattr(settings, "snapshot_actor_id", None)
                    png = _fetch_snapshot_png_via_http(dashboard_id=did, public_id=None, token=None, widget_id=str(wid), datasource_id=ds_id, width=w, height=h, theme=th, actor_id=actor_for_snapshot)
                    if png:
                        w_cid = f"widget_{uuid4().hex[:8]}"
                        tag = f"<img alt='Widget' src='cid:{w_cid}' style='max-width:100%;height:auto'/>"
                        inline_images.append((w_cid, png, "image/png", "widget.png"))
                        # Use for both KPI and Chart tokens
                        replacements_extra["KPI_IMG"] = tag
                        replacements_extra["CHART_IMG"] = tag
                        html = html + f"\n<div style='margin-top:8px'>{tag}</div>"
            except Exception:
                pass
            if render.get("mode") == "kpi" and ("KPI_IMG" not in replacements_extra):
                val = (kpi_value if (kpi_value is not None) else 0)
                k_cid = f"kpi_{uuid4().hex[:8]}"
                svg = _build_kpi_svg(val, render.get("label") or "KPI")
                inline_images.append((k_cid, svg, "image/svg+xml", "kpi.svg"))
                # Also include the inline image in the HTML content for recipients without template tokens
                html = html + f"\n<div style='margin-top:8px'><img alt='KPI' src='cid:{k_cid}' style='max-width:100%;height:auto;border:1px solid #e5e7eb;border-radius:10px'/></div>"
                replacements_extra["KPI_IMG"] = f"<img alt='KPI' src='cid:{k_cid}' style='max-width:100%;height:auto'/>"
            elif render.get("mode") == "table":
                try:
                    spec = render.get("querySpec") or {}
                    req = QuerySpecRequest(spec=spec, datasourceId=ds_id, limit=spec.get("limit") or 1000, offset=0, includeTotal=False)
                    res = run_query_spec(req, db)
                    replacements_extra["TABLE_HTML"] = _render_table_html(res)
                except Exception:
                    replacements_extra["TABLE_HTML"] = "<div>Failed to render table.</div>"
            elif render.get("mode") == "chart" and ("CHART_IMG" not in replacements_extra):
                c_cid = f"chart_{uuid4().hex[:8]}"
                svg = _build_chart_svg_placeholder()
                inline_images.append((c_cid, svg, "image/svg+xml", "chart.svg"))
                html = html + f"\n<div><img alt='Chart' src='cid:{c_cid}' style='max-width:100%;height:auto;border:1px solid #e5e7eb;border-radius:10px'/></div>"
                replacements_extra["CHART_IMG"] = f"<img alt='Chart' src='cid:{c_cid}' style='max-width:100%;height:auto'/>"
        except Exception:
            pass

        # Build token context and apply email inserts (single or multi-legend) so send matches preview
        # Threshold base for tokens
        thr_ctx = thr if isinstance(thr, dict) else {}
        try:
            op = str(thr_ctx.get("operator") or "").strip()
            val = thr_ctx.get("value")
            thr_low = val[0] if isinstance(val, list) and len(val) >= 2 else None
            thr_high = val[1] if isinstance(val, list) and len(val) >= 2 else None
            thr_raw = thr_low if thr_low is not None else val
            agg = str((thr_ctx.get("aggregator") or "")).lower()
            measure = thr_ctx.get("measure") or thr_ctx.get("y")
            x_field = thr_ctx.get("xField")
            x_val = thr_ctx.get("xValue")
            x_mode = thr_ctx.get("xMode")
            filters_obj = dict(thr_ctx.get("where") or {})
            filters_h = "; ".join([f"{k}={( '|'.join(map(str,v)) if isinstance(v, list) else v)}" for k, v in filters_obj.items()])
            # Build values-only list and HTML chips (match evaluate-v2)
            def _only_values(obj: dict[str, Any]) -> list[str]:
                out: list[str] = []
                try:
                    for _, vv in (obj or {}).items():
                        if isinstance(vv, list):
                            for it in vv:
                                if it is None: continue
                                out.append(str(it))
                        elif vv is not None:
                            out.append(str(vv))
                except Exception:
                    pass
                return out
            _vals = _only_values(filters_obj)
            _vals_html = " ".join([f"<span style='display:inline-block;border:1px solid #e5e7eb;border-radius:999px;padding:2px 8px;margin:1px 2px;font-size:11px'>{v}</span>" for v in _vals])
            # x pick helpers
            def _resolve_x_value() -> str:
                try:
                    if str(x_mode or "").lower() == "custom":
                        return "" if (x_val is None or x_val == "") else str(x_val)
                    if str(x_mode or "").lower() == "range":
                        xr = thr_ctx.get("xRange") or {}
                        return f"{xr.get('from') or ''}..{xr.get('to') or ''}"
                    if str(x_mode or "").lower() == "token":
                        tok = str(thr_ctx.get("xToken") or "").lower()
                        now = datetime.utcnow()
                        if tok == "today":
                            return now.date().isoformat()
                        if tok == "yesterday":
                            return (now.date() - timedelta(days=1)).isoformat()
                        if tok == "this_month":
                            return now.date().isoformat()[:7]
                        return tok
                    if str(x_mode or "").lower() == "special":
                        return str(thr_ctx.get("xSpecial") or "")
                    return ""
                except Exception:
                    return ""
            x_value_resolved = _resolve_x_value()
            # assemble base tokens
            ctx_tokens: dict[str, str] = {
                "kpi": "" if kpi_value is None else str(kpi_value),
                "kpi_fmt": "" if kpi_value is None else _fmt_num(kpi_value, 0),
                "operator": op,
                "threshold": "" if thr_raw is None else str(thr_raw),
                "threshold_low": "" if thr_low is None else str(thr_low),
                "threshold_high": "" if thr_high is None else str(thr_high),
                "agg": agg,
                "measure": "" if measure is None else str(measure),
                "xField": "" if x_field is None else str(x_field),
                "xValue": "" if x_val is None else str(x_val),
                "xPick": "" if (x_mode is None) else str(x_mode),
                "legend": "",
                "category": "",
                "xValueResolved": x_value_resolved,
                "xValuePretty": (lambda r: (f"{r[8:10]}/{r[5:7]}/{r[0:4]}" if (isinstance(r, str) and len(r)>=10 and r[4]=='-' and r[7]=='-') else (f"{['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][int(r[5:7])-1]}-{r[0:4]}" if (isinstance(r, str) and len(r)>=7 and r[4]=='-') else (r or ""))))(x_value_resolved),
                "filters": filters_h,
                "filters_json": json.dumps(filters_obj or {}),
                "filters_values": " | ".join(_vals),
                "filters_values_html": _vals_html,
                "source": str(thr_ctx.get("source") or cfg.get("source") or ""),
                "datasourceId": str(ds_id or ""),
                "alertName": rule.name or "",
                "runAt": _fmt_run_at(datetime.utcnow()),
            }
            # carry over widget image/table replacements
            for k, v in (replacements_extra or {}).items():
                try:
                    ctx_tokens[k] = str(v)
                except Exception:
                    pass
            # Email insert(s)
            email_insert = str(cfg.get("template") or "")
            legend_field = thr_ctx.get("legendField")
            want_multi = False
            if email_insert and legend_field:
                try:
                    has_leg = any(((k == legend_field) or str(k).startswith(f"{legend_field}__")) for k in (filters_obj or {}).keys())
                    want_multi = not has_leg
                except Exception:
                    want_multi = False
            # If multi, fetch per-legend values and append cards
            if want_multi:
                try:
                    src = str(thr_ctx.get("source") or cfg.get("source") or "").strip()
                    if src:
                        where_fb = dict(filters_obj)
                        # Apply x token/range gating similar to evaluation
                        try:
                            mode = str(x_mode or "").lower()
                            if x_field and mode == "token":
                                tok = str(thr_ctx.get("xToken") or "").lower()
                                today = datetime.utcnow().date()
                                if tok == "today":
                                    s = today; e = today + timedelta(days=1)
                                    where_fb[f"{x_field}__gte"] = s.isoformat()
                                    where_fb[f"{x_field}__lt"] = e.isoformat()
                                elif tok == "yesterday":
                                    e = today; s = today - timedelta(days=1)
                                    where_fb[f"{x_field}__gte"] = s.isoformat()
                                    where_fb[f"{x_field}__lt"] = e.isoformat()
                                elif tok == "this_month":
                                    s = datetime(today.year, today.month, 1).date()
                                    nm = (today.month + 1) if today.month < 12 else 1
                                    ny = today.year + 1 if nm == 1 else today.year
                                    e = datetime(ny, nm, 1).date()
                                    where_fb[f"{x_field}__gte"] = s.isoformat()
                                    where_fb[f"{x_field}__lt"] = e.isoformat()
                            elif x_field and mode == "range":
                                xr = thr_ctx.get("xRange") or {}
                                if xr.get("from"): where_fb[f"{x_field}__gte"] = xr.get("from")
                                if xr.get("to"): where_fb[f"{x_field}__lte"] = xr.get("to")
                            elif x_field and mode == "custom" and (x_val not in (None, "")):
                                where_fb[str(x_field)] = [x_val]
                        except Exception:
                            pass
                        spec_cat: dict[str, Any] = {"source": src, "agg": (agg or "count"), "legend": legend_field, "where": (where_fb or None)}
                        spec_cat["x"] = x_field or legend_field
                        if (agg or "count") != "count" and (measure or ""):
                            spec_cat["y"] = measure; spec_cat["measure"] = measure
                        res_cat = run_query_spec(QuerySpecRequest(spec=spec_cat, datasourceId=ds_id, limit=10000, offset=0, includeTotal=False), db)
                        cols = [str(c) for c in (res_cat.columns or [])]
                        rows = list(res_cat.rows or [])
                        cols_l = [c.lower() for c in cols]
                        # Identify indices for legend/x/value
                        try:
                            vi = cols_l.index("value") if "value" in cols_l else None
                        except Exception:
                            vi = None
                        try:
                            leg_i = cols_l.index("legend") if "legend" in cols_l else (cols.index(legend_field) if legend_field in cols else None)
                        except Exception:
                            leg_i = None
                        try:
                            xi = cols_l.index("x") if "x" in cols_l else (cols.index(x_field) if (x_field and x_field in cols) else None)
                        except Exception:
                            xi = None
                        # Apply x-dimension match only when not token/range (match evaluate-v2)
                        apply_x_match = not (str(x_mode or "").lower() in ("token", "range"))
                        sel_key = str(x_value_resolved or "").strip()
                        def _x_matches(xv: Any) -> bool:
                            try:
                                if not sel_key:
                                    return True
                                s = str(xv or "")
                                # Simple equality for custom/special
                                return s == sel_key
                            except Exception:
                                return True
                        cards: list[str] = []
                        # Track best value per legend to avoid multiple cards per legend
                        best_by_legend: dict[str, float] = {}
                        def _passes(op_val: str | None, x: float, lo: Optional[float], hi: Optional[float]) -> bool:
                            try:
                                if op_val == "between" and lo is not None and hi is not None:
                                    return (x >= min(lo, hi)) and (x <= max(lo, hi))
                                if op_val == ">": return x > (lo if lo is not None else 0)
                                if op_val == ">=": return x >= (lo if lo is not None else 0)
                                if op_val == "<": return x < (lo if lo is not None else 0)
                                if op_val == "<=": return x <= (lo if lo is not None else 0)
                                if op_val == "==": return (lo is not None) and (x == lo)
                                return True
                            except Exception:
                                return False
                        for r in rows:
                            try:
                                # X filter
                                if apply_x_match and (xi is not None):
                                    try:
                                        xv = r[xi] if isinstance(r, (list, tuple)) else (r.get(cols[xi]) if isinstance(r, dict) else None)
                                    except Exception:
                                        xv = None
                                    if not _x_matches(xv):
                                        continue
                                if isinstance(r, (list, tuple)):
                                    cat = (r[leg_i] if (leg_i is not None and leg_i < len(r)) else None)
                                    v_num = None
                                    if vi is not None and vi < len(r) and isinstance(r[vi], (int, float)):
                                        v_num = float(r[vi])
                                    else:
                                        for cell in r:
                                            if isinstance(cell, (int, float)):
                                                v_num = float(cell); break
                                elif isinstance(r, dict):
                                    cat = r.get("legend", r.get(legend_field))
                                    v0 = r.get("value")
                                    v_num = float(v0) if isinstance(v0, (int, float)) else None
                                    if v_num is None:
                                        for cell in r.values():
                                            if isinstance(cell, (int, float)):
                                                v_num = float(cell); break
                                else:
                                    continue
                                if v_num is None: continue
                                if not _passes(op, v_num, (float(thr_low) if thr_low is not None else (float(thr_raw) if thr_raw is not None else None)), (float(thr_high) if thr_high is not None else None)):
                                    continue
                                # Track best per legend
                                cat_key = "" if cat is None else str(cat)
                                try:
                                    if (cat_key not in best_by_legend) or (v_num > best_by_legend[cat_key]):
                                        best_by_legend[cat_key] = float(v_num)
                                except Exception:
                                    best_by_legend[cat_key] = float(v_num)
                            except Exception:
                                continue
                        # Compose one card per legend using best value
                        if best_by_legend:
                            try:
                                for cat_key, v_num in best_by_legend.items():
                                    ctx_tokens["legend"] = cat_key
                                    ctx_tokens["kpi"] = str(v_num)
                                    ctx_tokens["kpi_fmt"] = _fmt_num(v_num, 0)
                                    filled = _apply_placeholders(email_insert, ctx_tokens)
                                    cards.append(f"<div class='card'>{filled}</div>")
                            except Exception:
                                pass
                        if cards:
                            html = html + "\n" + "\n".join(cards)
                except Exception:
                    pass
            elif email_insert:
                try:
                    filled = _apply_placeholders(email_insert, ctx_tokens)
                    html = html + f"\n<div>{filled}</div>"
                except Exception:
                    pass
        except Exception:
            # If context building fails, continue with base HTML
            ctx_tokens = {}

        # Merge replacements for final send (so {{KPI_IMG}}/etc and tokens resolve inside base template)
        replacements_all = {}
        try:
            replacements_all.update(replacements_extra or {})
            replacements_all.update(ctx_tokens or {})
        except Exception:
            replacements_all = replacements_extra or {}

        for a in actions:
            at = str(a.get("type") or "").lower()
            if at == "email":
                to = [str(x).strip() for x in (a.get("to") or []) if str(x).strip()]
                subj = str(a.get("subject") or rule.name)
                ok, err = send_email(db, subject=subj, to=to, html=html, replacements=replacements_all, inline_images=inline_images)
                if not ok and err:
                    errs.append(f"email: {err}")
            elif at == "sms":
                to = [str(x).strip() for x in (a.get("to") or []) if str(x).strip()]
                text = str(a.get("message") or rule.name)
                try:
                    # Fill placeholders in SMS using same tokens
                    text = _apply_placeholders(text, ctx_tokens)
                except Exception:
                    pass
                ok, err = send_sms_hadara(db, to_numbers=to, message=text)
                if not ok and err:
                    errs.append(f"sms: {err}")
        return (len(errs) == 0), ("; ".join(errs) if errs else "ok")

    # V1: If any threshold trigger passes or time trigger invoked, send actions
    fired = False
    kpi_value: Optional[float] = None
    for t in triggers:
        ttype = str(t.get("type") or "").lower()
        if ttype == "threshold":
            ok, k = evaluate_threshold(db, trigger=t, datasource_id=ds_id)
            kpi_value = k
            if ok:
                fired = True
                break
        elif ttype == "time":
            # Time trigger: assume scheduler invoked us at the correct time
            fired = True
            break
    if not fired:
        return True, "No trigger fired"

    # Build HTML body based on render.mode
    html_parts = [f"<div style='font-family:Inter,Arial,sans-serif;font-size:13px'>Rule: {_html_escape(rule.name)}</div>"]
    if render.get("mode") == "table":
        try:
            spec = render.get("querySpec") or {}
            req = QuerySpecRequest(spec=spec, datasourceId=ds_id, limit=spec.get("limit") or 1000, offset=0, includeTotal=False)
            res = run_query_spec(req, db)
            parts = [_render_table_html(res)]
        except Exception:
            parts = ["<div>Failed to render table.</div>"]
    elif render.get("mode") == "kpi":
        kpi_label = render.get("label") or "KPI"
        render_mode_is_kpi = True
        val = kpi_value if (kpi_value is not None) else 0
        parts = [_render_kpi_html(val, label=kpi_label)]
        try:
            svg = _build_kpi_svg(val, kpi_label)
            parts.append(f"<div style='margin-top:8px'><img alt='KPI' src='{_to_svg_data_uri(svg)}' style='max-width:100%;height:auto;border:1px solid #e5e7eb;border-radius:10px'/></div>")
        except Exception:
            pass
    else:
        try:
            c_cid = f"chart_{uuid4().hex[:8]}"
            svg = _build_chart_svg_placeholder()
            html_parts.append(f"<div><img alt='Chart' src='cid:{c_cid}' style='max-width:100%;height:auto;border:1px solid #e5e7eb;border-radius:10px'/></div>")
        except Exception:
            html_parts.append("<div>Chart rendering not yet available.</div>")

    # Compute placeholders
    dash_name = None
    if rule.dashboard_id:
        try:
            d = db.get(Dashboard, rule.dashboard_id)
            dash_name = d.name if d else None
        except Exception:
            dash_name = None
    now = datetime.utcnow()
    placeholders: dict[str, str] = {
        "alertName": rule.name or "",
        "dashboardName": dash_name or "",
        "runAt": _fmt_run_at(now),
        "range": str(cfg.get("range") or "current period"),
        "kpi": ("" if kpi_value is None else str(kpi_value)),
    }

    # Add template tokens for widget placeholders
    try:
        # If KPI/chart images were added above, include their tokens
        if any("src='cid:kpi_" in s for s in html_parts):
            try:
                k_match = next((seg for seg in html_parts if "src='cid:kpi_" in seg), None)
                if k_match:
                    cid_start = k_match.split("cid:",1)[1]
                    k_cid = cid_start.split("'",1)[0]
                    placeholders["KPI_IMG"] = f"<img alt='KPI' src='cid:{k_cid}' style='max-width:100%;height:auto'/>"
            except Exception:
                pass
        if any("src='cid:chart_" in s for s in html_parts):
            try:
                c_match = next((seg for seg in html_parts if "src='cid:chart_" in seg), None)
                if c_match:
                    cid_start = c_match.split("cid:",1)[1]
                    c_cid = cid_start.split("'",1)[0]
                    placeholders["CHART_IMG"] = f"<img alt='Chart' src='cid:{c_cid}' style='max-width:100%;height:auto'/>"
            except Exception:
                pass
        if render.get("mode") == "table":
            try:
                spec = render.get("querySpec") or {}
                req = QuerySpecRequest(spec=spec, datasourceId=ds_id, limit=spec.get("limit") or 1000, offset=0, includeTotal=False)
                res = run_query_spec(req, db)
                placeholders["TABLE_HTML"] = _render_table_html(res)
            except Exception:
                placeholders["TABLE_HTML"] = "<div>Failed to render table.</div>"
    except Exception:
        pass

    # Append template if provided (admin-provided HTML). Allow placeholders.
    template = cfg.get("template") or None
    if template:
        try:
            t = _apply_placeholders(str(template), placeholders)
            html_parts.append(f"<div>{t}</div>")
        except Exception:
            pass

    html = "\n".join(html_parts)

    # Dispatch actions (attach inline images for KPI/Chart if present)
    errs: list[str] = []
    inline_images: list[tuple[str, bytes, str, str]] = []
    try:
        # Extract any inlined CIDs we added and attach corresponding images
        if any("src='cid:kpi_" in s for s in html_parts):
            try:
                val = kpi_value if (kpi_value is not None) else 0
                label = render.get("label") or "KPI"
                k_cid = next(seg.split("cid:",1)[1].split("'",1)[0] for seg in html_parts if "src='cid:kpi_" in seg)
                inline_images.append((k_cid, _build_kpi_svg(val, label), "image/svg+xml", "kpi.svg"))
            except Exception:
                pass
        if any("src='cid:chart_" in s for s in html_parts):
            try:
                c_cid = next(seg.split("cid:",1)[1].split("'",1)[0] for seg in html_parts if "src='cid:chart_" in seg)
                inline_images.append((c_cid, _build_chart_svg_placeholder(), "image/svg+xml", "chart.svg"))
            except Exception:
                pass
    except Exception:
        pass
    for a in actions:
        atype = str(a.get("type") or "").lower()
        if atype == "email":
            to = [str(x).strip() for x in (a.get("to") or []) if str(x).strip()]
            raw_subject = str(a.get("subject") or rule.name)
            subj = _apply_placeholders(raw_subject, placeholders)
            ok, err = send_email(db, subject=subj, to=to, html=html, replacements=placeholders, inline_images=inline_images)
            if not ok and err:
                errs.append(f"email: {err}")
        elif atype == "sms":
            to = [str(x).strip() for x in (a.get("to") or []) if str(x).strip()]
            text = str(a.get("message") or (template or rule.name))
            ok, err = send_sms_hadara(db, to_numbers=to, message=text)
            if not ok and err:
                errs.append(f"sms: {err}")

    return (len(errs) == 0), ("; ".join(errs) if errs else "ok")
