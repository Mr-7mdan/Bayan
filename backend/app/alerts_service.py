from __future__ import annotations

def _fetch_snapshot_png_direct(*, dashboard_id: Optional[str], public_id: Optional[str], token: Optional[str], widget_id: str, datasource_id: Optional[str], width: int, height: int, theme: str, actor_id: Optional[str], wait_ms: int = 4000, retries: int = 0, backoff_sec: float = 0.5) -> Optional[bytes]:
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception:
        return None
    base_f = (settings.frontend_base_url or "http://localhost:3000").rstrip("/")
    qs: dict[str, str] = {
        "widgetId": str(widget_id),
        "w": str(int(width)),
        "h": str(int(height)),
        "theme": str(theme or "dark"),
        "bg": "transparent",
        "snap": "1",
    }
    if datasource_id:
        qs["datasourceId"] = str(datasource_id)
    if dashboard_id:
        qs["dashboardId"] = str(dashboard_id)
        if actor_id:
            qs["actorId"] = str(actor_id)
    elif public_id:
        qs["publicId"] = str(public_id)
        if token:
            qs["token"] = str(token)
    else:
        return None
    url = f"{base_f}/render/embed/widget?{urlencode(qs)}"
    last_err: Exception | None = None
    for attempt in range(0, max(0, int(retries)) + 1):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch()
                try:
                    is_dark = str(theme or "dark").lower() == "dark"
                    context = browser.new_context(viewport={"width": int(width), "height": int(height)}, device_scale_factor=2, color_scheme=("dark" if is_dark else "light"), reduced_motion="reduce")
                    # Initialize theme and patch ECharts to disable animations, prefer SVG renderer
                    try:
                        context.add_init_script(
                            (
                                "try {\n"
                                f"  localStorage.setItem('theme', '{'dark' if is_dark else 'light'}');\n"
                                "  const r = document.documentElement;\n"
                                f"  if ({'true' if is_dark else 'false'}) r.classList.add('dark'); else r.classList.remove('dark');\n"
                                "} catch (e) {}\n"
                            )
                        )
                    except Exception:
                        pass
                    try:
                        context.add_init_script(
                            (
                                "(() => {\n"
                                "  try {\n"
                                "    try { const st = document.createElement('style'); st.innerHTML = '*{animation:none!important;transition:none!important}'; document.head.appendChild(st); } catch(e) {}\n"
                                "    const applyPatch = (echarts) => {\n"
                                "      try {\n"
                                "        if (!echarts || echarts.__snapPatched) return;\n"
                                "        echarts.__snapPatched = true;\n"
                                "        const _init = echarts.init.bind(echarts);\n"
                                "        echarts.init = function(dom, theme, opts) {\n"
                                "          try { opts = Object.assign({}, opts||{}, { renderer: 'svg' }); } catch(e) {}\n"
                                "          const inst = _init(dom, theme, opts);\n"
                                "          const _set = inst.setOption.bind(inst);\n"
                                "          inst.setOption = function(opt, ...rest) {\n"
                                "            try {\n"
                                "              if (opt) {\n"
                                "                opt.animation = false; opt.animationDuration = 0; opt.animationDurationUpdate = 0;\n"
                                "                if (Array.isArray(opt.series)) {\n"
                                "                  opt.series = opt.series.map((s) => ({ ...s, animation:false, animationDuration:0, animationDurationUpdate:0, progressive:0, progressiveThreshold:0 }));\n"
                                "                }\n"
                                "              }\n"
                                "            } catch(e) {}\n"
                                "            return _set(opt, ...rest);\n"
                                "          };\n"
                                "          return inst;\n"
                                "        };\n"
                                "      } catch(e) {}\n"
                                "    };\n"
                                "    Object.defineProperty(window, 'echarts', {\n"
                                "      configurable: true,\n"
                                "      get() { return this.__echarts__; },\n"
                                "      set(v) { this.__echarts__ = v; try { applyPatch(v) } catch(e) {} },\n"
                                "    });\n"
                                "    document.addEventListener('DOMContentLoaded', () => { try { applyPatch(window.echarts) } catch(e) {} });\n"
                                "  } catch(e) {}\n"
                                "})();\n"
                            )
                        )
                    except Exception:
                        pass
                    page = context.new_page()
                    try:
                        page.emulate_media(color_scheme=("dark" if is_dark else "light"))
                    except Exception:
                        pass
                    try:
                        page.set_default_navigation_timeout(15000)
                    except Exception:
                        pass
                    page.goto(url, wait_until="domcontentloaded")
                    try:
                        page.wait_for_function(
                            "() => { const root = document.getElementById('widget-root'); if (!root) return false; const wd = (window.__READY__ === true); const chartOk = (root.getAttribute('data-chart-ready') === '1'); return wd && chartOk; }",
                            timeout=wait_ms,
                        )
                    except Exception:
                        try:
                            page.wait_for_selector("#widget-root[data-widget-ready='1']", timeout=wait_ms)
                        except Exception:
                            try:
                                page.wait_for_selector("#widget-root", timeout=wait_ms)
                            except Exception:
                                pass
                    # Wait for quiescence: data-chart-finished-at stable for >= 1000ms
                    try:
                        page.wait_for_function(
                            "() => {\n"
                            "  const root = document.getElementById('widget-root');\n"
                            "  if (!root) return false;\n"
                            "  const t = Number(root.getAttribute('data-chart-finished-at') || '0');\n"
                            "  if (!t) return false;\n"
                            "  const now = (typeof performance!== 'undefined' && performance && typeof performance.now==='function') ? performance.now() : Date.now();\n"
                            "  return (now - t) >= 1000;\n"
                            "}",
                            timeout=2000,
                        )
                    except Exception:
                        pass
                    # Post-ready settle
                    try:
                        page.wait_for_function("() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)))", timeout=400)
                    except Exception:
                        pass
                    try:
                        page.wait_for_timeout(600)
                    except Exception:
                        pass
                    el = page.query_selector("#widget-root")
                    if el:
                        png = el.screenshot(type="png", omit_background=True)
                    else:
                        png = page.screenshot(type="png", full_page=False, omit_background=True)
                    context.close()
                    return png
                finally:
                    browser.close()
        except Exception as e:
            last_err = e
            try:
                logger.warning("snapshot sync attempt %s failed for %s: %s", attempt + 1, url, getattr(e, "message", str(e)))
            except Exception:
                pass
            if attempt < int(retries):
                try:
                    import time as _t
                    _t.sleep(max(0.0, float(backoff_sec) * (attempt + 1)))
                except Exception:
                    pass
            else:
                break
    return None
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
import re as _re
import logging
import time
import smtplib
from email.message import EmailMessage
from typing import Any, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import urlopen
import base64
from uuid import uuid4
import xml.etree.ElementTree as ET

from sqlalchemy.orm import Session
from .metrics import counter_inc

from .models import AlertRule, EmailConfig, SmsConfigHadara, Dashboard
from .security import decrypt_text
from .routers.query import run_query_spec
from .schemas import QuerySpecRequest, QueryResponse
from datetime import datetime, timedelta
from .config import settings
from pathlib import Path

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

def send_email(db: Session, *, subject: str, to: list[str], html: str, replacements: Optional[dict[str, str]] = None, inline_images: Optional[list[tuple[str, bytes, str, str]]] = None, already_wrapped: bool = False) -> Tuple[bool, Optional[str]]:
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
        # If caller provided a full HTML document, optionally bypass base template wrapping
        final_html = html if already_wrapped or (str(html).lstrip().lower().startswith("<!doctype") or str(html).lstrip().lower().startswith("<html")) else _apply_base_template(cfg, subject, html)
        # Apply extra placeholders after wrapping
        if replacements:
            # Be tolerant: case-insensitive keys and whitespace inside braces
            # Also duplicate common variants (TABLE_HTML vs table_html)
            try:
                reps = dict(replacements)
                if 'TABLE_HTML' in reps and 'table_html' not in reps:
                    reps['table_html'] = reps['TABLE_HTML']
                for k, v in reps.items():
                    try:
                        final_html = final_html.replace("{{" + k + "}}", v)
                    except Exception:
                        pass
                for k, v in reps.items():
                    try:
                        final_html = _re.sub(r"\{\{\s*" + _re.escape(k) + r"\s*\}\}", str(v), final_html, flags=_re.IGNORECASE)
                    except Exception:
                        pass
            except Exception:
                pass
        # Remove known unreplaced tokens (case-insensitive) to avoid showing {{CHART_IMG}} in emails
        try:
            for k in ("CHART_IMG", "KPI_IMG", "TABLE_HTML", "chart_img", "kpi_img", "table_html"):
                try:
                    final_html = final_html.replace("{{" + k + "}}", "")
                except Exception:
                    pass
                try:
                    final_html = _re.sub(r"\{\{\s*" + _re.escape(k) + r"\s*\}\}", "", final_html, flags=_re.IGNORECASE)
                except Exception:
                    pass
        except Exception:
            pass
        # Inline logo: ensure any referenced logo becomes a data URI
        inline_images_buf: list[tuple[str, bytes, str, str]] = []
        try:
            lu = (cfg.logo_url or "").strip()
            inline_logo: str | None = None
            if lu.startswith("data:") and ";base64," in lu:
                inline_logo = lu
            elif lu.lower().startswith("http://") or lu.lower().startswith("https://"):
                try:
                    with urlopen(lu, timeout=15) as resp:  # nosec B310
                        data = resp.read()
                        ct = (resp.headers.get("content-type") or "image/png").split(";")[0]
                    inline_logo = f"data:{ct};base64,{base64.b64encode(data).decode('ascii')}"
                    final_html = final_html.replace(lu, inline_logo)
                except Exception:
                    inline_logo = None
            # If still not set, try to inline /logo.svg from frontend
            if inline_logo is None and ("/logo.svg" in final_html or _re.search(r"originalsrc\s*=\s*['\"]/logo\.svg['\"]", final_html or "")):
                try:
                    base_f = (settings.frontend_base_url or "http://localhost:3000").rstrip("/")
                    absu = f"{base_f}/logo.svg"
                    with urlopen(absu, timeout=15) as resp:  # nosec B310
                        data = resp.read()
                        ct = (resp.headers.get("content-type") or "image/svg+xml").split(";")[0]
                    inline_logo = f"data:{ct};base64,{base64.b64encode(data).decode('ascii')}"
                except Exception:
                    inline_logo = None
            # If still not set, try branding logoLight from metadata
            if inline_logo is None:
                try:
                    data_dir = Path(settings.metadata_db_path).resolve().parent
                    f = data_dir / "branding.json"
                    if f.exists():
                        import json as _json
                        obj = _json.loads(f.read_text(encoding="utf-8") or "{}")
                        ll = (obj.get("logoLight") or "").strip()
                        if ll.startswith("data:") and ";base64," in ll:
                            inline_logo = ll
                        elif ll.lower().startswith("http://") or ll.lower().startswith("https://"):
                            try:
                                with urlopen(ll, timeout=15) as resp:  # nosec B310
                                    data = resp.read()
                                    ct = (resp.headers.get("content-type") or "image/png").split(";")[0]
                                inline_logo = f"data:{ct};base64,{base64.b64encode(data).decode('ascii')}"
                            except Exception:
                                inline_logo = None
                except Exception:
                    inline_logo = None
            # Apply inline logo replacement for any '/logo.svg' or missing 'src' variants
            if inline_logo:
                try:
                    final_html = final_html.replace("src='/logo.svg'", f"src='{inline_logo}'").replace('src="/logo.svg"', f'src="{inline_logo}"')
                    final_html = _re.sub(r"src\s*=\s*(['\"])\/logo\.svg\1", f"src='{inline_logo}'", final_html, flags=_re.IGNORECASE)
                    # Remove any originalsrc (Outlook, etc.) and ensure src is present for alt='Logo'
                    final_html = _re.sub(r"originalsrc\s*=\s*(['\"])\/logo\.svg\1", "", final_html, flags=_re.IGNORECASE)
                    final_html = _re.sub(r"originalsrc\s*=\s*(['\"][^'\"]+['\"])", "", final_html, flags=_re.IGNORECASE)
                    def _ensure_logo_src(m):
                        tag = m.group(0)
                        try:
                            if _re.search(r"\bsrc\s*=", tag, flags=_re.IGNORECASE):
                                return tag
                            return tag[:-1] + f" src='{inline_logo}'>"
                        except Exception:
                            return tag
                    final_html = _re.sub(r"<img\b[^>]*\balt=([\'\"])Logo\1[^>]*>", _ensure_logo_src, final_html, flags=_re.IGNORECASE)
                    # Convert inline data URI logo into CID so email clients that block data URIs still render it
                    if inline_logo.startswith('data:'):
                        try:
                            _m = _re.match(r"data:([^;]+);base64,(.+)$", inline_logo, flags=_re.IGNORECASE)
                            if _m:
                                _mime = (_m.group(1) or 'image/png').strip()
                                _b64 = _m.group(2)
                                _bytes = base64.b64decode(_b64)
                                _cid = f"logo_{uuid4().hex}"
                                inline_images_buf.append((_cid, _bytes, _mime, 'logo'))
                                # Replace any occurrence of the data URI (or alt='Logo' img) with CID reference
                                final_html = final_html.replace(inline_logo, f"cid:{_cid}")
                                final_html = _re.sub(r"(<img\b[^>]*\balt=([\'\"])Logo\2[^>]*\bsrc=)([\'\"])data:[^>]+?\3", r"\\1'cid:" + _cid + r"'", final_html, flags=_re.IGNORECASE)
                                if "/logo.svg" in final_html:
                                    final_html = final_html.replace("/logo.svg", f"cid:{_cid}")
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass
        msg.add_alternative(final_html, subtype="html")
        # Attach inline images (CID) if provided; mark as inline and avoid filename to reduce attachment previews
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
                    try:
                        target.add_related(data, maintype=maintype, subtype=subtype, cid=cid, disposition='inline')
                    except Exception:
                        target.add_related(data, maintype=maintype, subtype=subtype, cid=cid)
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
    logo = f"<img src='{lu}' alt='Logo' style='height:40px;display:block' height='40'/>" if lu else ""
    return f"""
<!doctype html>
<html>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>{{subject}}</title>
  <style>
    body{{margin:0;padding:0;color:#111827;font-family:Inter,Arial,sans-serif;}}
    .wrap{{width:100%;padding:24px 0;}}
    .container{{max-width:640px;margin:0 auto;border:1px solid #e5e7eb;border-radius:12px;box-shadow:0 1px 2px rgba(0,0,0,0.04);overflow:hidden;}}
    .header{{padding:16px 20px;border-bottom:1px solid #e5e7eb;display:flex;align-items:center;gap:12px;}}
    .brand{{font-size:14px;font-weight:600;color:#111827;}}
    .content{{padding:20px;}}
    .footer{{padding:16px 20px;border-top:1px solid #e5e7eb;color:#6b7280;font-size:12px;}}
    table{{border-collapse:collapse;width:100%}}
    th,td{{border:1px solid #e5e7eb;padding:6px;text-align:left;}}
    thead th{{background:#f3f4f6;}}
    tbody tr:nth-child(even){{background:#f9fafb;}}
  </style>
  <link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap' rel='stylesheet'>
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
    # Prefer branding logoLight for always-light emails; fall back to email config logoUrl
    branding_logo = None
    try:
        data_dir = Path(settings.metadata_db_path).resolve().parent
        f = data_dir / "branding.json"
        if f.exists():
            import json as _json
            obj = _json.loads(f.read_text(encoding="utf-8") or "{}")
            branding_logo = (obj.get("logoLight") or None)
    except Exception:
        branding_logo = None
    logo_src = (branding_logo or (cfg.logo_url or ""))
    # Resolve/inline logo: http(s) or relative '/...' via frontend_base_url
    try:
        if isinstance(logo_src, str) and logo_src:
            if logo_src.lower().startswith("http://") or logo_src.lower().startswith("https://"):
                with urlopen(logo_src, timeout=15) as resp:  # nosec B310
                    data = resp.read()
                    ct = (resp.headers.get("content-type") or "image/png").split(";")[0]
                import base64 as _b64
                logo_src = f"data:{ct};base64,{_b64.b64encode(data).decode('ascii')}"
            elif logo_src.startswith("/"):
                base_f = (settings.frontend_base_url or "http://localhost:3000").rstrip("/")
                absu = f"{base_f}{logo_src}"
                try:
                    with urlopen(absu, timeout=15) as resp:  # nosec B310
                        data = resp.read()
                        ct = (resp.headers.get("content-type") or "image/png").split(";")[0]
                    import base64 as _b64
                    logo_src = f"data:{ct};base64,{_b64.b64encode(data).decode('ascii')}"
                except Exception:
                    # leave as relative if fetch fails
                    pass
    except Exception:
        pass
    tpl = (cfg.base_template_html or "").strip() or _default_base_template(logo_src)
    # Replace tokens with tolerance for whitespace inside braces
    try:
        repl = {"content": body_html, "subject": subject or "", "logoUrl": logo_src, "year": str(datetime.utcnow().year)}
        out = _apply_placeholders(tpl, repl)
    except Exception:
        # Fallback to direct replace if regex helper fails
        out = tpl.replace("{{content}}", body_html)
        out = out.replace("{{subject}}", subject or "")
        out = out.replace("{{logoUrl}}", logo_src)
        out = out.replace("{{year}}", str(datetime.utcnow().year))
    # If base template hardcodes a relative logo path, replace it with computed/inlined logo_src or default /logo.svg inlined from frontend
    try:
        if isinstance(out, str):
            inline_val = None
            if isinstance(logo_src, str) and logo_src:
                inline_val = logo_src
            else:
                # Try fetching /logo.svg from frontend to inline
                try:
                    base_f = (settings.frontend_base_url or "http://localhost:3000").rstrip("/")
                    absu = f"{base_f}/logo.svg"
                    with urlopen(absu, timeout=15) as resp:  # nosec B310
                        data = resp.read()
                        ct = (resp.headers.get("content-type") or "image/svg+xml").split(";")[0]
                    import base64 as _b64
                    inline_val = f"data:{ct};base64,{_b64.b64encode(data).decode('ascii')}"
                except Exception:
                    inline_val = None
            if inline_val:
                out = out.replace("src='/logo.svg'", f"src='{inline_val}'").replace('src="/logo.svg"', f'src="{inline_val}"')
                try:
                    # Regex variants: allow spaces and no quotes
                    out = _re.sub(r"src\s*=\s*(['\"])\/logo\.svg\1", f"src='{inline_val}'", out, flags=_re.IGNORECASE)
                    out = _re.sub(r"src\s*=\s*\/logo\.svg(?![\w])", f"src='{inline_val}'", out, flags=_re.IGNORECASE)
                except Exception:
                    pass
    except Exception:
        pass
    return out


def send_sms_hadara(db: Session, *, to_numbers: list[str], message: str) -> Tuple[bool, Optional[str]]:
    cfg: SmsConfigHadara | None = db.query(SmsConfigHadara).first()
    if not cfg or not cfg.api_key_encrypted:
        return False, "SMS provider is not configured"
    api_key = decrypt_text(cfg.api_key_encrypted or "") or ""
    base = "http://smsservice.hadara.ps:4545/SMS.ashx/bulkservice/sessionvalue/sendmessage/"
    try:
        for p in to_numbers:
            params = {
                "apikey": api_key,
                "to": p,
                "msg": message,
            }
            # Include default sender if configured (provider may ignore if not needed)
            try:
                if (cfg.default_sender or "").strip():
                    params["sender"] = str(cfg.default_sender).strip()
            except Exception:
                pass
            qs = urlencode(params)
            with urlopen(f"{base}?{qs}", timeout=20) as resp:  # nosec B310
                status = getattr(resp, "status", 200)
                body = resp.read()
                text = ""
                try:
                    text = (body or b"").decode("utf-8", "ignore").strip()
                except Exception:
                    text = ""
                # Treat only plausible success tokens as success; otherwise bubble up provider message
                if status >= 400:
                    raise RuntimeError(f"HTTP {status}")
                low = text.lower()
                # First, try to parse XML and read <Status> codes
                parsed_ok = False
                try:
                    if text.startswith("<") or "<status>" in low:
                        root = ET.fromstring(text)
                        statuses: list[str] = []
                        for el in root.iter():
                            try:
                                if el.tag and str(el.tag).lower().endswith("status"):
                                    sval = (el.text or "").strip()
                                    if sval != "":
                                        statuses.append(sval)
                            except Exception:
                                pass
                        if statuses:
                            # Common semantics: 1 = success; 0 or -1 = failure
                            if any(s == "1" for s in statuses) and not any(s in {"0", "-1"} for s in statuses):
                                parsed_ok = True
                            elif any(s in {"0", "-1"} for s in statuses):
                                raise RuntimeError(text or "Provider error")
                except Exception:
                    parsed_ok = False
                if not parsed_ok:
                    # Only accept strict numeric success codes when not XML
                    if (low == "1") or low.startswith("1|"):
                        parsed_ok = True
                    else:
                        # Explicitly do NOT accept generic 'ok/sent/success' tokens to avoid false positives
                        raise RuntimeError(text or "Unknown provider response")
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
        # Return short snippet of provider response/error
        msg = str(e)
        try:
            if len(msg) > 240:
                msg = msg[:240]
        except Exception:
            pass
        return False, msg


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
    try:
        kpi = compute_kpi(db, datasource_id=datasource_id, source=source, agg=agg, measure=measure, where=where2, x_field=x_field, x_value=x_value2)
    except Exception:
        kpi = 0.0
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
            # Also tolerate whitespace inside braces and token case-insensitivity: {{ key }}
            out = _re.sub(r"\{\{\s*" + _re.escape(k) + r"\s*\}\}", str(v), out, flags=_re.IGNORECASE)
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


from typing import Callable

def run_rule(db: Session, rule: AlertRule, *, force_time_ok: bool = False, progress_cb: Optional[Callable[[dict], None]] = None) -> Tuple[bool, str]:
    try:
        cfg = json.loads(rule.config_json or "{}")
    except Exception:
        return False, "Invalid config"
    triggers = cfg.get("triggers") or []
    actions = cfg.get("actions") or []
    ds_id = cfg.get("datasourceId")
    render = cfg.get("render") or {}
    template_present = False
    try:
        template_present = bool(str(cfg.get("template") or "").strip())
    except Exception:
        template_present = False

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
        # Manual run: bypass time window and threshold if forced
        if force_time_ok:
            try:
                time_ok = True
            except Exception:
                time_ok = True
        # Threshold condition
        thr = tg.get("threshold") or {}
        if progress_cb:
            try: progress_cb({"id": "calc", "status": "start"})
            except Exception: pass
        if thr and thr.get("enabled"):
            try:
                if force_time_ok:
                    thr_ok = True
                    try:
                        _, kpi_value = evaluate_threshold(db, trigger=thr, datasource_id=ds_id)
                    except Exception:
                        kpi_value = kpi_value if 'kpi_value' in locals() else None
                else:
                    thr_ok, kpi_value = evaluate_threshold(db, trigger=thr, datasource_id=ds_id)
            except Exception:
                thr_ok = False
        else:
            thr_ok = True
            kpi_value = None
        if progress_cb:
            try: progress_cb({"id": "calc", "status": ("ok" if thr_ok else "error"), "kpi": (None if kpi_value is None else float(kpi_value))})
            except Exception: pass
        logic = str(tg.get("logic") or "AND").upper()
        fired = (time_ok and thr_ok) if logic == "AND" else (time_ok or thr_ok)
        if not fired:
            if force_time_ok:
                fired = True
            else:
                return True, "No trigger fired"
        # Continue to assemble and send as in v1 using computed kpi_value and render
        is_notification = str(getattr(rule, "kind", "") or "").lower() == "notification"
        html_parts = ([] if is_notification else [f"<div style='font-family:Inter,Arial,sans-serif;font-size:13px'>Rule: {_html_escape(rule.name)}</div>"])
        # Prepare replacements map BEFORE any pivot/table rendering so we can assign TABLE_HTML safely
        replacements_extra: dict[str, str] = {}
        if render.get("mode") == "kpi":
            # Emit snapshot-like progress for KPI (no Playwright involved)
            if progress_cb:
                try: progress_cb({"id": "snapshot", "status": "start", "mode": "kpi"})
                except Exception: pass
            val = (kpi_value if (kpi_value is not None) else 0)
            label = render.get("label") or "KPI"
            html_parts.append(_render_kpi_html(val, label=label))
            if progress_cb:
                try: progress_cb({"id": "snapshot", "status": "ok", "mode": "kpi"})
                except Exception: pass
            # Provide KPI image placeholder as SVG data URI so {{KPI_IMG}} resolves in template/preview
            try:
                svg_bytes = _build_kpi_svg(val, label)
                data_uri = _to_svg_data_uri(svg_bytes)
                if data_uri:
                    replacements_extra["KPI_IMG"] = f"<img alt='KPI' src='{data_uri}' style='max-width:100%;height:auto;border:1px solid #e5e7eb;border-radius:10px'/>"
            except Exception:
                pass
        elif render.get("mode") == "table":
            # Emit snapshot-like progress for Table (server-side HTML render)
            if progress_cb:
                try: progress_cb({"id": "snapshot", "status": "start", "mode": "table"})
                except Exception: pass
            try:
                # Try pivot render based on widgetRef configuration
                import json as _json
                widget_cfg = None
                try:
                    wref = (render.get("widgetRef") or {}) if isinstance(render, dict) else {}
                    wid = (wref.get("widgetId") if isinstance(wref, dict) else None) or None
                    did0 = (wref.get("dashboardId") if isinstance(wref, dict) else None) or getattr(rule, "dashboard_id", None)
                    if wid and not did0:
                        try:
                            rows = db.query(Dashboard).all()
                            for drow2 in rows:
                                try:
                                    definition2 = _json.loads(drow2.definition_json or "{}")
                                    ws2 = (definition2.get('widgets') or {})
                                    if str(wid) in ws2:
                                        did0 = drow2.id
                                        break
                                except Exception:
                                    continue
                        except Exception:
                            pass
                    if wid and did0:
                        drow = db.get(Dashboard, did0)
                        if drow and drow.definition_json:
                            definition = _json.loads(drow.definition_json or "{}")
                            widget_cfg = ((definition or {}).get('widgets') or {}).get(str(wid))
                except Exception:
                    widget_cfg = None
                is_pivot = False
                if isinstance(widget_cfg, dict) and ((widget_cfg.get('type') or '') == 'table'):
                    opts = (widget_cfg.get('options') or {})
                    tbl = (opts.get('table') or {})
                    pcfg_probe = (tbl.get('pivotConfig') or {})
                    pv_probe = ((widget_cfg.get('pivot') or {}).get('values') or [])
                    is_pivot = ((tbl.get('tableType') or 'data') == 'pivot') or bool(pcfg_probe.get('rows') or pcfg_probe.get('cols') or pv_probe)
                table_html_val = None
                if is_pivot:
                    # Build pivot request
                    from .routers.query import run_pivot  # lazy import to avoid cycles
                    from .schemas import PivotRequest
                    opts = (widget_cfg.get('options') or {})
                    tbl = (opts.get('table') or {})
                    pcfg = (tbl.get('pivotConfig') or {})
                    row_dims = list((pcfg.get('rows') or []))
                    col_dims = list((pcfg.get('cols') or []))
                    pv = ((widget_cfg.get('pivot') or {}).get('values') or [])
                    vals_list = list((pcfg.get('vals') or []))
                    try:
                        chip = (pv[0] if len(pv) > 0 else {}) or {}
                        value_field = chip.get('field') or chip.get('measureId') or (vals_list[0] if len(vals_list) > 0 else None)
                        agg_raw = chip.get('agg') or ('count' if not value_field else 'sum')
                        label = chip.get('label') or (value_field or 'Value')
                    except Exception:
                        value_field = (vals_list[0] if len(vals_list) > 0 else None); agg_raw = ('count' if not value_field else 'sum'); label = (value_field or 'Value')
                    agg = str(agg_raw or 'sum').lower()
                    if 'distinct' in agg: agg = 'distinct'
                    elif agg.startswith('avg'): agg = 'avg'
                    elif agg not in {'sum','avg','min','max','distinct','count'}: agg = 'count'
                    # Totals preferences
                    show_row_totals = (pcfg.get('rowTotals') is not False)
                    show_col_totals = (pcfg.get('colTotals') is not False)
                    # Source resolution
                    qspec_src = ((widget_cfg.get('querySpec') or {}) or {}).get('source') or (cfg.get('source') if isinstance(cfg, dict) else None) or ''
                    if not qspec_src:
                        raise Exception('No querySpec.source for server pivot')
                    payload_p = PivotRequest(
                        source=qspec_src,
                        rows=row_dims,
                        cols=col_dims,
                        valueField=(None if agg=='count' and not value_field else (value_field or None)),
                        aggregator=agg,
                        where=(thr.get('where') if isinstance(thr, dict) else (cfg.get('where') if isinstance(cfg, dict) else None)),
                        datasourceId=ds_id,
                        limit=int((tbl.get('pivotMaxRows') or 20000)),
                        widgetId=str((wref or {}).get('widgetId') or ''),
                    )
                    res_p = run_pivot(payload_p, db)
                    cols = list(res_p.columns or [])
                    data = list(res_p.rows or [])
                    # Light theme styles
                    TH_HEAD = "padding:6px 8px;border:1px solid #e5e7eb;color:#374151;background:#f3f4f6;text-align:left"
                    TH_ROW =  "padding:6px 8px;border:1px solid #e5e7eb;color:#111827;background:#ffffff;text-align:left"
                    TD_CELL =  "padding:6px 8px;border:1px solid #e5e7eb;color:#111827;background:#ffffff;text-align:right"
                    TD_TOTAL = "padding:6px 8px;border:1px solid #e5e7eb;color:#92400e;background:#fef3c7;text-align:right;font-weight:600"
                    table_open = "<table style='border-collapse:collapse;width:100%;font-family:Inter,Arial,sans-serif;font-size:13px;background:#ffffff;color:#111827'>"
                    def _esc(x: object) -> str:
                        try:
                            s = str(x if x is not None else '')
                            return s.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('"','&quot;').replace("'",'&#39;')
                        except Exception:
                            return ''
                    rdn = len(row_dims); cdn = len(col_dims)
                    if data and cdn == 0:
                        vi = len(cols) - 1 if cols else -1
                        total = 0.0; rows_html = []
                        for r in data:
                            if not isinstance(r, (list, tuple)): continue
                            name = " / ".join(_esc(r[i]) for i in range(0, rdn)) if rdn else ''
                            v = r[vi] if (vi >= 0 and vi < len(r)) else 0
                            try: fv = float(v) if v is not None else 0.0
                            except Exception: fv = 0.0
                            total += fv
                            rows_html.append(f"<tr><th style='{TH_ROW}'>{name}</th><td style='{TD_CELL}'>{fv:,.0f}</td></tr>")
                        head = f"<tr><th style='{TH_HEAD}'>{_esc(row_dims[0] if row_dims else 'Item')}</th><th style='{TH_HEAD}'>{_esc(label or 'Value')}</th></tr>"
                        total_row = f"<tr><th style='{TH_HEAD}'>Total</th><td style='{TD_TOTAL}'>{total:,.0f}</td></tr>"
                        table_html_val = table_open + "<thead>" + head + "</thead><tbody>" + ("".join(rows_html)) + total_row + "</tbody></table>"
                    elif data:
                        # General matrix
                        try:
                            vi = len(cols) - 1
                        except Exception:
                            vi = -1
                        # Build row/col keys
                        row_leaves = []
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
                            try: vv = float(v) if v is not None else 0.0
                            except Exception: vv = 0.0
                            valmap[(rk, ck)] = vv
                            if rk not in _r_seen:
                                _r_seen.add(rk); row_leaves.append(rk)
                            node = col_root
                            for lvl in range(cdn):
                                lb = str(ck[lvl] if lvl < len(ck) else '')
                                if lb not in node:
                                    node[lb] = {}
                                    if lb not in seen_by_level[lvl]:
                                        seen_by_level[lvl].add(lb); order_by_level[lvl].append(lb)
                                node = node[lb]
                        leaf_counts: dict[tuple, int] = {}
                        col_leaves: list[tuple] = []
                        def _count(n: dict, depth: int, path: tuple) -> int:
                            if depth >= cdn: return 1
                            s = 0; labels = order_by_level[depth]
                            for lb in labels:
                                if lb in n:
                                    s += _count(n[lb], depth + 1, path + (lb,))
                            leaf_counts[path] = max(1, s); return max(1, s)
                        _count(col_root, 0, tuple())
                        def _collect(n: dict, depth: int, path: tuple):
                            if depth >= cdn:
                                col_leaves.append(path); return
                            labels = order_by_level[depth]
                            for lb in labels:
                                if lb in n: _collect(n[lb], depth + 1, path + (lb,))
                        _collect(col_root, 0, tuple())
                        prefix_counts: dict[tuple, int] = {}
                        for rk in row_leaves:
                            for i in range(1, rdn + 1):
                                pf = rk[:i]; prefix_counts[pf] = prefix_counts.get(pf, 0) + 1
                        thead_parts: list[str] = []
                        if cdn > 0:
                            left_span = max(1, rdn)
                            # Emit row dimension titles instead of a blank top-left block
                            row0: list[str] = []
                            if left_span > 0:
                                for i in range(rdn):
                                    title_i = _esc(row_dims[i] if i < len(row_dims) else "")
                                    row0.append(f"<th style='{TH_HEAD}' rowspan='{cdn}'>{title_i}</th>")
                            for lb in order_by_level[0]:
                                if lb in col_root:
                                    cs = leaf_counts.get((lb,), 1)
                                    row0.append(f"<th style='{TH_HEAD}' colspan='{cs}'>{_esc(lb)}</th>")
                            if show_row_totals:
                                row0.append(f"<th style='{TH_HEAD}' rowspan='{cdn}'>Total</th>")
                            thead_parts.append("<tr>" + "".join(row0) + "</tr>")
                            def _emit_level(n: dict, depth: int, path: tuple):
                                if depth >= cdn: return
                                cells: list[str] = []
                                labels = order_by_level[depth]
                                for lb in labels:
                                    if lb in n:
                                        cs = leaf_counts.get(path + (lb,), 1)
                                        cells.append(f"<th style='{TH_HEAD}' colspan='{cs}'>{_esc(lb)}</th>")
                                if cells:
                                    thead_parts.append("<tr>" + "".join(cells) + "</tr>")
                                merged: dict = {}
                                for lb in labels:
                                    if lb in n:
                                        for k2, v2 in n[lb].items():
                                            if k2 not in merged: merged[k2] = v2
                                if depth + 1 < cdn:
                                    _emit_level(merged, depth + 1, path)
                            if cdn > 1:
                                _emit_level(col_root, 1, tuple())
                        else:
                            left_span = max(1, rdn)
                            thead_parts.append(f"<tr><th style='{TH_HEAD}' colspan='{left_span}'></th><th style='{TH_HEAD}'>Value</th></tr>")
                        tbody_parts: list[str] = []
                        seen_prefix: set[tuple] = set()
                        col_totals: list[float] = [0.0 for _ in range(len(col_leaves))]
                        grand_total = 0.0
                        for rk in row_leaves:
                            tds: list[str] = []
                            for d in range(0, rdn):
                                pf = rk[: d + 1]
                                if pf not in seen_prefix:
                                    seen_prefix.add(pf); rs = prefix_counts.get(pf, 1)
                                    tds.append(f"<th style='{TH_ROW}' rowspan='{rs}'>{_esc(rk[d])}</th>")
                            row_sum = 0.0
                            for j, ck in enumerate(col_leaves):
                                v = float(valmap.get((rk, ck), 0.0) or 0.0)
                                row_sum += v; col_totals[j] += v
                                tds.append(f"<td style='{TD_CELL}'>{v:,.0f}</td>")
                            if show_row_totals:
                                tds.append(f"<td style='{TD_TOTAL}'>{row_sum:,.0f}</td>")
                            grand_total += row_sum
                            tbody_parts.append("<tr>" + "".join(tds) + "</tr>")
                        if show_col_totals:
                            tr: list[str] = []
                            left_span = max(1, rdn)
                            tr.append(f"<th style='{TH_HEAD}' colspan='{left_span}'>Total</th>")
                            for j in range(len(col_leaves)):
                                tr.append(f"<td style='{TD_TOTAL}'>{col_totals[j]:,.0f}</td>")
                            if show_row_totals:
                                tr.append(f"<td style='{TD_TOTAL}'>{grand_total:,.0f}</td>")
                            tbody_parts.append("<tr>" + "".join(tr) + "</tr>")
                        table_html_val = table_open + "<thead>" + "".join(thead_parts) + "</thead><tbody>" + "".join(tbody_parts) + "</tbody></table>"
                else:
                    # Fallback: plain table from query spec
                    spec = render.get("querySpec") or {}
                    req = QuerySpecRequest(spec=spec, datasourceId=ds_id, limit=spec.get("limit") or 1000, offset=0, includeTotal=False)
                    res = run_query_spec(req, db)
                    table_html_val = _render_table_html(res)
                if progress_cb:
                    try: progress_cb({"id": "snapshot", "status": "ok", "mode": "table"})
                    except Exception: pass
                # Place into template or append directly depending on whether a template exists
                try:
                    if template_present:
                        if table_html_val:
                            replacements_extra["TABLE_HTML"] = table_html_val
                    else:
                        html_parts.append(table_html_val or "<div>Failed to render table.</div>")
                except Exception:
                    if not template_present:
                        html_parts.append("<div>Failed to render table.</div>")
            except Exception:
                if progress_cb:
                    try: progress_cb({"id": "snapshot", "status": "error", "mode": "table", "error": "render_failed"})
                    except Exception: pass
                html_parts.append("<div>Failed to render table.</div>")
        html = "\n".join(html_parts)
        # Dispatch
        errs: list[str] = []
        # Prepare inline images and replacements for template insert
        inline_images: list[tuple[str, bytes, str, str]] = []
        snapshot_expected = False
        snapshot_ok = False
        try:
            # Preferred: real widget snapshot via headless embed if widgetRef present and render is chart-like
            try:
                wref = (render or {}).get("widgetRef") or {}
                wid = (wref or {}).get("widgetId") or getattr(rule, "widget_id", None)
                did = (wref or {}).get("dashboardId") or getattr(rule, "dashboard_id", None)
                mode_ = str(((render or {}).get("mode") or "kpi")).lower()
                # Snapshot for all widget modes except 'table' (includes KPI and charts)
                should_snapshot = bool(wid) and (mode_ != "table")
                if should_snapshot:
                    snapshot_expected = True
                    w = int((render or {}).get("width") or 1000)
                    h = int((render or {}).get("height") or (280 if mode_ == "kpi" else 360))
                    th = "light"
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
                    if progress_cb:
                        try: progress_cb({"id": "snapshot", "status": "start", "mode": mode_, "wid": str(wid), "did": str(did), "actor": (str(actor_for_snapshot or '') or None)})
                        except Exception: pass
                    # If dashboardId is missing, try to resolve by scanning dashboards that contain this widget id
                    if not did:
                        try:
                            rows = db.query(Dashboard).all()
                            for drow in rows:
                                try:
                                    import json as _json
                                    definition = _json.loads(drow.definition_json or "{}")
                                    ws = (definition.get('widgets') or {})
                                    if str(wid) in ws:
                                        did = drow.id
                                        break
                                except Exception:
                                    continue
                        except Exception:
                            did = did
                    # If still missing, fail fast with a clear reason
                    if not did:
                        if progress_cb:
                            try: progress_cb({"id": "snapshot", "status": "error", "mode": mode_, "error": "missing_dashboard_id", "wid": str(wid), "did": str(did or '')})
                            except Exception: pass
                        # Do not attempt Playwright without dashboard context
                        raise Exception("snapshot_missing_dashboard_id")
                    png = _fetch_snapshot_png_direct(dashboard_id=did, public_id=None, token=None, widget_id=str(wid), datasource_id=ds_id, width=w, height=h, theme=th, actor_id=actor_for_snapshot, wait_ms=20000, retries=1)
                    if png:
                        snapshot_ok = True
                        if progress_cb:
                            try: progress_cb({"id": "snapshot", "status": "ok", "mode": mode_})
                            except Exception: pass
                        b64 = base64.b64encode(png).decode("ascii")
                        tag = f"<img alt='Widget' src='data:image/png;base64,{b64}' style='max-width:100%;height:auto'/>"
                        # Provide tokens and also append to HTML so it shows without template tokens
                        replacements_extra["CHART_IMG"] = tag
                        replacements_extra["KPI_IMG"] = tag
                        if not template_present:
                            try:
                                html = html + f"\n<div style='margin-top:8px'>{tag}</div>"
                            except Exception:
                                pass
                    else:
                        if progress_cb:
                            try: progress_cb({"id": "snapshot", "status": "error", "mode": mode_, "error": "returned_none"})
                            except Exception: pass
            except Exception:
                pass
            if render.get("mode") == "table":
                try:
                    # If not already set by the snapshot/table branch above, set a basic fallback
                    if "TABLE_HTML" not in replacements_extra:
                        spec = render.get("querySpec") or {}
                        req = QuerySpecRequest(spec=spec, datasourceId=ds_id, limit=spec.get("limit") or 1000, offset=0, includeTotal=False)
                        res = run_query_spec(req, db)
                        replacements_extra["TABLE_HTML"] = _render_table_html(res)
                except Exception:
                    if "TABLE_HTML" not in replacements_extra:
                        replacements_extra["TABLE_HTML"] = "<div>Failed to render table.</div>"
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
                # If snapshot was expected but not available, send anyway with placeholders
                to = [str(x).strip() for x in (a.get("to") or []) if str(x).strip()]
                if not to:
                    # No recipients configured; skip email send
                    continue
                subj = str(a.get("subject") or rule.name)
                if progress_cb:
                    try: progress_cb({"id": "email", "status": "start", "to": len(to)})
                    except Exception: pass
                ok, err = send_email(db, subject=subj, to=to, html=html, replacements=replacements_all, inline_images=inline_images)
                if not ok and err:
                    errs.append(f"email: {err}")
                    if progress_cb:
                        try: progress_cb({"id": "email", "status": "error", "to": len(to), "error": err})
                        except Exception: pass
                else:
                    if progress_cb:
                        try: progress_cb({"id": "email", "status": "ok", "to": len(to)})
                        except Exception: pass
            elif at == "sms":
                to = [str(x).strip() for x in (a.get("to") or []) if str(x).strip()]
                text = str(a.get("message") or rule.name)
                try:
                    # Fill placeholders in SMS using same tokens
                    text = _apply_placeholders(text, ctx_tokens)
                except Exception:
                    pass
                # No recipients configured: mark as error and skip provider call
                if not to:
                    if progress_cb:
                        try: progress_cb({"id": "sms", "status": "error", "to": 0, "error": "no_recipients"})
                        except Exception: pass
                    errs.append("sms: no recipients")
                    continue
                if progress_cb:
                    try: progress_cb({"id": "sms", "status": "start", "to": len(to)})
                    except Exception: pass
                ok, err = send_sms_hadara(db, to_numbers=to, message=text)
                if not ok and err:
                    errs.append(f"sms: {err}")
                    if progress_cb:
                        try: progress_cb({"id": "sms", "status": "error", "to": len(to), "error": err})
                        except Exception: pass
                else:
                    if progress_cb:
                        try: progress_cb({"id": "sms", "status": "ok", "to": len(to)})
                        except Exception: pass
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
        if force_time_ok:
            fired = True
        else:
            return True, "No trigger fired"

    # Build HTML body based on render.mode
    html_parts = [f"<div style='font-family:Inter,Arial,sans-serif;font-size:13px'>Rule: {_html_escape(rule.name)}</div>"]
    widget_img_cid: Optional[str] = None
    widget_img_bytes: Optional[bytes] = None
    snapshot_expected_v1 = False
    snapshot_ok_v1 = False
    if progress_cb:
        try: progress_cb({"id": "calc", "status": "start"})
        except Exception: pass
    if render.get("mode") == "table":
        # Emit snapshot-like progress for Table
        if progress_cb:
            try: progress_cb({"id": "snapshot", "status": "start", "mode": "table"})
            except Exception: pass
        try:
            spec = render.get("querySpec") or {}
            req = QuerySpecRequest(spec=spec, datasourceId=ds_id, limit=spec.get("limit") or 1000, offset=0, includeTotal=False)
            res = run_query_spec(req, db)
            parts = [_render_table_html(res)]
            if progress_cb:
                try: progress_cb({"id": "snapshot", "status": "ok", "mode": "table"})
                except Exception: pass
        except Exception:
            parts = ["<div>Failed to render table.</div>"]
            if progress_cb:
                try: progress_cb({"id": "snapshot", "status": "error", "mode": "table", "error": "render_failed"})
                except Exception: pass
    elif render.get("mode") == "kpi":
        # Emit snapshot-like progress for KPI
        if progress_cb:
            try: progress_cb({"id": "snapshot", "status": "start", "mode": "kpi"})
            except Exception: pass
        kpi_label = render.get("label") or "KPI"
        render_mode_is_kpi = True
        val = kpi_value if (kpi_value is not None) else 0
        parts = [_render_kpi_html(val, label=kpi_label)]
        if progress_cb:
            try: progress_cb({"id": "snapshot", "status": "ok", "mode": "kpi"})
            except Exception: pass
        try:
            svg = _build_kpi_svg(val, kpi_label)
            parts.append(f"<div style='margin-top:8px'><img alt='KPI' src='{_to_svg_data_uri(svg)}' style='max-width:100%;height:auto;border:1px solid #e5e7eb;border-radius:10px'/></div>")
        except Exception:
            pass
    else:
        pass  # No placeholder fallback for charts
    if progress_cb:
        try: progress_cb({"id": "calc", "status": "ok", "kpi": (None if kpi_value is None else float(kpi_value))})
        except Exception: pass

    # If a widgetRef is provided, compute a real snapshot (direct Playwright only) and provide tokens
    try:
        wref = (render or {}).get("widgetRef") or {}
        wid = (wref or {}).get("widgetId")
        did = (wref or {}).get("dashboardId") or getattr(rule, "dashboard_id", None)
        mode_ = str(((render or {}).get("mode") or "kpi")).lower()
        is_chart_like_v1 = (mode_ not in ("table", "kpi"))
        if wid and is_chart_like_v1:
            snapshot_expected_v1 = True
            w = int((render or {}).get("width") or 1000)
            h = int((render or {}).get("height") or 360)
            th = "light"
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
            if progress_cb:
                try: progress_cb({"id": "snapshot", "status": "start", "mode": "chart", "wid": str(wid), "did": str(did), "actor": (str(actor_for_snapshot or '') or None)})
                except Exception: pass
            # If dashboardId is missing, try to resolve by scanning dashboards for this widget id
            if not did:
                try:
                    rows = db.query(Dashboard).all()
                    for drow in rows:
                        try:
                            import json as _json
                            definition = _json.loads(drow.definition_json or "{}")
                            ws = (definition.get('widgets') or {})
                            if str(wid) in ws:
                                did = drow.id
                                break
                        except Exception:
                            continue
                except Exception:
                    did = did
            if not did:
                if progress_cb:
                    try: progress_cb({"id": "snapshot", "status": "error", "mode": "chart", "error": "missing_dashboard_id", "wid": str(wid), "did": str(did or '')})
                    except Exception: pass
                raise Exception("snapshot_missing_dashboard_id")
            png = _fetch_snapshot_png_direct(dashboard_id=did, public_id=None, token=None, widget_id=str(wid), datasource_id=ds_id, width=w, height=h, theme=th, actor_id=actor_for_snapshot, wait_ms=20000, retries=1)
            if png:
                widget_img_bytes = png
                snapshot_ok_v1 = True
                if progress_cb:
                    try: progress_cb({"id": "snapshot", "status": "ok", "mode": "chart"})
                    except Exception: pass
                try:
                    _b64_inline = base64.b64encode(png).decode('ascii')
                    html_parts.append(f"<div style='margin-top:8px'><img alt='Widget' src='data:image/png;base64,{_b64_inline}' style='max-width:100%;height:auto'/></div>")
                except Exception:
                    pass
            else:
                if progress_cb:
                    try: progress_cb({"id": "snapshot", "status": "error", "mode": "chart", "error": "returned_none"})
                    except Exception: pass
    except Exception:
        pass

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
        # If we computed a widget snapshot, provide CHART_IMG/KPI_IMG tokens with data URI
        if widget_img_bytes:
            try:
                _b64w = base64.b64encode(widget_img_bytes).decode('ascii')
                _tag = f"<img alt='Widget' src='data:image/png;base64,{_b64w}' style='max-width:100%;height:auto'/>"
                placeholders["CHART_IMG"] = _tag
                placeholders["KPI_IMG"] = _tag
            except Exception:
                pass
        # Fallback for KPI mode: if no widget snapshot, provide a KPI SVG data URI for {{KPI_IMG}}
        if render.get("mode") == "kpi" and "KPI_IMG" not in placeholders:
            try:
                val2 = kpi_value if (kpi_value is not None) else 0
                label2 = render.get("label") or "KPI"
                svg2 = _build_kpi_svg(val2, label2)
                tag2 = f"<img alt='KPI' src='{_to_svg_data_uri(svg2)}' style='max-width:100%;height:auto;border:1px solid #e5e7eb;border-radius:10px'/>"
                placeholders["KPI_IMG"] = tag2
            except Exception:
                pass
        if render.get("mode") == "table":
            try:
                # Prefer pivot HTML when widgetRef points to a pivot-configured table widget
                import json as _json
                table_html_val = None
                try:
                    wref = (render.get("widgetRef") or {}) if isinstance(render, dict) else {}
                    wid = (wref.get("widgetId") if isinstance(wref, dict) else None) or None
                    did0 = (wref.get("dashboardId") if isinstance(wref, dict) else None) or getattr(rule, "dashboard_id", None)
                    if wid and not did0:
                        try:
                            rows = db.query(Dashboard).all()
                            for drow2 in rows:
                                try:
                                    definition2 = _json.loads(drow2.definition_json or "{}")
                                    ws2 = (definition2.get('widgets') or {})
                                    if str(wid) in ws2:
                                        did0 = drow2.id
                                        break
                                except Exception:
                                    continue
                        except Exception:
                            pass
                    widget_cfg = None
                    if wid and did0:
                        drow = db.get(Dashboard, did0)
                        if drow and drow.definition_json:
                            definition = _json.loads(drow.definition_json or "{}")
                            widget_cfg = ((definition or {}).get('widgets') or {}).get(str(wid))
                except Exception:
                    widget_cfg = None
                is_pivot = False
                if isinstance(widget_cfg, dict) and ((widget_cfg.get('type') or '') == 'table'):
                    opts = (widget_cfg.get('options') or {})
                    tbl = (opts.get('table') or {})
                    pcfg_probe = (tbl.get('pivotConfig') or {})
                    pv_probe = ((widget_cfg.get('pivot') or {}).get('values') or [])
                    is_pivot = ((tbl.get('tableType') or 'data') == 'pivot') or bool(pcfg_probe.get('rows') or pcfg_probe.get('cols') or pv_probe)
                if is_pivot:
                    from .routers.query import run_pivot  # lazy import
                    from .schemas import PivotRequest
                    opts = (widget_cfg.get('options') or {})
                    tbl = (opts.get('table') or {})
                    pcfg = (tbl.get('pivotConfig') or {})
                    row_dims = list((pcfg.get('rows') or []))
                    col_dims = list((pcfg.get('cols') or []))
                    pv = ((widget_cfg.get('pivot') or {}).get('values') or [])
                    try:
                        chip = (pv[0] if len(pv) > 0 else {}) or {}
                        value_field = chip.get('field') or chip.get('measureId') or None
                        agg_raw = chip.get('agg') or 'sum'
                        label = chip.get('label') or (value_field or 'Value')
                    except Exception:
                        value_field = None; agg_raw = 'sum'; label = 'Value'
                    agg_p = str(agg_raw or 'sum').lower()
                    if 'distinct' in agg_p: agg_p = 'distinct'
                    elif agg_p.startswith('avg'): agg_p = 'avg'
                    elif agg_p not in {'sum','avg','min','max','distinct','count'}: agg_p = 'count'
                    show_row_totals = (pcfg.get('rowTotals') is not False)
                    show_col_totals = (pcfg.get('colTotals') is not False)
                    qspec_src = ((widget_cfg.get('querySpec') or {}) or {}).get('source') or (cfg.get('source') if isinstance(cfg, dict) else None) or ''
                    if not qspec_src:
                        raise Exception('No querySpec.source for server pivot')
                    payload_p = PivotRequest(
                        source=qspec_src,
                        rows=row_dims,
                        cols=col_dims,
                        valueField=(None if agg_p=='count' and not value_field else (value_field or None)),
                        aggregator=agg_p,
                        where=(cfg.get('where') if isinstance(cfg, dict) else None),
                        datasourceId=ds_id,
                        limit=int((tbl.get('pivotMaxRows') or 20000)),
                        widgetId=str((wref or {}).get('widgetId') or ''),
                    )
                    res_p = run_pivot(payload_p, db)
                    cols = list(res_p.columns or [])
                    data = list(res_p.rows or [])
                    TH_HEAD = "padding:6px 8px;border:1px solid #e5e7eb;color:#374151;background:#f3f4f6;text-align:left"
                    TH_ROW =  "padding:6px 8px;border:1px solid #e5e7eb;color:#111827;background:#ffffff;text-align:left"
                    TD_CELL =  "padding:6px 8px;border:1px solid #e5e7eb;color:#111827;background:#ffffff;text-align:right"
                    TD_TOTAL = "padding:6px 8px;border:1px solid #e5e7eb;color:#92400e;background:#fef3c7;text-align:right;font-weight:600"
                    table_open = "<table style='border-collapse:collapse;width:100%;font-family:Inter,Arial,sans-serif;font-size:13px;background:#ffffff;color:#111827'>"
                    def _esc(x: object) -> str:
                        try:
                            s = str(x if x is not None else '')
                            return s.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('"','&quot;').replace("'",'&#39;')
                        except Exception:
                            return ''
                    rdn = len(row_dims); cdn = len(col_dims)
                    if data and cdn == 0:
                        vi = len(cols) - 1 if cols else -1
                        total = 0.0; rows_html = []
                        for r in data:
                            if not isinstance(r, (list, tuple)): continue
                            name = " / ".join(_esc(r[i]) for i in range(0, rdn)) if rdn else ''
                            v = r[vi] if (vi >= 0 and vi < len(r)) else 0
                            try: fv = float(v) if v is not None else 0.0
                            except Exception: fv = 0.0
                            total += fv
                            rows_html.append(f"<tr><th style='{TH_ROW}'>{name}</th><td style='{TD_CELL}'>{fv:,.0f}</td></tr>")
                        head = f"<tr><th style='{TH_HEAD}'>{_esc(row_dims[0] if row_dims else 'Item')}</th><th style='{TH_HEAD}'>{_esc(label or 'Value')}</th></tr>"
                        total_row = f"<tr><th style='{TH_HEAD}'>Total</th><td style='{TD_TOTAL}'>{total:,.0f}</td></tr>"
                        table_html_val = table_open + "<thead>" + head + "</thead><tbody>" + ("".join(rows_html)) + total_row + "</tbody></table>"
                    elif data:
                        try: vi = len(cols) - 1
                        except Exception: vi = -1
                        row_leaves = []
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
                            try: vv = float(v) if v is not None else 0.0
                            except Exception: vv = 0.0
                            valmap[(rk, ck)] = vv
                            if rk not in _r_seen:
                                _r_seen.add(rk); row_leaves.append(rk)
                            node = col_root
                            for lvl in range(cdn):
                                lb = str(ck[lvl] if lvl < len(ck) else '')
                                if lb not in node:
                                    node[lb] = {}
                                    if lb not in seen_by_level[lvl]:
                                        seen_by_level[lvl].add(lb); order_by_level[lvl].append(lb)
                                node = node[lb]
                        leaf_counts: dict[tuple, int] = {}
                        col_leaves: list[tuple] = []
                        def _count(n: dict, depth: int, path: tuple) -> int:
                            if depth >= cdn: return 1
                            s = 0; labels = order_by_level[depth]
                            for lb in labels:
                                if lb in n:
                                    s += _count(n[lb], depth + 1, path + (lb,))
                            leaf_counts[path] = max(1, s); return max(1, s)
                        _count(col_root, 0, tuple())
                        def _collect(n: dict, depth: int, path: tuple):
                            if depth >= cdn:
                                col_leaves.append(path); return
                            labels = order_by_level[depth]
                            for lb in labels:
                                if lb in n: _collect(n[lb], depth + 1, path + (lb,))
                        _collect(col_root, 0, tuple())
                        prefix_counts: dict[tuple, int] = {}
                        for rk in row_leaves:
                            for i in range(1, rdn + 1):
                                pf = rk[:i]; prefix_counts[pf] = prefix_counts.get(pf, 0) + 1
                        thead_parts: list[str] = []
                        if cdn > 0:
                            left_span = max(1, rdn)
                            row0: list[str] = []
                            if left_span > 0:
                                for i in range(rdn):
                                    title_i = _esc(row_dims[i] if i < len(row_dims) else "")
                                    row0.append(f"<th style='{TH_HEAD}' rowspan='{cdn}'>" + title_i + "</th>")
                            for lb in order_by_level[0]:
                                if lb in col_root:
                                    cs = leaf_counts.get((lb,), 1)
                                    row0.append(f"<th style='{TH_HEAD}' colspan='{cs}'>" + _esc(lb) + "</th>")
                            if show_row_totals:
                                row0.append(f"<th style='{TH_HEAD}' rowspan='{cdn}'>Total</th>")
                            thead_parts.append("<tr>" + "".join(row0) + "</tr>")
                            def _emit_level(n: dict, depth: int, path: tuple):
                                if depth >= cdn: return
                                cells: list[str] = []
                                labels = order_by_level[depth]
                                for lb in labels:
                                    if lb in n:
                                        cs = leaf_counts.get(path + (lb,), 1)
                                        cells.append(f"<th style='{TH_HEAD}' colspan='{cs}'>" + _esc(lb) + "</th>")
                                if cells:
                                    thead_parts.append("<tr>" + "".join(cells) + "</tr>")
                                merged: dict = {}
                                for lb in labels:
                                    if lb in n:
                                        for k2, v2 in n[lb].items():
                                            if k2 not in merged: merged[k2] = v2
                                if depth + 1 < cdn:
                                    _emit_level(merged, depth + 1, path)
                            if cdn > 1:
                                _emit_level(col_root, 1, tuple())
                        else:
                            left_span = max(1, rdn)
                            thead_parts.append(f"<tr><th style='{TH_HEAD}' colspan='{left_span}'></th><th style='{TH_HEAD}'>Value</th></tr>")
                        tbody_parts: list[str] = []
                        seen_prefix: set[tuple] = set()
                        col_totals: list[float] = [0.0 for _ in range(len(col_leaves))]
                        grand_total = 0.0
                        for rk in row_leaves:
                            tds: list[str] = []
                            for d in range(0, rdn):
                                pf = rk[: d + 1]
                                if pf not in seen_prefix:
                                    seen_prefix.add(pf); rs = prefix_counts.get(pf, 1)
                                    tds.append(f"<th style='{TH_ROW}' rowspan='{rs}'>" + _esc(rk[d]) + "</th>")
                            row_sum = 0.0
                            for j, ck in enumerate(col_leaves):
                                v = float(valmap.get((rk, ck), 0.0) or 0.0)
                                row_sum += v; col_totals[j] += v
                                tds.append(f"<td style='{TD_CELL}'>" + f"{v:,.0f}" + "</td>")
                            if show_row_totals:
                                tds.append(f"<td style='{TD_TOTAL}'>" + f"{row_sum:,.0f}" + "</td>")
                            grand_total += row_sum
                            tbody_parts.append("<tr>" + "".join(tds) + "</tr>")
                        if show_col_totals:
                            tr: list[str] = []
                            left_span = max(1, rdn)
                            tr.append(f"<th style='{TH_HEAD}' colspan='{left_span}'>Total</th>")
                            for j in range(len(col_leaves)):
                                tr.append(f"<td style='{TD_TOTAL}'>" + f"{col_totals[j]:,.0f}" + "</td>")
                            if show_row_totals:
                                tr.append(f"<td style='{TD_TOTAL}'>" + f"{grand_total:,.0f}" + "</td>")
                            tbody_parts.append("<tr>" + "".join(tr) + "</tr>")
                        table_html_val = table_open + "<thead>" + "".join(thead_parts) + "</thead><tbody>" + "".join(tbody_parts) + "</tbody></table>"
                if table_html_val is None:
                    # Fallback: plain table
                    spec = render.get("querySpec") or {}
                    req = QuerySpecRequest(spec=spec, datasourceId=ds_id, limit=spec.get("limit") or 1000, offset=0, includeTotal=False)
                    res = run_query_spec(req, db)
                    table_html_val = _render_table_html(res)
                placeholders["TABLE_HTML"] = table_html_val
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

    # Dispatch actions (attach inline images for KPI if present and widget snapshot when available)
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
        # No widget/chart attachments when using data URIs
    except Exception:
        pass
    for a in actions:
        atype = str(a.get("type") or "").lower()
        if atype == "email":
            # If snapshot was expected but not available, suppress email send
            if snapshot_expected_v1 and not snapshot_ok_v1:
                continue
            to = [str(x).strip() for x in (a.get("to") or []) if str(x).strip()]
            if not to:
                # No recipients configured; skip email send
                continue
            raw_subject = str(a.get("subject") or rule.name)
            subj = _apply_placeholders(raw_subject, placeholders)
            ok, err = send_email(db, subject=subj, to=to, html=html, replacements=placeholders, inline_images=inline_images)
            if not ok and err:
                errs.append(f"email: {err}")
        elif atype == "sms":
            to = [str(x).strip() for x in (a.get("to") or []) if str(x).strip()]
            text = str(a.get("message") or (template or rule.name))
            try:
                text = _apply_placeholders(text, placeholders)
            except Exception:
                pass
            ok, err = send_sms_hadara(db, to_numbers=to, message=text)
            if not ok and err:
                errs.append(f"sms: {err}")

    return (len(errs) == 0), ("; ".join(errs) if errs else "ok")
