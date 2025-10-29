from __future__ import annotations

import asyncio
import base64
import logging
from typing import Optional
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response, JSONResponse

from ..config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/snapshot", tags=["snapshot"])


async def snapshot_embed_png(
    *,
    dashboard_id: Optional[str] = None,
    public_id: Optional[str] = None,
    token: Optional[str] = None,
    widget_id: str,
    datasource_id: Optional[str] = None,
    width: int = 800,
    height: int = 360,
    theme: str = "dark",
    actor_id: Optional[str] = None,
    wait_ms: int = 1200,
    retries: int = 0,
    backoff_ms: int = 800,
) -> bytes:
    try:
        from playwright.async_api import async_playwright  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError("playwright not installed; install backend dependency and browsers") from e

    base = settings.frontend_base_url.rstrip("/")
    qs = {
        "widgetId": widget_id,
        "w": str(int(width)),
        "h": str(int(height)),
        "theme": (theme or "dark"),
        "bg": "transparent",
        "snap": "1",
    }
    if datasource_id:
        qs["datasourceId"] = datasource_id
    if dashboard_id:
        qs["dashboardId"] = dashboard_id
        if actor_id:
            qs["actorId"] = actor_id
    elif public_id:
        qs["publicId"] = public_id
        if token:
            qs["token"] = token
    else:
        raise RuntimeError("dashboardId or publicId is required")
    url = f"{base}/render/embed/widget?{urlencode(qs)}"

    last_err: Exception | None = None
    attempt = 0
    while attempt <= max(0, int(retries)):
        attempt += 1
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch()
                try:
                    # Respect requested theme for CSS and JS that rely on either matchMedia or localStorage
                    _is_dark = str(theme or "dark").lower() == "dark"
                    context = await browser.new_context(
                        viewport={"width": int(width), "height": int(height)},
                        device_scale_factor=2,
                        color_scheme=("dark" if _is_dark else "light"),
                        reduced_motion="reduce",
                    )
                    try:
                        await context.add_init_script(
                            (
                                "try {\n"
                                f"  localStorage.setItem('theme', '{'dark' if _is_dark else 'light'}');\n"
                                "  const r = document.documentElement;\n"
                                f"  if ({'true' if _is_dark else 'false'}) r.classList.add('dark'); else r.classList.remove('dark');\n"
                                "} catch (e) {}\n"
                            )
                        )
                    except Exception:
                        pass
                    # Disable ECharts animation and progressive rendering for snapshots
                    try:
                        await context.add_init_script(
                            (
                                "(() => {\n"
                                "  try {\n"
                                "    // Global CSS kill-switch for CSS animations/transitions\n"
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
                    page = await context.new_page()
                    try:
                        await page.emulate_media(color_scheme=("dark" if _is_dark else "light"))
                    except Exception:
                        pass
                    try:
                        page.set_default_navigation_timeout(15000)
                    except Exception:
                        pass
                    await page.goto(url, wait_until="domcontentloaded")
                    try:
                        await page.wait_for_function(
                            "() => {\n"
                            "  const root = document.getElementById('widget-root');\n"
                            "  if (!root) return false;\n"
                            "  const wd = (window.__READY__ === true);\n"
                            "  const chartOk = (root.getAttribute('data-chart-ready') === '1');\n"
                            "  return wd && chartOk;\n"
                            "}",
                            timeout=wait_ms,
                        )
                    except Exception:
                        try:
                            await page.wait_for_selector("#widget-root[data-widget-ready='1']", timeout=wait_ms)
                        except Exception:
                            try:
                                await page.wait_for_selector("#widget-root", timeout=wait_ms)
                            except Exception:
                                pass
                    # Wait for quiescence: last finished timestamp should be stable for at least 1000ms
                    try:
                        await page.wait_for_function(
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
                    # Post-ready settle: wait for two more animation frames to ensure compositing is complete
                    try:
                        await page.wait_for_function("() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)))", timeout=400)
                    except Exception:
                        pass
                    # Extra settle to account for font layout/axis recalcs
                    try:
                        await page.wait_for_timeout(600)
                    except Exception:
                        pass
                    el = await page.query_selector("#widget-root")
                    if el:
                        png = await el.screenshot(type="png", omit_background=True)
                    else:
                        png = await page.screenshot(type="png", full_page=False, omit_background=True)
                    await context.close()
                    return png
                finally:
                    await browser.close()
        except Exception as e:
            last_err = e
            logger.warning("snapshot attempt %s failed for %s: %s", attempt, url, getattr(e, "message", str(e)))
            if attempt <= retries:
                await asyncio.sleep(max(0, (backoff_ms * attempt)) / 1000.0)
                continue
            break
    raise HTTPException(status_code=500, detail=f"Snapshot failed after {attempt} attempts: {last_err}")


@router.get("/widget")
async def snapshot_widget(
    dashboardId: Optional[str] = Query(default=None),
    publicId: Optional[str] = Query(default=None),
    token: Optional[str] = Query(default=None),
    widgetId: str = Query(...),
    datasourceId: Optional[str] = Query(default=None),
    w: int = Query(default=800),
    h: int = Query(default=360),
    theme: str = Query(default="dark"),
    actorId: Optional[str] = Query(default=None),
    waitMs: int = Query(default=1200),
):
    try:
        png = await snapshot_embed_png(
            dashboard_id=dashboardId,
            public_id=publicId,
            token=token,
            widget_id=widgetId,
            datasource_id=datasourceId,
            width=w,
            height=h,
            theme=theme,
            actor_id=actorId or settings.snapshot_actor_id,
            wait_ms=waitMs,
        )
        return Response(content=png, media_type="image/png")
    except RuntimeError as e:
        return JSONResponse(status_code=501, content={"error": str(e)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
