from __future__ import annotations

from fastapi import APIRouter, HTTPException, Depends, Query, Request
from pydantic import BaseModel, Field, ConfigDict
from typing import Any, Dict, List, Optional
import asyncio
import os
import httpx
import time as _t
from sqlalchemy.orm import Session

from ..models import SessionLocal, AiConfig, User
from ..security import encrypt_text, decrypt_text
from ..metrics import counter_inc, summary_observe

router = APIRouter(prefix="/ai", tags=["ai"]) 
_AI_LIMIT = 2
try:
    _AI_LIMIT = int(os.environ.get("AI_CONCURRENCY", "2") or "2")
except Exception:
    _AI_LIMIT = 2
if _AI_LIMIT <= 0:
    _AI_LIMIT = 1
_AI_SEM = asyncio.BoundedSemaphore(_AI_LIMIT)
_AI_TIMEOUT = 30
try:
    _AI_TIMEOUT = int(os.environ.get("AI_TIMEOUT_SECONDS", "30") or "30")
except Exception:
    _AI_TIMEOUT = 30
if _AI_TIMEOUT < 5:
    _AI_TIMEOUT = 5

# --- Schemas ---
class AiColumn(BaseModel):
    name: str
    type: Optional[str] = None

class AiSchema(BaseModel):
    table: str
    columns: List[AiColumn]

class AiBasePayload(BaseModel):
    provider: str = Field(pattern=r"^(gemini|openai|mistral|anthropic|openrouter)$")
    model: str
    apiKey: str
    baseUrl: Optional[str] = None
    model_config = ConfigDict(populate_by_name=True)

class AiDescribeRequest(AiBasePayload):
    dsSchema: AiSchema = Field(alias='schema')
    samples: List[Dict[str, Any]]

class AiDescribeResponse(BaseModel):
    description: str

class AiEnhanceRequest(AiBasePayload):
    dsSchema: AiSchema = Field(alias='schema')
    description: str
    userPrompt: str
    allowedTypes: List[str]

class AiEnhanceResponse(BaseModel):
    enhancedPrompt: str

class AiPlanRequest(AiBasePayload):
    dsSchema: AiSchema = Field(alias='schema')
    samples: List[Dict[str, Any]]
    prompt: str
    customColumns: Optional[List[str]] = None
    targetType: Optional[str] = None

class AiPlanResponse(BaseModel):
    plan: str

class AiSuggestRequest(AiBasePayload):
    dsSchema: AiSchema = Field(alias='schema')
    samples: List[Dict[str, Any]]
    prompt: str
    variantOffset: Optional[int] = 0
    plan: Optional[str] = None
    targetType: Optional[str] = None

class AiSuggestResponse(BaseModel):
    variants: List[Dict[str, Any]]


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class AiConfigPayload(BaseModel):
    provider: Optional[str] = None
    model: Optional[str] = None
    apiKey: Optional[str] = None
    baseUrl: Optional[str] = None


class AiConfigOut(BaseModel):
    provider: Optional[str] = None
    model: Optional[str] = None
    hasKey: bool = False
    baseUrl: Optional[str] = None


@router.get("/config", response_model=AiConfigOut)
async def get_ai_config(db: Session = Depends(get_db)) -> AiConfigOut:
    c = db.query(AiConfig).first()
    if not c:
        return AiConfigOut()
    return AiConfigOut(provider=c.provider, model=c.model, hasKey=bool(c.api_key_encrypted), baseUrl=c.base_url)


@router.put("/config")
async def put_ai_config(payload: AiConfigPayload, actorId: str | None = Query(default=None), db: Session = Depends(get_db)) -> dict:
    def _is_admin(db: Session, actor_id: str | None) -> bool:
        if not actor_id:
            return False
        u = db.query(User).filter(User.id == str(actor_id).strip()).first()
        return bool(u and (u.role or "user").lower() == "admin")
    if not _is_admin(db, actorId):
        raise HTTPException(status_code=403, detail="Forbidden")
    c = db.query(AiConfig).first()
    if not c:
        c = AiConfig(id="default")
    if payload.provider is not None:
        c.provider = payload.provider
    if payload.model is not None:
        c.model = payload.model
    if payload.apiKey is not None:
        if payload.apiKey == "":
            c.api_key_encrypted = None
        else:
            c.api_key_encrypted = encrypt_text(payload.apiKey)
    if payload.baseUrl is not None:
        bu = (payload.baseUrl or "").strip()
        c.base_url = (bu or None)
    db.add(c)
    db.commit()
    return {"ok": True}

# --- LLM call helpers ---
async def _call_gemini(model: str, api_key: str, system: Optional[str], user: str) -> str:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    headers = {"Content-Type": "application/json"}
    parts: List[Dict[str, Any]] = []
    if system:
        parts.append({"text": system})
    parts.append({"text": user})
    payload = {"contents": [{"parts": parts}]}
    async with httpx.AsyncClient(timeout=httpx.Timeout(_AI_TIMEOUT)) as client:
        r = await client.post(url, headers=headers, json=payload)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Gemini error: {r.text}")
        data = r.json()
        try:
            return data["candidates"][0]["content"]["parts"][0]["text"]
        except Exception:
            raise HTTPException(status_code=502, detail="Gemini malformed response")

async def _call_openai(model: str, api_key: str, system: Optional[str], user: str, base_url: Optional[str] = None) -> str:
    url = f"{(base_url or 'https://api.openai.com/v1').rstrip('/')}/chat/completions"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    messages: List[Dict[str, Any]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": user})
    payload = {"model": model, "messages": messages, "temperature": 0.2}
    async with httpx.AsyncClient(timeout=httpx.Timeout(_AI_TIMEOUT)) as client:
        r = await client.post(url, headers=headers, json=payload)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"OpenAI error: {r.text}")
        data = r.json()
        try:
            return data["choices"][0]["message"]["content"]
        except Exception:
            raise HTTPException(status_code=502, detail="OpenAI malformed response")

async def _call_mistral(model: str, api_key: str, system: Optional[str], user: str, base_url: Optional[str] = None) -> str:
    url = f"{(base_url or 'https://api.mistral.ai/v1').rstrip('/')}/chat/completions"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    messages: List[Dict[str, Any]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": user})
    payload = {"model": model, "messages": messages, "temperature": 0.2}
    async with httpx.AsyncClient(timeout=httpx.Timeout(_AI_TIMEOUT)) as client:
        r = await client.post(url, headers=headers, json=payload)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Mistral error: {r.text}")
        data = r.json()
        try:
            return data["choices"][0]["message"]["content"]
        except Exception:
            raise HTTPException(status_code=502, detail="Mistral malformed response")

async def _call_anthropic(model: str, api_key: str, system: Optional[str], user: str, base_url: Optional[str] = None) -> str:
    base = (base_url or 'https://api.anthropic.com/v1').rstrip('/')
    url = f"{base}/messages"
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }
    # Anthropic uses system string and messages array of {role, content}
    messages: List[Dict[str, Any]] = []
    if system:
        # Anthropic supports a system field separate from messages
        sys_text = system
    else:
        sys_text = None
    messages.append({"role": "user", "content": user})
    payload: Dict[str, Any] = {"model": model, "max_tokens": 1024, "messages": messages, "temperature": 0.2}
    if sys_text:
        payload["system"] = sys_text
    async with httpx.AsyncClient(timeout=httpx.Timeout(_AI_TIMEOUT)) as client:
        r = await client.post(url, headers=headers, json=payload)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Anthropic error: {r.text}")
        data = r.json()
        try:
            # messages API returns content: [{type: 'text', text: '...'}]
            return data["content"][0]["text"]
        except Exception:
            raise HTTPException(status_code=502, detail="Anthropic malformed response")

async def _call_openrouter(model: str, api_key: str, system: Optional[str], user: str, base_url: Optional[str] = None) -> str:
    url = f"{(base_url or 'https://openrouter.ai/api/v1').rstrip('/')}/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    messages: List[Dict[str, Any]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": user})
    payload = {"model": model, "messages": messages, "temperature": 0.2}
    async with httpx.AsyncClient(timeout=httpx.Timeout(_AI_TIMEOUT)) as client:
        r = await client.post(url, headers=headers, json=payload)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"OpenRouter error: {r.text}")
        data = r.json()
        try:
            return data["choices"][0]["message"]["content"]
        except Exception:
            raise HTTPException(status_code=502, detail="OpenRouter malformed response")

async def _call_llm(provider: str, model: str, api_key: str, system: Optional[str], user: str, base_url: Optional[str] = None) -> str:
    p = provider.lower()
    if p == "gemini":
        async with _AI_SEM:
            return await _call_gemini(model, api_key, system, user)
    if p == "openai":
        async with _AI_SEM:
            return await _call_openai(model, api_key, system, user, base_url)
    if p == "mistral":
        async with _AI_SEM:
            return await _call_mistral(model, api_key, system, user, base_url)
    if p == "anthropic":
        async with _AI_SEM:
            return await _call_anthropic(model, api_key, system, user, base_url)
    if p == "openrouter":
        async with _AI_SEM:
            return await _call_openrouter(model, api_key, system, user, base_url)
    raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")

# --- Prompts ---
_DEF_SYS = (
    "You are a data visualization assistant embedded in a dashboard builder. "
    "When asked, you will map user intent to available fields, choose appropriate chart types from the app, and output concise, strict JSON that matches the requested schema."
)

@router.post("/describe", response_model=AiDescribeResponse)
async def ai_describe(payload: AiDescribeRequest, request: Request, db: Session = Depends(get_db)) -> AiDescribeResponse:
    cols = ", ".join([f"{c.name}{f' ({c.type})' if c.type else ''}" for c in payload.dsSchema.columns])
    sample_text = "\n".join([str(s) for s in payload.samples[:10]])
    user = (
        "Describe the following table data in one paragraph.\n"
        f"Table: {payload.dsSchema.table}\n"
        f"Columns: {cols}\n"
        f"Sample rows (up to 10):\n{sample_text}\n"
        "Keep it concise."
    )
    prov = payload.provider
    mdl = payload.model
    key = payload.apiKey
    base_url = (payload.baseUrl or None)
    if not key or not base_url or not prov or not mdl:
        cfg = db.query(AiConfig).first()
        if cfg:
            if (not prov) and cfg.provider:
                prov = cfg.provider
            if (not mdl) and cfg.model:
                mdl = cfg.model
            if (not base_url) and cfg.base_url:
                base_url = cfg.base_url
            if (not key) and cfg.api_key_encrypted:
                key = decrypt_text(cfg.api_key_encrypted)
    if not key:
        raise HTTPException(status_code=400, detail="Missing API key")
    if await request.is_disconnected():
        raise HTTPException(status_code=499, detail="Client disconnected")
    _s = _t.perf_counter()
    text = await _call_llm(prov or "gemini", mdl or "gemini-1.5-flash", key, _DEF_SYS, user, base_url)
    _d = int((_t.perf_counter() - _s) * 1000)
    _pv = (prov or "gemini").lower()
    counter_inc("ai_requests_total", {"provider": _pv, "endpoint": "describe"})
    summary_observe("ai_request_duration_ms", _d, {"provider": _pv, "endpoint": "describe"})
    return AiDescribeResponse(description=text.strip())

@router.post("/enhance", response_model=AiEnhanceResponse)
async def ai_enhance(payload: AiEnhanceRequest, request: Request, db: Session = Depends(get_db)) -> AiEnhanceResponse:
    cols = ", ".join([c.name for c in payload.dsSchema.columns])
    allowed = ", ".join(payload.allowedTypes or [])
    user = (
        "Given the table schema and a user prompt, map the user intent to the available fields and chart types.\n"
        f"Table: {payload.dsSchema.table}\n"
        f"Fields: {cols}\n"
        f"Allowed types: {allowed}\n"
        f"Data description: {payload.description}\n"
        f"User prompt: {payload.userPrompt}\n"
        "Guidance: If the user asks for a time-based visualization (e.g., per hour/day/week/month/quarter/year), choose a date/datetime column for x and explicitly state the bucket. Detect ranking and limits: phrases like 'top 3', 'top 10', 'most', 'least', 'highest', 'lowest' imply sorting by value and applying LIMIT N. Detect dimensions after 'by' or 'per' (e.g., 'per merchant' -> group by [Merchant]). Detect filters such as 'last 7 days', 'this month', 'where [Field] = value' and mention them plainly in the instruction.\n"
        "Output format: Return ONLY a SINGLE LINE natural-language instruction that starts with an imperative verb (e.g., 'Create ...'). Do NOT return JSON or code blocks, and do NOT include quotes or markdown.\n"
        "Field references: When referencing specific fields, ALWAYS wrap the exact column names in square brackets using the schema-provided names, e.g., [OrderDate], [TotalAmount]. Include sorting direction and limit when applicable (e.g., 'sorted by value descending, top 3').\n"
        "Respond with an improved prompt that references concrete field names and one of the allowed chart types."
    )
    prov = payload.provider
    mdl = payload.model
    key = payload.apiKey
    base_url = (payload.baseUrl or None)
    if not key or not base_url or not prov or not mdl:
        cfg = db.query(AiConfig).first()
        if cfg:
            if not prov and cfg.provider:
                prov = cfg.provider
            if not mdl and cfg.model:
                mdl = cfg.model
            if (not base_url) and cfg.base_url:
                base_url = cfg.base_url
            if cfg.api_key_encrypted and not key:
                key = decrypt_text(cfg.api_key_encrypted)
    if not key:
        raise HTTPException(status_code=400, detail="Missing API key")
    if await request.is_disconnected():
        raise HTTPException(status_code=499, detail="Client disconnected")
    _s = _t.perf_counter()
    text = await _call_llm(prov or "gemini", mdl or "gemini-1.5-flash", key, _DEF_SYS, user, base_url)
    _d = int((_t.perf_counter() - _s) * 1000)
    _pv = (prov or "gemini").lower()
    counter_inc("ai_requests_total", {"provider": _pv, "endpoint": "enhance"})
    summary_observe("ai_request_duration_ms", _d, {"provider": _pv, "endpoint": "enhance"})
    return AiEnhanceResponse(enhancedPrompt=text.strip())

@router.post("/plan", response_model=AiPlanResponse)
async def ai_plan(payload: AiPlanRequest, request: Request, db: Session = Depends(get_db)) -> AiPlanResponse:
    cols = ", ".join([c.name for c in payload.dsSchema.columns])
    sample_text = "\n".join([str(s) for s in payload.samples[:5]])
    # Enumerate options explicitly in the system message for diversity
    palette_opts = "default, pastel, sunset, ocean, forest"
    chart_types = "line, bar, area, column, donut, categoryBar, spark, combo, scatter, tremorTable, heatmap, barList, gantt"
    kpi_presets = "single, delta, progress, donut, multiProgress, categoryBar"
    table_types = "data, pivot"
    groupby_opts = "none, day, week, month, quarter, year"
    sortby_opts = "x, value"
    legend_candidates = ", ".join([c.name for c in payload.dsSchema.columns])
    custom_cols_line = (
        f"Custom columns (prioritize for legend/categories/dimensions): {', '.join([str(n) for n in (payload.customColumns or [])])}.\n"
        if (payload.customColumns or []) else ""
    )
    target_line = (
        f"Target widget type is '{str(payload.targetType).lower()}'. Restrict all steps strictly to this target. If chart, only chart steps; if kpi, only KPI presets; if table, only table steps.\n"
        if (payload.targetType or '').strip() else ""
    )
    plan_system = (
        _DEF_SYS
        + "\nYou are creating a PLANNING document (not the final configs). Be explicit and DIVERSE.\n"
        + f"Available chart types: {chart_types}.\n"
        + f"Available KPI presets: {kpi_presets}.\n"
        + f"Available table types: {table_types}.\n"
        + f"Available groupBy options: {groupby_opts}.\n"
        + f"Available sortBy options: {sortby_opts}.\n"
        + f"Color palettes: {palette_opts}. Prefer options.palette.\n"
        + f"Possible legend fields (choose those that are categorical): {legend_candidates}.\n"
        + custom_cols_line
        + target_line
        + "Chart field mapping rule: Use legend for CATEGORICAL splits (categories), and reserve x for a TIME index only when groupBy is time-based. For donut/pie/categoryBar/barList, DO NOT put the categorical field in x; put it in legend. For simple single-series bar/column with no time grouping you may put the categorical field in x (legend optional).\n"
        + "Each step MUST include: fields.dimensionField? (e.g., [Customer] or [Merchant]), fields.timeField? (date/datetime), fields.measureField? (numeric), agg? ('count'|'distinct'|'avg'|'sum'|'min'|'max'), groupBy (none|hour|day|week|month|quarter|year), orderBy ('x'|'value'), order ('asc'|'desc'), limit? (integer), filters? (plain natural language like 'last 7 days' or simple [Field] op value).\n"
        + "When the prompt implies ranking (e.g., 'top 3', 'most', 'least'), set orderBy='value' and order to 'desc' for most/top or 'asc' for least, and set limit accordingly.\n"
        + "When targetType is not constrained, COVER MULTIPLE TARGETS: include at least one 'chart' step and also one 'table' or 'kpi' step, with distinct goals.\n"
        + "Provide stable stepIndex (0-based) and a short id for each step to enable continuation across calls."
    )
    user = (
        "Given the user intent and schema, produce a JSON plan to cover diverse representations.\n"
        f"Table: {payload.dsSchema.table} with fields: {cols}.\n"
        f"Samples (up to 5):\n{sample_text}\n"
        f"Enhanced Prompt: {payload.prompt}\n"
        "Plan format: Return STRICT JSON object: { steps: [ { stepIndex: number, id: string, target: 'chart'|'kpi'|'table', chartTypes?: string[], kpiPresets?: string[], tableType?: 'data'|'pivot', fields?: { dimensionField?: string, timeField?: string, measureField?: string }, agg?: 'none'|'count'|'distinct'|'avg'|'sum'|'min'|'max', groupBy: 'none'|'hour'|'day'|'week'|'month'|'quarter'|'year', orderBy?: 'x'|'value', order?: 'asc'|'desc', limit?: number, legendCandidates?: string[], palette?: string, filters?: string[], note?: string } ] }.\n"
        "Guidelines:\n"
        "- Detect ranking phrases ('top 10', 'most', 'least') and set orderBy/order/limit.\n"
        "- Detect grouping cues ('per hour', 'by merchant', 'per day') and set groupBy and fields accordingly.\n"
        "- Include at least one chart step; when not constrained, also include a table or kpi step.\n"
        "- Ensure steps are UNIQUE in purpose (different chart types or targets).\n"
        "- Keep steps <= 4; each step should define a distinct variant family.\n"
    )
    prov = payload.provider
    mdl = payload.model
    key = payload.apiKey
    base_url = (payload.baseUrl or None)
    if not key or not base_url or not prov or not mdl:
        cfg = db.query(AiConfig).first()
        if cfg:
            if not prov and cfg.provider:
                prov = cfg.provider
            if not mdl and cfg.model:
                mdl = cfg.model
            if (not base_url) and cfg.base_url:
                base_url = cfg.base_url
            if cfg.api_key_encrypted and not key:
                key = decrypt_text(cfg.api_key_encrypted)
    if not key:
        raise HTTPException(status_code=400, detail="Missing API key")
    if await request.is_disconnected():
        raise HTTPException(status_code=499, detail="Client disconnected")
    _s = _t.perf_counter()
    text = await _call_llm(prov or "gemini", mdl or "gemini-1.5-flash", key, plan_system, user, base_url)
    _d = int((_t.perf_counter() - _s) * 1000)
    _pv = (prov or "gemini").lower()
    counter_inc("ai_requests_total", {"provider": _pv, "endpoint": "plan"})
    summary_observe("ai_request_duration_ms", _d, {"provider": _pv, "endpoint": "plan"})
    return AiPlanResponse(plan=text.strip())

@router.post("/suggest", response_model=AiSuggestResponse)
async def ai_suggest(payload: AiSuggestRequest, request: Request, db: Session = Depends(get_db)) -> AiSuggestResponse:
    cols = ", ".join([c.name for c in payload.dsSchema.columns])
    sample_text = "\n".join([str(s) for s in payload.samples[:10]])
    offset = int(payload.variantOffset or 0)
    plan_line = f"Planning Guidance: {payload.plan}\n" if (payload.plan or '').strip() else ""
    target_line = (
        f"You MUST return only widgets of type '{str(payload.targetType).lower()}'. If chart: only charts; if kpi: only KPIs; if table: only tables.\n"
        if (payload.targetType or '').strip() else ""
    )
    user = (
        "You will propose THREE unique widget JSON configurations for a dashboard app, continuing from a prior PLAN.\n"
        "Each config must be STRICT JSON (no comments), matching: \n"
        "{ id: string; type: 'chart'|'table'|'kpi'; title: string; sql?: string; queryMode?: 'sql'|'spec'; querySpec?: { source: string; x?: string; y?: string; measure?: string; agg?: 'none'|'count'|'distinct'|'avg'|'sum'|'min'|'max'; legend?: string|string[]; series?: Array<{ x?: string; y: string; agg?: 'count'|'distinct'|'avg'|'sum'|'min'|'max'; label?: string }>; where?: Record<string, any> }; chartType?: 'line'|'bar'|'area'|'column'|'donut'|'categoryBar'|'spark'|'combo'|'scatter'|'tremorTable'|'heatmap'|'barList'|'gantt'; options?: { xTimeUnit?: 'hour'|'day'|'week'|'month'|'quarter'|'year'; orderBy?: 'x'|'value'; order?: 'asc'|'desc'; limit?: number; palette?: string } }\n"
        f"Table: {payload.dsSchema.table} with fields: {cols}.\n"
        f"Samples (up to 10):\n{sample_text}\n"
        f"Enhanced Prompt: {payload.prompt}\n"
        f"{plan_line}"
        f"{target_line}"
        f"Variant offset index: {offset}\n"
        "Continuation rule: Parse the PLAN JSON if present. Choose the step at index floor(variantOffset/3) modulo steps.length. Generate EXACTLY THREE variants aligned to that step's target, fields, groupBy, orderBy/order, limit, and palette. If chartTypes[] is provided for the step, vary chartType across the three; otherwise vary legend or series.\n"
        "Rules: Prefer querySpec over raw SQL. Ensure referenced fields exist.\n"
        "Field mapping: Use legend for CATEGORICAL categories, x for TIME index only when groupBy is time-based. For donut/pie/categoryBar/barList, map the category dimension to querySpec.legend (not x). For simple single-series bar/column with no time grouping, x may hold the category and legend can be omitted.\n"
        "If step.groupBy is time-based, set querySpec.groupBy accordingly (day|week|month|quarter|year) and use aggregation (e.g., sum/count).\n"
        "If the step implies ranking (top N/most/least), set options.orderBy='value', options.order, and options.limit=N.\n"
        "Return JSON object: { variants: [...] }."
    )
    prov = payload.provider
    mdl = payload.model
    key = payload.apiKey
    if not key:
        cfg = db.query(AiConfig).first()
        if cfg:
            if not prov and cfg.provider:
                prov = cfg.provider
            if not mdl and cfg.model:
                mdl = cfg.model
            if cfg.api_key_encrypted:
                key = decrypt_text(cfg.api_key_encrypted)
    if not key:
        raise HTTPException(status_code=400, detail="Missing API key")
    if await request.is_disconnected():
        raise HTTPException(status_code=499, detail="Client disconnected")
    _s = _t.perf_counter()
    text = await _call_llm(prov or "gemini", mdl or "gemini-1.5-flash", key, _DEF_SYS, user, base_url)
    _d = int((_t.perf_counter() - _s) * 1000)
    _pv = (prov or "gemini").lower()
    counter_inc("ai_requests_total", {"provider": _pv, "endpoint": "suggest"})
    summary_observe("ai_request_duration_ms", _d, {"provider": _pv, "endpoint": "suggest"})
    # Best-effort to extract JSON array or object with variants
    import json
    variants: List[Dict[str, Any]] = []
    try:
        data = json.loads(text)
        if isinstance(data, dict) and isinstance(data.get("variants"), list):
            variants = data["variants"]
        elif isinstance(data, list):
            variants = data
    except Exception:
        # try to locate first JSON array substring
        import re
        m = re.search(r"\[\s*\{[\s\S]*\}\s*\]", text)
        if m:
            try:
                variants = json.loads(m.group(0))
            except Exception:
                variants = []
    # Strict validation/sanitization
    def _normalize_name(s: Any) -> str:
        try:
            n = str(s or "").strip()
            if n.startswith("[") and n.endswith("]"):
                n = n[1:-1]
            return n
        except Exception:
            return ""

    # Build sets for exact-lower and punctuation-agnostic matching
    col_set = {(_normalize_name(c.name).lower()) for c in payload.dsSchema.columns}
    import re as _re
    def _normkey(s: str) -> str:
        try:
            return _re.sub(r"[^a-z0-9]", "", s.lower())
        except Exception:
            return s.lower()
    col_key_set = {_normkey(n) for n in col_set}
    allowed_chart_types = {
        "line","bar","area","column","donut","categorybar","spark","combo","scatter","tremortable","heatmap","barlist","gantt"
    }
    allowed_aggs = {"none","count","distinct","avg","sum","min","max"}

    def _field_exists(f: Any) -> bool:
        n = _normalize_name(f).lower()
        if not n:
            return False
        if n in col_set:
            return True
        # Fallback: ignore punctuation/whitespace differences
        nk = _normkey(n)
        if nk and nk in col_key_set:
            return True
        return False

    def _valid_chart(v: Dict[str, Any]) -> bool:
        chart_type = _normalize_name(v.get("chartType") or "").lower()
        # Map common synonyms
        if chart_type == "pie":
            chart_type = "donut"
        if chart_type not in allowed_chart_types:
            return False
        mode = (v.get("queryMode") or ("sql" if v.get("sql") else "spec")).lower()
        if mode == "sql":
            return bool(v.get("sql") and isinstance(v.get("sql"), str) and v.get("sql").strip())
        qs = v.get("querySpec") or {}
        if not isinstance(qs, dict):
            return False
        if not isinstance(qs.get("source"), str) or not qs.get("source"):
            return False
        # Series path
        series = qs.get("series")
        if isinstance(series, list) and len(series) > 0:
            for s in series:
                if not isinstance(s, dict):
                    return False
                if not _field_exists(s.get("y")):
                    return False
                a = (s.get("agg") or "count").lower()
                if a not in allowed_aggs:
                    return False
                # Optional x
                if s.get("x") is not None and not _field_exists(s.get("x")):
                    return False
            # Optional legend
            lg = qs.get("legend")
            if isinstance(lg, str) and lg and not _field_exists(lg):
                return False
            if isinstance(lg, list) and not all(_field_exists(x) for x in lg):
                return False
            return True
        # Single path: x + (y or measure)
        x = qs.get("x")
        y = qs.get("y")
        m = qs.get("measure")
        if not _field_exists(x):
            return False
        if not ( (_field_exists(y) if isinstance(y, str) else False) or (_field_exists(m) if isinstance(m, str) else False) ):
            return False
        a = (qs.get("agg") or "count").lower()
        if a not in allowed_aggs:
            return False
        lg = qs.get("legend")
        if isinstance(lg, str) and lg and not _field_exists(lg):
            return False
        if isinstance(lg, list) and not all(_field_exists(x) for x in lg):
            return False
        return True

    def _valid_table(v: Dict[str, Any]) -> bool:
        qs = v.get("querySpec") or {}
        if not isinstance(qs, dict):
            return False
        if not isinstance(qs.get("source"), str) or not qs.get("source"):
            return False
        sel = qs.get("select")
        if isinstance(sel, list) and len(sel) > 0:
            if not all(_field_exists(f) for f in sel):
                return False
        return True

    def _valid_kpi(v: Dict[str, Any]) -> bool:
        mode = (v.get("queryMode") or ("sql" if v.get("sql") else "spec")).lower()
        if mode == "sql":
            return bool(v.get("sql") and isinstance(v.get("sql"), str) and v.get("sql").strip())
        qs = v.get("querySpec") or {}
        if not isinstance(qs, dict):
            return False
        if not isinstance(qs.get("source"), str) or not qs.get("source"):
            return False
        y = qs.get("y")
        m = qs.get("measure")
        return (_field_exists(y) if isinstance(y, str) else False) or (_field_exists(m) if isinstance(m, str) else False)

    out: List[Dict[str, Any]] = []
    for i, v in enumerate(variants):
        if not isinstance(v, dict):
            continue
        vv: Dict[str, Any] = dict(v)
        # Defaults and stitching
        vv.setdefault("id", f"ai_{offset+i}")
        vv.setdefault("title", "AI Suggested")
        # Infer type
        t = str(vv.get("type") or ("chart" if vv.get("chartType") else "chart")).lower()
        vv["type"] = t
        # Normalize chartType
        if t == "chart":
            ctype = _normalize_name(vv.get("chartType") or "").lower()
            if not ctype:
                ctype = "column"
            vv["chartType"] = ctype
        # Normalize query mode and querySpec
        mode = (vv.get("queryMode") or ("sql" if vv.get("sql") else "spec")).lower()
        vv["queryMode"] = mode
        qs = vv.get("querySpec") or {}
        if not isinstance(qs, dict):
            qs = {}
        if not qs.get("source"):
            qs["source"] = payload.dsSchema.table
        # Enrich from PLAN when provided: choose step based on variantOffset and apply hints
        try:
            import json as _json
            if (payload.plan or '').strip():
                _plan = _json.loads(payload.plan)
            else:
                _plan = None
        except Exception:
            _plan = None
        try:
            if isinstance(_plan, dict) and isinstance(_plan.get("steps"), list) and len(_plan.get("steps") or []) > 0:
                _steps = _plan.get("steps") or []
                _idx = 0
                try:
                    _idx = int(max(0, (offset // 3) % max(1, len(_steps))))
                except Exception:
                    _idx = 0
                _st = _steps[_idx] or {}
                _fields = _st.get("fields") or {}
                _dim = _fields.get("dimensionField")
                _timef = _fields.get("timeField")
                _meas = _fields.get("measureField")
                _aggst = str((_st.get("agg") or qs.get("agg") or "count")).lower()
                if (not qs.get("x")) and isinstance(_timef, str) and _timef.strip():
                    qs["x"] = _timef
                if (not qs.get("legend")):
                    try:
                        _lc = [str(x) for x in (_st.get("legendCandidates") or []) if str(x).strip()]
                        if _lc:
                            qs["legend"] = _lc[:2] if len(_lc) > 1 else _lc[0]
                    except Exception:
                        pass
                    # Fallback: use dimensionField as legend when present
                    if (not qs.get("legend")) and isinstance(_dim, str) and _dim.strip():
                        qs["legend"] = _dim
                if (not qs.get("series")) and isinstance(_meas, str) and _meas.strip():
                    qs["series"] = [{"label": "Series 1", "y": _meas, "agg": _aggst, "secondaryAxis": False}]
                # groupBy
                _gbst = str((_st.get("groupBy") or "")).lower()
                if _gbst in {"hour","day","week","month","quarter","year","none"} and not qs.get("groupBy"):
                    qs["groupBy"] = ("day" if _gbst == "hour" else _gbst)
                # filters
                _fl = _st.get("filters")
                if isinstance(_fl, list):
                    _wh = dict(qs.get("where") or {})
                    for _f in _fl:
                        _name = _normalize_name(_f)
                        if _name and _name not in _wh:
                            _wh[_name] = None
                    if _wh:
                        qs["where"] = _wh
                # order/limit
                _ord_by = _st.get("orderBy")
                _ord = _st.get("order")
                _lim = _st.get("limit")
                _optsv = vv.get("options") or {}
                if isinstance(_optsv, dict):
                    if _ord_by in ("x","value"):
                        _optsv["orderBy"] = _ord_by
                    if _ord in ("asc","desc"):
                        _optsv["order"] = _ord
                    if _lim is not None and isinstance(_lim, int) and qs.get("limit") is None:
                        qs["limit"] = _lim
                    vv["options"] = _optsv
        except Exception:
            pass
        # Prefer querySpec.groupBy over options.xTimeUnit; map unsupported 'hour' to 'day'
        try:
            opts = vv.get("options") or {}
            if isinstance(opts, dict) and opts.get("xTimeUnit") and not qs.get("groupBy"):
                xtu = str(opts.get("xTimeUnit") or "").lower()
                gb_map = {"hour": "day", "day": "day", "week": "week", "month": "month", "quarter": "quarter", "year": "year"}
                if xtu in gb_map:
                    qs["groupBy"] = gb_map[xtu]
        except Exception:
            pass
        # If still no groupBy but x is time-like, rotate day/week/month across the 3 variants
        try:
            import re as _re
            if not qs.get("groupBy"):
                x_now = str(qs.get("x") or "")
                if _re.search(r"(date|time|timestamp|day|month|year)", x_now, _re.I):
                    cycle = ["day","week","month"]
                    qs["groupBy"] = cycle[i % len(cycle)]
        except Exception:
            pass
        # Move options.limit into querySpec.limit when present
        try:
            opts = vv.get("options") or {}
            if isinstance(opts, dict) and ("limit" in opts) and (qs.get("limit") is None):
                lim = opts.get("limit")
                if isinstance(lim, int):
                    qs["limit"] = lim
        except Exception:
            pass
        # Detect ranking (Top N / most / least) directly from the prompt when plan didn't provide it
        try:
            optsv = vv.get("options") or {}
            ptxt = str(payload.prompt or "")
            import re as _re
            # Only infer if not already specified by plan
            need_orderby = str(optsv.get("orderBy") or "").lower() not in ("x","value")
            has_limit = isinstance(qs.get("limit"), int) or isinstance(optsv.get("limit"), int)
            if need_orderby or (not has_limit):
                m_top = _re.search(r"\btop\s+(\d{1,3})\b", ptxt, _re.I)
                m_most = _re.search(r"\b(most|highest|max|largest|top)\b", ptxt, _re.I)
                m_least = _re.search(r"\b(least|lowest|min|smallest)\b", ptxt, _re.I)
                if m_top or m_most or m_least:
                    order = "desc" if m_most or (m_top and not m_least) else "asc"
                    if need_orderby:
                        optsv["orderBy"] = "value"
                        optsv["order"] = order
                    if not has_limit and m_top:
                        try:
                            limv = int(m_top.group(1))
                            if limv > 0:
                                qs["limit"] = limv
                                optsv["limit"] = limv
                        except Exception:
                            pass
            vv["options"] = optsv
        except Exception:
            pass
        # Default: hide legend in all variants (can be enabled later in UI)
        try:
            optsv = vv.get("options") or {}
            optsv["showLegend"] = False
            vv["options"] = optsv
        except Exception:
            pass
        # When groupBy is set on querySpec, ensure options.xTimeUnit mirrors it (for chart components that read xTimeUnit)
        try:
            gb = str(qs.get("groupBy") or "").lower()
            if gb in {"day","week","month","quarter","year"}:
                optsv = vv.get("options") or {}
                if not optsv.get("xTimeUnit"):
                    optsv["xTimeUnit"] = gb
                    vv["options"] = optsv
        except Exception:
            pass
        # Populate options.dataDefaults (sort/topN) to help downstream UI badges and defaults
        try:
            optsv = vv.get("options") or {}
            dd = (optsv.get("dataDefaults") or {}) if isinstance(optsv, dict) else {}
            dd["useDatasourceDefaults"] = False
            ob = str((optsv.get("orderBy") or "")).lower()
            if ob == "value":
                dd["sort"] = {"by": "value"}
            lim_any = qs.get("limit") if isinstance(qs.get("limit"), int) else optsv.get("limit")
            try:
                lim_int = int(lim_any) if lim_any is not None else None
            except Exception:
                lim_int = None
            if lim_int and lim_int > 0:
                dd["topN"] = {"n": lim_int, "by": "value"}
            optsv["dataDefaults"] = dd
            vv["options"] = optsv
        except Exception:
            pass
        # Copy ranking hints into querySpec so server can respect TopN order
        try:
            optsv = vv.get("options") or {}
            ob = str((optsv.get("orderBy") or "")).lower()
            od = str((optsv.get("order") or "")).lower()
            if (not qs.get("orderBy")) and (ob in ("x","value")):
                qs["orderBy"] = ob
            if (not qs.get("order")) and (od in ("asc","desc")):
                qs["order"] = od
        except Exception:
            pass
        # Safeguard: for charts that are category-first (donut/categoryBar/barList), ensure categories are in legend
        try:
            ct = str(vv.get("chartType") or "").lower()
            if ct in {"donut","categorybar","barlist"}:
                lg = qs.get("legend")
                has_lg = (isinstance(lg, str) and bool(str(lg).strip())) or (isinstance(lg, list) and len(lg) > 0)
                if not has_lg:
                    # If x is set and categorical, move it to legend (keep x for server shape; client will aggregate across x)
                    x_now = qs.get("x")
                    if isinstance(x_now, str) and x_now.strip():
                        qs["legend"] = x_now
                    else:
                        # fallback: pick first available column name
                        for c in payload.dsSchema.columns:
                            n = _normalize_name(c.name)
                            if n:
                                qs["legend"] = n
                                break
        except Exception:
            pass
        # If groupBy is time-based but x is not date-like, coerce x to a date/datetime column and move previous x to legend when missing
        try:
            import re as _re
            gb_now = str(qs.get("groupBy") or "").lower()
            if gb_now in {"day","week","month","quarter","year"}:
                x_now = str(qs.get("x") or "")
                is_time_like = bool(_re.search(r"(date|time|timestamp|day|month|year)", x_now, _re.I))
                if not is_time_like:
                    # try to keep previous x as legend if legend missing
                    if x_now and not qs.get("legend"):
                        qs["legend"] = x_now
                    # pick a date-like column from schema
                    for c in payload.dsSchema.columns:
                        n = _normalize_name(c.name)
                        if _re.search(r"(date|time|timestamp|day|month|year)", n, _re.I):
                            qs["x"] = n
                            break
        except Exception:
            pass
        # Light diversification across the 3 variants to increase variety (chartType rotation)
        try:
            if t == "chart":
                import re as _re
                x_now = str(qs.get("x") or "")
                is_time_like = bool(_re.search(r"(date|time|timestamp|day|month|year)", x_now, _re.I))
                cands_ts = ["line", "area", "column"]
                cands_cat = ["column", "bar", "donut"]
                base = cands_ts if is_time_like else cands_cat
                vv["chartType"] = base[i % len(base)]
        except Exception:
            pass
        # Normalize existing series entries: ensure 'y' is set when only 'measure' exists; ensure 'agg'
        try:
            if isinstance(qs.get("series"), list):
                _series_norm = []
                for s in (qs.get("series") or []):
                    if not isinstance(s, dict):
                        continue
                    if (not s.get("y")) and s.get("measure"):
                        s["y"] = s.get("measure")
                        s.pop("measure", None)
                    if not s.get("agg"):
                        s["agg"] = (qs.get("agg") or "count")
                    _series_norm.append(s)
                qs["series"] = _series_norm
        except Exception:
            pass
        # Ensure series array exists under querySpec for charts if only single y/measure provided
        try:
            if t == "chart":
                if not isinstance(qs.get("series"), list) or len(qs.get("series") or []) == 0:
                    y = qs.get("y")
                    m = qs.get("measure")
                    ag = (qs.get("agg") or "count").lower()
                    if isinstance(y, str) or isinstance(m, str):
                        series_item = {"label": "Series 1", "y": y or m, "agg": ag, "secondaryAxis": False}
                        qs["series"] = [series_item]
        except Exception:
            pass
        vv["querySpec"] = qs
        # Mirror to top-level series/xAxis for ChartCard convenience
        try:
            if t == "chart":
                # Top-level series from querySpec.series
                if (not isinstance(vv.get("series"), list) or len(vv.get("series") or []) == 0) and isinstance(qs.get("series"), list):
                    _ser = []
                    for s in (qs.get("series") or []):
                        try:
                            _ser.append({
                                "id": s.get("id") or None,
                                "x": qs.get("x") or s.get("x") or None,
                                "y": s.get("y"),
                                "agg": s.get("agg") or qs.get("agg") or "count",
                                "secondaryAxis": bool(s.get("secondaryAxis") or False),
                            })
                        except Exception:
                            continue
                    if _ser:
                        vv["series"] = _ser
                # xAxis.groupBy from querySpec.groupBy
                gb = (qs.get("groupBy") or "").lower()
                if gb in {"hour","day","week","month","quarter","year","none"}:
                    xa = vv.get("xAxis") or {}
                    if not isinstance(xa, dict):
                        xa = {}
                    xa["groupBy"] = gb
                    vv["xAxis"] = xa
                # Map options.palette -> options.colorPreset when present
                optsv = vv.get("options") or {}
                if isinstance(optsv, dict) and ("palette" in optsv) and ("colorPreset" not in optsv):
                    try:
                        optsv["colorPreset"] = str(optsv.get("palette") or "default")
                        optsv.pop("palette", None)
                        vv["options"] = optsv
                    except Exception:
                        pass
        except Exception:
            pass

        # Validate by type
        ok = True
        if (payload.targetType or '').strip():
            tt = str(payload.targetType).strip().lower()
            if t != tt:
                ok = False
        if ok:
            if t == "chart":
                ok = _valid_chart(vv)
            elif t == "table":
                ok = _valid_table(vv)
            elif t == "kpi":
                ok = _valid_kpi(vv)
        if ok:
            out.append(vv)
        if len(out) >= 3:
            break

    # Fallback: synthesize one variant from the enhanced prompt if none passed validation
    if not out:
        try:
            ptxt = str(payload.prompt or "")
            # Chart type heuristic
            ctype = "column"
            if _re.search(r"\b(line)\b", ptxt, _re.I):
                ctype = "line"
            elif _re.search(r"\b(bar)\b", ptxt, _re.I):
                ctype = "bar"
            elif _re.search(r"\b(donut|pie)\b", ptxt, _re.I):
                ctype = "donut"
            # Time unit heuristic
            unit = None
            if _re.search(r"\bday\b", ptxt, _re.I): unit = "day"
            elif _re.search(r"\bweek\b", ptxt, _re.I): unit = "week"
            elif _re.search(r"\bmonth\b", ptxt, _re.I): unit = "month"
            elif _re.search(r"\bquarter\b", ptxt, _re.I): unit = "quarter"
            elif _re.search(r"\byear\b", ptxt, _re.I): unit = "year"
            # Extract bracketed fields
            fields = _re.findall(r"\[([^\]]+)\]", ptxt) or []
            x_field = None
            measure_field = None
            # Prefer "from [Field]" as x
            m_from = _re.search(r"from\s*\[([^\]]+)\]", ptxt, _re.I)
            if m_from:
                x_field = m_from.group(1).strip()
            # Or "by [Field]" as x
            if not x_field:
                m_by = _re.search(r"by\s*\[([^\]]+)\]", ptxt, _re.I)
                if m_by:
                    x_field = m_by.group(1).strip()
            # Measure and aggregation
            agg = "count"
            m_of = _re.search(r"(sum|count|avg|average|min|max|distinct)\s+of\s*\[([^\]]+)\]", ptxt, _re.I)
            if m_of:
                word = (m_of.group(1) or "").lower()
                agg = "avg" if word == "average" else word
                measure_field = m_of.group(2).strip()
            # Fallback choices
            if not measure_field and fields:
                for f in fields:
                    if x_field and _normkey(f) == _normkey(x_field):
                        continue
                    measure_field = f
                    break
            if not x_field:
                # Pick a date-like column from schema
                for c in payload.dsSchema.columns:
                    n = _normalize_name(c.name)
                    if _re.search(r"(date|time|timestamp|day|month|year)", n, _re.I):
                        x_field = n
                        break
            v = {
                "id": "ai_fallback_0",
                "type": "chart",
                "title": "AI Suggested",
                "chartType": ctype,
                "queryMode": "spec",
                "querySpec": {"source": payload.dsSchema.table},
            }
            if x_field: v["querySpec"]["x"] = x_field
            if measure_field:
                v["querySpec"]["measure"] = measure_field
                v["querySpec"]["agg"] = agg
            else:
                v["querySpec"]["agg"] = "count"
            opts: Dict[str, Any] = {}
            if unit: opts["xTimeUnit"] = unit
            if opts: v["options"] = opts
            if _valid_chart(v):
                out.append(v)
        except Exception:
            pass

    return AiSuggestResponse(variants=out)
