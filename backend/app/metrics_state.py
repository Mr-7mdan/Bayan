from __future__ import annotations

import threading
import time
from typing import Dict, List, Tuple

_recent_actors_lock = threading.Lock()
_recent_actors: Dict[str, float] = {}

# Open dashboards trackers (session-scoped)
_open_lock = threading.Lock()
# builder: dashId -> { sessionId -> lastSeenTs }
_open_builder: Dict[str, Dict[str, float]] = {}
_open_public: Dict[str, Dict[str, float]] = {}


def touch_actor(actor_id: str | None) -> None:
    if not actor_id:
        return
    try:
        aid = str(actor_id).strip()
        if not aid:
            return
    except Exception:
        return
    now = time.time()
    with _recent_actors_lock:
        _recent_actors[aid] = now


def get_recent_actors(seconds: int = 900) -> List[Tuple[str, float]]:
    cutoff = time.time() - max(1, int(seconds))
    with _recent_actors_lock:
        items = [(k, v) for k, v in _recent_actors.items() if v >= cutoff]
    items.sort(key=lambda kv: kv[1], reverse=True)
    return items


def open_dashboard(kind: str, dashboard_id: str | None, session_id: str | None) -> None:
    if not dashboard_id or not session_id:
        return
    kind_l = (kind or 'builder').lower()
    now = time.time()
    with _open_lock:
        m = _open_builder if kind_l != 'public' else _open_public
        inner = m.get(dashboard_id)
        if inner is None:
            inner = {}
            m[dashboard_id] = inner
        inner[session_id] = now


def close_dashboard(kind: str, dashboard_id: str | None, session_id: str | None) -> None:
    if not dashboard_id or not session_id:
        return
    kind_l = (kind or 'builder').lower()
    with _open_lock:
        m = _open_builder if kind_l != 'public' else _open_public
        inner = m.get(dashboard_id)
        if inner and session_id in inner:
            try:
                del inner[session_id]
            except Exception:
                pass
            if not inner:
                try:
                    del m[dashboard_id]
                except Exception:
                    pass


def get_open_dashboards(seconds: int = 900) -> dict:
    cutoff = time.time() - max(1, int(seconds))
    out: dict = { 'builder': { 'total': 0, 'byId': [] }, 'public': { 'total': 0, 'byId': [] }, 'total': 0 }
    with _open_lock:
        def _collect(m: Dict[str, Dict[str, float]]) -> Tuple[int, List[Tuple[str, int]]]:
            total = 0
            pairs: List[Tuple[str, int]] = []
            for did, sess in m.items():
                # prune old sessions
                alive = { sid: ts for sid, ts in sess.items() if ts >= cutoff }
                m[did] = alive
                c = len(alive)
                if c > 0:
                    total += c
                    pairs.append((did, c))
            pairs.sort(key=lambda kv: kv[1], reverse=True)
            return total, pairs
        tb, pb = _collect(_open_builder)
        tp, pp = _collect(_open_public)
        out['builder']['total'] = tb
        out['builder']['byId'] = [ { 'dashboardId': did, 'sessions': cnt } for did, cnt in pb ]
        out['public']['total'] = tp
        out['public']['byId'] = [ { 'dashboardId': did, 'sessions': cnt } for did, cnt in pp ]
        out['total'] = tb + tp
    return out
