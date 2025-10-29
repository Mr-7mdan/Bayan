from __future__ import annotations

import threading
from time import time
from typing import Dict, Tuple

# Simple in-memory metrics registry (single-process)
# Counters, Gauges, and Summaries (sum,count)

_lock = threading.Lock()
_counters: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], float] = {}
_gauges: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], float] = {}
_summaries: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], Tuple[float, int]] = {}


def _key(name: str, labels: Dict[str, str] | None) -> Tuple[str, Tuple[Tuple[str, str], ...]]:
    items = tuple(sorted(((labels or {}) or {}).items()))
    return name, items


def counter_inc(name: str, labels: Dict[str, str] | None = None, amount: float = 1.0) -> None:
    with _lock:
        k = _key(name, labels)
        _counters[k] = _counters.get(k, 0.0) + float(amount)


def gauge_set(name: str, value: float, labels: Dict[str, str] | None = None) -> None:
    with _lock:
        k = _key(name, labels)
        _gauges[k] = float(value)


def gauge_inc(name: str, amount: float = 1.0, labels: Dict[str, str] | None = None) -> None:
    with _lock:
        k = _key(name, labels)
        _gauges[k] = _gauges.get(k, 0.0) + float(amount)


def gauge_dec(name: str, amount: float = 1.0, labels: Dict[str, str] | None = None) -> None:
    gauge_inc(name, -float(amount), labels)


def summary_observe(name: str, value: float, labels: Dict[str, str] | None = None) -> None:
    with _lock:
        k = _key(name, labels)
        s, c = _summaries.get(k, (0.0, 0))
        _summaries[k] = (s + float(value), c + 1)


def _fmt_labels(items: Tuple[Tuple[str, str], ...]) -> str:
    if not items:
        return ""
    parts = [f'{k}="{v}"' for k, v in items]
    return "{" + ",".join(parts) + "}"


def render_prometheus() -> str:
    lines: list[str] = []
    ts = int(time())
    with _lock:
        # Counters
        for (name, items), val in _counters.items():
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name}{_fmt_labels(items)} {val}")
        # Gauges
        for (name, items), val in _gauges.items():
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name}{_fmt_labels(items)} {val}")
        # Summaries: expose sum and count
        for (name, items), (s, c) in _summaries.items():
            lines.append(f"# TYPE {name} summary")
            lines.append(f"{name}_sum{_fmt_labels(items)} {s}")
            lines.append(f"{name}_count{_fmt_labels(items)} {c}")
    lines.append(f"# EOF {ts}")
    return "\n".join(lines) + "\n"


def snapshot() -> dict:
    """Return a programmatic snapshot of current metrics.
    Structure:
    {
      counters: [ { name, labels: {..}, value } ],
      gauges:   [ { name, labels: {..}, value } ],
      summaries:[ { name, labels: {..}, sum, count } ],
    }
    """
    out_c: list[dict] = []
    out_g: list[dict] = []
    out_s: list[dict] = []
    with _lock:
        for (name, items), val in _counters.items():
            out_c.append({"name": name, "labels": dict(items), "value": float(val)})
        for (name, items), val in _gauges.items():
            out_g.append({"name": name, "labels": dict(items), "value": float(val)})
        for (name, items), (s, c) in _summaries.items():
            out_s.append({"name": name, "labels": dict(items), "sum": float(s), "count": int(c)})
    return {"counters": out_c, "gauges": out_g, "summaries": out_s}
