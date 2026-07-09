from __future__ import annotations

import os
import threading
import logging
from typing import Any, Dict, Tuple

from prometheus_client import (
    Counter,
    Gauge,
    Summary,
    CollectorRegistry,
    generate_latest,
    REGISTRY,
)
from prometheus_client import multiprocess

# Facade over prometheus_client that preserves the historical public API
# (counter_inc/gauge_set/gauge_inc/gauge_dec/summary_observe/render_prometheus/
# snapshot) so the ~40 existing call sites and admin.py's /metrics-live keep
# working unchanged. Multi-worker-safe: when PROMETHEUS_MULTIPROC_DIR is set
# (prod gunicorn), values aggregate across workers via mmap files and survive
# worker recycles. Dev/single-process falls back to the default REGISTRY.
#
# NOTE: prometheus_client picks its value class (mmap vs in-memory) at IMPORT
# time based on PROMETHEUS_MULTIPROC_DIR — the var must be exported before this
# module is imported (done in run_prod_gunicorn.sh).

_log = logging.getLogger("metrics")

_lock = threading.Lock()
# name -> (metric_object, labelnames_tuple)
_metrics: Dict[str, Tuple[Any, Tuple[str, ...]]] = {}
_warned: set[str] = set()

# Fixed labelnames for metrics that historically emitted inconsistent label
# sets under the same name (prometheus_client fixes labelnames at creation).
_LABELNAMES: Dict[str, Tuple[str, ...]] = {
    "query_inflight": ("endpoint", "engine"),
    "query_semaphore_wait_ms": ("endpoint", "engine", "sem"),
}


def _get_or_create(name: str, kind: str, labels: Dict[str, str] | None):
    """Return (child, labelnames) for a metric, creating it on first use."""
    with _lock:
        entry = _metrics.get(name)
        if entry is None:
            labelnames = _LABELNAMES.get(name) or tuple(sorted((labels or {}).keys()))
            if kind == "counter":
                metric: Any = Counter(name, name, labelnames)
            elif kind == "gauge":
                # livesum: sum live workers, forget dead ones (inflight/active gauges).
                metric = Gauge(name, name, labelnames, multiprocess_mode="livesum")
            else:  # summary
                metric = Summary(name, name, labelnames)
            entry = (metric, labelnames)
            _metrics[name] = entry
    metric, labelnames = entry
    if not labelnames:
        return metric, labelnames
    padded = _pad(name, labelnames, labels)
    return metric.labels(**padded), labelnames


def _pad(name: str, labelnames: Tuple[str, ...], labels: Dict[str, str] | None) -> Dict[str, str]:
    """Pad missing labels with '' and drop keys not in labelnames (warn once)."""
    labels = labels or {}
    if name not in _warned and any(k not in labelnames for k in labels):
        _log.warning(
            "metric %s: dropping labels %s not in %s",
            name,
            [k for k in labels if k not in labelnames],
            list(labelnames),
        )
        _warned.add(name)
    return {ln: str(labels.get(ln, "")) for ln in labelnames}


def counter_inc(name: str, labels: Dict[str, str] | None = None, amount: float = 1.0) -> None:
    child, _ = _get_or_create(name, "counter", labels)
    child.inc(float(amount))


def gauge_set(name: str, value: float, labels: Dict[str, str] | None = None) -> None:
    child, _ = _get_or_create(name, "gauge", labels)
    child.set(float(value))


def gauge_inc(name: str, amount: float = 1.0, labels: Dict[str, str] | None = None) -> None:
    child, _ = _get_or_create(name, "gauge", labels)
    child.inc(float(amount))


def gauge_dec(name: str, amount: float = 1.0, labels: Dict[str, str] | None = None) -> None:
    child, _ = _get_or_create(name, "gauge", labels)
    child.dec(float(amount))


def summary_observe(name: str, value: float, labels: Dict[str, str] | None = None) -> None:
    child, _ = _get_or_create(name, "summary", labels)
    child.observe(float(value))


def _collect_registry() -> CollectorRegistry:
    """Registry to read from: aggregated multiprocess in prod, default otherwise."""
    if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
        reg = CollectorRegistry()
        multiprocess.MultiProcessCollector(reg)
        return reg
    return REGISTRY


def render_prometheus() -> str:
    return generate_latest(_collect_registry()).decode("utf-8")


def _clean(labels: Dict[str, str]) -> Dict[str, str]:
    # Drop padding labels (value "") so admin.py label equality matches legacy shape.
    return {k: v for k, v in labels.items() if v != ""}


def snapshot() -> dict:
    """Programmatic snapshot in the legacy shape consumed by admin.py:
    { counters:[{name,labels,value}], gauges:[...], summaries:[{name,labels,sum,count}] }.
    Counter sample names keep the `_total` suffix; summary names use the base name.
    """
    out_c: list[dict] = []
    out_g: list[dict] = []
    out_s: list[dict] = []
    reg = _collect_registry()
    for fam in reg.collect():
        if fam.type == "counter":
            for sm in fam.samples:
                if sm.name.endswith("_total"):
                    out_c.append({"name": sm.name, "labels": _clean(dict(sm.labels)), "value": float(sm.value)})
        elif fam.type == "gauge":
            for sm in fam.samples:
                out_g.append({"name": sm.name, "labels": _clean(dict(sm.labels)), "value": float(sm.value)})
        elif fam.type == "summary":
            pairs: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], Dict[str, float]] = {}
            for sm in fam.samples:
                if sm.name.endswith("_sum"):
                    base, field = sm.name[:-4], "sum"
                elif sm.name.endswith("_count"):
                    base, field = sm.name[:-6], "count"
                else:
                    continue  # skip _created
                labels = _clean(dict(sm.labels))
                key = (base, tuple(sorted(labels.items())))
                pairs.setdefault(key, {"labels": labels})[field] = float(sm.value)
            for (base, _k), vals in pairs.items():
                out_s.append({
                    "name": base,
                    "labels": vals.get("labels", {}),
                    "sum": float(vals.get("sum", 0.0)),
                    "count": int(vals.get("count", 0)),
                })
    return {"counters": out_c, "gauges": out_g, "summaries": out_s}
