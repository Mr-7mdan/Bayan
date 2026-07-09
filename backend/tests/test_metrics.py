"""Spec 08 verification: prometheus_client-backed metrics facade + health."""
from __future__ import annotations

from app import metrics
from app.metrics import counter_inc, summary_observe, gauge_inc, gauge_dec, render_prometheus, snapshot


def test_counter_render_monotonic():
    counter_inc("t_c_total", {"a": "x"})
    counter_inc("t_c_total", {"a": "x"})
    out = render_prometheus()
    assert 't_c_total{a="x"} 2.0' in out


def test_summary_snapshot_shape():
    summary_observe("t_s_ms", 5)
    rows = [s for s in snapshot()["summaries"] if s["name"] == "t_s_ms"]
    assert rows and rows[0]["sum"] == 5.0 and rows[0]["count"] == 1


def test_mixed_labels_pad_and_strip():
    # query_inflight has fixed labelnames (endpoint, engine); calls with and
    # without engine both succeed, and snapshot rows omit the padded empty label.
    gauge_inc("query_inflight", 1.0, {"endpoint": "query", "engine": "duckdb"})
    gauge_inc("query_inflight", 1.0, {"endpoint": "period_totals"})  # engine omitted
    gauge_dec("query_inflight", 1.0, {"endpoint": "query", "engine": "duckdb"})
    rows = [g for g in snapshot()["gauges"] if g["name"] == "query_inflight"]
    pt = [g for g in rows if g["labels"].get("endpoint") == "period_totals"]
    assert pt, "period_totals row present"
    assert "engine" not in pt[0]["labels"], "empty padded label stripped"


def test_healthz_ok():
    from fastapi.testclient import TestClient
    from app.main import app
    r = TestClient(app).get("/api/healthz")
    assert r.status_code == 200
    body = r.json()
    assert body["checks"]["sqlite"] == "ok"
