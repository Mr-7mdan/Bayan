"""Spec 12: query rate limiting defaults. In-process path only (no Redis in CI)."""
import pytest
from fastapi import HTTPException

from app.routers import query as q


@pytest.fixture(autouse=True)
def _reset_buckets(monkeypatch):
    # Fresh per-test bucket state; small limits so tests are fast/deterministic.
    monkeypatch.setattr(q, "_TB_STATE", {})
    monkeypatch.setattr(q, "_REDIS_URL", None)
    monkeypatch.setattr(q, "_Q_RATE", 2)
    monkeypatch.setattr(q, "_Q_BURST", 3)
    monkeypatch.setattr(q, "_Q_RATE_GLOBAL", 100)
    monkeypatch.setattr(q, "_Q_BURST_GLOBAL", 100)
    yield


class _StubReq:
    def __init__(self, user_id=None, host="1.2.3.4"):
        self.state = type("S", (), {"user_id": user_id})()
        self.client = type("C", (), {"host": host})()


def test_burst_then_throttled():
    # burst=3 -> first 3 immediate calls allowed, 4th must wait >= 1s
    assert q._throttle_take("u:a", 2, 3) is None
    assert q._throttle_take("u:a", 2, 3) is None
    assert q._throttle_take("u:a", 2, 3) is None
    wait = q._throttle_take("u:a", 2, 3)
    assert isinstance(wait, int) and wait >= 1


def test_disabled_when_rate_or_burst_zero():
    assert q._throttle_take("u:a", 0, 3) is None
    assert q._throttle_take("u:a", 2, 0) is None


def test_key_resolution_precedence():
    assert q._rl_key(None, "alice") == "u:alice"
    assert q._rl_key(None, None) == "ip:unknown"
    # authenticated user id (spec 02) wins over client-supplied actorId
    assert q._rl_key(_StubReq(user_id="bob"), "alice") == "u:bob"
    # anonymous falls back to client IP, never unlimited
    assert q._rl_key(_StubReq(), None) == "ip:1.2.3.4"


def test_enforce_raises_429_with_retry_after(monkeypatch):
    # global bucket of size 1 -> two DIFFERENT per-user keys, second call rejected
    monkeypatch.setattr(q, "_Q_RATE_GLOBAL", 1)
    monkeypatch.setattr(q, "_Q_BURST_GLOBAL", 1)
    q._enforce_rate_limit(_StubReq(user_id="x"), None, "query")  # consumes the one global token
    with pytest.raises(HTTPException) as ei:
        q._enforce_rate_limit(_StubReq(user_id="y"), None, "query")
    exc = ei.value
    assert exc.status_code == 429
    assert exc.detail == "Rate limit exceeded"
    ra = exc.headers["Retry-After"]
    assert int(ra) >= 1  # integer seconds


def test_per_user_isolation():
    # one user exhausting their bucket does not throttle another user
    for _ in range(3):
        assert q._throttle_take("u:a", 2, 3) is None
    assert q._throttle_take("u:a", 2, 3) >= 1
    assert q._throttle_take("u:b", 2, 3) is None


def test_http_boundary_returns_429(monkeypatch):
    """Flood POST /api/query as one anonymous client: first calls admitted (not
    429), then the global bucket empties and returns 429 with Retry-After."""
    from fastapi.testclient import TestClient
    import app.main as m

    monkeypatch.setattr(q, "_TB_STATE", {})
    monkeypatch.setattr(q, "_REDIS_URL", None)
    monkeypatch.setattr(q, "_Q_RATE_GLOBAL", 1)
    monkeypatch.setattr(q, "_Q_BURST_GLOBAL", 3)
    monkeypatch.setattr(q, "_Q_RATE", 0)  # per-user disabled -> only global gates

    # Stub query execution: the rate-limit gate runs BEFORE it, so admitted
    # requests must return fast. Real run_query blocks on DuckDB/datasource in a
    # bare test env; we only assert the rate-limit boundary here.
    async def _fast(_req, _fn):
        return {}
    monkeypatch.setattr(q, "_run_cancellable_in_pool", _fast)

    client = TestClient(m.app, raise_server_exceptions=False)
    codes = []
    for _ in range(12):
        r = client.post("/api/query", json={"sql": "SELECT 1"})
        codes.append(r.status_code)
        if r.status_code == 429:
            assert r.json()["detail"] == "Rate limit exceeded"
            assert int(r.headers["Retry-After"]) >= 1
    # boundary admitted the burst (first call not throttled) and eventually 429'd
    assert codes[0] != 429
    assert 429 in codes
