"""Per-request cancellation token plumbing.

The HTTP layer ``/query`` and ``/query/spec`` endpoints are async wrappers
that offload the (blocking, multi-second-to-multi-minute) SQL execution to
a dedicated thread pool. ``loop.run_in_executor`` does NOT cancel the
underlying worker thread when its future is cancelled — so without extra
plumbing, a client disconnecting mid-query (e.g. closing the dashboard
tab) leaves the SQL running on the server, holding both a thread slot
and a DuckDB connection until it eventually completes.

This module wires that gap:

* The async wrapper allocates a :class:`CancelToken` per request.
* Before invoking the sync work in the executor it stashes the token on a
  thread-local, then races the future against ``request.is_disconnected()``.
* The DuckDB connection-borrow path (``_PooledCursorWrap`` in ``db.py``)
  registers the active connection with the current thread's token. If the
  request gets cancelled, ``token.cancel()`` walks the registered
  connections and calls ``connection.interrupt()`` — DuckDB stops the
  running query immediately, the executor thread unblocks, the connection
  goes back to the pool.

Threads are reused across requests, so the token MUST be cleared as soon
as the work completes (use :func:`set_current_token` with ``None`` in a
``finally`` block).
"""

from __future__ import annotations

import threading
from typing import Any


# Thread-local: each executor worker keeps the token of the request it is
# currently servicing. Reads from db.py inside the same thread.
_THREAD_LOCAL = threading.local()


class CancelToken:
    """Tracks per-request DuckDB connections and broadcasts an interrupt.

    Safe to use from multiple threads: ``register``/``unregister`` are
    serialized by an internal lock and ``cancel`` makes an atomic snapshot
    before issuing interrupts so it never collides with a connection
    return.
    """

    __slots__ = ("_connections", "_lock", "_cancelled")

    def __init__(self) -> None:
        self._connections: list[Any] = []
        self._lock = threading.Lock()
        self._cancelled: bool = False

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    def register(self, conn: Any) -> None:
        """Mark *conn* as in use by this request.

        If the request was already cancelled before the connection was
        borrowed (race window), interrupt the connection immediately so we
        never start a query that nobody is waiting for.
        """
        if conn is None:
            return
        with self._lock:
            if self._cancelled:
                already_cancelled = True
            else:
                already_cancelled = False
                self._connections.append(conn)
        if already_cancelled:
            _safe_interrupt(conn)

    def unregister(self, conn: Any) -> None:
        if conn is None:
            return
        with self._lock:
            try:
                self._connections.remove(conn)
            except ValueError:
                pass

    def cancel(self) -> int:
        """Mark cancelled and interrupt every registered connection.

        Returns the number of connections that were interrupted.
        """
        with self._lock:
            if self._cancelled:
                # Already cancelled — nothing more to do
                return 0
            self._cancelled = True
            snapshot = list(self._connections)
        for conn in snapshot:
            _safe_interrupt(conn)
        return len(snapshot)


def _safe_interrupt(conn: Any) -> None:
    """Best-effort interrupt of a DuckDB or DB-API connection."""
    # DuckDB exposes ``interrupt()`` directly on Connection objects since
    # 0.10. SQLAlchemy / psycopg / pymysql connections expose ``cancel()``
    # but those are wrapped engines and not in our hot path; we tolerate
    # both shapes and silently ignore other connection types.
    fn = getattr(conn, "interrupt", None)
    if callable(fn):
        try:
            fn()
            return
        except Exception:
            pass
    fn = getattr(conn, "cancel", None)
    if callable(fn):
        try:
            fn()
        except Exception:
            pass


# ── Thread-local glue ────────────────────────────────────────────────


def set_current_token(token: CancelToken | None) -> None:
    """Attach (or detach) the cancel token for the calling thread.

    Call with the token at the start of an executor task, and with
    ``None`` in the matching ``finally`` block — threads are reused, so
    leaking a stale token between requests would let a future request's
    interrupt propagate to a previous one's connections.
    """
    _THREAD_LOCAL.token = token


def get_current_token() -> CancelToken | None:
    """Return the cancel token associated with the calling thread."""
    return getattr(_THREAD_LOCAL, "token", None)


def register_with_current_token(conn: Any) -> None:
    """Convenience: register *conn* with this thread's token (if any)."""
    tok = get_current_token()
    if tok is not None:
        tok.register(conn)


def unregister_with_current_token(conn: Any) -> None:
    """Convenience: unregister *conn* from this thread's token (if any)."""
    tok = get_current_token()
    if tok is not None:
        tok.unregister(conn)
