"""Dedicated thread pool for query endpoints.

Isolates heavy query work from lightweight CRUD routes so that dashboard
saves, navigation, and other metadata operations are never blocked by
long-running analytical queries.

The executor is lazy-initialized on first use (not at import time) to avoid
GIL/thread crashes on Windows where threads created before the uvicorn event
loop is ready cause fatal errors in signal handling.
"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor

# Default pool size needs headroom for: HEAVY_QUERY_CONCURRENCY (8) +
# SPEC_QUERY_CONCURRENCY (7) + a few light queries that go through the
# same /query path. 16 is comfortable; tune via QUERY_POOL_SIZE.
QUERY_POOL_SIZE = int(os.environ.get("QUERY_POOL_SIZE", "16") or "16")

# Lazy-initialized — do NOT create at import time (crashes on Windows)
_query_executor: ThreadPoolExecutor | None = None


def get_query_executor() -> ThreadPoolExecutor:
    """Return the shared query thread pool, creating it on first call."""
    global _query_executor
    if _query_executor is None:
        _query_executor = ThreadPoolExecutor(
            max_workers=QUERY_POOL_SIZE,
            thread_name_prefix="query",
        )
        # Saturation = query_inflight / query_pool_max (wait: query_semaphore_wait_ms).
        from .metrics import gauge_set
        gauge_set("query_pool_max", float(QUERY_POOL_SIZE))
    return _query_executor


# Admission control — only touched from the async event loop (single-threaded
# per Uvicorn worker), so a plain int is safe without locks.
_inflight = 0
QUERY_MAX_QUEUED = int(os.environ.get("QUERY_MAX_QUEUED", str(QUERY_POOL_SIZE * 2)) or str(QUERY_POOL_SIZE * 2))


def shutdown_query_pool() -> None:
    """Drain in-flight queries on graceful shutdown."""
    global _query_executor
    if _query_executor is not None:
        _query_executor.shutdown(wait=True, cancel_futures=False)
        _query_executor = None
