from __future__ import annotations

from datetime import datetime, timezone


def as_utc(dt: datetime | None) -> datetime | None:
    """DB convention: naive datetimes are UTC. Returns aware UTC.

    Standalone module (not models.py) so scheduler.py, routers, and models
    can all import it without an import cycle.
    """
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
