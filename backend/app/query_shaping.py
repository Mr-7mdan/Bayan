"""Pure result-shaping helpers extracted from routers/query.py (spec 11, Phase A).

These are self-contained (stdlib + fastapi/dateutil only, no app state), so they
live in their own module and are re-imported by routers/query.py. First slice of
the query.py decomposition; the bulk (endpoint modules) is a follow-up.

# ponytail: sibling module, not the full routers/query/ package -- a package
# can't coexist with routers/query.py; the big-bang split is parked as high-risk.
"""
from __future__ import annotations

import binascii
import decimal
import re
from typing import Any

from dateutil import parser as date_parser
from fastapi import HTTPException


def _http_for_db_error(e: Exception) -> HTTPException | None:
    """Classify common DB connectivity errors to clearer HTTP status codes.
    - HYT00/Login timeout → 504 Gateway Timeout
    - 08S01/TCP Provider (SQL Server) → 502 Bad Gateway
    Otherwise: None (caller should re-raise original error).
    """
    try:
        msg = str(e) if e is not None else ""
        up = msg.upper()
        if ("HYT00" in up) or ("LOGIN TIMEOUT" in up):
            return HTTPException(status_code=504, detail="Database connectivity timeout (HYT00)")
        if ("08S01" in up) or ("TCP PROVIDER" in up):
            return HTTPException(status_code=502, detail="Database connection lost (08S01/TCP Provider)")
    except Exception:
        return None
    return None


def _coerce_date_like(v: Any) -> Any:
    """Attempt to parse arbitrary date/time strings into ISO strings.
    Safe no-op if parsing fails or value is not a string.
    Example outputs: '2024-01-15' or '2024-01-15 13:45:00'.
    """
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return v
        # Only parse if it looks like a date (contains date separators or keywords)
        # This prevents parsing short strings like '10', '20', '50' as dates
        if not re.search(r'[-/:T]|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|mon|tue|wed|thu|fri|sat|sun|today|now|yesterday|tomorrow', s, re.IGNORECASE):
            return v
        try:
            dt = date_parser.parse(s)
            # Keep UTC time, don't convert to local timezone
            if getattr(dt, 'tzinfo', None) is not None:
                try:
                    # Convert to UTC and make naive (removes timezone info but keeps UTC time)
                    import datetime
                    dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
                except Exception:
                    dt = dt.replace(tzinfo=None)
            if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0:
                return dt.date().isoformat()
            return dt.replace(microsecond=0).isoformat(sep=' ')
        except Exception:
            return v
    return v


def _json_safe_cell(v: Any) -> Any:
    """Coerce DB values to JSON-serializable primitives.
    - bytes/bytearray/memoryview → hex string (0x...); try utf-8 first for readability
    - Decimal → float (fallback to str if NaN/Inf)
    - Default: return as-is
    """
    try:
        if isinstance(v, (bytes, bytearray, memoryview)):
            try:
                return bytes(v).decode('utf-8')
            except Exception:
                return '0x' + binascii.hexlify(bytes(v)).decode('ascii')
        if isinstance(v, decimal.Decimal):
            try:
                return float(v)
            except Exception:
                return str(v)
    except Exception:
        try:
            return str(v)
        except Exception:
            return None
    return v
