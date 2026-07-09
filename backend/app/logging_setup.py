"""Central logging configuration for the Bayan backend (spec 07).

Call ``configure_logging()`` once at process start (done in ``app.main``) to
install a single root handler. Everything else uses a module-level
``logger = logging.getLogger(__name__)`` and emits at the appropriate level.

Environment knobs (plain ``os.getenv`` — intentionally NOT in ``config.py``
``Settings`` so logging can be configured before ``settings`` side effects run):

    LOG_LEVEL   DEBUG|INFO|WARNING|ERROR   (default INFO)
    LOG_FORMAT  text|json                  (default text)
    LOG_FILE    <path>                      (empty = stdout; path enables a
                                             10MB x5 RotatingFileHandler)

Output goes to stdout by default so it interleaves with uvicorn/gunicorn and is
friendly to journald/docker/Loki/CloudWatch.

Swallowed-error policy (do NOT convert all ~650 ``except Exception: pass`` now):
- Swallow in a startup, scheduler, or sync path -> log at WARNING with
  ``exc_info=True`` instead of ``pass``.
- Swallow in a per-row/per-item best-effort loop (formatting, optional
  metadata) -> leave ``pass``; it is intentional.
- New code: a bare ``except Exception: pass`` needs a justifying comment or a
  ``logger.debug``.
"""
import json
import logging
import logging.handlers
import os
import sys
import time


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        d = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            d["exc"] = self.formatException(record.exc_info)
        return json.dumps(d, ensure_ascii=False, default=str)


def configure_logging() -> None:
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    fmt = os.getenv("LOG_FORMAT", "text").lower()
    handler: logging.Handler = logging.StreamHandler(sys.stdout)
    log_file = os.getenv("LOG_FILE", "").strip()
    if log_file:
        # ponytail: fixed 10MB x 5 backups; make configurable only if someone asks
        handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
        )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    # uvicorn/gunicorn loggers propagate to root; drop their duplicate handlers
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lg = logging.getLogger(name)
        lg.handlers[:] = []
        lg.propagate = True
