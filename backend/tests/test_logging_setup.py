"""Verification for spec 07 — central logging config.

Covers: level gating, JSON output, RotatingFileHandler wiring, secret scrubbing,
and that importing app modules is quiet at the default INFO level.
"""
import importlib
import json
import logging
import os

import pytest

from app.logging_setup import configure_logging, JsonFormatter
from app.sql_ident import scrub


@pytest.fixture(autouse=True)
def _restore_logging():
    """Snapshot/restore root logging so tests don't leak handlers."""
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    saved_env = {k: os.environ.get(k) for k in ("LOG_LEVEL", "LOG_FORMAT", "LOG_FILE")}
    yield
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)
    for k, v in saved_env.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def test_default_level_is_info():
    os.environ.pop("LOG_LEVEL", None)
    configure_logging()
    assert logging.getLogger().level == logging.INFO


def test_debug_gating(capsys):
    os.environ["LOG_LEVEL"] = "INFO"
    os.environ.pop("LOG_FORMAT", None)
    configure_logging()
    logger = logging.getLogger("app.sqlgen_glot")
    # At INFO, hot-path DEBUG lines must be suppressed entirely.
    assert not logger.isEnabledFor(logging.DEBUG)
    assert logger.isEnabledFor(logging.INFO)
    logger.debug("[SQLGlot] hot-path trace")   # must be filtered out
    logger.info("lifecycle event")             # must pass
    for h in logging.getLogger().handlers:
        h.flush()
    out = capsys.readouterr().out
    assert "lifecycle event" in out
    assert "hot-path trace" not in out


def test_json_formatter_emits_one_object():
    rec = logging.LogRecord("app.x", logging.INFO, __file__, 1, "hello", None, None)
    line = JsonFormatter().format(rec)
    obj = json.loads(line)
    assert obj["level"] == "INFO"
    assert obj["logger"] == "app.x"
    assert obj["msg"] == "hello"


def test_json_format_from_env(capsys):
    os.environ["LOG_FORMAT"] = "json"
    os.environ["LOG_LEVEL"] = "INFO"
    configure_logging()
    logging.getLogger("t").info("hello json")
    for h in logging.getLogger().handlers:
        h.flush()
    out = capsys.readouterr().out.strip().splitlines()
    assert out, "expected at least one log line on stdout"
    obj = json.loads(out[-1])
    assert obj["msg"] == "hello json"


def test_log_file_uses_rotating_handler(tmp_path):
    log_file = tmp_path / "bayan.log"
    os.environ["LOG_FILE"] = str(log_file)
    os.environ["LOG_FORMAT"] = "text"
    configure_logging()
    handlers = logging.getLogger().handlers
    assert len(handlers) == 1
    h = handlers[0]
    assert isinstance(h, logging.handlers.RotatingFileHandler)
    assert h.maxBytes == 10 * 1024 * 1024
    assert h.backupCount == 5
    logging.getLogger("t").warning("to file")
    h.flush()
    assert "to file" in log_file.read_text()
    # detach so the fixture-restored handlers take over cleanly
    h.close()


def test_secret_is_scrubbed():
    secret = "sup3r-secret-pw"
    msg = f"ATTACH failed dsn=postgresql://u:{secret}@host/db"
    assert scrub(msg, [secret]) == "ATTACH failed dsn=postgresql://u:***@host/db"
    assert secret not in scrub(msg, [secret])


def test_app_import_is_quiet_at_info(capsys):
    """Importing the app must not spew the old [SQLGlot] MODULE LOADED banner."""
    os.environ["LOG_LEVEL"] = "INFO"
    os.environ.pop("LOG_FORMAT", None)
    configure_logging()
    import app.sqlgen_glot as m
    importlib.reload(m)
    captured = capsys.readouterr()
    assert "MODULE LOADED" not in (captured.out + captured.err)
