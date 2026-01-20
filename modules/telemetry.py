"""Ludic telemetry logging for Hunter OS (JSON + emoji)."""

from __future__ import annotations

import logging
import sys
from datetime import datetime

from pythonjsonlogger import jsonlogger

try:
    import streamlit as st
    from streamlit.runtime.scriptrunner import get_script_run_ctx
except Exception:  # pragma: no cover - streamlit optional in background jobs
    st = None
    get_script_run_ctx = None


EMOJIS = {
    "startup": "🚀",
    "search": "🕵️",
    "wealth": "💰",
    "api": "⚡",
    "performance": "⚡",
    "bot": "🤖",
    "error": "❌",
    "success": "✅",
    "auth": "🔒",
}


class LudicFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)

        log_record["timestamp"] = datetime.utcnow().strftime("%H:%M:%S")
        log_record["level"] = record.levelname
        if "message" not in log_record:
            log_record["message"] = record.getMessage()

        event_type = log_record.get("event_type") or message_dict.get("event_type") or "system"
        log_record["icon"] = EMOJIS.get(str(event_type).lower(), "📝")

        session_id = "system"
        try:
            ctx = get_script_run_ctx() if get_script_run_ctx else None
            if ctx and getattr(ctx, "session_id", None):
                session_id = ctx.session_id[:6]
            elif st is not None and getattr(st, "session_state", None) is not None:
                session_id = str(st.session_state.get("session_id", "system"))[:6]
        except Exception:
            session_id = "system"
        log_record["session"] = session_id


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("hunter_os")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = LudicFormatter()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False

        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("asyncio").setLevel(logging.WARNING)
        logging.getLogger("faker").setLevel(logging.WARNING)

    return logger


logger = setup_logger()
