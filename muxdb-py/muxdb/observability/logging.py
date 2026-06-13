from __future__ import annotations

import contextvars
import logging
import uuid
from typing import Any, Dict

import structlog

# Contextvar to store correlation ID for distributed/request tracing
_correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar("correlation_id", default="")

def get_correlation_id() -> str:
    """Get the current correlation ID or generate a new one if not set."""
    val = _correlation_id.get()
    if not val:
        val = str(uuid.uuid4())
        _correlation_id.set(val)
    return val

def set_correlation_id(cid: str) -> None:
    """Set the correlation ID for the current context."""
    _correlation_id.set(cid)

def add_correlation_id(
    logger: Any, method_name: str, event_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """Structlog processor to inject correlation_id into logs."""
    cid = _correlation_id.get()
    if cid:
        event_dict["correlation_id"] = cid
    else:
        # Auto-initialize context variable
        event_dict["correlation_id"] = get_correlation_id()
    return event_dict

def setup_logging(json_format: bool = True, level: str = "INFO") -> None:
    """Configure global structlog settings."""
    processors = [
        structlog.contextvars.merge_contextvars,
        add_correlation_id,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    
    if json_format:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        logger_factory=structlog.PrintLoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(level.upper())
        ),
        cache_logger_on_first_use=True,
    )
