from __future__ import annotations

import time
from typing import Any, Dict

import structlog

# Dedicated logger for security and configuration audit logging
audit_logger = structlog.get_logger("muxdb.audit")

def log_audit_event(
    actor: str,
    action: str,
    target: str,
    status: str = "success",
    details: Dict[str, Any] | None = None,
) -> None:
    """Log a structured audit event.

    Logs actor, action type, target component, status, and extra details.
    """
    audit_logger.info(
        "audit_event",
        timestamp=int(time.time()),
        actor=actor,
        action=action,
        target=target,
        status=status,
        details=details or {},
    )
