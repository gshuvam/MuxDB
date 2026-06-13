"""
MuxDB Security Module.
Provides TLS, authentication, RBAC, audit logging, and secrets management.
"""

from muxdb.security.tls import (
    create_client_ssl_context,
    create_server_ssl_context,
)
from muxdb.security.auth import (
    validate_api_key,
    generate_token,
    verify_token,
)
from muxdb.security.rbac import (
    Role,
    has_permission,
    require_role,
)
from muxdb.security.audit import (
    audit_logger,
    log_audit_event,
)
from muxdb.security.secrets import (
    SecretsLoader,
)

__all__ = [
    "create_client_ssl_context",
    "create_server_ssl_context",
    "validate_api_key",
    "generate_token",
    "verify_token",
    "Role",
    "has_permission",
    "require_role",
    "audit_logger",
    "log_audit_event",
    "SecretsLoader",
]
