from __future__ import annotations

from typing import Any, Dict

class TenantConfig:
    """Manages and resolves per-tenant configuration overrides.

    Allows tuning parameters (e.g. rate limits, pool sizes) dynamically per tenant.
    """

    def __init__(self, overrides: Dict[str, Dict[str, Any]] | None = None) -> None:
        # Maps tenant_id -> config_key -> value
        self._overrides = overrides or {}

    def set_override(self, tenant_id: str, key: str, value: Any) -> None:
        """Set a configuration override for a tenant."""
        if tenant_id not in self._overrides:
            self._overrides[tenant_id] = {}
        self._overrides[tenant_id][key] = value

    def get_override(self, tenant_id: str, key: str, default: Any = None) -> Any:
        """Resolve a configuration key for a tenant, returning the default if no override exists."""
        tenant_opts = self._overrides.get(tenant_id)
        if tenant_opts and key in tenant_opts:
            return tenant_opts[key]
        return default
