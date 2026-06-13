from __future__ import annotations

import functools
from enum import IntEnum
from typing import Any, Callable, TypeVar, cast

from muxdb.errors import SecurityError

class Role(IntEnum):
    """MuxDB RBAC Roles.

    Using IntEnum permits simple hierarchy comparisons (e.g., ADMIN > OPERATOR).
    """
    VIEWER = 1
    OPERATOR = 2
    ADMIN = 3

    @classmethod
    def from_str(cls, name: str) -> Role:
        try:
            return cls[name.upper()]
        except KeyError:
            raise SecurityError(f"Unknown role: {name}")

def has_permission(user_role: Role | str, required_role: Role | str) -> bool:
    """Check if a user role meets or exceeds the required role hierarchy."""
    u_role = Role.from_str(user_role) if isinstance(user_role, str) else user_role
    r_role = Role.from_str(required_role) if isinstance(required_role, str) else required_role
    return u_role >= r_role

F = TypeVar("F", bound=Callable[..., Any])

def require_role(required_role: Role | str) -> Callable[[F], F]:
    """Decorator to enforce role permissions on client methods.

    Expects the wrapped method's object to have an `active_role` attribute or method.
    """
    r_role = Role.from_str(required_role) if isinstance(required_role, str) else required_role

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            # Retrieve role from self (e.g. self._active_role, self.active_role)
            user_role_attr = getattr(self, "_active_role", None) or getattr(self, "active_role", None)
            
            if user_role_attr is None:
                # Default to VIEWER if not specified
                user_role_attr = Role.VIEWER
            
            user_role = Role.from_str(user_role_attr) if isinstance(user_role_attr, str) else user_role_attr
            
            if not has_permission(user_role, r_role):
                raise SecurityError(
                    f"Insufficient permissions. Required role: {r_role.name}, "
                    f"current role: {user_role.name}"
                )
            return func(self, *args, **kwargs)
        return cast(F, wrapper)
    return decorator
