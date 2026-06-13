"""
MuxDB Django Integration.

Provides a custom database router, settings helpers to populate Django's
DATABASES configuration from MuxDB, and context managers to specify shard context.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Any, Generator, Dict

from muxdb.client import MuxDB
from muxdb.config import MuxConfig

# Thread-local storage for keeping track of the current request/thread's shard context
_local = threading.local()


def get_current_shard_id() -> str | None:
    """Retrieve the shard ID pinned to the current thread."""
    return getattr(_local, "shard_id", None)


def get_current_shard_key() -> Any | None:
    """Retrieve the shard key pinned to the current thread."""
    return getattr(_local, "shard_key", None)


def set_shard_context(shard_key: Any = None, shard_id: str | None = None) -> None:
    """Explicitly set the shard context for the current thread."""
    _local.shard_key = shard_key
    _local.shard_id = shard_id


def clear_shard_context() -> None:
    """Clear the shard context from the current thread."""
    if hasattr(_local, "shard_key"):
        del _local.shard_key
    if hasattr(_local, "shard_id"):
        del _local.shard_id


@contextmanager
def shard_context(shard_key: Any = None, shard_id: str | None = None) -> Generator[None, None, None]:
    """Context manager to scope database queries to a specific shard."""
    old_key = getattr(_local, "shard_key", None)
    old_id = getattr(_local, "shard_id", None)
    
    set_shard_context(shard_key=shard_key, shard_id=shard_id)
    try:
        yield
    finally:
        set_shard_context(shard_key=old_key, shard_id=old_id)


class MuxRouter:
    """
    Django database router for sharded configurations.
    
    Routes read/write operations by querying the active thread-local shard context,
    inspecting instance fields, or falling back to MuxDB's global Router.
    """

    # Singleton MuxDB instance placeholder to be populated by initialize_django
    _db: MuxDB | None = None

    @classmethod
    def set_mux_db(cls, db: MuxDB) -> None:
        cls._db = db

    def db_for_read(self, model: Any, **hints: Any) -> str | None:
        return self._route(model, **hints)

    def db_for_write(self, model: Any, **hints: Any) -> str | None:
        return self._route(model, **hints)

    def allow_relation(self, obj1: Any, obj2: Any, **hints: Any) -> bool | None:
        # Allow relations if they belong to the same shard database
        db1 = self._route(obj1.__class__, instance=obj1)
        db2 = self._route(obj2.__class__, instance=obj2)
        if db1 and db2:
            return db1 == db2
        return None

    def allow_migrate(self, db: str, app_label: str, model_name: str | None = None, **hints: Any) -> bool | None:
        # Allow migrations on all shards (broadcast)
        return True

    def _route(self, model: Any, **hints: Any) -> str | None:
        if self._db is None:
            return None

        # 1. Thread-local Context overrides
        shard_id = get_current_shard_id()
        if shard_id:
            return shard_id
            
        shard_key = get_current_shard_key()
        if shard_key is not None:
            return self._db.router.route_key(shard_key).id

        # 2. Inspect the model instance in hints
        instance = hints.get("instance")
        if instance is not None:
            shard_key_name = self._db.config.cluster.shard_key
            val = getattr(instance, shard_key_name, None)
            if val is not None:
                return self._db.router.route_key(val).id

        # Return default None (Django defaults to the "default" database)
        return None


def populate_django_databases(
    django_databases: Dict[str, Any],
    config: MuxConfig,
) -> None:
    """
    Populate a Django settings.DATABASES dictionary with connection options
    for all shards defined in the MuxConfig.
    """
    for shard in config.shards:
        engine = "django.db.backends.postgresql" if shard.backend == "postgresql" else shard.backend
        
        django_databases[shard.id] = {
            "ENGINE": engine,
            "NAME": shard.database,
            "USER": getattr(shard, "_username", "") or "",
            "PASSWORD": getattr(shard, "_password", "") or "",
            "HOST": shard.host,
            "PORT": shard.port,
            "CONN_MAX_AGE": 300,  # Match pool configuration lifecycle
        }


class MuxDBMiddleware:
    """
    Django middleware to extract routing headers (e.g. HTTP_X_SHARD_KEY)
    and set the current request thread's shard context automatically.
    """

    def __init__(self, get_response: Any) -> None:
        self.get_response = get_response

    def __call__(self, request: Any) -> Any:
        # Extract shard key from headers
        shard_key = request.headers.get("X-Shard-Key")
        shard_id = request.headers.get("X-Shard-Id")
        
        if shard_id:
            set_shard_context(shard_id=shard_id)
        elif shard_key:
            # Standardize numeric keys if possible
            try:
                shard_key = int(shard_key)
            except ValueError:
                pass
            set_shard_context(shard_key=shard_key)
            
        try:
            response = self.get_response(request)
            return response
        finally:
            clear_shard_context()
