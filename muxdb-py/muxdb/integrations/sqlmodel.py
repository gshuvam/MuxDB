"""
MuxDB SQLModel Integration.

Provides cooperative sharded session classes and model bases for SQLModel,
wrapping the underlying SQLAlchemy horizontal sharding logic.
"""

from __future__ import annotations

from typing import Any, Dict

from muxdb.client import MuxDB
from muxdb.integrations.sqlalchemy import (
    make_shard_chooser,
    make_id_chooser,
    make_query_chooser,
)


def get_sqlmodel_classes() -> tuple[Any, Any]:
    """Retrieve SQLModel base classes dynamically."""
    try:
        from sqlmodel import SQLModel, Session
        return SQLModel, Session
    except ImportError as exc:
        raise ImportError("SQLModel is not installed. Run: pip install sqlmodel") from exc


class MuxSQLModel:
    """
    Base class for sharded SQLModel models.
    
    Subclasses should define the `__shard_key__` attribute pointing to the sharding column name.
    """
    __shard_key__: str = "id"


def create_sqlmodel_session(
    db: MuxDB,
    engines: Dict[str, Any],
    **kwargs: Any,
) -> Any:
    """
    Create a sharded SQLModel Session instance that routes queries across sharded engines.
    """
    SQLModel, Session = get_sqlmodel_classes()
    from sqlalchemy.ext.horizontal_shard import ShardedSession
    
    # Define a cooperative class that inherits from both ShardedSession and SQLModel's Session
    class MuxShardedSQLModelSession(ShardedSession, Session):
        pass

    # Instantiate ShardedSession with our custom choosers
    session = MuxShardedSQLModelSession(
        shards=engines,
        shard_chooser=make_shard_chooser(db),
        id_chooser=make_id_chooser(db),
        query_chooser=make_query_chooser(db),
        **kwargs,
    )
    session.mux_db = db  # type: ignore
    return session
