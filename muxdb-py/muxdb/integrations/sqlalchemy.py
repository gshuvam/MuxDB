"""
MuxDB SQLAlchemy Integration.

Exposes custom ShardedSession wrappers and choosers to transparently route
SQLAlchemy ORM operations to shard-specific engines.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List

from muxdb.client import MuxDB
from muxdb.config import MuxConfig


def create_mux_engines(config: MuxConfig, **kwargs: Any) -> Dict[str, Any]:
    """
    Create a dictionary of SQLAlchemy engines, one for each configured shard.
    
    Returns:
        Dict[str, Engine]: Mapping of shard_id to SQLAlchemy Engine.
    """
    try:
        from sqlalchemy import create_engine
    except ImportError as exc:
        raise ImportError("SQLAlchemy is not installed. Run: pip install sqlalchemy") from exc

    engines = {}
    for shard in config.shards:
        # Construct SQLAlchemy connection URI
        # Support postgresql as backend.
        driver_prefix = "postgresql+psycopg" if shard.backend == "postgresql" else shard.backend
        
        username = getattr(shard, "_username", "")
        password = getattr(shard, "_password", "")
        creds = f"{username}:{password}@" if username else ""
        
        uri = f"{driver_prefix}://{creds}{shard.host}:{shard.port}/{shard.database}"
        engines[shard.id] = create_engine(uri, **kwargs)

    return engines


def make_shard_chooser(db: MuxDB) -> Callable[[Any, Any, Any], str]:
    """Create a shard_chooser callback for SQLAlchemy ShardedSession."""
    shard_key_name = db.config.cluster.shard_key

    def shard_chooser(session: Any, instance: Any, clause: Any = None) -> str:
        # Extract the shard key from the instance
        val = getattr(instance, shard_key_name, None)
        if val is None:
            raise ValueError(
                f"Instance of {instance.__class__.__name__} is missing "
                f"the shard key '{shard_key_name}' required for placement."
            )
        shard = db.router.route_key(val)
        return shard.id

    return shard_chooser


def make_id_chooser(db: MuxDB) -> Callable[[Any, Any, Any], List[str]]:
    """Create an id_chooser callback for SQLAlchemy ShardedSession."""
    shard_key_name = db.config.cluster.shard_key

    def id_chooser(session: Any, query: Any, ident: Any) -> List[str]:
        # ident is the PK value(s). If the PK is the shard key, route directly.
        # Otherwise, broadcast to all shards.
        mapper = query.column_descriptions[0]["expr"]
        pk_keys = [col.name for col in mapper.__table__.primary_key.columns]

        if shard_key_name in pk_keys:
            # If the primary key is single-column, ident is the value
            # If compound, ident is a tuple in the order of PK columns
            idx = pk_keys.index(shard_key_name)
            val = ident[idx] if isinstance(ident, tuple) else ident
            shard = db.router.route_key(val)
            return [shard.id]

        # Fallback: query all shards
        return db.shard_map.shard_ids

    return id_chooser


def make_query_chooser(db: MuxDB) -> Callable[[Any], List[str]]:
    """Create a query_chooser callback for SQLAlchemy ShardedSession."""
    shard_key_name = db.config.cluster.shard_key

    def query_chooser(session: Any, query: Any) -> List[str]:
        # Compile query to extract bind parameters and clauses
        # If we can extract the shard key value from the filters, route to a single shard.
        # Otherwise, return all shards.
        try:
            # Compile statement to query string
            statement = query.statement
            binds = query.session.connection().dialect.execution_options
            
            # Simple heuristic search: look at filters
            # SQLAlchemy's query.whereclause contains the binary expressions
            whereclause = query.whereclause
            if whereclause is not None:
                # We do a quick search in the where clause binary expressions
                val = _find_shard_key_value(whereclause, shard_key_name)
                if val is not None:
                    shard = db.router.route_key(val)
                    return [shard.id]
        except Exception:
            pass

        return db.shard_map.shard_ids

    return query_chooser


def _find_shard_key_value(clause: Any, key_name: str) -> Any | None:
    """Helper to traverse SQLAlchemy expressions and extract the shard key value."""
    from sqlalchemy.sql.elements import BinaryExpression, BindParameter
    
    if isinstance(clause, BinaryExpression):
        # Look for: column == value
        left = clause.left
        right = clause.right
        
        # Check if left is the column we want
        if getattr(left, "name", None) == key_name:
            if isinstance(right, BindParameter):
                return right.value
            return getattr(right, "value", None)
        # Check if right is the column we want
        elif getattr(right, "name", None) == key_name:
            if isinstance(left, BindParameter):
                return left.value
            return getattr(left, "value", None)
            
    # Recursively check children/clauses (e.g. in BooleanClauseList / AND)
    clauses = getattr(clause, "clauses", None)
    if clauses:
        for child in clauses:
            val = _find_shard_key_value(child, key_name)
            if val is not None:
                return val
                
    return None


def MuxShardedSession(db: MuxDB, engines: Dict[str, Any], **kwargs: Any) -> Any:
    """
    Factory to construct a SQLAlchemy ShardedSession configured with MuxDB router choosers.
    """
    try:
        from sqlalchemy.ext.horizontal_shard import ShardedSession
        from sqlalchemy.orm import sessionmaker
    except ImportError as exc:
        raise ImportError("SQLAlchemy is not installed. Run: pip install sqlalchemy") from exc

    session_factory = sessionmaker(
        class_=ShardedSession,
        shards=engines,
        shard_chooser=make_shard_chooser(db),
        id_chooser=make_id_chooser(db),
        query_chooser=make_query_chooser(db),
        **kwargs,
    )
    
    # Store MuxDB instance on the session class/factory
    session = session_factory()
    session.mux_db = db  # type: ignore
    return session
