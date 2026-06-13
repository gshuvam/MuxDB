"""
Integration smoke tests for MuxDB integrations.
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from muxdb.client import MuxDB
from muxdb.config import MuxConfig, ClusterConfig, ShardConfig
from muxdb.integrations.psycopg import MuxConnection
from muxdb.integrations.django import shard_context, get_current_shard_key, MuxRouter
from muxdb.integrations.redis import extract_routing_key


class TestPythonIntegrations(unittest.TestCase):

    def setUp(self) -> None:
        self.config = MuxConfig(
            cluster=ClusterConfig(name="test", strategy="consistent_hash", shard_key="user_id"),
            shards=[
                ShardConfig(id="s0", backend="postgresql", host="localhost", port=5432, database="db0"),
                ShardConfig(id="s1", backend="postgresql", host="localhost", port=5433, database="db1"),
            ],
        )
        self.db = MuxDB(self.config)
        self.db.connect()

    def tearDown(self) -> None:
        self.db.close()

    def test_psycopg_wrapper(self) -> None:
        conn = MuxConnection(self.db)
        self.assertFalse(conn.closed)
        cursor = conn.cursor()
        self.assertEqual(cursor.rowcount, -1)
        conn.close()
        self.assertTrue(conn.closed)

    def test_django_context(self) -> None:
        self.assertIsNone(get_current_shard_key())
        with shard_context(shard_key=42):
            self.assertEqual(get_current_shard_key(), 42)
        self.assertIsNone(get_current_shard_key())

    def test_django_router(self) -> None:
        MuxRouter.set_mux_db(self.db)
        router = MuxRouter()
        
        # Test routing with context
        with shard_context(shard_key=42):
            target = router.db_for_read(None)
            expected = self.db.router.route_key(42).id
            self.assertEqual(target, expected)

    def test_redis_hash_tags(self) -> None:
        self.assertEqual(extract_routing_key("user_123"), "user_123")
        self.assertEqual(extract_routing_key("{user_123}:profile"), "user_123")
        self.assertEqual(extract_routing_key("profile:{user_123}:details"), "user_123")

    def test_mongodb_routing(self) -> None:
        from muxdb.integrations.mongodb import MuxMongoClient
        mock_clients = {"s0": MagicMock(), "s1": MagicMock()}
        client = MuxMongoClient(self.db, mock_clients)
        
        # Test collection item access
        db_wrapper = client["my_db"]
        col = db_wrapper["my_collection"]
        self.assertEqual(col.name, "my_collection")

    def test_qdrant_routing(self) -> None:
        from muxdb.integrations.qdrant import MuxQdrantClient
        mock_clients = {"s0": MagicMock(), "s1": MagicMock()}
        client = MuxQdrantClient(self.db, mock_clients)
        
        # Test routing resolution
        shard_id = client._resolve_shard("point-123", {"user_id": 42})
        expected = self.db.router.route_key(42).id
        self.assertEqual(shard_id, expected)

    def test_elasticsearch_routing(self) -> None:
        from muxdb.integrations.elasticsearch import MuxElasticsearch
        mock_clients = {"s0": MagicMock(), "s1": MagicMock()}
        client = MuxElasticsearch(self.db, mock_clients)
        
        # Test resolve shard
        shard_id = client._resolve_shard(None, {"user_id": 100})
        expected = self.db.router.route_key(100).id
        self.assertEqual(shard_id, expected)

    def test_cassandra_routing(self) -> None:
        from muxdb.integrations.cassandra import MuxCassandraSession
        mock_sessions = {"s0": MagicMock(), "s1": MagicMock()}
        session = MuxCassandraSession(self.db, mock_sessions)
        self.assertIsNotNone(session)


if __name__ == "__main__":
    unittest.main()
