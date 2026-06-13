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


if __name__ == "__main__":
    unittest.main()
