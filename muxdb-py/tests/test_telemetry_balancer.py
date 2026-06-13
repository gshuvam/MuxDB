from __future__ import annotations

import time
import unittest
from unittest.mock import MagicMock

from muxdb.config import MuxConfig, ClusterConfig, ShardConfig
from muxdb import (
    Router,
    TelemetryCollector,
    PlacementEngine,
    Balancer,
)

from muxdb.integrations.kafka import MuxKafkaProducer, MuxKafkaConsumer


class TestTelemetryAndBalancer(unittest.TestCase):

    def test_telemetry_collector(self) -> None:
        collector = TelemetryCollector(window_seconds=10.0)
        
        # Test record query and QPS calculation
        collector.record_query("shard-0", "key-1", 10.0, is_write=False)
        collector.record_query("shard-0", "key-1", 20.0, is_write=False)
        collector.record_query("shard-0", "key-2", 30.0, is_write=True)
        
        self.assertAlmostEqual(collector.get_qps("shard-0"), 0.3, places=1)
        self.assertAlmostEqual(collector.get_qps("shard-0", is_write=False), 0.2, places=1)
        self.assertAlmostEqual(collector.get_qps("shard-0", is_write=True), 0.1, places=1)

        # Test Latency Percentiles
        self.assertEqual(collector.get_latency_percentile("shard-0", 50.0), 20.0)
        self.assertEqual(collector.get_latency_percentile("shard-0", 90.0), 30.0)

        # Test Hot Keys
        hot = collector.get_hot_keys("shard-0", threshold_qps=0.15)
        self.assertEqual(len(hot), 1)
        self.assertEqual(hot[0][0], "key-1")

    def test_placement_engine(self) -> None:
        engine = PlacementEngine(cache_shard_id="cache-0")
        
        self.assertFalse(engine.is_cached("key-1"))
        self.assertEqual(engine.resolve_read_target("key-1", "storage-0"), "storage-0")
        
        # Promote key
        engine.promote_to_cache("key-1")
        self.assertTrue(engine.is_cached("key-1"))
        self.assertEqual(engine.resolve_read_target("key-1", "storage-0"), "cache-0")
        
        # Write to key should evict
        self.assertEqual(engine.resolve_write_target("key-1", "storage-0"), "storage-0")
        self.assertFalse(engine.is_cached("key-1"))

    def test_balancer_decisions_and_suppression(self) -> None:
        balancer = Balancer(cooldown_seconds=1.0)
        
        # Test healthy cluster (no decisions)
        metrics = {
            "shard-0": {"read_qps": 5.0, "write_qps": 2.0},
            "shard-1": {"read_qps": 4.0, "write_qps": 1.0},
        }
        self.assertEqual(len(balancer.evaluate(metrics)), 0)

        # Test L3 replica scaling recommendation
        metrics_read_heavy = {
            "shard-0": {"read_qps": 60.0, "write_qps": 2.0},
        }
        decisions = balancer.evaluate(metrics_read_heavy)
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].action_type, "L3_REPLICA")
        
        # Test cooldown suppression (evaluating again immediately yields no actions)
        self.assertEqual(len(balancer.evaluate(metrics_read_heavy)), 0)

        # Reset balancer with 0 cooldown to test split & write suppression
        balancer_no_cooldown = Balancer(cooldown_seconds=0.0, write_suppression_threshold=20.0)
        
        # Test L2 Split recommendation
        metrics_hot_shard = {
            "shard-0": {"read_qps": 120.0, "write_qps": 10.0},
        }
        decisions_split = balancer_no_cooldown.evaluate(metrics_hot_shard)
        self.assertEqual(len(decisions_split), 1)
        self.assertEqual(decisions_split[0].action_type, "L2_SPLIT")

        # Test write suppression rule
        metrics_write_heavy = {
            "shard-0": {"read_qps": 120.0, "write_qps": 30.0},  # total write qps = 30 >= 20 threshold
        }
        self.assertEqual(len(balancer_no_cooldown.evaluate(metrics_write_heavy)), 0)

    def test_kafka_cdc_routing(self) -> None:
        # Mock Router setup
        config = MuxConfig(
            cluster=ClusterConfig(name="test", strategy="consistent_hash", shard_key="user_id"),
            shards=[
                ShardConfig(id="s0", backend="postgresql", host="localhost", port=5432, database="db0"),
                ShardConfig(id="s1", backend="postgresql", host="localhost", port=5433, database="db1"),
            ],
        )
        router = Router(config, MagicMock())
        # Override router.route_key to return a specific shard
        mock_shard = ShardConfig(id="s1", backend="postgresql", host="localhost", port=5433, database="db1")
        router.route_key = MagicMock(return_value=mock_shard)

        producer = MuxKafkaProducer({"bootstrap.servers": "localhost:9092"}, router)
        producer.produce("cdc-topic", "user_42", "updated_record")

        self.assertEqual(len(producer._sent_messages), 1)
        sent = producer._sent_messages[0]
        self.assertEqual(sent["topic"], "cdc-topic")
        self.assertEqual(sent["key"], "user_42")
        self.assertEqual(sent["target_shard_id"], "s1")

        # Test Consumer
        consumer = MuxKafkaConsumer({"bootstrap.servers": "localhost:9092", "group.id": "test-group"})
        self.assertIsNotNone(consumer)
        handler_called = False

        def mock_handler(msg):
            nonlocal handler_called
            handler_called = True

        consumer.register_handler("cdc-topic", mock_handler)
        consumer.subscribe(["cdc-topic"])
        consumer.poll(timeout=0.001)


if __name__ == "__main__":
    unittest.main()
