from __future__ import annotations

import time
import unittest

from muxdb.errors import MigrationError
from muxdb.migrator import LiveMigrator, PIDController
from muxdb.control_plane import (
    BridgeLeaseManager,
    ShardMapSynchronizer,
    ControlPlaneGRPCServer,
)


class TestMigrationAndControlPlane(unittest.TestCase):

    # --- Live Migration & PID Controller Tests ---

    def test_pid_controller(self) -> None:
        controller = PIDController(kp=0.5, ki=0.1, kd=0.2, target_deviation=0.1)
        
        # Test delay adjustment
        initial_delay = controller.current_delay
        
        # Simulate positive deviation (current latency exceeds target target latency deviation)
        # Should increase delay to throttle bandwidth
        delay_1 = controller.update(current_deviation=0.2)
        self.assertTrue(delay_1 > initial_delay)
        
        # Simulate negative deviation (current latency within targets)
        # Should decrease delay to speed up migration
        delay_2 = controller.update(current_deviation=0.0)
        self.assertTrue(delay_2 < delay_1)

    def test_live_migrator_on_demand_pull(self) -> None:
        migrator = LiveMigrator()
        
        migrator.start_migration(
            migration_id="mig-0",
            source_shard_id="s0",
            dest_shard_id="s1",
            keys=["user_1", "user_2"],
        )
        
        self.assertTrue(migrator.is_migrating_key("user_1"))
        self.assertFalse(migrator.is_migrating_key("user_3"))

        # Injects mock data on source
        migrator._active_migrations["mig-0"]["data_store"]["user_1"] = "data_val_1"

        # Pull key
        val = migrator.on_demand_pull("user_1")
        self.assertEqual(val, "data_val_1")
        
        # Pulling again returns None because it is already migrated
        self.assertIsNone(migrator.on_demand_pull("user_1"))

    def test_live_migrator_cold_push(self) -> None:
        migrator = LiveMigrator()
        keys = ["k1", "k2", "k3", "k4", "k5", "k6"]
        
        migrator.start_migration("mig-1", "s0", "s1", keys)
        
        # Take a step of size 3
        has_more = migrator.cold_push_step("mig-1", batch_size=3)
        self.assertTrue(has_more)
        self.assertEqual(len(migrator._active_migrations["mig-1"]["pending_keys"]), 3)
        self.assertEqual(len(migrator._active_migrations["mig-1"]["migrated_keys"]), 3)

        # Take another step of size 4 (remaining 3)
        has_more_2 = migrator.cold_push_step("mig-1", batch_size=4)
        self.assertFalse(has_more_2)
        self.assertEqual(len(migrator._active_migrations["mig-1"]["pending_keys"]), 0)

        # Telemetry-based PID tuning check
        new_delay = migrator.adjust_throttling(current_latency=150.0, target_latency=100.0)
        self.assertTrue(new_delay > 0.0)

    # --- Control Plane (Lease, Shard Sync, gRPC Server) Tests ---

    def test_bridge_lease_manager(self) -> None:
        manager = BridgeLeaseManager()
        
        # Acquire
        self.assertTrue(manager.acquire_lease("lease-1", "node-A", duration_s=1.0))
        self.assertTrue(manager.is_lease_active("lease-1"))
        self.assertEqual(manager.get_holder("lease-1"), "node-A")
        
        # Refuse duplicate lock from node-B
        self.assertFalse(manager.acquire_lease("lease-1", "node-B", duration_s=1.0))

        # Renew
        self.assertTrue(manager.renew_lease("lease-1", "node-A", duration_s=2.0))

        # Release
        self.assertTrue(manager.release_lease("lease-1", "node-A"))
        self.assertFalse(manager.is_lease_active("lease-1"))

    def test_shard_map_synchronizer(self) -> None:
        sync = ShardMapSynchronizer("node-1")
        
        topology_1 = {"s0": "localhost:5432"}
        topology_2 = {"s0": "localhost:5432", "s1": "localhost:5433"}
        
        # Reject older version updates
        self.assertFalse(sync.accept_map_update("node-2", 0, topology_1))

        # Accept newer version updates
        self.assertTrue(sync.accept_map_update("node-2", 5, topology_2))
        self.assertEqual(sync.current_version, 5)
        self.assertEqual(sync.topology, topology_2)

        # Broadcast update updates local state
        sync.broadcast_map_update(10, topology_1)
        self.assertEqual(sync.current_version, 10)

    def test_control_plane_grpc_server(self) -> None:
        server = ControlPlaneGRPCServer("node-0")
        
        # Check endpoints return False when server is stopped
        self.assertFalse(server.AcquireLease("lease-x", "node-A"))
        
        server.start(port=9090)
        self.assertTrue(server.is_running)
        self.assertEqual(server.port, 9090)
        
        # Test RPC calls
        self.assertTrue(server.AcquireLease("lease-x", "node-A", duration_s=1.0))
        self.assertTrue(server.SyncShardMap("node-1", 2, {"s0": "localhost:5432"}))
        self.assertTrue(server.ReleaseLease("lease-x", "node-A"))
        
        server.stop()
        self.assertFalse(server.is_running)
        self.assertIsNone(server.port)


if __name__ == "__main__":
    unittest.main()
