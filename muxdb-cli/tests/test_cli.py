from __future__ import annotations

import os
import unittest
from click.testing import CliRunner
from muxdb_cli.main import main


class TestMuxDBCli(unittest.TestCase):

    def setUp(self) -> None:
        self.runner = CliRunner()
        self.config_path = "test_muxdb.yaml"

    def tearDown(self) -> None:
        if os.path.exists(self.config_path):
            os.remove(self.config_path)

    def test_cli_init_and_validate(self) -> None:
        # 1. Test init
        result = self.runner.invoke(main, ["init", "--output", self.config_path])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Successfully initialized default configuration file", result.output)
        self.assertTrue(os.path.exists(self.config_path))

        # 2. Test validate
        result_val = self.runner.invoke(main, ["validate", "--config", self.config_path])
        self.assertEqual(result_val.exit_code, 0)
        self.assertIn("is fully valid", result_val.output)

    def test_cli_status_and_list(self) -> None:
        self.runner.invoke(main, ["init", "--output", self.config_path])

        # Test status
        result_status = self.runner.invoke(main, ["status", "--config", self.config_path])
        self.assertEqual(result_status.exit_code, 0)
        self.assertIn("MuxDB Cluster Status", result_status.output)
        self.assertIn("shard-0", result_status.output)

        # Test list
        result_list = self.runner.invoke(main, ["list", "--config", self.config_path])
        self.assertEqual(result_list.exit_code, 0)
        self.assertIn("shard-1", result_list.output)

    def test_cli_rebalance(self) -> None:
        self.runner.invoke(main, ["init", "--output", self.config_path])
        result = self.runner.invoke(main, ["rebalance", "--config", self.config_path])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Cluster is balanced", result.output)

    def test_cli_migrate(self) -> None:
        self.runner.invoke(main, ["init", "--output", self.config_path])
        result = self.runner.invoke(main, [
            "migrate",
            "--config", self.config_path,
            "--id", "mig-123",
            "--source", "shard-0",
            "--dest", "shard-1",
            "--keys", "k1,k2,k3",
        ])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("migration mig-123 of 3 keys", result.output)
        self.assertIn("completed successfully", result.output)

    def test_cli_tenant_move(self) -> None:
        self.runner.invoke(main, ["init", "--output", self.config_path])
        result = self.runner.invoke(main, [
            "tenant-move",
            "--config", self.config_path,
            "--id", "tenant-alpha",
            "--dest", "shard-group-2",
        ])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("tenant tenant-alpha relocation", result.output)

    def test_cli_telemetry(self) -> None:
        self.runner.invoke(main, ["init", "--output", self.config_path])
        result = self.runner.invoke(main, ["telemetry", "--config", self.config_path])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Real-time Telemetry Metrics", result.output)
        self.assertIn("shard-0", result.output)


if __name__ == "__main__":
    unittest.main()
