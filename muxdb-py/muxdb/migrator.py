from __future__ import annotations

import time
from typing import Any, Dict, List, Set

from muxdb.errors import MigrationError

class PIDController:
    """Proportional-Integral-Derivative controller to govern live migration bandwidth.

    Adjusts batch delays dynamically to regulate latency deviation.
    """

    def __init__(
        self,
        kp: float = 0.5,
        ki: float = 0.1,
        kd: float = 0.2,
        target_deviation: float = 0.1,
    ) -> None:
        self.kp = kp
        self.ki = ki
        self.kd = kd
        self.target_deviation = target_deviation
        self.integral = 0.0
        self.last_error = 0.0
        self.last_time = time.time()
        self.current_delay = 0.01

    def update(self, current_deviation: float) -> float:
        """Compute PID output and update the throttling backoff delay."""
        now = time.time()
        dt = now - self.last_time
        if dt <= 0:
            dt = 0.001
            
        error = current_deviation - self.target_deviation
        self.integral += error * dt
        derivative = (error - self.last_error) / dt
        
        output = (self.kp * error) + (self.ki * self.integral) + (self.kd * derivative)
        
        self.last_error = error
        self.last_time = now
        
        # Shift sleep backoff delay based on feedback loop output
        self.current_delay = max(0.0, self.current_delay + output * 0.01)
        return self.current_delay


class LiveMigrator:
    """Orchestrates live key migrations using the Zephyr dual-mode pull/push protocol."""

    def __init__(self, driver: Any = None, telemetry: Any = None) -> None:
        self.driver = driver
        self.telemetry = telemetry
        self.pid = PIDController()
        
        # Structure: migration_id -> metadata details dict
        self._active_migrations: Dict[str, Dict[str, Any]] = {}
        
        # Structure: key -> migration_id
        self._key_migration_map: Dict[str, str] = {}

    def start_migration(
        self,
        migration_id: str,
        source_shard_id: str,
        dest_shard_id: str,
        keys: List[str],
    ) -> None:
        """Initiate active migration process for a set of keys."""
        if migration_id in self._active_migrations:
            raise MigrationError(f"Migration '{migration_id}' is already active.")
            
        self._active_migrations[migration_id] = {
            "source": source_shard_id,
            "destination": dest_shard_id,
            "pending_keys": set(keys),
            "migrated_keys": set(),
            "data_store": {},  # Simulated data container
        }
        
        for key in keys:
            self._key_migration_map[key] = migration_id

    def is_migrating_key(self, key: str) -> bool:
        """Check if the given key is currently undergoing migration."""
        return key in self._key_migration_map

    def on_demand_pull(self, key: str) -> Any:
        """Zephyr protocol Pull: immediately pull and migrate key on-demand."""
        mig_id = self._key_migration_map.get(key)
        if not mig_id:
            return None
            
        mig = self._active_migrations[mig_id]
        if key in mig["migrated_keys"]:
            return None  # Key already pulled/migrated
            
        source_val = mig["data_store"].get(key, f"value_of_{key}")
        mig["migrated_keys"].add(key)
        mig["pending_keys"].discard(key)
        
        return source_val

    def cold_push_step(self, migration_id: str, batch_size: int = 5) -> bool:
        """Zephyr protocol Push: transfer a batch of pending keys in background.

        Returns True if more keys are pending, False if migration is finished.
        """
        mig = self._active_migrations.get(migration_id)
        if not mig or not mig["pending_keys"]:
            return False
            
        batch = list(mig["pending_keys"])[:batch_size]
        for key in batch:
            source_val = mig["data_store"].get(key, f"value_of_{key}")
            mig["migrated_keys"].add(key)
            mig["pending_keys"].remove(key)
            
        # Throttling step sleep using PID value
        time.sleep(self.pid.current_delay)
        return len(mig["pending_keys"]) > 0

    def adjust_throttling(self, current_latency: float, target_latency: float) -> float:
        """Adjust background push rate by feeding query latency deviations into PID loop."""
        deviation = (current_latency - target_latency) / max(0.001, target_latency)
        return self.pid.update(deviation)
