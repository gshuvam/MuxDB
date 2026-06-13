from __future__ import annotations

import collections
import time
from threading import Lock
from typing import Any, Dict, List, Tuple

class TelemetryCollector:
    """Aggregates rolling-window telemetry data for query counts, latency, and hot keys."""

    def __init__(self, window_seconds: float = 60.0) -> None:
        self.window_seconds = window_seconds
        self._lock = Lock()
        
        # Tracks query logs: shard_id -> deque of (timestamp, latency_ms, is_write)
        self._query_logs: Dict[str, collections.deque[Tuple[float, float, bool]]] = collections.defaultdict(
            collections.deque
        )
        
        # Tracks access logs for individual keys: shard_id -> key -> deque of timestamps
        self._key_logs: Dict[str, Dict[str, collections.deque[float]]] = collections.defaultdict(
            lambda: collections.defaultdict(collections.deque)
        )

    def record_query(self, shard_id: str, key: str | None, latency_ms: float, is_write: bool) -> None:
        """Record query metrics for a shard."""
        now = time.time()
        with self._lock:
            self._query_logs[shard_id].append((now, latency_ms, is_write))
            if key:
                self._key_logs[shard_id][key].append(now)

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        
        for logs in self._query_logs.values():
            while logs and logs[0][0] < cutoff:
                logs.popleft()
                
        for key_map in self._key_logs.values():
            for key, logs in list(key_map.items()):
                while logs and logs[0] < cutoff:
                    logs.popleft()
                if not logs:
                    del key_map[key]

    def get_qps(self, shard_id: str, is_write: bool | None = None) -> float:
        """Calculate QPS for a shard in the current window."""
        now = time.time()
        with self._lock:
            self._prune(now)
            logs = self._query_logs.get(shard_id)
            if not logs:
                return 0.0
            
            if is_write is None:
                count = len(logs)
            else:
                count = sum(1 for _, _, iw in logs if iw == is_write)
                
            return count / self.window_seconds

    def get_latency_percentile(self, shard_id: str, percentile: float) -> float:
        """Get the given latency percentile for a shard in the current window."""
        now = time.time()
        with self._lock:
            self._prune(now)
            logs = self._query_logs.get(shard_id)
            if not logs:
                return 0.0
                
            latencies = sorted(lat for _, lat, _ in logs)
            if not latencies:
                return 0.0
                
            idx = int(len(latencies) * (percentile / 100.0))
            idx = min(len(latencies) - 1, max(0, idx))
            return latencies[idx]

    def get_hot_keys(self, shard_id: str, threshold_qps: float) -> List[Tuple[str, float]]:
        """Identify hot keys on a shard whose access rate exceeds the threshold."""
        now = time.time()
        with self._lock:
            self._prune(now)
            key_map = self._key_logs.get(shard_id)
            if not key_map:
                return []
                
            hot = []
            for key, logs in key_map.items():
                qps = len(logs) / self.window_seconds
                if qps >= threshold_qps:
                    hot.append((key, qps))
                    
            return sorted(hot, key=lambda x: x[1], reverse=True)

    def get_metrics(self) -> Dict[str, Any]:
        """Retrieve metrics for all shards."""
        now = time.time()
        with self._lock:
            self._prune(now)
            shards = list(self._query_logs.keys())
            
        metrics = {}
        for shard_id in shards:
            metrics[shard_id] = {
                "read_qps": self.get_qps(shard_id, is_write=False),
                "write_qps": self.get_qps(shard_id, is_write=True),
                "p50_latency_ms": self.get_latency_percentile(shard_id, 50.0),
                "p90_latency_ms": self.get_latency_percentile(shard_id, 90.0),
                "p99_latency_ms": self.get_latency_percentile(shard_id, 99.0),
            }
        return metrics
