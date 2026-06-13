from __future__ import annotations

import time
from typing import Any, Callable, Dict, List

import structlog

logger = structlog.get_logger(__name__)

# Attempt to load confluent_kafka, fallback to mock if unavailable
try:
    from confluent_kafka import Consumer, Producer
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False
    Producer = Any
    Consumer = Any

class MuxKafkaProducer:
    """Producer that routes CDC partition messages in alignment with MuxDB shard maps."""

    def __init__(self, kafka_config: Dict[str, Any], router: Any) -> None:
        self.router = router
        self.kafka_config = kafka_config
        self.producer = Producer(kafka_config) if HAS_KAFKA else None
        self._sent_messages: List[Dict[str, Any]] = []

    def produce(self, topic: str, key: str, value: str, *args: Any, **kwargs: Any) -> None:
        """Route message to appropriate partition by shard-key mapping."""
        shard = self.router.route_key(key)
        num_partitions = kwargs.pop("num_partitions", 10)
        partition = hash(shard.id) % num_partitions

        payload = {
            "topic": topic,
            "key": key,
            "value": value,
            "partition": partition,
            "target_shard_id": shard.id,
        }
        self._sent_messages.append(payload)

        if self.producer:
            self.producer.produce(topic, key=key, value=value, partition=partition, *args, **kwargs)
            self.producer.flush()
        else:
            logger.info("kafka.mock_produce", **payload)


class MuxKafkaConsumer:
    """Consumer to read events from Kafka partition and trigger replica syncs."""

    def __init__(self, kafka_config: Dict[str, Any]) -> None:
        self.kafka_config = kafka_config
        self.consumer = Consumer(kafka_config) if HAS_KAFKA else None
        self._handlers: Dict[str, Callable[[Any], None]] = {}

    def subscribe(self, topics: List[str]) -> None:
        """Subscribe to Kafka topics."""
        if self.consumer:
            self.consumer.subscribe(topics)
        else:
            logger.info("kafka.mock_subscribe", topics=topics)

    def register_handler(self, topic: str, handler: Callable[[Any], None]) -> None:
        """Register a callback handler for a topic."""
        self._handlers[topic] = handler

    def poll(self, timeout: float = 1.0) -> Any:
        """Poll for new messages."""
        if self.consumer:
            msg = self.consumer.poll(timeout)
            if msg and not msg.error() and msg.topic() in self._handlers:
                self._handlers[msg.topic()](msg)
            return msg
        else:
            time.sleep(timeout)
            return None
