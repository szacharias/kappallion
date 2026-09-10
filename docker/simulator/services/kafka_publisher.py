"""
kafka_publisher.py
Resilient Kafka publisher service with connection retry logic and JSON serialization.
"""

import json
import logging
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class KafkaTelemetryPublisher:
    """Wrapper around KafkaProducer with resilience and clean lifecycle management."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        enabled: bool = True,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.enabled = enabled
        self._producer = None

    def connect(self, max_retries: int = 30, retry_delay_seconds: float = 2.0) -> bool:
        """Connect to Kafka broker with progressive retry loop."""
        if not self.enabled:
            logger.info("Kafka integration disabled. Skipping connection.")
            return True

        from kafka import KafkaProducer

        for attempt in range(max_retries):
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
                logger.info("Connected to Kafka broker at %s", self.bootstrap_servers)
                return True
            except Exception as ex:
                logger.warning(
                    "Kafka broker unavailable (%d/%d): %s. Retrying...",
                    attempt + 1,
                    max_retries,
                    ex,
                )
                time.sleep(retry_delay_seconds)

        logger.error("Failed to connect to Kafka broker after %d attempts.", max_retries)
        return False

    def publish(self, payload: Dict[str, Any]) -> bool:
        """Publish a serialized JSON dictionary to the configured topic."""
        if not self.enabled or self._producer is None:
            return False

        try:
            self._producer.send(self.topic, value=payload)
            return True
        except Exception as ex:
            logger.error("Failed to publish message to topic %s: %s", self.topic, ex)
            return False

    def close(self):
        """Flush and close Kafka producer connection cleanly."""
        if self._producer:
            try:
                self._producer.flush()
                self._producer.close()
            except Exception as ex:
                logger.warning("Error during Kafka producer shutdown: %s", ex)
