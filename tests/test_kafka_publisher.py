"""Unit tests for Kafka telemetry publisher service.

Target module: docker/simulator/services/kafka_publisher.py
Adheres strictly to FIRST principles and AAA pattern with mocked Kafka.
"""

import os
import sys
from unittest.mock import MagicMock, patch
import pytest

_SIM_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docker", "simulator"))
if _SIM_DIR not in sys.path:
    sys.path.insert(0, _SIM_DIR)

from services.kafka_publisher import KafkaTelemetryPublisher


@pytest.mark.unit
class TestKafkaTelemetryPublisher:
    """Unit test suite for Kafka publisher."""

    def test_disabled_publisher_bypasses_connection(self):
        """When enabled=False, connect() returns True immediately without connecting."""
        # Arrange
        publisher = KafkaTelemetryPublisher(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            enabled=False,
        )

        # Act
        connected = publisher.connect(max_retries=1)

        # Assert
        assert connected is True
        assert publisher.publish({"key": "value"}) is False

    @patch("kafka.KafkaProducer")
    def test_successful_connect_and_publish(self, mock_producer_cls):
        """Valid connection sends message to specified topic."""
        # Arrange
        mock_instance = MagicMock()
        mock_producer_cls.return_value = mock_instance
        publisher = KafkaTelemetryPublisher(
            bootstrap_servers="localhost:9092",
            topic="cold-chain-telemetry",
            enabled=True,
        )

        # Act
        connected = publisher.connect(max_retries=1)
        published = publisher.publish({"eventId": "test-123"})
        publisher.close()

        # Assert
        assert connected is True
        assert published is True
        mock_instance.send.assert_called_once_with("cold-chain-telemetry", value={"eventId": "test-123"})
        mock_instance.flush.assert_called_once()
        mock_instance.close.assert_called_once()
