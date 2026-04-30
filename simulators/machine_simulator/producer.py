"""Kafka producer wrapper using confluent-kafka.

Handles:
  - Topic creation via AdminClient (3 partitions, RF=1)
  - Serialisation (UTF-8 JSON bytes)
  - Delivery error logging (no crash on error)
  - Producer flush on shutdown
"""
from __future__ import annotations

import json
import logging
import os
from typing import Dict, List, Optional

from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------

def _delivery_callback(err: Optional[KafkaError], msg) -> None:
    if err is not None:
        log.error(
            "Delivery failed for key=%s on %s [%d]: %s",
            msg.key(),
            msg.topic(),
            msg.partition(),
            err,
        )


# ---------------------------------------------------------------------------
# KafkaProducerWrapper
# ---------------------------------------------------------------------------

class KafkaProducerWrapper:
    """Thin wrapper around confluent_kafka.Producer.

    Args:
        bootstrap_servers: Comma-separated host:port list.
        sasl_username:      SASL/PLAIN username.
        sasl_password:      SASL/PLAIN password.
        verbose:            If True, log every produced message to stdout.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        sasl_username: str,
        sasl_password: str,
        verbose: bool = False,
    ) -> None:
        self._verbose = verbose
        self._bootstrap = bootstrap_servers
        self._username = sasl_username
        self._password = sasl_password

        conf = self._build_conf(bootstrap_servers, sasl_username, sasl_password)
        self._producer = Producer(conf)

    @staticmethod
    def _build_conf(
        bootstrap_servers: str,
        sasl_username: str,
        sasl_password: str,
    ) -> Dict[str, str]:
        return {
            "bootstrap.servers": bootstrap_servers,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": sasl_username,
            "sasl.password": sasl_password,
            # Throughput-friendly defaults
            "linger.ms": "5",
            "batch.num.messages": "1000",
            "compression.type": "lz4",
            "queue.buffering.max.messages": "100000",
        }

    # ------------------------------------------------------------------
    # Topic management
    # ------------------------------------------------------------------

    def ensure_topics(self, topics: List[str], num_partitions: int = 3) -> None:
        """Create topics that do not yet exist (idempotent)."""
        admin_conf = {
            "bootstrap.servers": self._bootstrap,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": self._username,
            "sasl.password": self._password,
        }
        admin = AdminClient(admin_conf)

        new_topics = [
            NewTopic(topic, num_partitions=num_partitions, replication_factor=1)
            for topic in topics
        ]

        fs = admin.create_topics(new_topics)
        for topic, future in fs.items():
            try:
                future.result()
                log.info("Topic created: %s", topic)
            except KafkaException as exc:
                # Error code 36 = TOPIC_ALREADY_EXISTS — treat as success
                if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    log.debug("Topic already exists: %s", topic)
                else:
                    log.error("Failed to create topic %s: %s", topic, exc)

    # ------------------------------------------------------------------
    # Produce
    # ------------------------------------------------------------------

    def send(self, topic: str, key: str, value: dict) -> None:
        """Serialise *value* to JSON and produce to *topic*.

        Non-blocking: delivery errors are surfaced via the delivery callback.
        """
        payload = json.dumps(value, ensure_ascii=False).encode("utf-8")
        key_bytes = key.encode("utf-8")

        if self._verbose:
            log.info("[%s] key=%s  %s", topic, key, json.dumps(value))

        self._producer.produce(
            topic=topic,
            key=key_bytes,
            value=payload,
            on_delivery=_delivery_callback,
        )
        # Trigger delivery callbacks without blocking
        self._producer.poll(0)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def flush(self, timeout: float = 30.0) -> None:
        """Block until all outstanding messages are delivered (or timeout)."""
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            log.warning("%d message(s) were not delivered before flush timeout", remaining)
        else:
            log.info("Producer flushed — all messages delivered")
