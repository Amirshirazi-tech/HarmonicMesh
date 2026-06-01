"""Pattern-match → Graphiti ingestion consumer.

A long-lived Kafka consumer subscribed to all ``harmonicmesh.patterns.*`` topics
via a regex subscription.  Each Flink CEP pattern match is handed to
``graphiti_layer.ingest_pattern_match``
and the offset is committed only after the Graphiti write succeeds — so a crash
mid-ingest replays the message rather than dropping it (at-least-once).

Error handling splits two cases:
  - Poison message (bad JSON, fails ontology validation): logged and the offset
    is committed, so a single malformed record cannot wedge the partition.
  - Transient failure (Graphiti/Neo4j unreachable): the offset is *not*
    committed, the consumer seeks back to the record, and retries after a
    backoff until the dependency recovers.

Run:
    python -m consumers.pattern_ingester
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal

from confluent_kafka import Consumer, KafkaError, TopicPartition

from graphiti_layer import ingest_pattern_match
from graphiti_layer.client import close_graphiti, get_graphiti

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("pattern_ingester")

# Regex matches all current and future pattern topics regardless of machine or
# pattern type.  rdkafka interprets topics starting with '^' as regex.
TOPICS_PATTERN = "^harmonicmesh\\.patterns\\..+"
CONSUMER_GROUP = "harmonicmesh-graphiti-ingester"
RETRY_BACKOFF_SECONDS = 5.0
POLL_TIMEOUT_SECONDS = 1.0


def _build_consumer() -> Consumer:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    username = os.getenv("KAFKA_SASL_USERNAME", "")
    password = os.getenv("KAFKA_SASL_PASSWORD", "")
    if not username or not password:
        raise RuntimeError("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD must be set")

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": username,
            "sasl.password": password,
            "group.id": CONSUMER_GROUP,
            # Manual commit: the offset moves only after a successful Graphiti
            # write, which is what makes the pipeline at-least-once.
            "enable.auto.commit": False,
            # Pick up history written before the ingester started (warm-up).
            "auto.offset.reset": "earliest",
        }
    )
    log.info("Kafka consumer configured: %s -> group %s", bootstrap, CONSUMER_GROUP)
    return consumer


async def _handle_message(consumer: Consumer, msg) -> None:
    """Ingest one Kafka message, committing only on a successful write."""
    try:
        pattern_match = json.loads(msg.value())
    except json.JSONDecodeError as exc:
        # Poison message — committing prevents it wedging the partition.
        log.error("Skipping malformed JSON at offset %d: %s", msg.offset(), exc)
        consumer.commit(message=msg, asynchronous=False)
        return

    try:
        await ingest_pattern_match(pattern_match)
    except ValueError as exc:
        # Poison message — payload failed ontology/formatter validation.
        log.error("Skipping invalid pattern match at offset %d: %s", msg.offset(), exc)
        consumer.commit(message=msg, asynchronous=False)
        return
    except Exception as exc:  # noqa: BLE001 - transient: Graphiti/Neo4j/etc.
        # Do NOT commit. Seek back so the next poll re-delivers this record,
        # then back off to let the dependency recover.
        log.warning(
            "Transient ingest failure at offset %d (%s); will retry",
            msg.offset(),
            exc,
        )
        consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
        await asyncio.sleep(RETRY_BACKOFF_SECONDS)
        return

    consumer.commit(message=msg, asynchronous=False)
    log.debug("Committed offset %d", msg.offset())


async def run() -> None:
    """Consume the pattern topic until a shutdown signal arrives."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    # Initialise Graphiti (loads models, builds Neo4j indices) before consuming
    # so misconfiguration fails fast instead of on the first message.
    log.info("Initialising Graphiti layer...")
    await get_graphiti()

    consumer = _build_consumer()
    consumer.subscribe([TOPICS_PATTERN])
    log.info("Subscribed via pattern %s; consuming...", TOPICS_PATTERN)

    try:
        while not stop.is_set():
            msg = await asyncio.to_thread(consumer.poll, POLL_TIMEOUT_SECONDS)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                continue
            await _handle_message(consumer, msg)
    finally:
        log.info("Shutting down...")
        consumer.close()
        await close_graphiti()
        log.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(run())
