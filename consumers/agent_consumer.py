"""Reactive Kafka consumer for the Phase 5 LangGraph reasoning agent.

A long-lived Kafka consumer subscribed to all ``harmonicmesh.patterns.*`` topics
via a regex subscription.
The LangGraph is compiled **once** at startup and reused for the lifetime of
the process — never re-compiled per message.

Error handling mirrors the Phase 4 pattern_ingester:
  - Poison message (bad JSON or InvalidPatternMatchError): logged and offset
    committed, so a single malformed record cannot wedge the partition.
  - Transient failure (Graphiti down, LLM timeout): offset is *not* committed;
    consumer seeks back and retries after an exponential backoff.

Single-worker only in v1; the async worker pool is deferred.

Run:
    python -m consumers.agent_consumer
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal

from confluent_kafka import Consumer, KafkaError, TopicPartition

from agent.errors import InvalidPatternMatchError
from agent.graph import build_agent
from graphiti_layer.client import close_graphiti, get_graphiti

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("agent_consumer")

# Regex matches all current and future pattern topics regardless of machine or
# pattern type.  rdkafka interprets topics starting with '^' as regex.
TOPICS_PATTERN = "^harmonicmesh\\.patterns\\..+"
CONSUMER_GROUP = "harmonicmesh-agent"
INITIAL_BACKOFF_SECONDS = 2.0
MAX_BACKOFF_SECONDS = 60.0
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
            # Manual commit: offset moves only after the LangGraph invocation
            # returns successfully. This is what makes the pipeline at-least-once.
            "enable.auto.commit": False,
            # New consumer group starts at the topic tail — backlog (~105 historical
            # matches from Phase 4 testing) is skipped on first run. After the group
            # has committed an offset Kafka uses that; this default applies only on
            # the very first subscription.
            "auto.offset.reset": "latest",
        }
    )
    log.info("Kafka consumer configured: %s -> group %s", bootstrap, CONSUMER_GROUP)
    return consumer


class _BackoffState:
    """Track per-partition retry backoff for transient failures."""

    def __init__(self) -> None:
        self._delay = INITIAL_BACKOFF_SECONDS

    def next_delay(self) -> float:
        delay = self._delay
        self._delay = min(self._delay * 2.0, MAX_BACKOFF_SECONDS)
        return delay

    def reset(self) -> None:
        self._delay = INITIAL_BACKOFF_SECONDS


async def _handle_message(consumer: Consumer, msg, agent, backoff: _BackoffState) -> None:
    """Invoke the agent on one Kafka message; commit only on success."""
    try:
        pattern_match = json.loads(msg.value())
    except json.JSONDecodeError as exc:
        log.error("Skipping malformed JSON at offset %d: %s", msg.offset(), exc)
        consumer.commit(message=msg, asynchronous=False)
        return

    try:
        await agent.ainvoke({"pattern_match": pattern_match})
    except InvalidPatternMatchError as exc:
        log.error(
            "Skipping invalid pattern match at offset %d: %s", msg.offset(), exc
        )
        consumer.commit(message=msg, asynchronous=False)
        return
    except Exception as exc:  # noqa: BLE001
        delay = backoff.next_delay()
        log.warning(
            "Transient failure at offset %d (%s); seeking back, retry in %.1fs",
            msg.offset(),
            exc,
            delay,
        )
        consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
        await asyncio.sleep(delay)
        return

    consumer.commit(message=msg, asynchronous=False)
    backoff.reset()
    log.debug("Committed offset %d", msg.offset())


async def run() -> None:
    """Consume the pattern topic until a shutdown signal arrives."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    # Preload Graphiti so misconfiguration fails fast at startup rather than on
    # the first message — same pattern as pattern_ingester.
    log.info("Initialising Graphiti layer...")
    await get_graphiti()

    # Compile the LangGraph ONCE. Reused for every message. Re-compilation per
    # message is forbidden by the architectural directive.
    log.info("Compiling LangGraph agent...")
    agent = build_agent()
    log.info("Agent compiled.")

    consumer = _build_consumer()
    consumer.subscribe([TOPICS_PATTERN])
    log.info("Subscribed via pattern %s; consuming...", TOPICS_PATTERN)

    backoff = _BackoffState()
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
            await _handle_message(consumer, msg, agent, backoff)
    finally:
        log.info("Shutting down...")
        consumer.close()
        await close_graphiti()
        log.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(run())
