"""Graphiti / Neo4j initialisation for the HarmonicMesh temporal graph.

This module owns every connection to Graphiti and Neo4j. Nothing else in the
project constructs a Graphiti instance — the Phase 5 agent calls the three
functions in ``graphiti_layer/__init__.py``, which in turn obtain their
Graphiti handle from :func:`get_graphiti` here.

Stack wired up here:
  - Neo4j        — connection from environment variables
  - LLM          — Claude Haiku via OpenRouter (OpenAI-compatible API), for
                   ingestion-time entity extraction
  - Embedder     — BAAI/bge-m3, run locally (no embedding API, no per-call cost)
  - Cross-encoder— BAAI/bge-reranker-v2-m3, run locally

The local bge-m3 embedder is wired in as a custom Graphiti ``EmbedderClient``
so the semantic half of hybrid search needs no third-party embedding key.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

# Best-effort: load repo-root .env for local runs (smoke test, dev). In Docker
# the environment is supplied by compose, so a missing .env / dotenv is fine.
try:  # pragma: no cover - convenience only
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # noqa: BLE001
    pass


# --------------------------------------------------------------------------
# Configuration (environment-driven)
# --------------------------------------------------------------------------

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")

# LLM used by Graphiti for entity/edge extraction, reached through OpenRouter's
# OpenAI-compatible API. The model id keeps the provider prefix OpenRouter
# expects (e.g. "anthropic/claude-haiku-4-5").
GENERATION_MODEL = os.getenv("GENERATION_MODEL", "anthropic/claude-haiku-4-5")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")

EMBEDDER_MODEL = os.getenv("EMBEDDER_MODEL", "BAAI/bge-m3")
EMBEDDER_DIM = int(os.getenv("EMBEDDER_DIM", "1024"))  # bge-m3 produces 1024-d vectors
RERANKER_MODEL = os.getenv("RERANKER_MODEL", "BAAI/bge-reranker-v2-m3")
# bge models run fine on CPU at this scale; override to "cuda" only if desired.
MODEL_DEVICE = os.getenv("MODEL_DEVICE", "cpu")


def _require(value: str, name: str) -> str:
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            f"Set it in .env or the container environment."
        )
    return value


# --------------------------------------------------------------------------
# Retrieval result model — the public return type of search_history()
# --------------------------------------------------------------------------

class Episode(BaseModel):
    """One unit of historical memory returned by search_history().

    Deliberately a project-defined type, not Graphiti's internal node/edge
    class: it keeps Graphiti's types from leaking into the Phase 5 agent, so
    graphiti_layer stays the only module coupled to Graphiti.
    """

    content: str = Field(description="Natural-language text of the retrieved memory.")
    name: str = Field(description="Short label for the retrieved item.")
    occurred_at: Optional[datetime] = Field(
        default=None, description="Event-time the memory refers to, if known."
    )
    reranker_score: float = Field(
        description="bge-reranker cross-encoder score; higher is more relevant."
    )
    retrieval_rank: int = Field(
        description="0-based position in Graphiti's pre-rerank hybrid result. "
        "Differs from the post-rerank order whenever the cross-encoder moved "
        "this candidate."
    )
    episode_type: Optional[str] = Field(
        default=None,
        description="Logical kind of source episode (pattern_occurrence, "
        "intervention, outcome, agent_alert) — None when the source episode "
        "could not be resolved.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Provenance (edge uuid, source episodes)."
    )


# --------------------------------------------------------------------------
# Local bge-m3 embedder — custom Graphiti EmbedderClient
# --------------------------------------------------------------------------

def _build_embedder():
    """Construct the local bge-m3 embedder as a Graphiti EmbedderClient.

    Defined as a function so the heavy graphiti_core / sentence-transformers
    imports happen only when a Graphiti instance is actually built.
    """
    from graphiti_core.embedder.client import EmbedderClient, EmbedderConfig
    from sentence_transformers import SentenceTransformer

    class LocalBGEEmbedder(EmbedderClient):
        """Embeds text locally with BAAI/bge-m3 (1024-d, L2-normalised)."""

        def __init__(self) -> None:
            self.config = EmbedderConfig(embedding_dim=EMBEDDER_DIM)
            log.info("Loading embedder %s on %s", EMBEDDER_MODEL, MODEL_DEVICE)
            self._model = SentenceTransformer(EMBEDDER_MODEL, device=MODEL_DEVICE)

        def _encode(self, texts: list[str]) -> list[list[float]]:
            vectors = self._model.encode(texts, normalize_embeddings=True)
            return [[float(x) for x in row] for row in vectors]

        async def create(
            self, input_data: str | list[str]
        ) -> list[float]:
            # Graphiti's text path calls create() with a single string.
            text = input_data if isinstance(input_data, str) else str(input_data[0])
            vectors = await asyncio.to_thread(self._encode, [text])
            return vectors[0]

        async def create_batch(self, input_data_list: list[str]) -> list[list[float]]:
            return await asyncio.to_thread(self._encode, list(input_data_list))

    return LocalBGEEmbedder()


def _build_cross_encoder(reranker):
    """Adapt a BGEReranker to Graphiti's CrossEncoderClient interface.

    Passed to the Graphiti constructor so its default OpenAI reranker — which
    would require an OPENAI_API_KEY — is never constructed. search_history
    does its own explicit rerank pass; this adapter only satisfies the
    constructor and serves any Graphiti-internal cross-encoder recipe.
    """
    from graphiti_core.cross_encoder.client import CrossEncoderClient

    class _RerankerAdapter(CrossEncoderClient):
        def __init__(self) -> None:
            self._reranker = reranker

        async def rank(
            self, query: str, passages: list[str]
        ) -> list[tuple[str, float]]:
            ranked = await asyncio.to_thread(
                self._reranker.rerank, query, passages
            )
            return [(passages[idx], score) for idx, score in ranked]

    return _RerankerAdapter()


# --------------------------------------------------------------------------
# Process-lifetime singletons
# --------------------------------------------------------------------------

_reranker = None
_graphiti = None
_graphiti_lock = asyncio.Lock()


def get_reranker():
    """Return the process-wide BGEReranker, loading the model on first call."""
    global _reranker
    if _reranker is None:
        from .reranker import BGEReranker

        _reranker = BGEReranker(model_name=RERANKER_MODEL, device=MODEL_DEVICE)
    return _reranker


async def get_graphiti():
    """Return the process-wide Graphiti handle, initialising it on first call.

    First call builds the LLM client, the local embedder and the cross-encoder,
    connects to Neo4j, and runs build_indices_and_constraints() (idempotent).
    """
    global _graphiti
    if _graphiti is not None:
        return _graphiti

    async with _graphiti_lock:
        if _graphiti is not None:  # re-check after acquiring the lock
            return _graphiti

        from graphiti_core import Graphiti
        from graphiti_core.llm_client.config import LLMConfig
        from graphiti_core.llm_client.openai_generic_client import (
            OpenAIGenericClient,
        )

        _require(NEO4J_PASSWORD, "NEO4J_PASSWORD")
        _require(OPENROUTER_API_KEY, "OPENROUTER_API_KEY")

        # OpenRouter is OpenAI-compatible; Graphiti's generic OpenAI client is
        # built for exactly this (third-party OpenAI-compatible endpoints).
        llm_client = OpenAIGenericClient(
            LLMConfig(
                api_key=OPENROUTER_API_KEY,
                model=GENERATION_MODEL,
                small_model=GENERATION_MODEL,
                base_url=OPENROUTER_BASE_URL,
            )
        )

        graphiti = Graphiti(
            NEO4J_URI,
            NEO4J_USER,
            NEO4J_PASSWORD,
            llm_client=llm_client,
            embedder=_build_embedder(),
            cross_encoder=_build_cross_encoder(get_reranker()),
        )
        await graphiti.build_indices_and_constraints()
        log.info("Graphiti initialised against Neo4j at %s", NEO4J_URI)

        _graphiti = graphiti
        return _graphiti


async def close_graphiti() -> None:
    """Close the Neo4j driver. Safe to call when Graphiti was never built."""
    global _graphiti
    if _graphiti is not None:
        await _graphiti.close()
        _graphiti = None
        log.info("Graphiti connection closed")
