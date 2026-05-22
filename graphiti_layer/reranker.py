"""Local cross-encoder reranking for Graphiti hybrid-search results.

Graphiti's hybrid search (semantic + BM25 + graph) returns a candidate set
ranked by reciprocal-rank fusion. RRF is order-aware but query-agnostic — it
never reads the candidate text against the query. A cross-encoder does: it
scores the full (query, candidate) pair jointly, which is what turns "three
roughly similar prior patterns" into "the three that actually match".

``BAAI/bge-reranker-v2-m3`` runs locally on CPU — no API call, no per-query
cost. Scoring 30 candidates is well under 500 ms, so search_history reranks
every candidate, not just a top-K slice.
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)

DEFAULT_MODEL = "BAAI/bge-reranker-v2-m3"


class BGEReranker:
    """Wraps a sentence-transformers CrossEncoder for (query, passage) scoring.

    The model is loaded once at construction; instances are meant to be held
    for the lifetime of the process (see graphiti_layer.client.get_reranker).
    """

    def __init__(self, model_name: str = DEFAULT_MODEL, device: str = "cpu") -> None:
        # Imported lazily so that importing this module does not pull in torch
        # for callers that only need the ontology or the formatter.
        from sentence_transformers import CrossEncoder

        log.info("Loading cross-encoder %s on %s", model_name, device)
        self.model_name = model_name
        self.device = device
        self._model = CrossEncoder(model_name, device=device)

    def score(self, query: str, passages: list[str]) -> list[float]:
        """Return one relevance score per passage, aligned to input order.

        Higher is more relevant. Scores are raw cross-encoder logits — only
        their relative ordering is meaningful, not their absolute value.
        """
        if not passages:
            return []
        pairs = [(query, passage) for passage in passages]
        raw = self._model.predict(pairs)
        return [float(s) for s in raw]

    def rerank(
        self, query: str, passages: list[str], top_k: int | None = None
    ) -> list[tuple[int, float]]:
        """Score and sort passages by relevance to the query.

        Returns ``(original_index, score)`` tuples in descending score order.
        The original index lets the caller carry candidate metadata across the
        reorder and lets tests see how far the reranker moved each candidate
        from its order of arrival.
        """
        scores = self.score(query, passages)
        ranked = sorted(enumerate(scores), key=lambda pair: pair[1], reverse=True)
        return ranked[:top_k] if top_k is not None else ranked
