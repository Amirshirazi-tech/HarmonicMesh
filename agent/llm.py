"""Thin OpenRouter client for the Phase 5 reasoning node.

Lives outside ``agent/nodes/`` so the LLM call is exercisable from tests
without importing the full graph. The directive pins LLM access to OpenRouter
(``anthropic/claude-haiku-4-5`` by default), never the Anthropic SDK directly.

Response format is JSON-mode enforced — see :func:`generate_json`.
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

import httpx

from .errors import LLMReasoningError

log = logging.getLogger(__name__)

OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
GENERATION_MODEL = os.getenv("GENERATION_MODEL", "anthropic/claude-haiku-4-5")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("LLM_REQUEST_TIMEOUT_SECONDS", "60"))


def _api_key() -> str:
    key = os.getenv("OPENROUTER_API_KEY", "")
    if not key:
        raise LLMReasoningError(
            "OPENROUTER_API_KEY is not set; cannot call OpenRouter."
        )
    return key


async def generate_json(
    system_prompt: str,
    user_prompt: str,
    *,
    model: str | None = None,
    temperature: float = 0.2,
) -> dict[str, Any]:
    """Call the LLM in JSON-mode and parse the response into a dict.

    Raises:
        LLMReasoningError: on HTTP errors, non-2xx responses, missing content,
            or JSON parse failures. The consumer treats this as transient and
            retries after backoff.
    """
    payload = {
        "model": model or GENERATION_MODEL,
        "temperature": temperature,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    headers = {
        "Authorization": f"Bearer {_api_key()}",
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
            response = await client.post(
                f"{OPENROUTER_BASE_URL}/chat/completions",
                json=payload,
                headers=headers,
            )
    except httpx.HTTPError as exc:
        raise LLMReasoningError(f"OpenRouter request failed: {exc}") from exc

    if response.status_code >= 400:
        raise LLMReasoningError(
            f"OpenRouter returned {response.status_code}: {response.text[:500]}"
        )

    try:
        body = response.json()
        content = body["choices"][0]["message"]["content"]
    except (KeyError, IndexError, ValueError) as exc:
        raise LLMReasoningError(
            f"OpenRouter response missing content: {exc}; body={response.text[:500]}"
        ) from exc

    # Some OpenRouter providers (notably Anthropic models) ignore the
    # response_format hint and wrap the JSON in a markdown ```json``` fence.
    # Strip fences defensively before parsing.
    cleaned = _strip_code_fence(content)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise LLMReasoningError(
            f"LLM response was not valid JSON: {exc}; content={content[:500]}"
        ) from exc


_FENCE_RE = re.compile(
    r"^\s*```(?:json|JSON)?\s*\n?(.*?)\n?```\s*$",
    re.DOTALL,
)


def _strip_code_fence(text: str) -> str:
    """Remove a surrounding ```json ... ``` markdown fence if present."""
    match = _FENCE_RE.match(text)
    if match:
        return match.group(1)
    return text
