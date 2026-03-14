"""Call the LLM and extract a SQL query from the response."""
from __future__ import annotations

import re

from app.core.llm_client import LLMClient
from config import settings

_FENCE_RE = re.compile(r"```(?:sql)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)


class SQLGenerationError(Exception):
    pass


class SQLGenerator:
    def __init__(
        self,
        model: str | None = None,
        max_tokens: int | None = None,
        provider: str | None = None,
    ) -> None:
        self._max_tokens = max_tokens if max_tokens is not None else settings.max_tokens_sql
        self._client = LLMClient(model=model, provider=provider)

    async def astream_generate(self, messages: list[dict[str, str]]):
        """Yield raw LLM tokens as they arrive. Caller accumulates and extracts SQL."""
        from app.core.prompt_builder import _SYSTEM_PROMPT
        async for token in self._client.astream(
            messages,
            system_prompt=_SYSTEM_PROMPT,
            max_tokens=self._max_tokens,
        ):
            yield token

    def generate(self, messages: list[dict[str, str]], *, retries: int = 1) -> str:
        """Call the LLM and return a clean SQL string."""
        from app.core.prompt_builder import _SYSTEM_PROMPT  # avoid circular at module level

        for attempt in range(retries + 1):
            raw = self._client.complete(
                messages,
                system_prompt=_SYSTEM_PROMPT,
                max_tokens=self._max_tokens,
            )
            sql = self._extract_sql(raw)
            if sql:
                return sql
            if attempt == retries:
                raise SQLGenerationError(
                    f"Could not extract SQL from model response:\n{raw}"
                )

        raise SQLGenerationError("SQL generation failed after retries.")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_sql(text: str) -> str:
        """Pull SQL out of a markdown code fence, or return the raw text."""
        match = _FENCE_RE.search(text)
        if match:
            return match.group(1).strip().rstrip(";").strip()
        # If no fence, treat the whole response as SQL (model sometimes skips fences)
        stripped = text.strip().rstrip(";")
        if stripped.upper().startswith("SELECT"):
            return stripped
        return ""
