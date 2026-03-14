"""Thin adapter that hides Anthropic vs OpenAI client differences.

Provides both sync (complete) and async-streaming (astream) interfaces.
"""
from __future__ import annotations

from typing import AsyncIterator

from config import settings


class LLMClient:
    def __init__(self, model: str | None = None, provider: str | None = None) -> None:
        self._model = model or settings.llm_model
        self._provider = provider or settings.llm_provider

    # ------------------------------------------------------------------
    # Sync — used by the standard pipeline
    # ------------------------------------------------------------------

    def complete(
        self,
        messages: list[dict[str, str]],
        *,
        system_prompt: str | None = None,
        max_tokens: int = 1024,
    ) -> str:
        """Call the active provider and return the full response text."""
        if self._provider == "anthropic":
            return self._complete_anthropic(messages, system_prompt=system_prompt, max_tokens=max_tokens)
        return self._complete_openai(messages, system_prompt=system_prompt, max_tokens=max_tokens)

    # ------------------------------------------------------------------
    # Async streaming — used by the SSE endpoint
    # ------------------------------------------------------------------

    async def astream(
        self,
        messages: list[dict[str, str]],
        *,
        system_prompt: str | None = None,
        max_tokens: int = 1024,
    ) -> AsyncIterator[str]:
        """Yield text tokens as they arrive from the LLM."""
        if self._provider == "anthropic":
            async for token in self._astream_anthropic(messages, system_prompt=system_prompt, max_tokens=max_tokens):
                yield token
        else:
            async for token in self._astream_openai(messages, system_prompt=system_prompt, max_tokens=max_tokens):
                yield token

    # ------------------------------------------------------------------
    # Sync provider implementations
    # ------------------------------------------------------------------

    def _complete_anthropic(
        self,
        messages: list[dict[str, str]],
        *,
        system_prompt: str | None,
        max_tokens: int,
    ) -> str:
        import anthropic  # lazy import — only needed when provider=anthropic

        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        kwargs: dict = dict(model=self._model, max_tokens=max_tokens, messages=messages)
        if system_prompt:
            kwargs["system"] = system_prompt
        response = client.messages.create(**kwargs)
        return response.content[0].text.strip()

    def _complete_openai(
        self,
        messages: list[dict[str, str]],
        *,
        system_prompt: str | None,
        max_tokens: int,
    ) -> str:
        import openai  # lazy import — only needed when provider=openai

        client = openai.OpenAI(api_key=settings.openai_api_key)
        full_messages = messages
        if system_prompt:
            full_messages = [{"role": "system", "content": system_prompt}] + messages
        response = client.chat.completions.create(
            model=self._model,
            max_tokens=max_tokens,
            messages=full_messages,
        )
        return response.choices[0].message.content.strip()

    # ------------------------------------------------------------------
    # Async streaming provider implementations
    # ------------------------------------------------------------------

    async def _astream_anthropic(
        self,
        messages: list[dict[str, str]],
        *,
        system_prompt: str | None,
        max_tokens: int,
    ) -> AsyncIterator[str]:
        import anthropic

        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        kwargs: dict = dict(model=self._model, max_tokens=max_tokens, messages=messages)
        if system_prompt:
            kwargs["system"] = system_prompt
        async with client.messages.stream(**kwargs) as stream:
            async for text in stream.text_stream:
                yield text

    async def _astream_openai(
        self,
        messages: list[dict[str, str]],
        *,
        system_prompt: str | None,
        max_tokens: int,
    ) -> AsyncIterator[str]:
        import openai

        client = openai.AsyncOpenAI(api_key=settings.openai_api_key)
        full_messages = messages
        if system_prompt:
            full_messages = [{"role": "system", "content": system_prompt}] + messages
        stream = await client.chat.completions.create(
            model=self._model,
            max_tokens=max_tokens,
            messages=full_messages,
            stream=True,
        )
        async for chunk in stream:
            token = chunk.choices[0].delta.content
            if token:
                yield token
