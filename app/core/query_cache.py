"""Semantic query cache.

Before hitting the LLM, embed the incoming question and check whether a
semantically-similar question was answered recently.  If cosine similarity
exceeds `threshold` (default 0.92), return the cached result — zero LLM
tokens consumed and ~instant response time.

The cache is persisted to disk (JSON + npy) so it survives server restarts.
"""
from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Optional

import numpy as np

from app.core.example_store import TFIDFEmbedder, Embedder

logger = logging.getLogger(__name__)

DEFAULT_THRESHOLD = 0.92


@dataclass
class CachedResult:
    question: str
    db_id: str
    payload: dict[str, Any]   # serialised QueryResponse dict


class QueryCache:
    """Persist and retrieve full query results by semantic question similarity.

    Usage::

        cache = QueryCache(Path("data/query_cache"))

        # Before running the pipeline
        hit = cache.lookup("How many users?", db_id="demo")
        if hit:
            return QueryResponse(**{**hit.payload, "cache_hit": True})

        # After a successful pipeline run
        cache.store("How many users?", result_payload, db_id="demo")
    """

    def __init__(
        self,
        cache_path: Path,
        embedder: Optional[Embedder] = None,
        threshold: float = DEFAULT_THRESHOLD,
    ) -> None:
        self._path = Path(cache_path)
        self._embedder: Embedder = embedder or TFIDFEmbedder()
        self._threshold = threshold
        self._entries: list[CachedResult] = []
        self._vectors: list[list[float]] = []
        self._lock = threading.Lock()
        self._load()

    @property
    def size(self) -> int:
        return len(self._entries)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def lookup(self, question: str, db_id: str = "default") -> Optional[CachedResult]:
        """Return a cached result if a similar question exists, else None."""
        with self._lock:
            entries = list(self._entries)
            vectors = list(self._vectors)

        if not entries:
            return None

        pairs = [(e, v) for e, v in zip(entries, vectors) if e.db_id == db_id]
        if not pairs:
            return None

        filtered_entries, filtered_vectors = zip(*pairs)
        query_vec = np.array(self._embedder.embed(question), dtype=np.float32)
        store_mat = np.array(filtered_vectors, dtype=np.float32)
        sims = store_mat @ query_vec

        best_idx = int(np.argmax(sims))
        if sims[best_idx] >= self._threshold:
            logger.info(
                "QueryCache HIT (sim=%.3f): '%s' → '%s'",
                sims[best_idx], question, filtered_entries[best_idx].question,
            )
            return filtered_entries[best_idx]

        return None

    def store(
        self,
        question: str,
        payload: dict[str, Any],
        db_id: str = "default",
    ) -> None:
        """Embed and persist a successful result."""
        vector = self._embedder.embed(question)
        with self._lock:
            self._entries.append(
                CachedResult(question=question, db_id=db_id, payload=payload)
            )
            self._vectors.append(vector)
            self._save_locked()
        logger.debug("QueryCache store: db=%s total=%d", db_id, self.size)

    def clear(self) -> None:
        """Remove all cached entries."""
        with self._lock:
            self._entries.clear()
            self._vectors.clear()
            self._save_locked()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self) -> None:
        meta_path = self._path.with_suffix(".json")
        vec_path = self._path.with_suffix(".npy")

        if not meta_path.exists():
            return

        try:
            with open(meta_path) as f:
                data = json.load(f)
            self._entries = [CachedResult(**d) for d in data]

            if vec_path.exists():
                self._vectors = np.load(vec_path, allow_pickle=False).tolist()
            else:
                logger.warning(
                    "QueryCache: vectors missing, re-embedding %d entries",
                    len(self._entries),
                )
                self._vectors = [
                    self._embedder.embed(e.question) for e in self._entries
                ]
                self._save_locked()
        except Exception as exc:
            logger.warning("QueryCache: load failed, starting fresh — %s", exc)
            self._entries = []
            self._vectors = []

    def _save_locked(self) -> None:
        """Must be called while holding self._lock."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        meta_path = self._path.with_suffix(".json")
        vec_path = self._path.with_suffix(".npy")

        with open(meta_path, "w") as f:
            json.dump([asdict(e) for e in self._entries], f, indent=2)

        if self._vectors:
            np.save(vec_path, np.array(self._vectors, dtype=np.float32))
