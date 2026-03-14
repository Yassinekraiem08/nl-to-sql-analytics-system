"""Few-shot example retrieval via embedding-based cosine similarity.

Implements the core idea from DAIL-SQL (Gao et al., 2023): select the
most question-similar examples from a store of past successful queries
and inject them as few-shot demonstrations into the prompt.

Two embedders are provided:
  - TFIDFEmbedder  — character bigram TF-IDF, deterministic, no API needed.
                     Good enough for similar questions (same words → same
                     bigrams → high cosine similarity). Used in tests and
                     as a fallback when no API key is configured.
  - OpenAIEmbedder — text-embedding-3-small. Higher quality at the cost of
                     an API call per question.
"""
from __future__ import annotations

import json
import logging
import math
import threading
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional, Protocol

import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Embedder protocol + implementations
# ---------------------------------------------------------------------------

class Embedder(Protocol):
    def embed(self, text: str) -> list[float]: ...


class TFIDFEmbedder:
    """Character bigram TF-IDF embedder — deterministic, zero-cost.

    Hashes each bigram into a fixed-size vector and L2-normalises the result.
    Works well for retrieval when questions share vocabulary (which is typical
    for queries on the same schema).
    """

    DIM = 512

    def embed(self, text: str) -> list[float]:
        text = text.lower()
        bigrams = [text[i: i + 2] for i in range(len(text) - 1)]

        counts: dict[str, float] = {}
        for bg in bigrams:
            counts[bg] = counts.get(bg, 0.0) + 1.0

        vector = [0.0] * self.DIM
        for bg, count in counts.items():
            vector[abs(hash(bg)) % self.DIM] += count

        norm = math.sqrt(sum(x * x for x in vector))
        if norm > 0:
            vector = [x / norm for x in vector]
        return vector


class OpenAIEmbedder:
    """OpenAI text-embedding-3-small embedder.

    Produces 1536-dimensional embeddings. Requires OPENAI_API_KEY.
    Costs ~$0.02 / 1M tokens — negligible for a few hundred examples.
    """

    MODEL = "text-embedding-3-small"

    def __init__(self) -> None:
        from config import settings
        self._api_key = settings.openai_api_key

    def embed(self, text: str) -> list[float]:
        import openai
        client = openai.OpenAI(api_key=self._api_key)
        response = client.embeddings.create(model=self.MODEL, input=text)
        return response.data[0].embedding


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class FewShotExample:
    question: str
    sql: str
    db_id: str


# ---------------------------------------------------------------------------
# Example store
# ---------------------------------------------------------------------------

class ExampleStore:
    """Persistent few-shot example retrieval via cosine similarity.

    Stores (question, sql, db_id) triples alongside their embedding vectors.
    Vectors are persisted to a .npy file; metadata to a .json file.

    Thread-safe: a lock guards in-memory state and disk writes.

    Usage::

        store = ExampleStore(Path("data/examples"))

        # After a successful query
        store.add("Top 5 products by revenue", "SELECT ...", db_id="demo")

        # Before building the next prompt
        examples = store.retrieve("Best selling products", k=3, db_id="demo")
    """

    def __init__(
        self,
        store_path: Path,
        embedder: Optional[Embedder] = None,
    ) -> None:
        self._path = Path(store_path)
        self._embedder: Embedder = embedder or TFIDFEmbedder()
        self._examples: list[FewShotExample] = []
        self._vectors: list[list[float]] = []
        self._lock = threading.Lock()
        self._load()

    @property
    def size(self) -> int:
        return len(self._examples)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def add(self, question: str, sql: str, db_id: str = "default") -> None:
        """Embed and persist a successful (question, sql) pair."""
        vector = self._embedder.embed(question)
        with self._lock:
            self._examples.append(FewShotExample(question=question, sql=sql, db_id=db_id))
            self._vectors.append(vector)
            self._save_locked()
        logger.debug("ExampleStore add: db=%s total=%d", db_id, self.size)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def retrieve(
        self,
        question: str,
        k: int = 3,
        db_id: Optional[str] = None,
    ) -> list[FewShotExample]:
        """Return up to k most similar examples ordered by cosine similarity.

        Args:
            question: New natural-language question.
            k:        Maximum number of examples to return.
            db_id:    If set, only return examples from this database.
        """
        with self._lock:
            examples = list(self._examples)
            vectors = list(self._vectors)

        if not examples:
            return []

        # Optionally filter by db_id
        if db_id is not None:
            pairs = [(e, v) for e, v in zip(examples, vectors) if e.db_id == db_id]
            if not pairs:
                return []
            examples, vectors = map(list, zip(*pairs))

        query_vec = np.array(self._embedder.embed(question), dtype=np.float32)
        store_mat = np.array(vectors, dtype=np.float32)

        # Cosine similarity — vectors are L2-normalised so this is just a dot product
        sims = store_mat @ query_vec

        top_k = min(k, len(examples))
        indices = np.argsort(sims)[::-1][:top_k]
        return [examples[i] for i in indices]

    def clear(self) -> None:
        """Remove all stored examples (useful between eval runs)."""
        with self._lock:
            self._examples.clear()
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
            self._examples = [FewShotExample(**d) for d in data]

            if vec_path.exists():
                self._vectors = np.load(vec_path, allow_pickle=False).tolist()
            else:
                logger.warning("ExampleStore: vectors missing, re-embedding %d examples", len(self._examples))
                self._vectors = [self._embedder.embed(e.question) for e in self._examples]
                self._save_locked()
        except Exception as exc:
            logger.warning("ExampleStore: load failed, starting fresh — %s", exc)
            self._examples = []
            self._vectors = []

    def _save_locked(self) -> None:
        """Must be called while holding self._lock."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        meta_path = self._path.with_suffix(".json")
        vec_path = self._path.with_suffix(".npy")

        with open(meta_path, "w") as f:
            json.dump([asdict(e) for e in self._examples], f, indent=2)

        if self._vectors:
            np.save(vec_path, np.array(self._vectors, dtype=np.float32))
