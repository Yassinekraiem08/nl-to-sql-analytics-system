"""Multi-turn conversation session management.

Each session maintains a history of (question, sql, result_columns) turns.
When the user asks a follow-up question, the previous SQL and column names are
injected into the prompt so the LLM can resolve references like:
  "now filter by New York"
  "show only the top 5"
  "break that down by category"

Sessions are stored in-process. For production, replace the in-memory store
with Redis or a database-backed session store.
"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class ConversationTurn:
    question: str
    sql: str
    result_columns: list[str]    # column names from the result DataFrame
    row_count: int
    summary: str


@dataclass
class ConversationSession:
    session_id: str
    turns: list[ConversationTurn] = field(default_factory=list)
    database_url: Optional[str] = None

    @property
    def last_turn(self) -> Optional[ConversationTurn]:
        return self.turns[-1] if self.turns else None

    def add_turn(self, turn: ConversationTurn) -> None:
        self.turns.append(turn)

    def context_block(self) -> str:
        """Render the last turn as a context block for the next prompt.

        Gives the LLM the previous SQL and column names so it can resolve
        pronouns and filters ("now filter by X", "show only the top 5", etc.).
        """
        t = self.last_turn
        if t is None:
            return ""

        cols = ", ".join(t.result_columns) if t.result_columns else "—"
        return (
            f"Previous question: {t.question}\n"
            f"Previous SQL:\n```sql\n{t.sql}\n```\n"
            f"Previous result columns: {cols}\n"
            f"Previous row count: {t.row_count}\n"
            "\nThe user is asking a follow-up. Reuse or modify the previous SQL as needed."
        )


# ---------------------------------------------------------------------------
# Session store
# ---------------------------------------------------------------------------

class SessionStore:
    """Thread-safe in-memory session store."""

    def __init__(self) -> None:
        self._sessions: dict[str, ConversationSession] = {}
        self._lock = threading.Lock()

    def create(self, database_url: Optional[str] = None) -> ConversationSession:
        session = ConversationSession(
            session_id=str(uuid.uuid4()),
            database_url=database_url,
        )
        with self._lock:
            self._sessions[session.session_id] = session
        return session

    def get(self, session_id: str) -> Optional[ConversationSession]:
        with self._lock:
            return self._sessions.get(session_id)

    def delete(self, session_id: str) -> bool:
        with self._lock:
            return self._sessions.pop(session_id, None) is not None

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._sessions)


# Global singleton
session_store = SessionStore()
