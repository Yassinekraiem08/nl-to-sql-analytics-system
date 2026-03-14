"""Application-level singletons.

Import from here to share state across routes without circular imports.
"""
from __future__ import annotations

from pathlib import Path

from app.core.example_store import ExampleStore

# Shared example store — populated as queries succeed, used to improve
# subsequent query generation via few-shot retrieval.
example_store = ExampleStore(Path("data/examples"))
