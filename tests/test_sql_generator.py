"""Unit tests for SQLGenerator — mock LLMClient.complete."""
from unittest.mock import patch

import os
os.environ.setdefault("LLM_PROVIDER", "openai")
os.environ.setdefault("OPENAI_API_KEY", "sk-test")

import pytest

from app.core.sql_generator import SQLGenerator, SQLGenerationError


MESSAGES = [{"role": "user", "content": "Write SQL for: count all users"}]


@patch("app.core.llm_client.LLMClient.complete")
def test_extract_sql_from_code_fence(mock_complete):
    mock_complete.return_value = "```sql\nSELECT COUNT(*) FROM users\n```"
    gen = SQLGenerator()
    sql = gen.generate(MESSAGES)
    assert sql == "SELECT COUNT(*) FROM users"


@patch("app.core.llm_client.LLMClient.complete")
def test_extract_sql_no_fence(mock_complete):
    mock_complete.return_value = "SELECT COUNT(*) FROM users"
    gen = SQLGenerator()
    sql = gen.generate(MESSAGES)
    assert sql == "SELECT COUNT(*) FROM users"


@patch("app.core.llm_client.LLMClient.complete")
def test_generation_error_on_bad_response(mock_complete):
    mock_complete.return_value = "I cannot answer that."
    gen = SQLGenerator()
    with pytest.raises(SQLGenerationError):
        gen.generate(MESSAGES, retries=0)


@patch("app.core.llm_client.LLMClient.complete")
def test_strips_trailing_semicolon(mock_complete):
    mock_complete.return_value = "```sql\nSELECT * FROM users;\n```"
    gen = SQLGenerator()
    sql = gen.generate(MESSAGES)
    assert not sql.endswith(";")
