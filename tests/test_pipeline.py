"""Tests for the self-correcting pipeline."""
from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from app.core.pipeline import (
    SelfCorrectingPipeline,
    PipelineResult,
    PipelineError,
    CorrectionStep,
)
from app.core.sql_generator import SQLGenerationError
from app.core.sql_validator import SQLValidationError
from app.core.sql_executor import SQLExecutionError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pipeline(
    sql_sequence: list,       # list of str | SQLGenerationError — returned per generate() call
    exec_sequence: list,      # list of pd.DataFrame | SQLExecutionError — returned per execute() call
    max_retries: int = 2,
) -> SelfCorrectingPipeline:
    """Build a pipeline with mocked generator, validator, and executor."""
    generator = MagicMock()
    generator.generate.side_effect = sql_sequence

    validator = MagicMock()  # always passes by default

    executor = MagicMock()
    executor.execute.side_effect = exec_sequence

    return SelfCorrectingPipeline(
        generator=generator,
        validator=validator,
        executor=executor,
        max_retries=max_retries,
    )


GOOD_DF = pd.DataFrame({"count": [42]})
MESSAGES = [{"role": "user", "content": "How many users?"}]


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_succeeds_first_attempt():
    pipeline = _make_pipeline(["SELECT COUNT(*) FROM users"], [GOOD_DF])
    result = pipeline.run(MESSAGES)

    assert result.sql == "SELECT COUNT(*) FROM users"
    assert result.attempts == 1
    assert result.correction_history == []
    assert result.df.equals(GOOD_DF)


def test_result_has_elapsed_ms():
    pipeline = _make_pipeline(["SELECT 1"], [GOOD_DF])
    result = pipeline.run(MESSAGES)
    assert result.elapsed_ms >= 0


# ---------------------------------------------------------------------------
# Self-correction
# ---------------------------------------------------------------------------

def test_corrects_on_first_execution_error():
    pipeline = _make_pipeline(
        sql_sequence=["SELECT * FROM wrong", "SELECT COUNT(*) FROM users"],
        exec_sequence=[SQLExecutionError("no such table: wrong"), GOOD_DF],
    )
    result = pipeline.run(MESSAGES)

    assert result.attempts == 2
    assert result.sql == "SELECT COUNT(*) FROM users"
    assert len(result.correction_history) == 1
    assert result.correction_history[0].attempt == 1
    assert result.correction_history[0].sql == "SELECT * FROM wrong"
    assert "no such table" in result.correction_history[0].error


def test_corrects_twice_then_succeeds():
    pipeline = _make_pipeline(
        sql_sequence=["BAD1", "BAD2", "SELECT 1"],
        exec_sequence=[
            SQLExecutionError("error 1"),
            SQLExecutionError("error 2"),
            GOOD_DF,
        ],
        max_retries=2,
    )
    result = pipeline.run(MESSAGES)
    assert result.attempts == 3
    assert len(result.correction_history) == 2


def test_correction_prompt_appended_to_messages():
    """Each failed attempt must append the error context to the message list."""
    pipeline = _make_pipeline(
        sql_sequence=["BAD", "SELECT 1"],
        exec_sequence=[SQLExecutionError("boom"), GOOD_DF],
    )
    pipeline.run(MESSAGES)

    calls = pipeline._generator.generate.call_args_list
    # First call: original messages only
    assert calls[0] == call(MESSAGES)
    # Second call: messages extended with assistant reply + correction request
    extended = calls[1][0][0]
    assert len(extended) == 3
    assert extended[1]["role"] == "assistant"
    assert "BAD" in extended[1]["content"]
    assert extended[2]["role"] == "user"
    assert "boom" in extended[2]["content"]


# ---------------------------------------------------------------------------
# Exhaustion
# ---------------------------------------------------------------------------

def test_raises_pipeline_error_when_all_attempts_exhausted():
    pipeline = _make_pipeline(
        sql_sequence=["BAD"] * 3,
        exec_sequence=[SQLExecutionError("fail")] * 3,
        max_retries=2,
    )
    with pytest.raises(PipelineError) as exc_info:
        pipeline.run(MESSAGES)

    assert "3 attempt" in str(exc_info.value)


def test_pipeline_error_carries_last_sql():
    pipeline = _make_pipeline(
        sql_sequence=["SELECT bad"],
        exec_sequence=[SQLExecutionError("error")],
        max_retries=0,
    )
    with pytest.raises(PipelineError) as exc_info:
        pipeline.run(MESSAGES)

    assert exc_info.value.last_sql == "SELECT bad"


# ---------------------------------------------------------------------------
# Safety validation — never retry
# ---------------------------------------------------------------------------

def test_unsafe_sql_raises_immediately_no_retry():
    generator = MagicMock()
    generator.generate.return_value = "DROP TABLE users"

    validator = MagicMock()
    validator.validate.side_effect = SQLValidationError("unsafe")

    executor = MagicMock()

    pipeline = SelfCorrectingPipeline(generator, validator, executor, max_retries=2)

    with pytest.raises(SQLValidationError):
        pipeline.run(MESSAGES)

    # Must not attempt execution at all
    executor.execute.assert_not_called()
    # Must not retry — generate called only once
    assert generator.generate.call_count == 1


# ---------------------------------------------------------------------------
# Generation error
# ---------------------------------------------------------------------------

def test_generation_error_raises_pipeline_error():
    generator = MagicMock()
    generator.generate.side_effect = SQLGenerationError("LLM down")

    pipeline = SelfCorrectingPipeline(
        generator, MagicMock(), MagicMock(), max_retries=2
    )

    with pytest.raises(PipelineError, match="generation failed"):
        pipeline.run(MESSAGES)


# ---------------------------------------------------------------------------
# max_retries boundary
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("max_retries", [0, 1, 3])
def test_max_retries_respected(max_retries: int):
    """Total attempts must equal max_retries + 1 when all fail."""
    pipeline = _make_pipeline(
        sql_sequence=["SELECT 1"] * (max_retries + 1),
        exec_sequence=[SQLExecutionError("fail")] * (max_retries + 1),
        max_retries=max_retries,
    )
    with pytest.raises(PipelineError):
        pipeline.run(MESSAGES)

    assert pipeline._generator.generate.call_count == max_retries + 1
