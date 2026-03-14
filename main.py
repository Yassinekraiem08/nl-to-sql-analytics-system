"""CLI entrypoint for quick end-to-end testing."""
import sys
import json
import time

from app.db.connection import get_engine
from app.core.schema_loader import SchemaLoader
from app.core.prompt_builder import PromptBuilder
from app.core.sql_generator import SQLGenerator
from app.core.sql_validator import SQLValidator
from app.core.sql_executor import SQLExecutor
from app.core.result_formatter import ResultFormatter


def run(question: str) -> None:
    engine = get_engine()

    loader = SchemaLoader(engine)
    schema = loader.load()

    builder = PromptBuilder(schema)
    generator = SQLGenerator()
    validator = SQLValidator()
    executor = SQLExecutor(engine)
    formatter = ResultFormatter()

    prompt = builder.build(question)
    sql = generator.generate(prompt)
    validator.validate(sql)

    t0 = time.perf_counter()
    df = executor.execute(sql)
    elapsed_ms = (time.perf_counter() - t0) * 1000

    result = formatter.format(df, question, sql, elapsed_ms)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Show me the first 5 rows of each table."
    run(q)
