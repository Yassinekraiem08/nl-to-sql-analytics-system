#!/usr/bin/env python3
"""Evaluate the NL-to-SQL pipeline on the Spider benchmark (or demo DB).

Execution accuracy is the primary metric: run both gold and predicted SQL,
compare result sets order-insensitively.

Usage — Spider:
    python scripts/evaluate_spider.py \\
        --data spider/spider/dev.json \\
        --db-dir spider/spider/database \\
        --n 100 \\
        --output eval_results/spider_baseline.json

Usage — demo DB (no Spider download needed):
    python scripts/evaluate_spider.py --demo --n 20

Results are saved incrementally so evaluation can be interrupted and resumed.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from sqlalchemy import create_engine

# Make sure project root is on the path when invoked directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.schema_loader import SchemaLoader
from app.core.schema_analyzer import SchemaAnalyzer
from app.core.prompt_builder import PromptBuilder
from app.core.sql_generator import SQLGenerator
from app.core.sql_validator import SQLValidator, SQLValidationError
from app.core.sql_executor import SQLExecutor
from app.core.pipeline import SelfCorrectingPipeline, PipelineError
from app.core.example_store import ExampleStore
from app.core.evaluator import ExecutionEvaluator, EvalResult, classify_hardness

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DEMO_BENCHMARK_PATH = Path(__file__).parent / "mini_benchmark.json"


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def run_pipeline(
    question: str,
    engine,
    provider: str | None = None,
    example_store: ExampleStore | None = None,
) -> tuple[str | None, float, int]:
    """Run the full self-correcting pipeline.

    Returns (sql | None, latency_ms, retry_count).
    retry_count = attempts - 1  (0 means succeeded first try)
    """
    schema = SchemaLoader(engine).load()
    analyzer = SchemaAnalyzer(schema, engine)
    graph = analyzer.build_graph()
    counts = analyzer.row_counts()

    builder = PromptBuilder(schema, graph=graph, row_counts=counts, example_store=example_store)
    generator = SQLGenerator(provider=provider)
    validator = SQLValidator()
    executor = SQLExecutor(engine)

    messages = builder.build(question)

    pipeline = SelfCorrectingPipeline(
        generator=generator,
        validator=validator,
        executor=executor,
        max_retries=2,
    )

    try:
        result = pipeline.run(messages)
        return result.sql, result.elapsed_ms, result.attempts - 1
    except (SQLValidationError, PipelineError) as exc:
        logger.warning(f"Pipeline failed: {exc}")
        return None, 0.0, 0


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def load_spider_examples(data_path: str, n: int) -> list[dict]:
    with open(data_path) as f:
        data = json.load(f)
    return data[:n]


def load_demo_examples(n: int) -> list[dict]:
    """Load from the mini benchmark created by create_mini_benchmark.py."""
    if not DEMO_BENCHMARK_PATH.exists():
        logger.error(
            f"Mini benchmark not found at {DEMO_BENCHMARK_PATH}. "
            "Run: python scripts/create_mini_benchmark.py"
        )
        sys.exit(1)
    with open(DEMO_BENCHMARK_PATH) as f:
        data = json.load(f)
    return data[:n]


# ---------------------------------------------------------------------------
# Evaluation loop
# ---------------------------------------------------------------------------

def evaluate(
    examples: list[dict],
    get_engine_fn,
    output_path: str,
    provider: str | None,
    use_rag: bool = False,
) -> None:
    evaluator = ExecutionEvaluator()
    results: list[EvalResult] = []

    # RAG: in-run example store — seeded from examples that succeed
    # (online / transductive mode; note this in results for transparency)
    rag_store = ExampleStore(Path("data/eval_rag_store")) if use_rag else None
    if rag_store:
        rag_store.clear()  # start fresh for each evaluation run
        logger.info("RAG mode: few-shot store enabled (online seeding from correct predictions)")

    logger.info(f"Starting evaluation: {len(examples)} examples")
    logger.info(f"Output: {output_path}")
    logger.info("-" * 60)

    for i, ex in enumerate(examples):
        question = ex["question"]
        gold_sql = ex["query"]
        db_id = ex.get("db_id", "demo")

        engine = get_engine_fn(ex)
        if engine is None:
            logger.warning(f"[{i+1}/{len(examples)}] Skipping {db_id} — DB not found")
            continue

        pred_sql, latency_ms, retry_count = run_pipeline(
            question, engine, provider, example_store=rag_store
        )
        hardness = classify_hardness(gold_sql)

        if pred_sql is None:
            result = EvalResult(
                question=question,
                db_id=db_id,
                gold_sql=gold_sql,
                pred_sql=None,
                execution_match=False,
                error="Pipeline failed",
                latency_ms=latency_ms,
                hardness=hardness,
                retry_count=retry_count,
            )
        else:
            match, error = evaluator.evaluate_pair(gold_sql, pred_sql, engine)
            result = EvalResult(
                question=question,
                db_id=db_id,
                gold_sql=gold_sql,
                pred_sql=pred_sql,
                execution_match=match,
                error=error,
                latency_ms=latency_ms,
                hardness=hardness,
                retry_count=retry_count,
            )

        results.append(result)

        # RAG: seed the store with correct predictions so later questions benefit
        if rag_store and result.execution_match and pred_sql:
            rag_store.add(question, pred_sql, db_id=db_id)

        status = "✓" if result.execution_match else "✗"
        err_tag = f"  [{result.error}]" if result.error else ""
        logger.info(
            f"[{i+1:>3}/{len(examples)}] {status} {db_id:<20} "
            f"{latency_ms:>6.0f}ms  {question[:55]}{err_tag}"
        )

        # Save incrementally — safe to interrupt
        _save(results, output_path, evaluator)

    # Final summary
    metrics = evaluator.accuracy(results)
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"  Execution Accuracy : {metrics['execution_accuracy']:.1%}  ({metrics['matched']}/{metrics['total']})")
    logger.info(f"  Error Rate         : {metrics['error_rate']:.1%}")
    logger.info(f"  Latency p50 / p95  : {metrics['latency_p50_ms']:.0f}ms / {metrics['latency_p95_ms']:.0f}ms")
    if metrics.get("by_hardness"):
        logger.info("  By hardness:")
        for h, acc in sorted(metrics["by_hardness"].items()):
            logger.info(f"    {h:<12} {acc:.1%}")
    logger.info("=" * 60)
    logger.info(f"Results saved to {output_path}")


def _save(results: list[EvalResult], path: str, evaluator: ExecutionEvaluator) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    output = {
        "metrics": evaluator.accuracy(results),
        "results": [vars(r) for r in results],
    }
    with open(path, "w") as f:
        json.dump(output, f, indent=2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="NL-to-SQL benchmark evaluation")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--demo", action="store_true", help="Evaluate on demo DB (no Spider needed)")
    group.add_argument("--data", help="Path to Spider dev.json or train_spider.json")

    parser.add_argument("--db-dir", help="Path to Spider database/ directory (required with --data)")
    parser.add_argument("--n", type=int, default=50, help="Number of examples to evaluate")
    parser.add_argument("--output", default="eval_results/spider_baseline.json")
    parser.add_argument("--provider", default=None, choices=["openai", "anthropic"],
                        help="LLM provider (default: use settings)")
    parser.add_argument("--rag", action="store_true",
                        help="Enable few-shot RAG: seed from correct predictions (online mode)")
    args = parser.parse_args()

    if args.demo:
        examples = load_demo_examples(args.n)
        demo_db = Path(__file__).parents[1] / "dev.db"
        if not demo_db.exists():
            logger.error("dev.db not found. Run: python scripts/seed_demo.py")
            sys.exit(1)
        demo_engine = create_engine(f"sqlite:///{demo_db}")

        def get_engine(_ex):
            return demo_engine

        output = args.output.replace("spider_baseline", "demo_baseline")
    else:
        if not args.db_dir:
            parser.error("--db-dir is required when using --data")
        examples = load_spider_examples(args.data, args.n)
        db_dir = Path(args.db_dir)

        def get_engine(ex):
            db_path = db_dir / ex["db_id"] / f"{ex['db_id']}.sqlite"
            if not db_path.exists():
                return None
            return create_engine(f"sqlite:///{db_path}")

        output = args.output

    if args.rag:
        output = output.replace(".json", "_rag.json")

    evaluate(examples, get_engine, output, args.provider, use_rag=args.rag)


if __name__ == "__main__":
    main()
