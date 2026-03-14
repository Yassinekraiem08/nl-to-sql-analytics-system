# Benchmark & Evaluation

Evaluation uses **execution accuracy** as the primary metric: both the gold SQL
and the predicted SQL are executed on the same database, and their result sets
are compared order-insensitively. This is more meaningful than exact string match
because many semantically equivalent SQL queries differ syntactically.

---

## Quick start — demo DB (no download needed)

Run a baseline on the hand-authored 21-question benchmark covering all
difficulty levels (easy / medium / hard / extra_hard):

```bash
# 1. Seed the demo database (once)
python scripts/seed_demo.py

# 2. Create the mini benchmark (once)
python scripts/create_mini_benchmark.py

# 3. Run evaluation
python scripts/evaluate_spider.py --demo --output eval_results/demo_baseline.json
```

Results are saved incrementally to `eval_results/demo_baseline.json`.

---

## Spider benchmark

Spider is the standard cross-domain text-to-SQL benchmark
(7,000 train / 1,034 dev questions across 200 databases).

### Download

```bash
pip install gdown
python - <<'EOF'
import gdown, zipfile, pathlib
out = pathlib.Path("spider")
out.mkdir(exist_ok=True)
gdown.download(
    "https://drive.google.com/uc?id=1TqleXec_OykOYFREKKtschzY29dUcVAQ",
    str(out / "spider_data.zip"), quiet=False
)
with zipfile.ZipFile(out / "spider_data.zip") as z:
    z.extractall(out)
(out / "spider_data.zip").unlink()
print("Done:", list(out.iterdir()))
EOF
```

### Run evaluation

```bash
# 50-example sample (cheap, ~$0.10 with GPT-4o-mini)
python scripts/evaluate_spider.py \
    --data spider/spider/dev.json \
    --db-dir spider/spider/database \
    --n 50 \
    --output eval_results/spider_baseline.json

# Full dev set (1034 examples)
python scripts/evaluate_spider.py \
    --data spider/spider/dev.json \
    --db-dir spider/spider/database \
    --n 1034 \
    --output eval_results/spider_full.json
```

### Provider selection

```bash
# OpenAI (default)
python scripts/evaluate_spider.py --demo --provider openai

# Anthropic
python scripts/evaluate_spider.py --demo --provider anthropic
```

---

## Results

Update this table after each evaluation run.

### Demo DB — mini benchmark (21 questions)

| Model / Variant | Easy | Medium | Hard | Extra Hard | **Overall EX** | p50 ms |
|---|---|---|---|---|---|---|
| Baseline (fill in) | — | — | — | — | — | — |
| + Self-correcting loop (Phase 2) | — | — | — | — | — | — |
| + Few-shot RAG (Phase 4) | — | — | — | — | — | — |

### Spider dev set

| Model / Variant | EX Acc | p50 ms | Notes |
|---|---|---|---|
| Baseline (fill in) | — | — | n=50 |

---

## Metric definitions

| Metric | Definition |
|---|---|
| **EX (execution accuracy)** | % of questions where predicted result set == gold result set (order-insensitive) |
| **Error rate** | % of questions where the pipeline failed entirely (generation or validation error) |
| **Retry rate** | % of questions that required at least one self-correction loop (Phase 2+) |
| **p50 / p95 latency** | Pipeline latency in ms (nearest-rank percentile) |

Hardness classification approximates Spider's official categorisation based on
SQL structural complexity (JOINs, GROUP BY, subqueries, set operations).
