# QueryMind — Natural Language Analytics System

> Ask questions in plain English. Get SQL, charts, narratives, and insights — instantly.

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111-green?logo=fastapi)
![React](https://img.shields.io/badge/React-18-61DAFB?logo=react)
![Tests](https://img.shields.io/badge/tests-212%20passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue)

**[Live Demo →](https://querymind-demo.vercel.app)** · **[API Docs →](https://web-production-2d03f.up.railway.app/docs)**

---

## What it does

QueryMind translates natural language questions into SQL queries, executes them against a relational database, and returns structured results — tables, charts, plain-English narratives, and a full pipeline trace — all streamed live to the browser.

```
"Top 10 customers by total spend"
        ↓  schema-aware prompt + few-shot examples
SELECT u.name, SUM(oi.quantity * oi.unit_price) AS total_spend
FROM users u
JOIN orders o ON u.id = o.user_id
JOIN order_items oi ON o.id = oi.order_id
GROUP BY u.id ORDER BY total_spend DESC LIMIT 10
        ↓  execute → format → stream
📊 Bar chart  •  📋 Results table  •  📝 Narrative  •  🔍 Pipeline trace
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     React Frontend                          │
│  QueryTerminal · SchemaGraph · SSE streaming · Export       │
└──────────────────────┬──────────────────────────────────────┘
                       │ HTTP / SSE
┌──────────────────────▼──────────────────────────────────────┐
│                   FastAPI Backend                           │
│                                                             │
│  /ask/stream ──► QueryCache ──► (cache hit: instant return) │
│                      │                                      │
│                      ▼                                      │
│             AmbiguityDetector                               │
│                      │                                      │
│                      ▼                                      │
│  SchemaLoader → SchemaAnalyzer → RelationshipGraph          │
│                      │                                      │
│                      ▼                                      │
│  PromptBuilder (few-shot RAG + schema + context)            │
│                      │                                      │
│                      ▼                                      │
│  ┌─────────────────────────────────────────┐               │
│  │   SelfCorrectingPipeline (DIN-SQL)      │               │
│  │  generate → validate → execute          │               │
│  │  on error: append correction prompt     │               │
│  │  retry up to 3 attempts                 │               │
│  └─────────────────────────────────────────┘               │
│                      │                                      │
│                      ▼                                      │
│  SchemaValidator · PerformanceHints · ConfidenceScorer      │
│                      │                                      │
│                      ▼                                      │
│  ResultFormatter (narrative + chart config)                 │
│                      │                                      │
│                      ▼                                      │
│  ExampleStore (few-shot RAG) · SessionStore (multi-turn)    │
└─────────────────────────────────────────────────────────────┘
         OpenAI (gpt-4o-mini) · Anthropic (claude-opus-4-6)
```

---

## Key Features

### Research-grade NL-to-SQL
| Technique | Paper | Implementation |
|---|---|---|
| Execution-guided refinement | DIN-SQL (Pourreza & Rafiei, 2023) | `SelfCorrectingPipeline` — appends DB error to prompt, retries up to 3× |
| Few-shot RAG retrieval | DAIL-SQL (Gao et al., 2023) | `ExampleStore` — TF-IDF cosine similarity over past successful queries |
| Schema-aware generation | CHESS (Wang et al., 2024) | `SchemaAnalyzer` — heuristic FK detection, join hints, row counts in prompt |
| Multi-turn context | — | `ConversationSession` — injects last SQL + columns into follow-up prompts |

### Engineering excellence
- **Semantic query cache** — embeds every question; returns cached result (0 LLM calls) when cosine similarity ≥ 0.92
- **Ambiguity detection** — flags vague terms (`recent`, `top`, `large`…) and surfaces clarification suggestions
- **Performance hints** — analyzes WHERE/JOIN/ORDER BY columns and recommends missing indexes (skips PKs/FKs)
- **SSE streaming** — live SQL token rendering in the browser via `fetch` + `ReadableStream` (not `EventSource`, which doesn't support POST)
- **CSV + Excel export** — `POST /export` streams file downloads; Excel has bold headers + auto-sized columns
- **Confidence scoring** — heuristic per-query score based on schema issues, retries, and result emptiness
- **Schema graph visualization** — interactive SVG with circular layout, bezier edges, solid FK vs dashed inferred

### Evaluation harness
- Spider-compatible execution accuracy metric (order-insensitive result set comparison)
- Hardness classification: easy / medium / hard / extra_hard (JOIN/GROUP BY/subquery scoring)
- Mini benchmark: 21 hand-verified examples across all hardness levels (`scripts/create_mini_benchmark.py`)
- RAG mode: seeds the example store from correct predictions during evaluation

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python 3.12 · FastAPI · SQLAlchemy · SSE-Starlette |
| LLMs | OpenAI (gpt-4o-mini) · Anthropic (claude-opus-4-6) — switchable via env |
| Embeddings | TF-IDF character bigrams (zero-cost, deterministic) · OpenAI text-embedding-3-small |
| Database | SQLite (dev) · PostgreSQL-ready |
| Frontend | React 18 · TypeScript · Vite · Tailwind CSS · Recharts · Framer Motion |
| Testing | pytest · 212 tests · httpx TestClient |

---

## Quick Start

### Prerequisites
- Python 3.12+
- Node.js 18+
- An OpenAI or Anthropic API key

### 1. Clone & install backend
```bash
git clone https://github.com/Yassinekraiem08/nl-to-sql-analytics-system.git
cd nl-to-sql-analytics-system
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env — set your API key and provider:
# LLM_PROVIDER=openai
# OPENAI_API_KEY=sk-...
# DATABASE_URL=sqlite:///dev.db
```

### 3. Start the backend
```bash
uvicorn app.api.main:app --reload --reload-dir app
# API running on http://127.0.0.1:8000
# Docs at http://127.0.0.1:8000/docs
```

> `dev.db` is included in the repo — pre-seeded with 1 000 users, 60 products, 5 000 orders, ~16 k order items. Run `python scripts/seed_demo.py` to regenerate it.

### 4. Start the frontend
```bash
cd frontend
npm install
npm run dev
# UI running on http://localhost:8080
```

### 5. Run the tests
```bash
pytest tests/ -v
# 212 tests passing
```

---

## Sample Queries

```
Top 10 customers by total spend
Monthly revenue for 2023
Revenue by product category
Best-selling products by units sold
Orders placed by users from New York
Average order value for completed orders
```

---

## Evaluation

Run the built-in mini benchmark (no Spider download required):

```bash
python scripts/evaluate_spider.py --demo --provider openai
```

With few-shot RAG enabled:
```bash
python scripts/evaluate_spider.py --demo --provider openai --rag
```

See [BENCHMARK.md](BENCHMARK.md) for full results and Spider evaluation instructions.

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/ask` | Synchronous NL-to-SQL query |
| `POST` | `/ask/stream` | SSE streaming NL-to-SQL query |
| `GET` | `/schema` | Full database schema |
| `GET` | `/schema/graph` | Schema graph with relationship edges |
| `POST` | `/sessions` | Create conversation session |
| `GET` | `/sessions/{id}` | Get session history |
| `DELETE` | `/sessions/{id}` | Delete session |
| `POST` | `/export` | Export query results as CSV or Excel |
| `GET` | `/health` | Health check |

Interactive docs: `http://127.0.0.1:8000/docs`

---

## Project Structure

```
├── app/
│   ├── api/routes/          # FastAPI route handlers
│   ├── core/                # Pipeline logic
│   │   ├── pipeline.py          # Self-correcting execution loop
│   │   ├── schema_analyzer.py   # Relationship graph + heuristic FK detection
│   │   ├── prompt_builder.py    # Few-shot + schema + context prompt assembly
│   │   ├── example_store.py     # TF-IDF embedding + cosine similarity retrieval
│   │   ├── query_cache.py       # Semantic result cache
│   │   ├── ambiguity.py         # Vague term detection
│   │   ├── performance_hints.py # Missing index analysis
│   │   ├── confidence.py        # Per-query confidence scoring
│   │   └── conversation.py      # Multi-turn session management
│   ├── db/                  # SQLAlchemy connection
│   └── models/              # Pydantic schemas
├── frontend/src/
│   ├── components/
│   │   ├── QueryTerminal.tsx    # Main query UI with SSE streaming
│   │   └── SchemaGraph.tsx      # Interactive SVG schema visualization
│   └── services/api.ts          # Typed API client
├── scripts/
│   ├── seed_demo.py             # Demo database seeder
│   ├── evaluate_spider.py       # Benchmark evaluation harness
│   └── create_mini_benchmark.py # Mini benchmark generator
└── tests/                   # 212 tests across 15 modules
```

---

## Deployment

| Service | Role |
|---|---|
| [Railway](https://railway.app) | FastAPI backend + SQLite |
| [Vercel](https://vercel.com) | React frontend |

**Backend (Railway)** — connects via `Procfile` and `railway.toml`. Set env vars: `LLM_PROVIDER`, `OPENAI_API_KEY`, `LLM_MODEL`, `DATABASE_URL`, `ALLOWED_ORIGINS`.

**Frontend (Vercel)** — set root directory to `frontend`, framework to Vite, and env var `VITE_API_URL` to the Railway backend URL.

---

## License

MIT
