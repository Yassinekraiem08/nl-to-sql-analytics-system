#!/usr/bin/env python3
"""Create a mini benchmark from the seeded demo database.

Writes scripts/mini_benchmark.json with (question, gold SQL, hardness) pairs
covering all difficulty levels. Run this once after seed_demo.py.

Usage:
    python scripts/create_mini_benchmark.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Ground-truth benchmark — hand-authored for our demo schema
# (users, products, orders, order_items)
# ---------------------------------------------------------------------------

EXAMPLES = [
    # ── Easy ────────────────────────────────────────────────────────────────
    {
        "question": "How many users are there?",
        "query": "SELECT COUNT(*) FROM users",
        "hardness": "easy",
    },
    {
        "question": "List all distinct product categories",
        "query": "SELECT DISTINCT category FROM products ORDER BY category",
        "hardness": "easy",
    },
    {
        "question": "What is the most expensive product?",
        "query": "SELECT name, price FROM products ORDER BY price DESC LIMIT 1",
        "hardness": "easy",
    },
    {
        "question": "How many orders have status 'completed'?",
        "query": "SELECT COUNT(*) FROM orders WHERE status = 'completed'",
        "hardness": "easy",
    },
    {
        "question": "What is the average product price?",
        "query": "SELECT AVG(price) FROM products",
        "hardness": "easy",
    },
    {
        "question": "List the 5 cheapest products",
        "query": "SELECT name, price FROM products ORDER BY price ASC LIMIT 5",
        "hardness": "easy",
    },
    {
        "question": "How many distinct cities do our users come from?",
        "query": "SELECT COUNT(DISTINCT city) FROM users",
        "hardness": "easy",
    },

    # ── Medium ───────────────────────────────────────────────────────────────
    {
        "question": "Total revenue by product category",
        "query": (
            "SELECT p.category, SUM(oi.quantity * oi.unit_price) AS revenue "
            "FROM order_items oi "
            "JOIN products p ON oi.product_id = p.id "
            "GROUP BY p.category "
            "ORDER BY revenue DESC"
        ),
        "hardness": "medium",
    },
    {
        "question": "Average order value for completed orders",
        "query": (
            "SELECT AVG(order_total) AS avg_order_value "
            "FROM ("
            "  SELECT oi.order_id, SUM(oi.quantity * oi.unit_price) AS order_total "
            "  FROM order_items oi "
            "  JOIN orders o ON oi.order_id = o.id "
            "  WHERE o.status = 'completed' "
            "  GROUP BY oi.order_id"
            ") sub"
        ),
        "hardness": "medium",
    },
    {
        "question": "Number of orders per status",
        "query": (
            "SELECT status, COUNT(*) AS order_count "
            "FROM orders "
            "GROUP BY status "
            "ORDER BY order_count DESC"
        ),
        "hardness": "medium",
    },
    {
        "question": "Top 5 users by number of orders placed",
        "query": (
            "SELECT u.name, COUNT(o.id) AS order_count "
            "FROM users u "
            "JOIN orders o ON u.id = o.user_id "
            "GROUP BY u.id "
            "ORDER BY order_count DESC "
            "LIMIT 5"
        ),
        "hardness": "medium",
    },
    {
        "question": "Total quantity sold per product",
        "query": (
            "SELECT p.name, SUM(oi.quantity) AS total_sold "
            "FROM order_items oi "
            "JOIN products p ON oi.product_id = p.id "
            "GROUP BY p.id "
            "ORDER BY total_sold DESC"
        ),
        "hardness": "medium",
    },
    {
        "question": "How many users have placed at least one order?",
        "query": (
            "SELECT COUNT(DISTINCT user_id) FROM orders"
        ),
        "hardness": "medium",
    },

    # ── Hard ─────────────────────────────────────────────────────────────────
    {
        "question": "Top 10 customers by total spend",
        "query": (
            "SELECT u.name, SUM(oi.quantity * oi.unit_price) AS total_spend "
            "FROM users u "
            "JOIN orders o ON u.id = o.user_id "
            "JOIN order_items oi ON o.id = oi.order_id "
            "WHERE o.status = 'completed' "
            "GROUP BY u.id "
            "ORDER BY total_spend DESC "
            "LIMIT 10"
        ),
        "hardness": "hard",
    },
    {
        "question": "Best-selling products by units sold in the top 5",
        "query": (
            "SELECT p.name, SUM(oi.quantity) AS units_sold "
            "FROM order_items oi "
            "JOIN products p ON oi.product_id = p.id "
            "GROUP BY p.id "
            "ORDER BY units_sold DESC "
            "LIMIT 5"
        ),
        "hardness": "hard",
    },
    {
        "question": "Revenue by product category for completed orders only",
        "query": (
            "SELECT p.category, SUM(oi.quantity * oi.unit_price) AS revenue "
            "FROM order_items oi "
            "JOIN products p ON oi.product_id = p.id "
            "JOIN orders o ON oi.order_id = o.id "
            "WHERE o.status = 'completed' "
            "GROUP BY p.category "
            "ORDER BY revenue DESC"
        ),
        "hardness": "hard",
    },
    {
        "question": "Which users have never placed an order?",
        "query": (
            "SELECT u.name, u.email "
            "FROM users u "
            "LEFT JOIN orders o ON u.id = o.user_id "
            "WHERE o.id IS NULL"
        ),
        "hardness": "hard",
    },
    {
        "question": "Average number of items per order",
        "query": (
            "SELECT AVG(item_count) AS avg_items_per_order "
            "FROM ("
            "  SELECT order_id, SUM(quantity) AS item_count "
            "  FROM order_items "
            "  GROUP BY order_id"
            ") sub"
        ),
        "hardness": "hard",
    },

    # ── Extra hard ────────────────────────────────────────────────────────────
    {
        "question": "Top 3 cities by total revenue from completed orders",
        "query": (
            "SELECT u.city, SUM(oi.quantity * oi.unit_price) AS revenue "
            "FROM users u "
            "JOIN orders o ON u.id = o.user_id "
            "JOIN order_items oi ON o.id = oi.order_id "
            "WHERE o.status = 'completed' "
            "GROUP BY u.city "
            "ORDER BY revenue DESC "
            "LIMIT 3"
        ),
        "hardness": "extra_hard",
    },
    {
        "question": "Products that appear in more than 100 orders",
        "query": (
            "SELECT p.name, COUNT(DISTINCT oi.order_id) AS order_count "
            "FROM order_items oi "
            "JOIN products p ON oi.product_id = p.id "
            "GROUP BY p.id "
            "HAVING order_count > 100 "
            "ORDER BY order_count DESC"
        ),
        "hardness": "extra_hard",
    },
    {
        "question": "For each category, the product with the highest total revenue",
        "query": (
            "SELECT category, name, revenue FROM ("
            "  SELECT p.category, p.name, "
            "         SUM(oi.quantity * oi.unit_price) AS revenue,"
            "         RANK() OVER (PARTITION BY p.category ORDER BY SUM(oi.quantity * oi.unit_price) DESC) AS rnk "
            "  FROM order_items oi "
            "  JOIN products p ON oi.product_id = p.id "
            "  GROUP BY p.id"
            ") ranked WHERE rnk = 1"
        ),
        "hardness": "extra_hard",
    },
]


def verify_gold_sql(examples: list[dict], engine) -> list[dict]:
    """Run each gold SQL to catch authoring errors before saving."""
    verified = []
    errors = 0
    with engine.connect() as conn:
        for ex in examples:
            try:
                conn.execute(text(ex["query"]))
                verified.append(ex)
            except Exception as e:
                print(f"  ERROR in gold SQL [{ex['question'][:50]}]: {e}")
                errors += 1
    if errors:
        print(f"\n{errors} gold SQL error(s) found. Fix before using this benchmark.")
    return verified


def main() -> None:
    db_path = Path(__file__).parents[1] / "dev.db"
    if not db_path.exists():
        print("dev.db not found. Run: python scripts/seed_demo.py")
        sys.exit(1)

    engine = create_engine(f"sqlite:///{db_path}")

    print(f"Verifying {len(EXAMPLES)} gold SQL statements against dev.db...")
    verified = verify_gold_sql(EXAMPLES, engine)

    # Add db_id field so evaluate_spider.py can parse them uniformly
    for ex in verified:
        ex.setdefault("db_id", "demo")

    out_path = Path(__file__).parent / "mini_benchmark.json"
    with open(out_path, "w") as f:
        json.dump(verified, f, indent=2)

    by_hardness: dict[str, int] = {}
    for ex in verified:
        h = ex.get("hardness", "unknown")
        by_hardness[h] = by_hardness.get(h, 0) + 1

    print(f"\nWrote {len(verified)} examples to {out_path}")
    for h, n in sorted(by_hardness.items()):
        print(f"  {h:<12} {n}")


if __name__ == "__main__":
    main()
