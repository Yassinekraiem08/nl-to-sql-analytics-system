"""
Generate a realistic e-commerce demo database.

Usage:
    python scripts/seed_demo.py              # writes to dev.db
    python scripts/seed_demo.py my.db        # writes to a custom path

Schema:
    users        ~1 000 rows
    products     ~200 rows
    orders       ~5 000 rows
    order_items  ~15 000 rows

The data is synthetic but plausible — realistic names, prices, dates,
and a long-tail order distribution so aggregate queries look interesting.
"""
from __future__ import annotations

import random
import sys
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import (
    Column, Date, Float, ForeignKey, Integer, String, Text,
    create_engine, text,
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id         = Column(Integer, primary_key=True)
    name       = Column(String(120), nullable=False)
    email      = Column(String(200), nullable=False, unique=True)
    city       = Column(String(80))
    signup_date = Column(Date, nullable=False)
    orders     = relationship("Order", back_populates="user")


class Product(Base):
    __tablename__ = "products"
    id       = Column(Integer, primary_key=True)
    name     = Column(String(200), nullable=False)
    category = Column(String(80), nullable=False)
    price    = Column(Float, nullable=False)
    order_items = relationship("OrderItem", back_populates="product")


class Order(Base):
    __tablename__ = "orders"
    id         = Column(Integer, primary_key=True)
    user_id    = Column(Integer, ForeignKey("users.id"), nullable=False)
    status     = Column(String(20), nullable=False)   # completed / returned / pending
    created_at = Column(Date, nullable=False)
    user       = relationship("User", back_populates="orders")
    items      = relationship("OrderItem", back_populates="order")


class OrderItem(Base):
    __tablename__ = "order_items"
    id         = Column(Integer, primary_key=True)
    order_id   = Column(Integer, ForeignKey("orders.id"), nullable=False)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    quantity   = Column(Integer, nullable=False)
    unit_price = Column(Float, nullable=False)
    order      = relationship("Order", back_populates="items")
    product    = relationship("Product", back_populates="order_items")


# ---------------------------------------------------------------------------
# Raw data pools
# ---------------------------------------------------------------------------

_FIRST_NAMES = [
    "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank",
    "Iris", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia", "Paul",
    "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
    "Yara", "Zoe", "Aaron", "Bella", "Carlos", "Diana", "Ethan", "Fiona",
    "George", "Hannah", "Ivan", "Julia", "Kevin", "Laura", "Mike", "Nora",
    "Oscar", "Petra", "Quentin", "Rosa", "Steve", "Tara", "Ulric", "Vera",
]

_LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
]

_CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "Austin",
    "Jacksonville", "Fort Worth", "Columbus", "Charlotte", "Indianapolis",
    "San Francisco", "Seattle", "Denver", "Nashville", "Boston",
    "Miami", "Atlanta", "Portland", "Las Vegas", "Minneapolis",
]

_CATEGORIES = {
    "Electronics":   [
        "Wireless Earbuds", "Mechanical Keyboard", "USB-C Hub", "Monitor Stand",
        "Webcam HD", "Laptop Sleeve", "Smart Plug", "Portable Charger",
        "Bluetooth Speaker", "LED Desk Lamp",
    ],
    "Books": [
        "Python Crash Course", "Designing Data-Intensive Applications",
        "The Pragmatic Programmer", "Clean Code", "System Design Interview",
        "Database Internals", "Staff Engineer", "An Elegant Puzzle",
        "The Phoenix Project", "Accelerate",
    ],
    "Clothing": [
        "Merino Wool Hoodie", "Slim Chinos", "Running Shorts", "Graphic Tee",
        "Waterproof Jacket", "Casual Sneakers", "Wool Socks (6-pack)",
        "Baseball Cap", "Compression Leggings", "Fleece Vest",
    ],
    "Home & Kitchen": [
        "Pour-Over Coffee Set", "Cast Iron Skillet", "Bamboo Cutting Board",
        "Insulated Water Bottle", "Electric Kettle", "French Press",
        "Meal Prep Containers", "Knife Sharpener", "Dish Drying Rack",
        "Silicone Baking Mats",
    ],
    "Sports": [
        "Resistance Bands Set", "Foam Roller", "Jump Rope", "Yoga Mat",
        "Pull-Up Bar", "Adjustable Dumbbells", "Gym Bag", "Protein Shaker",
        "Knee Sleeves", "Weightlifting Belt",
    ],
    "Beauty": [
        "Vitamin C Serum", "Retinol Moisturizer", "SPF 50 Sunscreen",
        "Hyaluronic Acid Toner", "Eye Cream", "Facial Cleanser",
        "Niacinamide Serum", "Sheet Masks (10-pack)", "Lip Balm SPF",
        "Micellar Water",
    ],
}

_STATUSES = ["completed", "completed", "completed", "returned", "pending"]


# ---------------------------------------------------------------------------
# Generator helpers
# ---------------------------------------------------------------------------

def _random_date(start: date, end: date, rng: random.Random) -> date:
    delta = (end - start).days
    return start + timedelta(days=rng.randint(0, delta))


def _make_users(n: int, rng: random.Random) -> list[dict]:
    used_emails: set[str] = set()
    rows = []
    start = date(2021, 1, 1)
    end   = date(2024, 6, 30)
    for i in range(1, n + 1):
        first = rng.choice(_FIRST_NAMES)
        last  = rng.choice(_LAST_NAMES)
        base  = f"{first.lower()}.{last.lower()}"
        email = f"{base}@example.com"
        suffix = 1
        while email in used_emails:
            email = f"{base}{suffix}@example.com"
            suffix += 1
        used_emails.add(email)
        rows.append({
            "id": i,
            "name": f"{first} {last}",
            "email": email,
            "city": rng.choice(_CITIES),
            "signup_date": _random_date(start, end, rng),
        })
    return rows


def _make_products(rng: random.Random) -> list[dict]:
    rows = []
    pid  = 1
    price_ranges = {
        "Electronics":   (19.99, 149.99),
        "Books":         (9.99,  49.99),
        "Clothing":      (14.99, 89.99),
        "Home & Kitchen": (12.99, 79.99),
        "Sports":        (9.99,  129.99),
        "Beauty":        (7.99,  59.99),
    }
    for category, items in _CATEGORIES.items():
        lo, hi = price_ranges[category]
        for name in items:
            price = round(rng.uniform(lo, hi), 2)
            rows.append({
                "id": pid,
                "name": name,
                "category": category,
                "price": price,
            })
            pid += 1
    return rows


def _make_orders(
    n: int,
    user_ids: list[int],
    rng: random.Random,
) -> list[dict]:
    # Long-tail: a small slice of users orders a lot more than average.
    weights = [rng.paretovariate(1.5) for _ in user_ids]
    total_w = sum(weights)
    weights = [w / total_w for w in weights]

    start = date(2022, 1, 1)
    end   = date(2024, 9, 30)

    rows = []
    for oid in range(1, n + 1):
        uid = rng.choices(user_ids, weights=weights, k=1)[0]
        rows.append({
            "id":         oid,
            "user_id":    uid,
            "status":     rng.choice(_STATUSES),
            "created_at": _random_date(start, end, rng),
        })
    return rows


def _make_order_items(
    order_ids: list[int],
    product_rows: list[dict],
    rng: random.Random,
    avg_items_per_order: float = 3.0,
) -> list[dict]:
    rows = []
    iid  = 1
    for oid in order_ids:
        n_items = max(1, round(rng.expovariate(1 / avg_items_per_order)))
        chosen  = rng.sample(product_rows, min(n_items, len(product_rows)))
        for prod in chosen:
            qty   = rng.randint(1, 4)
            # Slight price variation: discount or markup up to ±10 %
            price = round(prod["price"] * rng.uniform(0.9, 1.1), 2)
            rows.append({
                "id":         iid,
                "order_id":   oid,
                "product_id": prod["id"],
                "quantity":   qty,
                "unit_price": price,
            })
            iid += 1
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def seed(db_path: str = "dev.db", seed: int = 42) -> None:
    rng = random.Random(seed)

    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url, echo=False)

    print(f"Creating schema in {db_path} ...")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    print("Generating users ...")
    users = _make_users(1_000, rng)

    print("Generating products ...")
    products = _make_products(rng)

    print("Generating orders ...")
    orders = _make_orders(5_000, [u["id"] for u in users], rng)

    print("Generating order_items ...")
    items = _make_order_items([o["id"] for o in orders], products, rng)

    print(f"Writing to database ...")
    with Session(engine) as session:
        session.bulk_insert_mappings(User, users)          # type: ignore[arg-type]
        session.bulk_insert_mappings(Product, products)    # type: ignore[arg-type]
        session.bulk_insert_mappings(Order, orders)        # type: ignore[arg-type]
        session.bulk_insert_mappings(OrderItem, items)     # type: ignore[arg-type]
        session.commit()

    # Quick sanity check
    with engine.connect() as conn:
        for tbl in ("users", "products", "orders", "order_items"):
            n = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()
            print(f"  {tbl:<15} {n:>6} rows")

    print(f"\nDone. Set DATABASE_URL=sqlite:///{db_path} and start the API.")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "dev.db"
    seed(path)
