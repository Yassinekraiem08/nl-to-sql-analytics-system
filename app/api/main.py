from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import health, schema, query, stream, export

app = FastAPI(
    title="Natural Language Analytics System",
    description="Ask questions in plain English, get SQL + results + chart.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://0ce436b5-3f3c-416e-a2b1-f60a793a4fa8.lovable.app",
        "http://localhost:8080",   # Vite dev server
        "http://localhost:5173",   # Vite alt port
        "http://localhost:3000",   # fallback local dev
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(schema.router)
app.include_router(query.router)
app.include_router(stream.router)
app.include_router(export.router)
