"""
Database configuration for the scraper.

Defaults to PostgreSQL via `DATABASE_URL` env var if provided,
falls back to a local SQLite file for development.
"""
import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Fallback SQLite location (used if `DATABASE_URL` env var is not set)
DATA_DIR = Path(__file__).resolve().parents[2] / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_SQLITE_URL = f"sqlite:///{DATA_DIR / 'scraper.db'}"

# Prefer PostgreSQL (or any SQLAlchemy URL) from environment
DATABASE_URL = "postgresql+psycopg2://postgres:root@127.0.0.1:5432/scraper"

engine = create_engine(
    DATABASE_URL,
    echo=False,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

__all__ = ["DATABASE_URL", "engine", "SessionLocal"]