#!/usr/bin/env python3
from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from db.db import SessionLocal
from core.models import EbayYMMResult


def main():
    session = SessionLocal()
    try:
        rows = (
            session.query(EbayYMMResult)
            .order_by(EbayYMMResult.created_at.desc(), EbayYMMResult.id.desc())
            .limit(5)
            .all()
        )
        if not rows:
            print("No ebay_ymm_results rows found.")
            return
        for r in rows:
            print(f"id={r.id} year={r.year} make={r.make} model={r.model} engine={r.engine} created_at={r.created_at}")
    finally:
        session.close()


if __name__ == "__main__":
    main()