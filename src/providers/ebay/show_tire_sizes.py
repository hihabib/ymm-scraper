#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from services.repository_optimized import get_db_session
from core.models import EbayTireSize


def main() -> None:
    with get_db_session() as session:
        rows = (
            session.query(EbayTireSize)
            .order_by(EbayTireSize.created_at.desc(), EbayTireSize.id.desc())
            .limit(10)
            .all()
        )
        for r in rows:
            print(
                f"tire_id={r.id} ymm_id={r.ymm_id} {r.year} {r.make} {r.model} {r.trim} {r.submodel} {r.engine} size={r.tire_size} created_at={r.created_at}"
            )


if __name__ == "__main__":
    main()