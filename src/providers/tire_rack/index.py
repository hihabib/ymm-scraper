#!/usr/bin/env python3
"""
Entry point for Tire Rack scraping with recursive retry on errors.
"""

from typing import Optional
import time
import sys
from pathlib import Path
from importlib.util import spec_from_file_location, module_from_spec

# Prefer package import; fallback to file-based import to handle hyphenated dir names
try:
    from providers.tire_rack.tire_rack import run_scrape
    try:
        from config.worker import TIRE_RACK_WORKERS
    except ImportError:
        SRC_DIR = Path(__file__).resolve().parents[2]
        if str(SRC_DIR) not in sys.path:
            sys.path.insert(0, str(SRC_DIR))
        WK_FILE = Path(__file__).resolve().parents[2] / "config" / "worker.py"
        spec_wk = spec_from_file_location("worker_config_module", str(WK_FILE))
        wmod = module_from_spec(spec_wk)
        assert spec_wk and spec_wk.loader
        spec_wk.loader.exec_module(wmod)
        TIRE_RACK_WORKERS = getattr(wmod, "TIRE_RACK_WORKERS", 8)
except ImportError:
    # Ensure src is on sys.path for repository import
    SRC_DIR = Path(__file__).resolve().parents[2]
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    # Resolve tire_rack.py by file path and import dynamically
    TR_FILE = Path(__file__).resolve().parent / "tire_rack.py"
    if not TR_FILE.exists():
        raise ImportError(f"Cannot locate tire_rack.py at {TR_FILE}")
    spec = spec_from_file_location("tire_rack_module", str(TR_FILE))
    mod = module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    run_scrape = getattr(mod, "run_scrape")

from services.repository import insert_error_log

# Default worker count; overridden by config if available
TIRE_RACK_WORKERS = 8
try:
    from config.worker import TIRE_RACK_WORKERS as _TR_WORKERS
    TIRE_RACK_WORKERS = _TR_WORKERS
except ImportError:
    SRC_DIR = Path(__file__).resolve().parents[2]
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    WK_FILE = Path(__file__).resolve().parents[2] / "config" / "worker.py"
    spec_wk = spec_from_file_location("worker_config_module", str(WK_FILE))
    wmod = module_from_spec(spec_wk)
    if spec_wk and spec_wk.loader:
        spec_wk.loader.exec_module(wmod)
        TIRE_RACK_WORKERS = getattr(wmod, "TIRE_RACK_WORKERS", TIRE_RACK_WORKERS)

def start_scraping(*, attempt: int = 1, max_attempts: int = 5, sleep_secs: float = 2.0) -> None:
    """
    Start scraping; if any error is thrown, log it and recursively retry until max_attempts.
    """
    try:
        run_scrape(max_workers=TIRE_RACK_WORKERS)
    except Exception as e:
        insert_error_log(
            source="tire_rack",
            context={"op": "start_scraping", "attempt": attempt},
            message=f"{type(e).__name__}: {e}"
        )
        if attempt < max_attempts:
            time.sleep(sleep_secs)
            start_scraping(attempt=attempt + 1, max_attempts=max_attempts, sleep_secs=sleep_secs)
        else:
            # Exceeded max attempts; re-raise for visibility
            raise


if __name__ == "__main__":
    start_scraping()