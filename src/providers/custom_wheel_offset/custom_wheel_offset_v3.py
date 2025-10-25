#!/usr/bin/env python3
"""
Custom Wheel Offset scraper V3 (orchestrator)
- Thin entrypoint that delegates to workflow_v3 for maintainability.
"""

import sys
from pathlib import Path

# Ensure `src` is on sys.path for absolute imports
SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from providers.custom_wheel_offset.workflow_v3 import run as run_scraper_v3


if __name__ == "__main__":
    run_scraper_v3()