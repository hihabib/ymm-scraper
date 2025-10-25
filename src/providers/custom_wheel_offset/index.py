#!/usr/bin/env python3
"""
Entry point for Custom Wheel Offset provider.
"""

import sys
from pathlib import Path
from importlib.util import spec_from_file_location, module_from_spec

# Prefer package import; fallback to file-based import for robustness
try:
    from providers.custom_wheel_offset.custom_wheel_offset import run_scrape
except ImportError:
    SRC_DIR = Path(__file__).resolve().parents[2]
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    script_file = Path(__file__).resolve().parent / "custom_wheel_offset.py"
    if not script_file.exists():
        raise ImportError(f"Cannot locate custom_wheel_offset.py at {script_file}")
    spec = spec_from_file_location("custom_wheel_offset_module", str(script_file))
    mod = module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    run_scrape = getattr(mod, "run_scrape")


if __name__ == "__main__":
    run_scrape()