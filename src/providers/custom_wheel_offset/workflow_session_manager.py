#!/usr/bin/env python3
"""
Workflow session manager for Custom Wheel Offset scraper.
Manages session state and persistence across workflow stages.
"""

import json
import time
from pathlib import Path
from typing import Dict, Any, Optional
from .logging_config import init_module_logger

logger = init_module_logger(__name__)

from .session_manager_threaded import threaded_session_manager


def initialize_session() -> None:
    """Initialize and log session details."""
    session = threaded_session_manager.get_session()
    logger.info(f"[ScraperV3] Using single session id={id(session)}")

    logger.info("[ScraperV3] Session initialized, ready for processing")
    logger.info(f"[ScraperV3] Current session cookies: {len(session.cookies)}")