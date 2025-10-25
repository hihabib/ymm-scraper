"""
Delay utilities for Custom Wheel Offset Scraper V2.
Provides human-like timing and delay functions.
"""

import asyncio
import random
import logging
import os
import importlib.util
from typing import Tuple

# Import local config from the parent directory
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
config_path = os.path.join(parent_dir, 'config.py')
spec = importlib.util.spec_from_file_location("local_config", config_path)
local_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local_config)

HUMAN_DELAY_RANGE = local_config.HUMAN_DELAY_RANGE
NETWORK_DELAY_RANGE = local_config.NETWORK_DELAY_RANGE

logger = logging.getLogger(__name__)


async def human_delay(min_delay: float = None, max_delay: float = None) -> None:
    """
    Add a human-like delay with random timing.
    
    Args:
        min_delay: Minimum delay in seconds (uses config default if None)
        max_delay: Maximum delay in seconds (uses config default if None)
    """
    if min_delay is None or max_delay is None:
        min_delay, max_delay = HUMAN_DELAY_RANGE
    
    delay = random.uniform(min_delay, max_delay)
    logger.debug(f"Human delay: {delay:.2f}s")
    await asyncio.sleep(delay)


async def network_delay() -> None:
    """Add a network-like delay to simulate connection latency."""
    min_delay, max_delay = NETWORK_DELAY_RANGE
    delay = random.uniform(min_delay, max_delay)
    logger.debug(f"Network delay: {delay:.2f}s")
    await asyncio.sleep(delay)


async def random_pause() -> None:
    """Add a random pause that occasionally includes longer delays."""
    # 20% chance of longer pause (3-7 seconds)
    if random.random() < 0.2:
        delay = random.uniform(3, 7)
        logger.debug(f"Long random pause: {delay:.2f}s")
    else:
        # Normal short pause (0.5-2 seconds)
        delay = random.uniform(0.5, 2)
        logger.debug(f"Short random pause: {delay:.2f}s")
    
    await asyncio.sleep(delay)


async def typing_delay(text_length: int) -> None:
    """
    Add delay based on text length to simulate human typing speed.
    
    Args:
        text_length: Length of text being typed
    """
    # Average human typing speed: 40 WPM = ~200 characters per minute
    # Add some randomness: 150-250 characters per minute
    chars_per_second = random.uniform(2.5, 4.2)  # 150-250 CPM
    delay = text_length / chars_per_second
    
    # Add some random variation
    delay *= random.uniform(0.8, 1.2)
    
    logger.debug(f"Typing delay for {text_length} chars: {delay:.2f}s")
    await asyncio.sleep(delay)