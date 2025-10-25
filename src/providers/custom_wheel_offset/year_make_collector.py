#!/usr/bin/env python3
"""
Year/Make combination collector for Custom Wheel Offset scraper.
Handles collection and caching of year/make combinations.
"""

import sys
from pathlib import Path
from typing import Dict, Set, Tuple

# Ensure `src` is on sys.path for absolute imports
SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Import centralized logging
from .logging_config import init_module_logger

# Initialize logger for this module
logger = init_module_logger("year_make_collector")

from .vehicle_data_extractor import VehicleDataExtractor
from .cache_ops import load_cache, save_cache


def collect_year_make(config: Dict) -> Set[Tuple[str, str]]:
    """Collect year/make combinations, persisting to cache.
    Returns the set of existing (year, make) combinations.
    """
    cache = load_cache()
    existing = {(c.get("year"), c.get("make")) for c in cache.get("combinations", [])}
    logger.info(f"Loaded cache with {len(existing)} combinations")

    if existing:
        logger.info(
            f"Cache already contains {len(existing)} combinations. "
            "Skipping year/make collection."
        )
        return existing

    # Get years using VehicleDataExtractor (no parameters = get all years)
    extractor = VehicleDataExtractor()
    years = extractor.get_years()
    if config.get("limit_years"):
        years = years[: config["limit_years"]]
    logger.info(f"Years count: {len(years)}")

    new_count = 0
    for year in years:
        # Get makes for this year using VehicleDataExtractor
        year_extractor = VehicleDataExtractor(year=year)
        makes = year_extractor.get_models()  # When year is provided, get_models returns makes
        if config.get("limit_makes"):
            makes = makes[: config["limit_makes"]]
        logger.info(f"{year}: makes={len(makes)}")
        for make in makes:
            cache["combinations"].append({"year": year, "make": make})
            save_cache(cache)
            existing.add((year, make))
            new_count += 1
            logger.info(
                f"SAVED new combination: {year} {make} "
                f"(total={cache['total_combinations']})"
            )

    logger.info(
        f"Stage 1 done. New combinations added: {new_count}. "
        f"Total now: {cache['total_combinations']}"
    )
    return existing