#!/usr/bin/env python3
"""
Vehicle data processor for Custom Wheel Offset scraper.
Handles backfilling of vehicle types and related data.
"""

from typing import Dict

from .utils import get_vehicle_data
from .cache_ops import load_full_cache_from_db, save_combination_to_db


def backfill_vehicle_types(full_cache: Dict, config: Dict) -> int:
    """Backfill missing vehicleType for existing full-cache entries."""
    if config.get("fast"):
        return 0
    
    # Load from database instead of JSON cache
    db_cache = load_full_cache_from_db()
    full_combos = db_cache.get("combinations", {})
    
    if not isinstance(full_combos, dict):
        full_combos = {}

    backfilled = 0
    for key, entry in list(full_combos.items()):
        need_vt = "vehicleType" not in entry or not entry.get("vehicleType")
        need_bp = "boltpattern" not in entry or not entry.get("boltpattern")
        if not (need_vt or need_bp):
            continue
        y = entry.get("year")
        mk = entry.get("make")
        md = entry.get("model")
        tr = entry.get("trim")
        dv = entry.get("drive")
        vt = entry.get("vehicleType", "") if not need_vt else ""
        bp = entry.get("boltpattern", "") if not need_bp else ""
        if config.get("fetch_vehicle_data", True):
            data = get_vehicle_data(y, mk, md, tr, dv)
            if need_vt:
                vt = (data or {}).get("vehicleType", "")
            if need_bp:
                bp = (data or {}).get("boltpattern", "") or (data or {}).get("boltpatternInches", "")
        entry["vehicleType"] = vt
        entry["boltpattern"] = bp
        
        # Save to database instead of JSON cache
        save_combination_to_db(entry)
        backfilled += 1
        print(f"[ScraperV3] FULLCACHE BACKFILL vehicleType/boltpattern for {key}: vt='{vt}', boltpattern='{bp}'")

    if backfilled:
        print(f"[ScraperV3] Backfilled vehicleType/boltpattern on {backfilled} existing combinations")
    return backfilled


def get_vehicle_data_with_fallback(year: str, make: str, model: str, trim: str, drive: str, config: Dict) -> Dict:
    """Get vehicle data with configuration-based fallback."""
    if config.get("fetch_vehicle_data", True):
        data = get_vehicle_data(year, make, model, trim, drive)
        return {
            "vehicleType": (data or {}).get("vehicleType", ""),
            "boltpattern": (data or {}).get("boltpattern", "") or (data or {}).get("boltpatternInches", ""),
            "drchassisid": (data or {}).get("drchassisid", "")
        }
    else:
        return {
            "vehicleType": "",
            "boltpattern": "",
            "drchassisid": ""
        }