#!/usr/bin/env python3
"""
Preference processor for custom wheel offset scraper.
Handles generation and processing of fitment preference combinations.
"""

import json
import os
from typing import List, Dict, Any, Set
from .logging_config import init_module_logger

logger = init_module_logger(__name__)

from .utils import get_vehicle_data
from .fitment_preferences import get_fitment_preferences
from .resolve_captcha import get_phpsessid_from_api
from .cache_ops import (
    load_full_cache_from_db,
    save_combination_to_db,
    check_combination_exists_in_db
)
from .key_utils import make_full_pref_key


def backfill_preferences(full_cache: Dict, config: Dict) -> int:
    """Generate preference entries for existing base keys (5-part keys)."""
    if not config.get("pref_fetch", True):
        return 0
    
    # Load from database instead of JSON cache
    db_cache = load_full_cache_from_db()
    full_combos = db_cache.get("combinations", {})
    
    if not isinstance(full_combos, dict):
        full_combos = {}

    existing_full_keys = set(full_combos.keys())
    generated = 0

    for key, entry in list(full_combos.items()):
        parts = key.split("__")
        if len(parts) != 5:
            continue
        y, mk, md, tr, dv = parts
        vt = entry.get("vehicleType") or "car"
        bp = entry.get("boltpattern", "")
        drchassisid = entry.get("drchassisid", "")
        if (not bp or not drchassisid) and config.get("fetch_vehicle_data", True):
            data = get_vehicle_data(y, mk, md, tr, dv)
            if not bp:
                bp = (data or {}).get("boltpattern", "") or (data or {}).get("boltpatternInches", "")
            if not drchassisid:
                drchassisid = (data or {}).get("drchassisid", "")
        
        # Get fresh PHPSESSID for this vehicle configuration before fetching preferences
        phpsessid = get_phpsessid_from_api(
            vehicle_type=vt or "Car",
            year=y,
            make=mk,
            model=md,
            trim=tr,
            drive=dv,
            chassis_id=drchassisid or "80349"
        )
        
        # Set PHPSESSID in thread-local session to avoid race conditions
        if phpsessid:
            from .session_manager_threaded import threaded_session_manager
            session = threaded_session_manager.get_session()
            from urllib.parse import urlparse
            try:
                session.cookies.set("PHPSESSID", phpsessid, domain="customwheeloffset.com", path="/")
            except Exception:
                session.cookies.set("PHPSESSID", phpsessid)
            logger.info(f"Set PHPSESSID {phpsessid} in thread-local session for {y} {mk} {md}")
        
        prefs = get_fitment_preferences(vt)
        if not prefs:
            continue
        for p in prefs:
            susp = p.get("suspension", "")
            mod = p.get("modification", "")
            rub = p.get("rubbing", "")
            pref_key = make_full_pref_key(y, mk, md, tr, dv, vt, bp, drchassisid, susp, mod, rub)
            if pref_key in existing_full_keys:
                continue
            
            # Check if combination already exists in database
            if check_combination_exists_in_db(y, mk, md, tr, dv, vt, drchassisid, susp, mod, rub):
                continue
                
            combination_data = {
                "year": y,
                "make": mk,
                "model": md,
                "trim": tr,
                "drive": dv,
                "vehicleType": vt,
                "boltpattern": bp,
                "drchassisid": drchassisid,
                "suspension": susp,
                "modification": mod,
                "rubbing": rub,
                "processed": False,
            }
            
            # Save to database instead of JSON cache
            save_combination_to_db(combination_data)
            existing_full_keys.add(pref_key)
            generated += 1
            logger.info(f"[ScraperV3] FULLCACHE GENERATED preference entry: {pref_key}")

    if generated:
        logger.info(f"[ScraperV3] Generated {generated} preference entries from base keys")
    return generated


def process_preference_combinations(year: str, make: str, model: str, trim: str, drive: str, 
                                  vt: str, bp: str, drchassisid: str, config: Dict, 
                                  existing_full_keys: set) -> int:
    """Process preference combinations for a given vehicle configuration."""
    added_count = 0
    
    # Get fresh PHPSESSID for this vehicle configuration before fetching preferences
    if config.get("pref_fetch", True):
        phpsessid = get_phpsessid_from_api(
            vehicle_type=vt or "Car",
            year=year,
            make=make,
            model=model,
            trim=trim,
            drive=drive,
            chassis_id=drchassisid or "80349"
        )
        
        # Set PHPSESSID in thread-local session to avoid race conditions
        if phpsessid:
            from .session_manager_threaded import threaded_session_manager
            session = threaded_session_manager.get_session()
            try:
                session.cookies.set("PHPSESSID", phpsessid, domain="customwheeloffset.com", path="/")
            except Exception:
                session.cookies.set("PHPSESSID", phpsessid)
            logger.info(f"Set PHPSESSID {phpsessid} in thread-local session for {year} {make} {model}")
    
    prefs = get_fitment_preferences(vt or "car") if config.get("pref_fetch", True) else []
    
    if not prefs:
        # Check if combination already exists in database
        if not check_combination_exists_in_db(year, make, model, trim, drive, vt, drchassisid):
            combination_data = {
                "year": year,
                "make": make,
                "model": model,
                "trim": trim,
                "drive": drive,
                "vehicleType": vt,
                "boltpattern": bp,
                "drchassisid": drchassisid,
                "processed": False,
            }
            
            # Save to database instead of JSON cache
            save_combination_to_db(combination_data)
            logger.info(
                f"[ScraperV3] FULLCACHE SAVED base combination (no prefs): "
                f"{year}__{make}__{model}__{trim}__{drive} "
                f"(vehicleType='{vt}', boltpattern='{bp}', drchassisid='{drchassisid}')"
            )
    else:
        for p in prefs:
            susp = p.get("suspension", "")
            mod = p.get("modification", "")
            rub = p.get("rubbing", "")
            pref_key = make_full_pref_key(
                year, make, model, trim, drive, vt, bp, drchassisid, susp, mod, rub
            )
            if pref_key in existing_full_keys:
                logger.info(f"[ScraperV3] FULLCACHE SKIP existing preference: {pref_key}")
                continue
            
            # Check if combination already exists in database
                if check_combination_exists_in_db(year, make, model, trim, drive, vt, drchassisid, susp, mod, rub):
                    continue
                
            combination_data = {
                "year": year,
                "make": make,
                "model": model,
                "trim": trim,
                "drive": drive,
                "vehicleType": vt,
                "boltpattern": bp,
                "drchassisid": drchassisid,
                "suspension": susp,
                "modification": mod,
                "rubbing": rub,
                "processed": False,
            }
            
            # Save to database instead of JSON cache
            save_combination_to_db(combination_data)
            existing_full_keys.add(pref_key)
            added_count += 1
            logger.info(
                f"[ScraperV3] FULLCACHE SAVED new preference combination: {pref_key}"
            )
    
    return added_count