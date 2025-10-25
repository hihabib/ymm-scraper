#!/usr/bin/env python3
"""
Combination processor for Custom Wheel Offset scraper.
Handles processing of full vehicle combinations (models/trims/drives).
"""

from typing import Dict, Set, Tuple

from .utils import get_vehicle_data
from .vehicle_data_extractor import VehicleDataExtractor
from .fitment_preferences import get_fitment_preferences
from .cache_ops import (
    load_full_cache_from_db,
    save_combination_to_db,
    check_combination_exists_in_db
)
from .key_utils import make_full_key, make_full_pref_key
from .vehicle_data_processor import get_vehicle_data_with_fallback
from .preference_processor import process_preference_combinations


def update_existing_vehicle_data(key: str, full_combos: Dict, year: str, make: str, 
                                model: str, trim: str, drive: str, vt: str, bp: str, 
                                drchassisid: str, config: Dict) -> Tuple[str, str, str]:
    """Update existing vehicle data if missing and return current values."""
    existing_vt = full_combos.get(key, {}).get("vehicleType")
    existing_bp = full_combos.get(key, {}).get("boltpattern", "")
    existing_drchassisid = full_combos.get(key, {}).get("drchassisid", "")
    
    # Update vehicleType if missing
    if not existing_vt:
        if not vt and config.get("fetch_vehicle_data", True):
            data = get_vehicle_data(year, make, model, trim, drive)
            vt = (data or {}).get("vehicleType", "")
        if vt:
            update_data = {"vehicleType": vt}
            save_combination_to_db(update_data)
    else:
        vt = existing_vt
    
    # Update boltpattern if missing
    if not existing_bp and config.get("fetch_vehicle_data", True):
        if not bp:
            data = get_vehicle_data(year, make, model, trim, drive)
            bp = (data or {}).get("boltpattern", "") or (data or {}).get("boltpatternInches", "")
        if bp:
            update_data = {"boltpattern": bp}
            save_combination_to_db(update_data)
    else:
        bp = existing_bp
    
    # Update drchassisid if missing
    if not existing_drchassisid and config.get("fetch_vehicle_data", True):
        if not drchassisid:
            data = get_vehicle_data(year, make, model, trim, drive)
            drchassisid = (data or {}).get("drchassisid", "")
        if drchassisid:
            update_data = {"drchassisid": drchassisid}
            save_combination_to_db(update_data)
    else:
        drchassisid = existing_drchassisid
    
    return vt, bp, drchassisid


def process_existing_combination_preferences(year: str, make: str, model: str, trim: str, 
                                           drive: str, vt: str, bp: str, drchassisid: str, 
                                           config: Dict, existing_full_keys: set) -> int:
    """Process preferences for existing base combinations."""
    added_count = 0
    prefs = get_fitment_preferences(vt or "car")
    
    for p in prefs:
        susp = p.get("suspension", "")
        mod = p.get("modification", "")
        rub = p.get("rubbing", "")
        pref_key = make_full_pref_key(
            year, make, model, trim, drive, vt, bp, drchassisid, susp, mod, rub
        )
        if pref_key in existing_full_keys:
            print(f"[ScraperV3] FULLCACHE SKIP existing preference: {pref_key}")
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
        print(
            f"[ScraperV3] FULLCACHE SAVED new preference combination: {pref_key}"
        )
    
    return added_count


def process_single_vehicle_combination(year: str, make: str, model: str, trim: str, drive: str,
                                     full_combos: Dict, existing_full_keys: set, config: Dict) -> int:
    """Process a single vehicle combination (year/make/model/trim/drive)."""
    added_count = 0
    
    # Get vehicle data
    vehicle_data = get_vehicle_data_with_fallback(year, make, model, trim, drive, config)
    vt = vehicle_data["vehicleType"]
    bp = vehicle_data["boltpattern"]
    drchassisid = vehicle_data["drchassisid"]
    
    key = make_full_key(year, make, model, trim, drive, vt, bp, drchassisid)

    # If base combination already exists, ensure preference entries
    if key in existing_full_keys:
        print(f"[ScraperV3] FULLCACHE SKIP existing base: {key}")
        if not config.get("pref_fetch", True):
            return 0
        
        # Update existing vehicle data if needed
        vt, bp, drchassisid = update_existing_vehicle_data(
            key, full_combos, year, make, model, trim, drive, vt, bp, drchassisid, config
        )
        
        # Process preferences for existing combination
        added_count += process_existing_combination_preferences(
            year, make, model, trim, drive, vt, bp, drchassisid, config, existing_full_keys
        )
    else:
        # Handle new base combination
        added_count += process_preference_combinations(
            year, make, model, trim, drive, vt, bp, drchassisid, config, existing_full_keys
        )
    
    return added_count


def process_new_full_combinations(
    existing_year_make: Set[Tuple[str, str]], full_cache: Dict, config: Dict
) -> int:
    """Process models/trims/drives for each (year, make) and add full combinations."""
    # Load from database instead of JSON cache
    db_cache = load_full_cache_from_db()
    full_combos = db_cache.get("combinations", {})
    
    if not isinstance(full_combos, dict):
        full_combos = {}
        
    existing_full_keys = set(full_combos.keys())

    added_full = 0
    for year, make in sorted(existing_year_make):
        # Get models for this year/make using VehicleDataExtractor
        year_make_extractor = VehicleDataExtractor(year=year, make=make)
        models = year_make_extractor.get_models()
        if not models:
            print(f"[ScraperV3] No models found for {year} {make}, skipping")
            continue
        print(f"[ScraperV3] Processing {len(models)} models for {year} {make}")
        
        for model in models:
            # Get trims for this year/make/model using VehicleDataExtractor
            year_make_model_extractor = VehicleDataExtractor(year=year, make=make, model=model)
            trims = year_make_model_extractor.get_trims()
            if not trims:
                print(f"[ScraperV3] No trims found for {year} {make} {model}, skipping")
                continue
            print(f"[ScraperV3] {year} {make} {model}: trims={len(trims)}")
            
            for trim in trims:
                # Get drives for this year/make/model/trim using VehicleDataExtractor
                year_make_model_trim_extractor = VehicleDataExtractor(year=year, make=make, model=model, trim=trim)
                drives = year_make_model_trim_extractor.get_drives()
                if not drives:
                    print(f"[ScraperV3] No drives found for {year} {make} {model} {trim}, skipping")
                    continue
                print(f"[ScraperV3] {year} {make} {model} {trim}: drives={len(drives)}")
                
                for drive in drives:
                    added_full += process_single_vehicle_combination(
                        year, make, model, trim, drive, full_combos, existing_full_keys, config
                    )

    return added_full