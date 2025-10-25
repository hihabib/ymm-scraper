#!/usr/bin/env python3
"""
Optimized combination processor for Custom Wheel Offset scraper.
Uses batch operations and optimized database access for better multithreading performance.
"""

from typing import Dict, Set, Tuple, List
from .utils import get_vehicle_data
from .fitment_preferences import get_fitment_preferences
from .optimized_cache_ops import (
    load_full_cache_from_db_optimized,
    batch_save_combinations_optimized,
    batch_check_combinations_exist_optimized,
    optimized_db_manager
)
from .key_utils import make_full_key, make_full_pref_key
from .vehicle_data_processor import get_vehicle_data_with_fallback
from .logging_config import init_module_logger

logger = init_module_logger(__name__)

class OptimizedCombinationProcessor:
    """Optimized combination processor with batch operations."""
    
    def __init__(self):
        self.batch_size = 50
        self.pending_combinations = []
        
    def process_single_vehicle_combination_optimized(self, year: str, make: str, model: str, 
                                                   trim: str, drive: str, full_combos: Dict, 
                                                   existing_full_keys: set, config: Dict) -> int:
        """Process a single vehicle combination with optimized database operations."""
        added_count = 0
        
        # Get vehicle data
        vehicle_data = get_vehicle_data_with_fallback(year, make, model, trim, drive, config)
        vt = vehicle_data["vehicleType"]
        bp = vehicle_data["boltpattern"]
        drchassisid = vehicle_data["drchassisid"]
        
        key = make_full_key(year, make, model, trim, drive, vt, bp, drchassisid)
        
        # If base combination already exists, ensure preference entries
        if key in existing_full_keys:
            logger.debug(f"[OptimizedProcessor] SKIP existing base: {key}")
            if not config.get("pref_fetch", True):
                return 0
            
            # Process preferences for existing combination
            added_count += self._process_existing_combination_preferences_optimized(
                year, make, model, trim, drive, vt, bp, drchassisid, config, existing_full_keys
            )
        else:
            # Handle new base combination
            added_count += self._process_preference_combinations_optimized(
                year, make, model, trim, drive, vt, bp, drchassisid, config, existing_full_keys
            )
        
        return added_count
    
    def _process_existing_combination_preferences_optimized(self, year: str, make: str, model: str, 
                                                          trim: str, drive: str, vt: str, bp: str, 
                                                          drchassisid: str, config: Dict, 
                                                          existing_full_keys: set) -> int:
        """Process preferences for existing base combinations with batch operations."""
        added_count = 0
        prefs = get_fitment_preferences(vt or "car")
        
        # Collect all preference combinations to check in batch
        combinations_to_check = []
        preference_data = []
        
        for p in prefs:
            susp = p.get("suspension", "")
            mod = p.get("modification", "")
            rub = p.get("rubbing", "")
            pref_key = make_full_pref_key(
                year, make, model, trim, drive, vt, bp, drchassisid, susp, mod, rub
            )
            
            if pref_key in existing_full_keys:
                logger.debug(f"[OptimizedProcessor] SKIP existing preference: {pref_key}")
                continue
            
            # Add to batch check
            combinations_to_check.append((year, make, model, trim, drive, vt, drchassisid, susp, mod, rub))
            preference_data.append({
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
                "pref_key": pref_key
            })
        
        if combinations_to_check:
            # Batch check existing combinations
            existing_combinations = batch_check_combinations_exist_optimized(combinations_to_check)
            
            # Filter out existing combinations
            new_combinations = []
            for i, combo_tuple in enumerate(combinations_to_check):
                if combo_tuple not in existing_combinations:
                    new_combinations.append(preference_data[i])
                    existing_full_keys.add(preference_data[i]["pref_key"])
            
            if new_combinations:
                # Batch save new combinations
                batch_save_combinations_optimized(new_combinations)
                added_count = len(new_combinations)
                logger.info(f"[OptimizedProcessor] Batch saved {added_count} preference combinations")
        
        return added_count
    
    def _process_preference_combinations_optimized(self, year: str, make: str, model: str, 
                                                 trim: str, drive: str, vt: str, bp: str, 
                                                 drchassisid: str, config: Dict, 
                                                 existing_full_keys: set) -> int:
        """Process preference combinations for new base combinations with batch operations."""
        added_count = 0
        
        # Get PHPSESSID for this combination
        phpsessid = None
        if config.get("fetch_phpsessid", True):
            try:
                from .resolve_captcha import get_phpsessid_from_api
                phpsessid = get_phpsessid_from_api(vt or "car", year, make, model, trim, drive, drchassisid)
                logger.info(f"[OptimizedProcessor] Got PHPSESSID for {year} {make} {model}: {phpsessid}")
            except Exception as e:
                logger.warning(f"[OptimizedProcessor] Failed to get PHPSESSID for {year} {make} {model}: {e}")
        
        # Set PHPSESSID in thread-local session
        if phpsessid:
            from .session_manager_threaded import threaded_session_manager
            session = threaded_session_manager.get_session()
            try:
                session.cookies.set("PHPSESSID", phpsessid, domain="customwheeloffset.com", path="/")
            except Exception:
                session.cookies.set("PHPSESSID", phpsessid)
            logger.info(f"[OptimizedProcessor] Set PHPSESSID {phpsessid} in thread-local session")
        
        # Get preferences and prepare batch operations
        prefs = get_fitment_preferences(vt or "car")
        if not prefs:
            return 0
        
        combinations_to_check = []
        preference_data = []
        
        for p in prefs:
            susp = p.get("suspension", "")
            mod = p.get("modification", "")
            rub = p.get("rubbing", "")
            pref_key = make_full_pref_key(year, make, model, trim, drive, vt, bp, drchassisid, susp, mod, rub)
            
            if pref_key in existing_full_keys:
                continue
            
            # Add to batch check
            combinations_to_check.append((year, make, model, trim, drive, vt, drchassisid, susp, mod, rub))
            preference_data.append({
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
                "pref_key": pref_key
            })
        
        if combinations_to_check:
            # Batch check existing combinations
            existing_combinations = batch_check_combinations_exist_optimized(combinations_to_check)
            
            # Filter out existing combinations
            new_combinations = []
            for i, combo_tuple in enumerate(combinations_to_check):
                if combo_tuple not in existing_combinations:
                    new_combinations.append(preference_data[i])
                    existing_full_keys.add(preference_data[i]["pref_key"])
            
            if new_combinations:
                # Batch save new combinations
                batch_save_combinations_optimized(new_combinations)
                added_count = len(new_combinations)
                logger.info(f"[OptimizedProcessor] Generated {added_count} preference entries from base keys")
        
        return added_count
    
    def process_combinations_batch_optimized(self, work_items: List, config: Dict) -> int:
        """Process multiple combinations in an optimized batch."""
        if not work_items:
            return 0
        
        # Load database cache once for the entire batch
        db_cache = load_full_cache_from_db_optimized()
        full_combos = db_cache.get("combinations", {})
        
        if not isinstance(full_combos, dict):
            full_combos = {}
        
        existing_full_keys = set(full_combos.keys())
        
        total_added = 0
        
        # Process work items in smaller batches to manage memory
        batch_size = 10
        for i in range(0, len(work_items), batch_size):
            batch = work_items[i:i + batch_size]
            
            for work_item in batch:
                try:
                    added = self.process_single_vehicle_combination_optimized(
                        work_item.year,
                        work_item.make,
                        work_item.model,
                        work_item.trim,
                        work_item.drive,
                        full_combos,
                        existing_full_keys,
                        config
                    )
                    total_added += added
                    
                    if added > 0:
                        logger.info(f"[OptimizedProcessor] Processed {work_item.year} {work_item.make} {work_item.model} {work_item.trim} {work_item.drive}: +{added}")
                
                except Exception as e:
                    logger.error(f"[OptimizedProcessor] Error processing {work_item.year} {work_item.make} {work_item.model} {work_item.trim} {work_item.drive}: {e}")
            
            # Clean up thread-local database session periodically
            if i % (batch_size * 5) == 0:
                optimized_db_manager.close_thread_session()
        
        # Final cleanup
        optimized_db_manager.close_thread_session()
        
        logger.info(f"[OptimizedProcessor] Batch processing completed: {total_added} combinations added")
        return total_added

# Global optimized processor instance
optimized_processor = OptimizedCombinationProcessor()

# Optimized function for backward compatibility
def process_single_vehicle_combination_optimized(year: str, make: str, model: str, trim: str, drive: str,
                                               full_combos: Dict, existing_full_keys: set, config: Dict) -> int:
    """Optimized version of process_single_vehicle_combination."""
    return optimized_processor.process_single_vehicle_combination_optimized(
        year, make, model, trim, drive, full_combos, existing_full_keys, config
    )