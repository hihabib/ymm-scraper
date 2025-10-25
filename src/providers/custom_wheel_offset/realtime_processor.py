#!/usr/bin/env python3
"""
Real-time processor for Custom Wheel Offset scraper.
Integrates model/trim/drive scraping with fitment preferences processing,
saving each combination immediately to minimize memory usage.
"""

from typing import Dict, Set, Tuple, List, Optional
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import centralized logging
from .logging_config import init_module_logger

# Import error logging functionality
try:
    from services.repository_optimized import insert_error_log
except ImportError:
    try:
        from services.repository import insert_error_log
    except ImportError:
        def insert_error_log(source, context, message):
            pass

# Initialize logger for this module
logger = init_module_logger("realtime_processor")

from .vehicle_data_extractor import VehicleDataExtractor
from .fitment_preferences import get_fitment_preferences
from .vehicle_data_processor import get_vehicle_data_with_fallback
from .key_utils import make_full_key, make_full_pref_key
from .optimized_cache_ops import (
    batch_save_combinations_optimized,
    batch_check_combinations_exist_optimized,
    optimized_db_manager
)
from .config_manager import get_config
from .performance_monitor import MetricTracker, performance_monitor


class RealtimeProcessor:
    """
    Real-time processor that scrapes model/trim/drive data and processes
    fitment preferences immediately, saving to database to minimize memory usage.
    """
    
    def __init__(self, max_workers: int = None):
        """Initialize real-time processor."""
        self.config = get_config()
        self.max_workers = max_workers or self.config.get('workers', 200)
        self.processed_combinations = set()
        self.total_added = 0
        self.batch_size = 50
        self.pending_combinations = []
        self.lock = threading.Lock()
        
    def process_year_make_realtime(self, existing_year_make: Set[Tuple[str, str]]) -> int:
        """
        Process year/make combinations in real-time, scraping and saving immediately.
        Returns the total number of new entries added.
        """
        logger.info(f"Starting real-time processing for {len(existing_year_make)} year/make combinations")
        start_time = time.time()
        total_added = 0
        
        with MetricTracker("process_year_make_realtime"):
            try:
                # Process year/make combinations with controlled parallelism
                max_year_make_workers = min(8, len(existing_year_make))
                
                with ThreadPoolExecutor(max_workers=max_year_make_workers, thread_name_prefix="RealtimeYM") as executor:
                    future_to_year_make = {
                        executor.submit(self._process_single_year_make, year, make): (year, make)
                        for year, make in sorted(existing_year_make)
                    }
                    
                    for future in as_completed(future_to_year_make):
                        year, make = future_to_year_make[future]
                        try:
                            added = future.result()
                            total_added += added
                            logger.info(f"Completed {year} {make}: +{added} combinations")
                        except Exception as e:
                            logger.error(f"Error processing {year} {make}: {e}")
                            self._log_error("year_make_processing", {"year": year, "make": make}, str(e))
                
                # Save any remaining pending combinations
                self._flush_pending_combinations()
                
                elapsed_time = time.time() - start_time
                logger.info(f"Real-time processing completed:")
                logger.info(f"  Total added: {total_added}")
                logger.info(f"  Time elapsed: {elapsed_time:.2f} seconds")
                logger.info(f"  Average rate: {total_added / elapsed_time:.2f} combinations/second")
                
                return total_added
                
            except Exception as e:
                logger.error(f"Fatal error during real-time processing: {e}")
                self._log_error("realtime_processing_fatal", {}, str(e))
                raise
    
    def _process_single_year_make(self, year: str, make: str) -> int:
        """Process a single year/make combination in real-time."""
        thread_id = threading.current_thread().ident
        added_count = 0
        
        try:
            # Start performance tracking for this thread
            if thread_id not in performance_monitor._thread_metrics:
                performance_monitor.start_thread_tracking(thread_id)
            
            with MetricTracker("process_year_make", {"year_make": f"{year} {make}"}):
                # Get models for this year/make using VehicleDataExtractor
                year_make_extractor = VehicleDataExtractor(year=year, make=make)
                models = year_make_extractor.get_models()
                if not models:
                    logger.warning(f"No models found for {year} {make}")
                    return 0
                
                logger.debug(f"Processing {len(models)} models for {year} {make}")
                
                # Process each model in real-time
                for model in models:
                    try:
                        model_added = self._process_single_model_realtime(year, make, model)
                        added_count += model_added
                    except Exception as e:
                        logger.error(f"Error processing model {year} {make} {model}: {e}")
                        self._log_error("model_processing", {
                            "year": year, "make": make, "model": model
                        }, str(e))
                
                return added_count
                
        except Exception as e:
            logger.error(f"Error processing year/make {year} {make}: {e}")
            self._log_error("year_make_processing", {"year": year, "make": make}, str(e))
            return added_count
    
    def _process_single_model_realtime(self, year: str, make: str, model: str) -> int:
        """Process a single model in real-time, getting trims and drives immediately."""
        added_count = 0
        
        try:
            with MetricTracker("process_model", {"model": f"{year} {make} {model}"}):
                # Get trims for this model using VehicleDataExtractor
                year_make_model_extractor = VehicleDataExtractor(year=year, make=make, model=model)
                trims = year_make_model_extractor.get_trims()
                if not trims:
                    logger.warning(f"No trims found for {year} {make} {model}")
                    return 0
                
                # Process each trim in real-time
                for trim in trims:
                    try:
                        trim_added = self._process_single_trim_realtime(year, make, model, trim)
                        added_count += trim_added
                    except Exception as e:
                        logger.error(f"Error processing trim {year} {make} {model} {trim}: {e}")
                        self._log_error("trim_processing", {
                            "year": year, "make": make, "model": model, "trim": trim
                        }, str(e))
                
                return added_count
                
        except Exception as e:
            logger.error(f"Error processing model {year} {make} {model}: {e}")
            self._log_error("model_processing", {
                "year": year, "make": make, "model": model
            }, str(e))
            return added_count
    
    def _process_single_trim_realtime(self, year: str, make: str, model: str, trim: str) -> int:
        """Process a single trim in real-time, getting drives and processing fitment preferences immediately."""
        added_count = 0
        
        try:
            with MetricTracker("process_trim", {"trim": f"{year} {make} {model} {trim}"}):
                # Get drive types for this trim using VehicleDataExtractor
                year_make_model_trim_extractor = VehicleDataExtractor(year=year, make=make, model=model, trim=trim)
                drives = year_make_model_trim_extractor.get_drives()
                if not drives:
                    logger.warning(f"No drives found for {year} {make} {model} {trim}")
                    return 0
                
                # Process each drive in real-time with fitment preferences
                for drive in drives:
                    try:
                        drive_added = self._process_single_drive_with_preferences_realtime(
                            year, make, model, trim, drive
                        )
                        added_count += drive_added
                    except Exception as e:
                        logger.error(f"Error processing drive {year} {make} {model} {trim} {drive}: {e}")
                        self._log_error("drive_processing", {
                            "year": year, "make": make, "model": model, "trim": trim, "drive": drive
                        }, str(e))
                
                return added_count
                
        except Exception as e:
            logger.error(f"Error processing trim {year} {make} {model} {trim}: {e}")
            self._log_error("trim_processing", {
                "year": year, "make": make, "model": model, "trim": trim
            }, str(e))
            return added_count
    
    def _process_single_drive_with_preferences_realtime(self, year: str, make: str, model: str, 
                                                       trim: str, drive: str) -> int:
        """
        Process a single drive combination with fitment preferences in real-time.
        This is the core method that integrates vehicle data scraping with preference processing.
        """
        added_count = 0
        
        try:
            with MetricTracker("process_drive_with_preferences", {
                "combination": f"{year} {make} {model} {trim} {drive}"
            }):
                # Get vehicle data immediately
                vehicle_data = get_vehicle_data_with_fallback(year, make, model, trim, drive, self.config)
                vt = vehicle_data["vehicleType"]
                bp = vehicle_data["boltpattern"]
                drchassisid = vehicle_data["drchassisid"]
                
                # Create base combination key
                base_key = make_full_key(year, make, model, trim, drive, vt, bp, drchassisid)
                
                # Check if base combination already exists
                existing_combinations = batch_check_combinations_exist_optimized([
                    (year, make, model, trim, drive, vt, drchassisid, "", "", "")
                ])
                
                base_exists = len(existing_combinations) > 0
                
                if not base_exists:
                    # Add base combination to pending list
                    self._add_to_pending_batch({
                        "year": year,
                        "make": make,
                        "model": model,
                        "trim": trim,
                        "drive": drive,
                        "vehicleType": vt,
                        "boltpattern": bp,
                        "drchassisid": drchassisid,
                        "suspension": "",
                        "modification": "",
                        "rubbing": "",
                        "processed": False
                    })
                    added_count += 1
                
                # Process fitment preferences if enabled
                if self.config.get("pref_fetch", True):
                    prefs_added = self._process_fitment_preferences_realtime(
                        year, make, model, trim, drive, vt, bp, drchassisid
                    )
                    added_count += prefs_added
                
                return added_count
                
        except Exception as e:
            logger.error(f"Error processing drive with preferences {year} {make} {model} {trim} {drive}: {e}")
            self._log_error("drive_preferences_processing", {
                "year": year, "make": make, "model": model, "trim": trim, "drive": drive
            }, str(e))
            return added_count
    
    def _process_fitment_preferences_realtime(self, year: str, make: str, model: str, 
                                            trim: str, drive: str, vt: str, bp: str, 
                                            drchassisid: str) -> int:
        """Process fitment preferences for a vehicle combination in real-time."""
        added_count = 0
        
        try:
            with MetricTracker("process_fitment_preferences"):
                # Get fitment preferences
                prefs = get_fitment_preferences(vt or "car")
                if not prefs:
                    logger.warning(f"No fitment preferences found for vehicle type: {vt}")
                    return 0
                
                # Prepare combinations to check for existence
                combinations_to_check = []
                preference_data = []
                
                for p in prefs:
                    susp = p.get("suspension", "")
                    mod = p.get("modification", "")
                    rub = p.get("rubbing", "")
                    
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
                        "processed": False
                    })
                
                # Batch check existing combinations
                existing_combinations = batch_check_combinations_exist_optimized(combinations_to_check)
                existing_keys = {
                    make_full_pref_key(combo[0], combo[1], combo[2], combo[3], combo[4], 
                                     combo[5], bp, combo[6], combo[7], combo[8], combo[9])
                    for combo in existing_combinations
                }
                
                # Add new combinations to pending batch
                for pref_data in preference_data:
                    pref_key = make_full_pref_key(
                        pref_data["year"], pref_data["make"], pref_data["model"],
                        pref_data["trim"], pref_data["drive"], pref_data["vehicleType"],
                        pref_data["boltpattern"], pref_data["drchassisid"],
                        pref_data["suspension"], pref_data["modification"], pref_data["rubbing"]
                    )
                    
                    if pref_key not in existing_keys:
                        self._add_to_pending_batch(pref_data)
                        added_count += 1
                
                return added_count
                
        except Exception as e:
            logger.error(f"Error processing fitment preferences for {year} {make} {model} {trim} {drive}: {e}")
            self._log_error("fitment_preferences_processing", {
                "year": year, "make": make, "model": model, "trim": trim, "drive": drive
            }, str(e))
            return added_count
    
    def _add_to_pending_batch(self, combination_data: Dict) -> None:
        """Add combination to pending batch and save if batch is full."""
        with self.lock:
            self.pending_combinations.append(combination_data)
            
            if len(self.pending_combinations) >= self.batch_size:
                self._save_pending_batch()
    
    def _save_pending_batch(self) -> None:
        """Save pending combinations to database."""
        if not self.pending_combinations:
            return
        
        try:
            with MetricTracker("save_batch", {"batch_size": len(self.pending_combinations)}):
                batch_save_combinations_optimized(self.pending_combinations)
                logger.debug(f"Saved batch of {len(self.pending_combinations)} combinations")
                performance_monitor.increment_counter("batches_saved")
                performance_monitor.increment_counter("combinations_saved", len(self.pending_combinations))
                self.pending_combinations.clear()
        except Exception as e:
            logger.error(f"Error saving batch: {e}")
            self._log_error("batch_save", {"batch_size": len(self.pending_combinations)}, str(e))
            # Don't clear the batch on error - will retry later
    
    def _flush_pending_combinations(self) -> None:
        """Flush any remaining pending combinations."""
        with self.lock:
            if self.pending_combinations:
                logger.info(f"Flushing {len(self.pending_combinations)} remaining combinations")
                self._save_pending_batch()
    
    def _log_error(self, error_type: str, context: Dict, message: str) -> None:
        """Log error to database."""
        try:
            context.update({
                'error_type': error_type,
                'processing_stage': 'realtime_processing'
            })
            insert_error_log('custom_wheel_offset_realtime', context, message)
        except Exception as db_error:
            logger.error(f"Failed to log error to database: {db_error}")
    
    def get_statistics(self) -> Dict:
        """Get processing statistics."""
        return {
            "total_added": self.total_added,
            "pending_combinations": len(self.pending_combinations),
            "batch_size": self.batch_size
        }
    
    def shutdown(self) -> None:
        """Shutdown processor and flush remaining data."""
        logger.info("Shutting down RealtimeProcessor")
        self._flush_pending_combinations()
        logger.info("RealtimeProcessor shutdown complete")


# Global instance for easy access
realtime_processor = RealtimeProcessor()