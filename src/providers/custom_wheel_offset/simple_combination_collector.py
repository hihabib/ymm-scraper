"""
Simple combination collector for Custom Wheel Offset that bypasses captcha requirements.
This module focuses only on collecting year/make/model/trim/drive combinations
without the need for session management or captcha solving.
"""
import json
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from .logging_config import init_module_logger

logger = init_module_logger(__name__)

# Import the utility functions that use the enthusiastenterprises.us API
try:
    from .vehicle_data_extractor import VehicleDataExtractor
except ImportError:
    from utils import get_years, get_makes, get_models, get_trims, get_drive_types


class SimpleCombinationCollector:
    """Simplified collector for vehicle combinations without captcha dependencies."""
    
    def __init__(self, cache_expiry_days: int = 7):
        self.cache_expiry_days = cache_expiry_days
        self.cache_file = Path("data/custom_wheel_offset_combinations_cache.json")
        self.full_cache_file = Path("data/custom_wheel_offset_full_combinations_cache.json")
        self.shutdown_requested = threading.Event()
        self.combinations_lock = threading.Lock()
        
    def collect_year_make_combinations(self, target_year: str = None) -> List[Tuple[str, str]]:
        """Collect year/make combinations, optionally filtered by year."""
        logger.info("[SimpleCombinationCollector] Collecting year/make combinations...")
        
        # Get all years
        years = get_years()
        if not years:
            logger.warning("[SimpleCombinationCollector] No years found")
            return []
        
        # Filter for specific year if requested
        if target_year:
            years = [year for year in years if year == target_year]
            logger.info(f"[SimpleCombinationCollector] Filtered to year {target_year}: {len(years)} years")
        
        combinations = []
        
        for year in years:
            if self.shutdown_requested.is_set():
                break
                
            logger.info(f"[SimpleCombinationCollector] Processing year {year}...")
            makes = get_makes(year)
            
            for make in makes:
                if self.shutdown_requested.is_set():
                    break
                combinations.append((year, make))
        
        logger.info(f"[SimpleCombinationCollector] Collected {len(combinations)} year/make combinations")
        return combinations
    
    def save_combinations_to_cache(self, combinations: List[Tuple[str, str]]):
        """Save year/make combinations to cache file."""
        try:
            # Ensure data directory exists
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            
            cache_data = {
                'created_at': datetime.now().isoformat(),
                'total_combinations': len(combinations),
                'combinations': [{'year': year, 'make': make} for year, make in combinations]
            }
            
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"[SimpleCombinationCollector] Saved {len(combinations)} combinations to cache: {self.cache_file}")
            
        except Exception as e:
            logger.error(f"[SimpleCombinationCollector] Error saving cache: {e}")
    
    def load_cached_combinations(self, target_year: str = None) -> Optional[List[Tuple[str, str]]]:
        """Load year/make combinations from cache if valid."""
        try:
            if not self.cache_file.exists():
                logger.info("[SimpleCombinationCollector] No cache file found")
                return None
            
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # Check cache expiry
            cache_date = datetime.fromisoformat(cache_data.get('created_at', ''))
            expiry_date = cache_date + timedelta(days=self.cache_expiry_days)
            
            if datetime.now() > expiry_date:
                logger.info(f"[SimpleCombinationCollector] Cache expired (created: {cache_date.strftime('%Y-%m-%d')}, expired: {expiry_date.strftime('%Y-%m-%d')})")
                return None
            
            combinations = [(item['year'], item['make']) for item in cache_data.get('combinations', [])]
            
            # Filter for specific year if requested
            if target_year:
                combinations = [(year, make) for year, make in combinations if year == target_year]
                logger.info(f"[SimpleCombinationCollector] Loaded {len(combinations)} combinations from cache (created: {cache_date.strftime('%Y-%m-%d %H:%M:%S')}, filtered for {target_year})")
            else:
                logger.info(f"[SimpleCombinationCollector] Loaded {len(combinations)} combinations from cache (created: {cache_date.strftime('%Y-%m-%d %H:%M:%S')})")
            
            return combinations
            
        except Exception as e:
            logger.error(f"[SimpleCombinationCollector] Error loading cache: {e}")
            return None
    
    def process_year_make_combination(self, year_make_tuple: Tuple[str, str]) -> List[dict]:
        """Process a single year/make combination to get all nested combinations."""
        year, make = year_make_tuple
        local_combinations = []
        
        try:
            # Check for shutdown signal
            if self.shutdown_requested.is_set():
                return []
            
            thread_id = str(threading.current_thread().ident)
            logger.info(f"[Thread-{thread_id}] Processing {year} {make}...")
            
            # Get models for this year/make using VehicleDataExtractor
            year_make_extractor = VehicleDataExtractor(year=year, make=make)
            models = year_make_extractor.get_models()
            if not models:
                logger.warning(f"[Thread-{thread_id}] No models found for {year} {make}")
                return []
            
            # Process models with threading
            with ThreadPoolExecutor(max_workers=5, thread_name_prefix=f"Model-{year}-{make}") as model_executor:
                model_futures = []
                
                for model in models:
                    if self.shutdown_requested.is_set():
                        break
                    future = model_executor.submit(self.process_model_combinations, year, make, model)
                    model_futures.append(future)
                
                # Collect results
                for future in as_completed(model_futures):
                    if self.shutdown_requested.is_set():
                        break
                    try:
                        model_combinations = future.result()
                        local_combinations.extend(model_combinations)
                    except Exception as e:
                        logger.error(f"[Thread-{thread_id}] Error processing model: {e}")
            
            logger.info(f"[Thread-{thread_id}] Completed {year} {make}: {len(local_combinations)} combinations")
            return local_combinations
            
        except Exception as e:
            thread_id = str(threading.current_thread().ident)
            logger.error(f"[Thread-{thread_id}] Error processing {year} {make}: {e}")
            return []
    
    def process_model_combinations(self, year: str, make: str, model: str) -> List[dict]:
        """Process a single model to get all trim/drive combinations."""
        combinations = []
        
        try:
            # Check for shutdown signal
            if self.shutdown_requested.is_set():
                return []
            
            # Get trims for this year/make/model using VehicleDataExtractor
            year_make_model_extractor = VehicleDataExtractor(year=year, make=make, model=model)
            trims = year_make_model_extractor.get_trims()
            if not trims:
                return []
            
            # Process trims with threading
            with ThreadPoolExecutor(max_workers=3, thread_name_prefix=f"Trim-{year}-{make}-{model}") as trim_executor:
                trim_futures = []
                
                for trim in trims:
                    if self.shutdown_requested.is_set():
                        break
                    future = trim_executor.submit(self.process_trim_combinations, year, make, model, trim)
                    trim_futures.append(future)
                
                # Collect results
                for future in as_completed(trim_futures):
                    if self.shutdown_requested.is_set():
                        break
                    try:
                        trim_combinations = future.result()
                        combinations.extend(trim_combinations)
                    except Exception as e:
                        logger.error(f"Error processing trim: {e}")
            
            return combinations
            
        except Exception as e:
            logger.error(f"Error processing model {year} {make} {model}: {e}")
            return []
    
    def process_trim_combinations(self, year: str, make: str, model: str, trim: str) -> List[dict]:
        """Process a single trim to get all drive combinations."""
        combinations = []
        
        try:
            # Check for shutdown signal
            if self.shutdown_requested.is_set():
                return []
            
            # Get drive types for this year/make/model/trim using VehicleDataExtractor
            year_make_model_trim_extractor = VehicleDataExtractor(year=year, make=make, model=model, trim=trim)
            drive_types = year_make_model_trim_extractor.get_drives()
            if not drive_types:
                return []
            
            for drive in drive_types:
                combination = {
                    'year': year,
                    'make': make,
                    'model': model,
                    'trim': trim,
                    'drive': drive,
                    'processed': False  # Track processing status
                }
                combinations.append(combination)
            
            return combinations
            
        except Exception as e:
            logger.error(f"Error processing trim {year} {make} {model} {trim}: {e}")
            return []
    
    def cache_all_combinations(self, target_year: str = None, force_refresh: bool = False) -> List[dict]:
        """Cache all year/make/model/trim/drive combinations with multi-threading."""
        
        # Try to load existing full cache first
        if not force_refresh and self.full_cache_file.exists():
            try:
                with open(self.full_cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                
                # Check cache expiry
                cache_date = datetime.fromisoformat(cache_data.get('created_at', ''))
                expiry_date = cache_date + timedelta(days=self.cache_expiry_days)
                
                if datetime.now() <= expiry_date:
                    combinations = cache_data.get('combinations', [])
                    
                    # Filter for specific year if requested
                    if target_year:
                        combinations = [c for c in combinations if c.get('year') == target_year]
                        logger.info(f"[SimpleCombinationCollector] Loaded {len(combinations)} full combinations from cache (filtered for {target_year})")
                    else:
                        logger.info(f"[SimpleCombinationCollector] Loaded {len(combinations)} full combinations from cache")
                    
                    return combinations
                else:
                    logger.info(f"[SimpleCombinationCollector] Full combinations cache expired, refreshing...")
            except Exception as e:
                logger.error(f"[SimpleCombinationCollector] Error loading full combinations cache: {e}")
        
        # Build full combinations cache with multi-threading
        logger.info("[SimpleCombinationCollector] Building full combinations cache with multi-threading...")
        all_combinations = []
        
        # Get year/make combinations (lightweight)
        year_make_combinations = self.load_cached_combinations(target_year)
        if year_make_combinations is None:
            year_make_combinations = self.collect_year_make_combinations(target_year)
            if year_make_combinations:
                self.save_combinations_to_cache(year_make_combinations)
        
        if not year_make_combinations:
            return []
        
        total_year_makes = len(year_make_combinations)
        processed_count = 0
        
        logger.info(f"[SimpleCombinationCollector] Processing {total_year_makes} year/make combinations with multi-threading...")
        
        # Process year/make combinations with threading
        with ThreadPoolExecutor(max_workers=8, thread_name_prefix="YearMake") as executor:
            future_to_year_make = {}
            
            for year_make_tuple in year_make_combinations:
                if self.shutdown_requested.is_set():
                    break
                future = executor.submit(self.process_year_make_combination, year_make_tuple)
                future_to_year_make[future] = year_make_tuple
            
            # Collect results as they complete
            for future in as_completed(future_to_year_make):
                if self.shutdown_requested.is_set():
                    break
                
                year_make_tuple = future_to_year_make[future]
                try:
                    combinations = future.result()
                    
                    # Thread-safe update of results
                    with self.combinations_lock:
                        all_combinations.extend(combinations)
                        processed_count += 1
                    
                    year, make = year_make_tuple
                    logger.info(f"[SimpleCombinationCollector] Progress: {processed_count}/{total_year_makes} - {year} {make}: {len(combinations)} combinations")
                    
                except Exception as e:
                    year, make = year_make_tuple
                    logger.error(f"[SimpleCombinationCollector] Error processing {year} {make}: {e}")
                    with self.combinations_lock:
                        processed_count += 1
        
        # Save full combinations to cache
        try:
            self.full_cache_file.parent.mkdir(parents=True, exist_ok=True)
            
            cache_data = {
                'created_at': datetime.now().isoformat(),
                'total_combinations': len(all_combinations),
                'target_year': target_year,
                'combinations': all_combinations
            }
            
            with open(self.full_cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"[SimpleCombinationCollector] Saved {len(all_combinations)} full combinations to cache: {self.full_cache_file}")
            
        except Exception as e:
            logger.error(f"[SimpleCombinationCollector] Error saving full combinations cache: {e}")
        
        return all_combinations


def main():
    """Test the simple combination collector."""
    collector = SimpleCombinationCollector()
    
    # Test with year 2026 only
    logger.info("=== Testing Simple Combination Collector for Year 2026 ===")
    combinations = collector.cache_all_combinations(target_year="2026", force_refresh=True)
    
    logger.info(f"\nTotal combinations found: {len(combinations)}")
    
    # Show some examples
    if combinations:
        logger.info("\nFirst 5 combinations:")
        for i, combo in enumerate(combinations[:5]):
            logger.info(f"  {i+1}. {combo['year']} {combo['make']} {combo['model']} {combo['trim']} {combo['drive']}")


if __name__ == "__main__":
    main()