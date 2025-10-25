"""
Integrated processor that combines vehicle data scraping with fitment preference processing
in a single step per vehicle combination, eliminating the need for two-step database saving.
"""

import time
import logging
from typing import Dict, List, Set, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Import database and network components
try:
    from .database_manager import DatabaseManager
except ImportError:
    DatabaseManager = None

try:
    from .performance_monitor import performance_monitor
except ImportError:
    performance_monitor = None

try:
    from .config import config
except ImportError:
    config = {}

logger = logging.getLogger(__name__)


class IntegratedProcessor:
    """
    Processes vehicle combinations by integrating model/trim/drive scraping
    with fitment preference processing in a single step per combination.
    """
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        # Initialize components only if available
        self.scraper = None
        self.db_manager = DatabaseManager() if DatabaseManager else None
        self.stats_lock = Lock()
        self.stats = {
            'processed_combinations': 0,
            'new_base_combinations': 0,
            'new_preference_entries': 0,
            'errors': 0,
            'skipped_existing': 0
        }
        
    def process_combinations_integrated(self, existing_combinations: Dict) -> int:
        """
        Process all vehicle combinations using integrated approach.
        Each combination is processed completely in one step.
        
        Args:
            existing_combinations: Dictionary of existing year/make combinations
            
        Returns:
            Total number of new preference entries added
        """
        start_time = time.time()
        logger.info("Starting integrated combination processing")
        
        # Generate work items for all combinations
        work_items = self._generate_integrated_work_items(existing_combinations)
        total_items = len(work_items)
        
        if total_items == 0:
            logger.info("No work items to process")
            return 0
            
        logger.info(f"Generated {total_items} integrated work items")
        
        # Process work items using thread pool
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all work items
            future_to_item = {
                executor.submit(self._process_integrated_work_item, item): item 
                for item in work_items
            }
            
            # Process completed futures
            completed = 0
            for future in as_completed(future_to_item):
                completed += 1
                item = future_to_item[future]
                
                try:
                    result = future.result()
                    if result:
                        with self.stats_lock:
                            self.stats['processed_combinations'] += 1
                            if result.get('new_base_combination'):
                                self.stats['new_base_combinations'] += 1
                            self.stats['new_preference_entries'] += result.get('new_preferences', 0)
                    else:
                        with self.stats_lock:
                            self.stats['skipped_existing'] += 1
                            
                except Exception as e:
                    logger.error(f"Error processing work item {item}: {e}")
                    with self.stats_lock:
                        self.stats['errors'] += 1
                
                # Log progress every 100 items or at completion
                if completed % 100 == 0 or completed == total_items:
                    progress = (completed / total_items) * 100
                    logger.info(f"Progress: {completed}/{total_items} ({progress:.1f}%)")
        
        # Log final statistics
        elapsed_time = time.time() - start_time
        self._log_final_stats(elapsed_time)
        
        return self.stats['new_preference_entries']
    
    def _generate_integrated_work_items(self, existing_combinations: Dict) -> List[Dict]:
        """
        Generate work items for integrated processing.
        Each work item represents a complete vehicle combination to process.
        """
        work_items = []
        
        for year_make_key, year_make_data in existing_combinations.items():
            year, make = year_make_key.split('|')
            
            # Get models for this year/make
            models = year_make_data.get('models', {})
            
            for model_name, model_data in models.items():
                # Get trims for this model
                trims = model_data.get('trims', {})
                
                for trim_name, trim_data in trims.items():
                    # Get drives for this trim
                    drives = trim_data.get('drives', {})
                    
                    for drive_name in drives.keys():
                        # Create work item for this complete combination
                        work_item = {
                            'year': year,
                            'make': make,
                            'model': model_name,
                            'trim': trim_name,
                            'drive': drive_name,
                            'combination_key': f"{year}|{make}|{model_name}|{trim_name}|{drive_name}"
                        }
                        work_items.append(work_item)
        
        logger.info(f"Generated {len(work_items)} integrated work items")
        return work_items
    
    def _process_integrated_work_item(self, work_item: Dict) -> Optional[Dict]:
        """
        Process a single work item using integrated approach.
        This combines vehicle data processing with fitment preference processing.
        
        Args:
            work_item: Dictionary containing year, make, model, trim, drive
            
        Returns:
            Dictionary with processing results or None if skipped
        """
        try:
            year = work_item['year']
            make = work_item['make']
            model = work_item['model']
            trim = work_item['trim']
            drive = work_item['drive']
            combination_key = work_item['combination_key']
            
            # Step 1: Check if base combination already exists
            existing_combination = self.db_manager.get_combination_by_key(combination_key)
            
            if existing_combination:
                # Base combination exists, check if we need to process preferences
                existing_preferences = self.db_manager.get_preferences_for_combination(existing_combination['id'])
                
                if existing_preferences:
                    # Both base combination and preferences exist, skip
                    return None
                else:
                    # Base combination exists but no preferences, process preferences only
                    return self._process_preferences_for_existing_combination(existing_combination)
            
            # Step 2: Base combination doesn't exist, create it and process preferences
            return self._process_new_combination_integrated(work_item)
            
        except Exception as e:
            logger.error(f"Error in integrated processing for {work_item.get('combination_key', 'unknown')}: {e}")
            return None
    
    def _process_new_combination_integrated(self, work_item: Dict) -> Dict:
        """
        Process a completely new combination by creating base combination
        and immediately processing its fitment preferences.
        """
        year = work_item['year']
        make = work_item['make']
        model = work_item['model']
        trim = work_item['trim']
        drive = work_item['drive']
        
        # Step 1: Create base combination entry
        base_combination_data = {
            'year': year,
            'make': make,
            'model': model,
            'trim': trim,
            'drive': drive,
            'combination_key': work_item['combination_key']
        }
        
        # Save base combination to database
        combination_id = self.db_manager.save_base_combination(base_combination_data)
        
        if not combination_id:
            logger.error(f"Failed to save base combination: {work_item['combination_key']}")
            return {'new_base_combination': False, 'new_preferences': 0}
        
        # Step 2: Immediately process fitment preferences for this combination
        preferences_result = self._process_fitment_preferences_integrated(
            combination_id, year, make, model, trim, drive
        )
        
        return {
            'new_base_combination': True,
            'new_preferences': preferences_result.get('new_preferences', 0),
            'combination_id': combination_id
        }
    
    def _process_preferences_for_existing_combination(self, existing_combination: Dict) -> Dict:
        """
        Process fitment preferences for an existing base combination.
        """
        combination_id = existing_combination['id']
        year = existing_combination['year']
        make = existing_combination['make']
        model = existing_combination['model']
        trim = existing_combination['trim']
        drive = existing_combination['drive']
        
        preferences_result = self._process_fitment_preferences_integrated(
            combination_id, year, make, model, trim, drive
        )
        
        return {
            'new_base_combination': False,
            'new_preferences': preferences_result.get('new_preferences', 0),
            'combination_id': combination_id
        }
    
    def _process_fitment_preferences_integrated(self, combination_id: int, year: str, make: str, 
                                             model: str, trim: str, drive: str) -> Dict:
        """
        Process fitment preferences for a specific combination.
        This fetches PHPSESSID, gets fitment data, and saves preferences.
        """
        try:
            # Get PHPSESSID for API calls
            phpsessid = self.scraper.get_phpsessid()
            if not phpsessid:
                logger.error(f"Failed to get PHPSESSID for {year}|{make}|{model}|{trim}|{drive}")
                return {'new_preferences': 0}
            
            # Get fitment preferences from API
            fitment_data = self.scraper.get_fitment_preferences(
                year=year,
                make=make,
                model=model,
                trim=trim,
                drive=drive,
                phpsessid=phpsessid
            )
            
            if not fitment_data:
                logger.warning(f"No fitment data found for {year}|{make}|{model}|{trim}|{drive}")
                return {'new_preferences': 0}
            
            # Process and save fitment preferences
            new_preferences = 0
            preference_entries = []
            
            for preference in fitment_data:
                # Create preference entry
                preference_entry = {
                    'combination_id': combination_id,
                    'wheel_diameter': preference.get('wheel_diameter'),
                    'wheel_width': preference.get('wheel_width'),
                    'wheel_offset': preference.get('wheel_offset'),
                    'tire_size': preference.get('tire_size'),
                    'bolt_pattern': preference.get('bolt_pattern'),
                    'center_bore': preference.get('center_bore'),
                    'additional_data': preference.get('additional_data', {})
                }
                preference_entries.append(preference_entry)
            
            # Batch save preferences
            if preference_entries:
                saved_count = self.db_manager.batch_save_preferences(preference_entries)
                new_preferences = saved_count
                logger.info(f"Saved {saved_count} preferences for {year}|{make}|{model}|{trim}|{drive}")
            
            return {'new_preferences': new_preferences}
            
        except Exception as e:
            logger.error(f"Error processing preferences for {year}|{make}|{model}|{trim}|{drive}: {e}")
            return {'new_preferences': 0}
    
    def _log_final_stats(self, elapsed_time: float):
        """Log final processing statistics."""
        logger.info("=== Integrated Processing Complete ===")
        logger.info(f"Total processing time: {elapsed_time:.2f} seconds")
        logger.info(f"Processed combinations: {self.stats['processed_combinations']}")
        logger.info(f"New base combinations: {self.stats['new_base_combinations']}")
        logger.info(f"New preference entries: {self.stats['new_preference_entries']}")
        logger.info(f"Skipped existing: {self.stats['skipped_existing']}")
        logger.info(f"Errors: {self.stats['errors']}")
        
        if self.stats['processed_combinations'] > 0:
            avg_time = elapsed_time / self.stats['processed_combinations']
            logger.info(f"Average time per combination: {avg_time:.3f} seconds")
    
    def shutdown(self):
        """Clean up resources."""
        # Gracefully shutdown components if they exist
        if self.scraper and hasattr(self.scraper, 'close'):
            self.scraper.close()
        if self.db_manager and hasattr(self.db_manager, 'close'):
            self.db_manager.close()
        logger.info("IntegratedProcessor shutdown complete")
        logger.info("Integrated processor shut down")