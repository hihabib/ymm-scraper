#!/usr/bin/env python3
"""
Custom Wheel Offset scraper V2 - Simplified version
Single session, no multithreading, no resume functionality
Iterates through years, makes, models, trims, and drive types.
Uses existing utility functions for session management, captcha solving, and data persistence.
"""

import time
import sys
import os
import json
import threading
import signal
from pathlib import Path
from typing import Optional, List, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from threading import Condition, Lock, Event

# Determine if we're running as a script or module
if __name__ == "__main__":
    # Running as script - set up paths
    SCRIPT_DIR = Path(__file__).resolve().parent
    SRC_DIR = SCRIPT_DIR.parents[1]  # Go up to src directory
    PROJECT_ROOT = SRC_DIR.parent    # Go up to project root
    
    # Add both src and project root to path
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    
    # Change working directory to project root for relative file paths
    os.chdir(PROJECT_ROOT)

# Import existing utility functions
# Always use absolute imports to avoid relative import issues
SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Import centralized logging
from providers.custom_wheel_offset.logging_config import init_module_logger

# Initialize logger for this module
logger = init_module_logger("custom_wheel_offset_v2")

from providers.custom_wheel_offset.session_manager import get_shared_session, reset_shared_session
from providers.custom_wheel_offset.resolve_captcha import ensure_token_by_visiting_homepage, _load_saved_phpsessid, _load_saved_token
from providers.custom_wheel_offset.utils import get_years, get_makes, get_models, get_trims, get_drive_types
from providers.custom_wheel_offset.session_restart import handle_session_expired_error
from config.worker import CUSTOM_WHEEL_OFFSET_WORKERS

class CustomWheelOffsetScraperV2:
    """Simplified Custom Wheel Offset scraper with single session management using existing utilities."""
    
    def __init__(self):
        self.session = get_shared_session()
        self.processed_combinations: Set[Tuple[str, str, str, str, str]] = set()
        self.lock = threading.Lock()
        self.worker_count = CUSTOM_WHEEL_OFFSET_WORKERS
        self.cache_file = Path("data/custom_wheel_offset_combinations_cache.json")
        self.cache_expiry_days = 7  # Cache expires after 7 days
        
        # Thread coordination for error handling and restart scenarios
        self.restart_condition = Condition()  # Conditional variable for restart coordination
        self.restart_required = Event()       # Signal that system restart is required
        self.stop_all_threads = Event()       # Signal to stop all threads
        self.restart_in_progress = False      # Flag to track restart state
        self.restart_initiated_by = None      # Track which thread initiated restart
        
        # Signal handling for graceful shutdown
        self.shutdown_requested = Event()     # Signal for graceful shutdown (Ctrl+C)
        self.setup_signal_handlers()
        
        logger.info(f"Initialized scraper with {self.worker_count} workers")
        logger.info("Using shared session manager")
        
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown on Ctrl+C."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum} (Ctrl+C), initiating graceful shutdown...")
            self.shutdown_requested.set()
            self.stop_all_threads.set()
            logger.info("All threads will stop gracefully. Please wait...")
        
        # Register signal handler for SIGINT (Ctrl+C)
        signal.signal(signal.SIGINT, signal_handler)
        
        # Also handle SIGTERM for proper process termination
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, signal_handler)
    
    def initialize_session(self) -> bool:
        """Initialize session using existing complete session setup function."""
        logger.info("Initializing session using existing utilities...")
        
        try:
            # Use the existing complete session initialization from ensure_token_by_visiting_homepage
            # This function handles both PHPSESSID retrieval and captcha solving in one call
            # Note: This function returns None but performs the initialization and saves tokens
            ensure_token_by_visiting_homepage()
            
            # Check if initialization was successful by verifying saved tokens
            if self.check_session_status():
                logger.info("Session initialized successfully")
                return True
            else:
                logger.error("Failed to initialize session - tokens not found after initialization")
                return False
            
        except Exception as e:
            logger.error(f"Error initializing session: {e}")
            return False

    def initialize_lightweight_session(self) -> bool:
        """
        Initialize session with lightweight approach for cached combinations processing.
        Uses ymm-temp API to get PHPSESSID without visiting home page.
        Falls back to full initialization if needed.
        """
        logger.info("Initializing lightweight session for cached processing...")
        
        try:
            # First try to get PHPSESSID from ymm-temp API (lightweight approach)
            from .resolve_captcha import get_phpsessid_from_api
            # Use default parameters since we don't have specific vehicle context here
            phpsessid = get_phpsessid_from_api()
            
            if phpsessid:
                logger.info(f"Successfully obtained PHPSESSID from API: {phpsessid[:20]}...")
                
                # Check if we have a valid AWS WAF token
                token = _load_saved_token()
                if token:
                    logger.info(f"Found existing AWS WAF token: {token[:20]}...")
                    return True
                else:
                    logger.info("No AWS WAF token found, need full initialization")
            else:
                logger.info("Failed to get PHPSESSID from API, need full initialization")
            
            # Fall back to full initialization if lightweight approach fails
            logger.info("Falling back to full session initialization...")
            return self.initialize_session()
            
        except Exception as e:
            logger.error(f"Error in lightweight session initialization: {e}")
            logger.info("Falling back to full session initialization...")
            return self.initialize_session()
    
    def is_restart_required_error(self, error_msg: str, error_type: str = "") -> bool:
        """
        Determine if an error requires system restart.
        
        Args:
            error_msg: Error message to analyze
            error_type: Type of error (optional)
            
        Returns:
            True if system restart is required
        """
        restart_indicators = [
            "session expired",
            "invalid session", 
            "authentication failed",
            "unauthorized",
            "token expired",
            "captcha",
            "human verification",
            "aws waf",
            "forbidden",
            "access denied",
            "connection error",
            "timeout",
            "network error",
            "http error",
            "ssl error",
            "certificate error",
            # Database/session binding errors
            "is not bound to a session",
            "attribute refresh operation cannot proceed",
            "object is not bound to a session",
            "session is closed",
            "session has been closed",
            "detached instance",
            "instance is not persistent"
        ]
        
        error_lower = error_msg.lower()
        return any(indicator in error_lower for indicator in restart_indicators)

    def handle_restart_scenario(self, context: str = "unknown", thread_id: str = None) -> bool:
        """
        Handle system restart scenario using conditional variable to prevent race conditions.
        Only one thread will perform the restart operation.
        
        Args:
            context: Context where restart was triggered
            thread_id: ID of the thread triggering restart
            
        Returns:
            bool: True if this thread performed the restart, False if another thread already did
        """
        if thread_id is None:
            thread_id = str(threading.current_thread().ident)
            
        with self.restart_condition:
            # Check if restart is already in progress
            if self.restart_in_progress:
                print(f"[RESTART] Thread {thread_id} detected restart already in progress by {self.restart_initiated_by}, waiting...")
                # Wait for restart to complete
                while self.restart_in_progress:
                    self.restart_condition.wait()
                print(f"[RESTART] Thread {thread_id} detected restart completed, exiting gracefully")
                return False
            
            # This thread will handle the restart
            print(f"\n[RESTART] Thread {thread_id} initiating system restart from context: {context}")
            self.restart_in_progress = True
            self.restart_initiated_by = thread_id
            self.restart_required.set()
            self.stop_all_threads.set()
            
            # Notify all waiting threads that restart is starting
            self.restart_condition.notify_all()
        
        # Give other threads time to stop gracefully
        print(f"[RESTART] Thread {thread_id} signaling all threads to stop...")
        time.sleep(2)
        
        # Perform the actual restart
        print(f"[RESTART] Thread {thread_id} initiating system restart...")
        try:
            handle_session_expired_error(context)
            print(f"[RESTART] System restart completed by thread {thread_id}")
            
            # Mark restart as completed
            with self.restart_condition:
                self.restart_in_progress = False
                self.restart_initiated_by = None
                self.restart_condition.notify_all()
            
            return True
        except Exception as restart_error:
            print(f"[RESTART] Failed to restart system: {restart_error}")
            
            # Mark restart as failed/completed
            with self.restart_condition:
                self.restart_in_progress = False
                self.restart_initiated_by = None
                self.restart_condition.notify_all()
            
            raise

    def check_session_status(self) -> bool:
        """Check if we have valid session tokens."""
        phpsessid = _load_saved_phpsessid()
        token = _load_saved_token()
        
        if phpsessid and token:
            logger.info(f"Session status: PHPSESSID={phpsessid[:20]}..., Token={token[:20]}...")
            return True
        else:
            logger.info("Session status: Missing tokens")
            return False
    
    def refresh_session_if_needed(self) -> bool:
        """Refresh session if tokens are missing or expired using existing utilities."""
        if not self.check_session_status():
            logger.info("Session needs refresh, using existing session restart utilities")
            # Use existing session restart functionality instead of custom refresh
            try:
                handle_session_expired_error("session_refresh_needed")
                return self.check_session_status()
            except Exception as e:
                logger.error(f"Error during session restart: {e}")
                return False
        return True
    
    def process_year_make_combination(self, year: str, make: str) -> int:
        """Process all models/trims/drives for a year/make combination in a thread-safe manner."""
        processed_count = 0
        thread_id = str(threading.current_thread().ident)
        
        try:
            # Check for restart or shutdown signal at the beginning
            if self.stop_all_threads.is_set() or self.restart_required.is_set() or self.shutdown_requested.is_set():
                if self.shutdown_requested.is_set():
                    print(f"[Thread-{thread_id}] Shutdown signal detected (Ctrl+C), stopping immediately")
                else:
                    print(f"[Thread-{thread_id}] Restart signal detected, stopping immediately")
                return 0
            
            # Get models for this year/make using existing utility function
            models = get_models(year, make)
            if not models:
                print(f"[Thread-{thread_id}] No models found for {year} {make}, skipping")
                return 0

            print(f"[Thread-{thread_id}] Processing {len(models)} models for {year} {make}...")
            
            # Loop through models
            for model in models:
                # Check for restart or shutdown signal before each model
                if self.stop_all_threads.is_set() or self.restart_required.is_set() or self.shutdown_requested.is_set():
                    if self.shutdown_requested.is_set():
                        print(f"[Thread-{thread_id}] Shutdown signal detected (Ctrl+C) during model processing, stopping")
                    else:
                        print(f"[Thread-{thread_id}] Restart signal detected during model processing, stopping")
                    return processed_count
                
                # Get trims for this year/make/model using existing utility function
                trims = get_trims(year, make, model)
                if not trims:
                    print(f"[Thread-{thread_id}] No trims found for {year} {make} {model}, skipping")
                    continue
                
                # Loop through trims
                for trim in trims:
                    # Check for restart or shutdown signal before each trim
                    if self.stop_all_threads.is_set() or self.restart_required.is_set() or self.shutdown_requested.is_set():
                        if self.shutdown_requested.is_set():
                            print(f"[Thread-{thread_id}] Shutdown signal detected (Ctrl+C) during trim processing, stopping")
                        else:
                            print(f"[Thread-{thread_id}] Restart signal detected during trim processing, stopping")
                        return processed_count
                    
                    # Get drive types for this year/make/model/trim using existing utility function
                    drive_types = get_drive_types(year, make, model, trim)
                    if not drive_types:
                        print(f"[Thread-{thread_id}] No drive types found for {year} {make} {model} {trim}, skipping")
                        continue
                    
                    # Process each drive type
                    for drive in drive_types:
                        # Check for restart or shutdown signal before each drive type
                        if self.stop_all_threads.is_set() or self.restart_required.is_set() or self.shutdown_requested.is_set():
                            if self.shutdown_requested.is_set():
                                print(f"[Thread-{thread_id}] Shutdown signal detected (Ctrl+C) during drive processing, stopping")
                            else:
                                print(f"[Thread-{thread_id}] Restart signal detected during drive processing, stopping")
                            return processed_count
                        
                        combination = (year, make, model, trim, drive)
                        
                        # Check if already processed (thread-safe)
                        with self.lock:
                            if combination in self.processed_combinations:
                                continue  # Already processed, skip
                            self.processed_combinations.add(combination)
                        
                        # Get vehicle data to construct URL
                        try:
                            # Import based on execution context
                            # Add src directory to path for absolute imports
                            import sys
                            import os
                            if __name__ == "__main__":
                                src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
                                if src_path not in sys.path:
                                    sys.path.insert(0, src_path)
                                from providers.custom_wheel_offset.utils import get_vehicle_data
                                from providers.custom_wheel_offset.wheel_size import get_parsed_data_with_saved_token
                                from services.repository_optimized import insert_custom_wheel_offset_ymm, insert_custom_wheel_offset_data
                            else:
                                from .utils import get_vehicle_data
                                from .wheel_size import get_parsed_data_with_saved_token
                                from ...services.repository_optimized import insert_custom_wheel_offset_ymm, insert_custom_wheel_offset_data
                            from urllib.parse import urlencode
                            
                            print(f"        → {year} {make} {model} {trim} {drive} - Fetching vehicle data...")
                            vehicle_data = get_vehicle_data(year, make, model, trim, drive)
                            
                            if vehicle_data:
                                # Extract required data for URL construction
                                dr_chassis_id = vehicle_data.get('drchassisid')
                                vehicle_type = vehicle_data.get('vehicleType')
                                
                                if dr_chassis_id and vehicle_type:
                                    # Construct the URL
                                    url_params = {
                                        'sort': 'instock',
                                        'year': year,
                                        'make': make,
                                        'model': model,
                                        'trim': trim,
                                        'drive': drive,
                                        'DRChassisID': dr_chassis_id,
                                        'vehicle_type': vehicle_type
                                    }
                                    
                                    wheel_url = f"https://www.customwheeloffset.com/store/wheels?{urlencode(url_params)}"
                                    print(f"        → Fetching wheel fitment data from: {wheel_url}")
                                    
                                    # Get wheel fitment data
                                    wheel_data = get_parsed_data_with_saved_token(wheel_url)
                                    
                                    if wheel_data:
                                        # Save to database
                                        try:
                                            # Insert YMM record
                                            ymm_id = insert_custom_wheel_offset_ymm(
                                                year=year,
                                                make=make,
                                                model=model,
                                                trim=trim,
                                                drive=drive,
                                                vehicle_type=vehicle_type,
                                                dr_chassis_id=dr_chassis_id,
                                                bolt_pattern=wheel_data.get('bolt_pattern')
                                            )
                                            
                                            # Insert wheel fitment data
                                            data_count = insert_custom_wheel_offset_data(ymm_id, wheel_data)
                                            
                                            print(f"        ✓ {year} {make} {model} {trim} {drive} - Saved YMM ID: {ymm_id}, Data records: {data_count}")
                                            
                                        except Exception as db_error:
                                            print(f"        ✗ Database error for {year} {make} {model} {trim} {drive}: {db_error}")
                                    else:
                                        print(f"        ✗ No wheel fitment data retrieved for {year} {make} {model} {trim} {drive}")
                                else:
                                    print(f"        ✗ Missing DRChassisID or vehicle_type for {year} {make} {model} {trim} {drive}")
                            else:
                                print(f"        ✗ No vehicle data found for {year} {make} {model} {trim} {drive}")
                                
                        except Exception as fetch_error:
                            print(f"        ✗ Error processing {year} {make} {model} {trim} {drive}: {fetch_error}")
                        
                        processed_count += 1
                        
                        # Small delay to avoid overwhelming the server
                        time.sleep(0.1)  # Slightly increased delay for API calls
            
            return processed_count
            
        except Exception as e:
            error_msg = str(e)
            print(f"[Thread-{thread_id}] Error processing {year} {make}: {error_msg}")
            
            # Check if this error requires restart
            if self.is_restart_required_error(error_msg, "processing"):
                print(f"[Thread-{thread_id}] Processing error requires restart: {error_msg}")
                restart_performed = self.handle_restart_scenario(f"processing_error_{year}_{make}", thread_id)
                if restart_performed:
                    # This thread performed the restart, exit the entire process
                    print(f"[Thread-{thread_id}] Restart completed, exiting process")
                    sys.exit(0)
                else:
                    # Another thread handled restart, this thread should stop gracefully
                    print(f"[Thread-{thread_id}] Another thread handled restart, stopping gracefully")
                    return processed_count
            else:
                # Non-critical error, log and continue
                print(f"[Thread-{thread_id}] Non-critical error, continuing: {error_msg}")
            
            return processed_count

    def load_cached_combinations(self) -> Optional[List[Tuple[str, str]]]:
        """Load year/make combinations from cache if valid."""
        try:
            if not self.cache_file.exists():
                logger.info("[ScraperV2] No cache file found")
                return None
            
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # Check cache expiry
            cache_date = datetime.fromisoformat(cache_data.get('created_at', ''))
            expiry_date = cache_date + timedelta(days=self.cache_expiry_days)
            
            if datetime.now() > expiry_date:
                logger.info(f"[ScraperV2] Cache expired (created: {cache_date.strftime('%Y-%m-%d')}, expired: {expiry_date.strftime('%Y-%m-%d')})")
                return None
            
            combinations = [(item['year'], item['make']) for item in cache_data.get('combinations', [])]
            
            # Filter for year 2026 only for testing
            combinations = [(year, make) for year, make in combinations if year == "2026"]
            
            logger.info(f"[ScraperV2] Loaded {len(combinations)} combinations from cache (created: {cache_date.strftime('%Y-%m-%d %H:%M:%S')}, filtered for 2026 only)")
            return combinations
            
        except Exception as e:
            logger.info(f"[ScraperV2] Error loading cache: {e}")
            return None
    
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
            
            logger.info(f"[ScraperV2] Saved {len(combinations)} combinations to cache: {self.cache_file}")
            
        except Exception as e:
            logger.info(f"[ScraperV2] Error saving cache: {e}")

    def collect_year_make_combinations(self, force_refresh: bool = False):
        """Collect year/make combinations to be processed in parallel with caching support."""
        # Try to load from cache first (unless force refresh)
        if not force_refresh:
            cached_combinations = self.load_cached_combinations()
            if cached_combinations:
                return cached_combinations
        
        logger.info("[ScraperV2] Fetching fresh year/make combinations from API...")
        combinations = []
        
        try:
            # Check for restart signal before starting
            if self.stop_all_threads.is_set() or self.restart_required.is_set():
                logger.info("[ScraperV2] Restart signal detected during combination collection, stopping")
                return combinations
            
            # Get years using existing utility function
            years = get_years()
            if not years:
                logger.info("[ScraperV2] No years found")
                return combinations

            # Filter for year 2026 only for testing
            years = [year for year in years if year == "2026"]
            if not years:
                logger.info("[ScraperV2] Year 2026 not found in available years")
                return combinations

            logger.info(f"[ScraperV2] Collecting year/make combinations from {len(years)} years (filtered for 2026 only)...")
            
            # Loop through years
            for year in years:
                # Check for restart signal before each year
                if self.stop_all_threads.is_set() or self.restart_required.is_set():
                    logger.info(f"[ScraperV2] Restart signal detected during year {year} processing, stopping")
                    break
                
                logger.info(f"--- Collecting makes for year: {year} ---")
                
                # Get makes for this year using existing utility function
                makes = get_makes(year)
                if not makes:
                    logger.info(f"[ScraperV2] No makes found for year {year}, skipping")
                    continue
                
                # Add year/make combinations
                for make in makes:
                    # Check for restart signal before each make
                    if self.stop_all_threads.is_set() or self.restart_required.is_set():
                        logger.info(f"[ScraperV2] Restart signal detected during make {make} processing, stopping")
                        break
                    
                    combinations.append((year, make))
            
            logger.info(f"[ScraperV2] Collected {len(combinations)} year/make combinations to process")
            
            # Save to cache for future runs (only if not interrupted)
            if combinations and not (self.stop_all_threads.is_set() or self.restart_required.is_set()):
                self.save_combinations_to_cache(combinations)
            
            return combinations
            
        except Exception as e:
            error_msg = str(e)
            logger.info(f"[ScraperV2] Error collecting year/make combinations: {error_msg}")
            
            # Check if this error requires restart
            if self.is_restart_required_error(error_msg, "combination_collection"):
                logger.info(f"[ScraperV2] Combination collection error requires restart: {error_msg}")
                restart_performed = self.handle_restart_scenario(f"combination_collection_error")
                if restart_performed:
                    # This thread performed the restart, exit the entire process
                    logger.info(f"[ScraperV2] Restart completed, exiting process")
                    sys.exit(0)
            
            return combinations

    def run_scraper(self, target_year: str = None, force_refresh: bool = False) -> List[dict]:
        """Main scraper entry point with two phases: combination caching and wheel size scraping."""
        logger.info(f"[ScraperV2] Starting scraper with target_year={target_year}, force_refresh={force_refresh}")
        
        # Phase 1: Cache all combinations (no captcha required)
        logger.info("[ScraperV2] Phase 1: Caching all year/make/model/trim/drive combinations...")
        combinations = self.cache_all_combinations(force_refresh=force_refresh, target_year=target_year)
        
        if not combinations:
            logger.info("[ScraperV2] No combinations found, stopping scraper")
            return []
        
        logger.info(f"[ScraperV2] Phase 1 completed: {len(combinations)} combinations cached")
        
        # Phase 2: Scrape wheel sizes (requires captcha handling)
        logger.info("[ScraperV2] Phase 2: Scraping wheel sizes (captcha handling required)...")
        return self.scrape_wheel_sizes_for_combinations(combinations)
    
    def run_scrape(self, force_refresh_cache: bool = False):
        """Main scraping function with two-phase approach: cache all combinations first, then process them."""
        logger.info("[ScraperV2] Starting Custom Wheel Offset scraper V2 with two-phase approach...")
        
        # Reset restart flags at the beginning
        self.restart_required.clear()
        self.stop_all_threads.clear()
        
        try:
            # Phase 1: Cache all year/make/model/trim/drive combinations (no captcha required)
            logger.info("\n=== Phase 1: Caching all vehicle combinations ===")
            logger.info("[ScraperV2] Using simplified combination collector (no session initialization required)...")
            
            # Use the new simplified approach - no session initialization needed
            full_combinations_cache = self.cache_all_combinations(force_refresh=force_refresh_cache, target_year="2026")
            
            if not full_combinations_cache:
                logger.info("[ScraperV2] No combinations cached, exiting")
                return
            
            # Phase 2: Process cached combinations and fetch wheel data (requires captcha handling)
            logger.info(f"\n=== Phase 2: Processing {len(full_combinations_cache)} cached combinations ===")
            logger.info("[ScraperV2] Initializing session for wheel size scraping (captcha handling required)...")
            if not self.initialize_session():
                logger.info("[ScraperV2] Failed to initialize session for wheel scraping, exiting")
                return
            
            self.process_cached_combinations(full_combinations_cache)
            
        except Exception as e:
            error_msg = str(e)
            logger.info(f"[ScraperV2] Error during scraping: {error_msg}")
            
            # Check if this error requires restart
            if self.is_restart_required_error(error_msg, "main_scraping"):
                logger.info(f"[ScraperV2] Main scraping error requires restart: {error_msg}")
                restart_performed = self.handle_restart_scenario(f"main_scraping_error")
                if restart_performed:
                    # This thread performed the restart, exit the entire process
                    logger.info(f"[ScraperV2] Restart completed, exiting process")
                    sys.exit(0)
            else:
                # Handle session expired error using existing function for non-restart errors
                handle_session_expired_error(str(e))

    def cache_all_combinations(self, force_refresh: bool = False, target_year: str = None) -> List[dict]:
        """Phase 1: Cache all year/make/model/trim/drive combinations using simplified collector (no captcha needed)."""
        
        # Import the simplified collector with proper path handling
        import sys
        import os
        
        # Add the src directory to the path if not already there
        src_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        if src_path not in sys.path:
            sys.path.insert(0, src_path)
        
        from providers.custom_wheel_offset.simple_combination_collector import SimpleCombinationCollector
        
        logger.info("[ScraperV2] Using simplified combination collector (no captcha required)...")
        collector = SimpleCombinationCollector(cache_expiry_days=self.cache_expiry_days)
        
        # Use the simplified collector to get all combinations
        combinations = collector.cache_all_combinations(target_year=target_year, force_refresh=force_refresh)
        
        logger.info(f"[ScraperV2] Combination caching completed: {len(combinations)} combinations collected")
        return combinations
    
    def process_model_combinations(self, year: str, make: str, model: str) -> List[dict]:
        """Process a single model to get all trim/drive combinations."""
        combinations = []
        thread_id = str(threading.current_thread().ident)
        
        try:
            # Check for shutdown signal
            if self.shutdown_requested.is_set() or self.stop_all_threads.is_set():
                return []
            
            # Get trims for this year/make/model
            trims = get_trims(year, make, model)
            if not trims:
                return []
            
            # Process trims with nested threading for drive types
            with ThreadPoolExecutor(max_workers=min(2, len(trims))) as trim_executor:
                trim_futures = {
                    trim_executor.submit(self.process_trim_combinations, year, make, model, trim): trim 
                    for trim in trims
                }
                
                for future in as_completed(trim_futures):
                    if self.shutdown_requested.is_set() or self.stop_all_threads.is_set():
                        break
                    
                    try:
                        trim_combos = future.result()
                        combinations.extend(trim_combos)
                    except Exception as e:
                        trim = trim_futures[future]
                        logger.info(f"[Thread-{thread_id}] Error processing trim {trim}: {e}")
            
            return combinations
            
        except Exception as e:
            logger.info(f"[Thread-{thread_id}] Error processing model {year} {make} {model}: {e}")
            return []
    
    def process_trim_combinations(self, year: str, make: str, model: str, trim: str) -> List[dict]:
        """Process a single trim to get all drive combinations."""
        combinations = []
        
        try:
            # Check for shutdown signal
            if self.shutdown_requested.is_set() or self.stop_all_threads.is_set():
                return []
            
            # Get drive types for this year/make/model/trim
            drive_types = get_drive_types(year, make, model, trim)
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
            thread_id = str(threading.current_thread().ident)
            logger.info(f"[Thread-{thread_id}] Error processing trim {year} {make} {model} {trim}: {e}")
            return []

    def process_cached_combinations(self, combinations: List[dict]):
        """Phase 2: Process cached combinations and fetch wheel data with restart recovery."""
        # Import database check function
        # Add src directory to path for absolute imports
        import sys
        import os
        if __name__ == "__main__":
            src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
            if src_path not in sys.path:
                sys.path.insert(0, src_path)
            from services.repository import check_custom_wheel_offset_combination_exists
        else:
            from ...services.repository import check_custom_wheel_offset_combination_exists
        
        # Filter out already processed combinations
        unprocessed_combinations = []
        for combo in combinations:
            if check_custom_wheel_offset_combination_exists(
                combo['year'], combo['make'], combo['model'], combo['trim'], combo['drive']
            ):
                logger.info(f"[ScraperV2] Skipping already processed: {combo['year']} {combo['make']} {combo['model']} {combo['trim']} {combo['drive']}")
                continue
            unprocessed_combinations.append(combo)
        
        logger.info(f"[ScraperV2] Found {len(unprocessed_combinations)} unprocessed combinations out of {len(combinations)} total")
        
        if not unprocessed_combinations:
            logger.info("[ScraperV2] All combinations already processed!")
            return
        
        # Process with threading
        total_processed = 0
        failed_count = 0
        restart_triggered = False
        
        with ThreadPoolExecutor(max_workers=self.worker_count) as executor:
            # Submit all tasks
            future_to_combination = {
                executor.submit(self.process_single_combination, combo): combo 
                for combo in unprocessed_combinations
            }
            
            # Process completed tasks
            for future in as_completed(future_to_combination):
                # Check for restart or shutdown signal during processing
                if self.stop_all_threads.is_set() or self.restart_required.is_set() or self.shutdown_requested.is_set():
                    if self.shutdown_requested.is_set():
                        logger.info("[ScraperV2] Shutdown signal detected (Ctrl+C), cancelling remaining tasks...")
                    else:
                        logger.info("[ScraperV2] Restart signal detected, cancelling remaining tasks...")
                    # Cancel remaining futures
                    for remaining_future in future_to_combination:
                        if not remaining_future.done():
                            remaining_future.cancel()
                    if self.shutdown_requested.is_set():
                        logger.info("[ScraperV2] Graceful shutdown completed")
                        return  # Exit gracefully on Ctrl+C
                    restart_triggered = True
                    break
                
                combination = future_to_combination[future]
                try:
                    success = future.result()
                    if success:
                        total_processed += 1
                        if total_processed % 10 == 0:
                            logger.info(f"[ScraperV2] Progress: {total_processed}/{len(unprocessed_combinations)} combinations processed")
                    else:
                        failed_count += 1
                        
                except Exception as e:
                    error_msg = str(e)
                    failed_count += 1
                    combo_str = f"{combination['year']} {combination['make']} {combination['model']} {combination['trim']} {combination['drive']}"
                    logger.info(f"[ScraperV2] Exception processing {combo_str}: {error_msg}")
                    
                    # Check if this error requires restart
                    if self.is_restart_required_error(error_msg, "thread_execution"):
                        logger.info(f"[ScraperV2] Thread execution error requires restart: {error_msg}")
                        restart_performed = self.handle_restart_scenario(f"thread_execution_error_{combo_str}")
                        if restart_performed:
                            restart_triggered = True
                            break
        
        # Check if restart was triggered
        if restart_triggered:
            logger.info("[ScraperV2] Restart was triggered, exiting current process")
            return
        
        logger.info(f"\n[ScraperV2] Processing completed!")
        logger.info(f"[ScraperV2] Total combinations processed: {total_processed}")
        logger.info(f"[ScraperV2] Failed combinations: {failed_count}")
        logger.info(f"[ScraperV2] Total unprocessed combinations: {len(unprocessed_combinations)}")

    def process_single_combination(self, combination: dict) -> bool:
        """Process a single year/make/model/trim/drive combination."""
        thread_id = str(threading.current_thread().ident)
        year = combination['year']
        make = combination['make']
        model = combination['model']
        trim = combination['trim']
        drive = combination['drive']
        
        try:
            # Check for restart or shutdown signal
            if self.stop_all_threads.is_set() or self.restart_required.is_set() or self.shutdown_requested.is_set():
                if self.shutdown_requested.is_set():
                    logger.info(f"[Thread-{thread_id}] Shutdown signal detected (Ctrl+C), stopping immediately")
                else:
                    logger.info(f"[Thread-{thread_id}] Restart signal detected, stopping immediately")
                return False
            
            # Get vehicle data to construct URL
            # Add src directory to path for absolute imports
            import sys
            import os
            if __name__ == "__main__":
                src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
                if src_path not in sys.path:
                    sys.path.insert(0, src_path)
                from providers.custom_wheel_offset.utils import get_vehicle_data
                from providers.custom_wheel_offset.wheel_size import get_parsed_data_with_saved_token
                from services.repository_optimized import insert_custom_wheel_offset_ymm, insert_custom_wheel_offset_data
            else:
                from .utils import get_vehicle_data
                from .wheel_size import get_parsed_data_with_saved_token
                from ...services.repository_optimized import insert_custom_wheel_offset_ymm, insert_custom_wheel_offset_data
            from urllib.parse import urlencode
            
            logger.info(f"[Thread-{thread_id}] Processing: {year} {make} {model} {trim} {drive}")
            vehicle_data = get_vehicle_data(year, make, model, trim, drive)
            
            if vehicle_data:
                # Extract required data for URL construction
                dr_chassis_id = vehicle_data.get('drchassisid')
                vehicle_type = vehicle_data.get('vehicleType')
                
                if dr_chassis_id and vehicle_type:
                    # Construct the URL
                    url_params = {
                        'sort': 'instock',
                        'year': year,
                        'make': make,
                        'model': model,
                        'trim': trim,
                        'drive': drive,
                        'DRChassisID': dr_chassis_id,
                        'vehicle_type': vehicle_type
                    }
                    
                    wheel_url = f"https://www.customwheeloffset.com/store/wheels?{urlencode(url_params)}"
                    logger.info(f"[Thread-{thread_id}] Fetching wheel data from: {wheel_url}")
                    
                    # Get wheel fitment data
                    wheel_data = get_parsed_data_with_saved_token(wheel_url)
                    
                    if wheel_data:
                        # Save to database
                        try:
                            # Insert YMM record
                            ymm_id = insert_custom_wheel_offset_ymm(
                                year=year,
                                make=make,
                                model=model,
                                trim=trim,
                                drive=drive,
                                vehicle_type=vehicle_type,
                                dr_chassis_id=dr_chassis_id,
                                bolt_pattern=wheel_data.get('bolt_pattern')
                            )
                            
                            # Insert wheel fitment data
                            data_count = insert_custom_wheel_offset_data(ymm_id, wheel_data)
                            
                            logger.info(f"[Thread-{thread_id}] ✓ Saved: {year} {make} {model} {trim} {drive} - YMM ID: {ymm_id}, Data records: {data_count}")
                            return True
                            
                        except Exception as db_error:
                            logger.info(f"[Thread-{thread_id}] ✗ Database error for {year} {make} {model} {trim} {drive}: {db_error}")
                            return False
                    else:
                        logger.info(f"[Thread-{thread_id}] ✗ No wheel fitment data retrieved for {year} {make} {model} {trim} {drive}")
                        return False
                else:
                    logger.info(f"[Thread-{thread_id}] ✗ Missing DRChassisID or vehicle_type for {year} {make} {model} {trim} {drive}")
                    return False
            else:
                logger.info(f"[Thread-{thread_id}] ✗ No vehicle data found for {year} {make} {model} {trim} {drive}")
                return False
                
        except Exception as e:
            logger.info(f"[Thread-{thread_id}] ✗ Error processing {year} {make} {model} {trim} {drive}: {e}")
            return False
        
        # Small delay to avoid overwhelming the server
        time.sleep(0.1)
        return True


def main():
    """Main entry point with cache refresh option and restart prevention."""
    import argparse
    import os
    
    # Check if this is a restart loop by looking for a restart marker file
    restart_marker = os.path.join(os.path.dirname(__file__), '.restart_marker')
    
    if os.path.exists(restart_marker):
        # Check if the marker is recent (within last 30 seconds)
        import time
        marker_age = time.time() - os.path.getmtime(restart_marker)
        if marker_age < 30:
            logger.info("[RESTART_PREVENTION] Recent restart detected, preventing restart loop")
            logger.info(f"[RESTART_PREVENTION] Marker age: {marker_age:.1f} seconds")
            # Clean up the marker and exit
            try:
                os.remove(restart_marker)
            except:
                pass
            logger.info("[RESTART_PREVENTION] Exiting to prevent infinite restart loop")
            return
        else:
            # Old marker, safe to remove and continue
            try:
                os.remove(restart_marker)
            except:
                pass
    
    # Create restart marker
    try:
        with open(restart_marker, 'w') as f:
            f.write(str(time.time()))
    except:
        pass  # Continue even if we can't create the marker
    
    parser = argparse.ArgumentParser(description='Custom Wheel Offset Scraper V2 with caching')
    parser.add_argument('--refresh-cache', action='store_true', 
                       help='Force refresh the year/make combinations cache')
    
    args = parser.parse_args()
    
    try:
        scraper = CustomWheelOffsetScraperV2()
        # Set up signal handlers for graceful shutdown on Ctrl+C
        scraper.setup_signal_handlers()
        scraper.run_scrape(force_refresh_cache=args.refresh_cache)
    finally:
        # Clean up restart marker on normal exit
        try:
            if os.path.exists(restart_marker):
                os.remove(restart_marker)
        except:
            pass


if __name__ == "__main__":
    main()