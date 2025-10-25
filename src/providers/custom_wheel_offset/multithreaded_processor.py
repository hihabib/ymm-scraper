#!/usr/bin/env python3
"""
Multithreaded processor for Custom Wheel Offset scraper.
Handles work distributifitment_preferences_request_failureon and prevents duplicate data processing.
"""

from typing import Dict, Set, Tuple, List
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import centralized logging
from .logging_config import init_module_logger

# Import error logging functionality
try:
    from services.repository_optimized import insert_error_log
except ImportError:
    try:
        # Fallback to regular repository if optimized version not available
        from services.repository import insert_error_log
    except ImportError:
        # If both fail, define a no-op function to prevent crashes
        def insert_error_log(source, context, message):
            pass

# Initialize logger for this module
logger = init_module_logger("multithreaded_processor")

from .thread_manager import ThreadManager, WorkItem
from .vehicle_data_extractor import VehicleDataExtractor
from .cache_ops import load_full_cache_from_db
from .combination_processor import process_single_vehicle_combination
from .config_manager import get_config
from .performance_monitor import MetricTracker, performance_monitor
from .optimized_network_manager import optimized_network_manager
from .realtime_processor import realtime_processor
from .integrated_processor import IntegratedProcessor


class MultithreadedProcessor:
    """
    Multithreaded processor for stage 2 operations.
    Distributes work across threads while preventing duplicate processing.
    """
    
    def __init__(self, max_workers: int = None):
        """Initialize multithreaded processor."""
        self.config = get_config()
        self.thread_manager = ThreadManager(max_workers)
        self.processed_combinations = set()
        self.total_added = 0
        
    def _generate_work_items(self, existing_year_make: Set[Tuple[str, str]]) -> List[WorkItem]:
        """Generate work items from year/make combinations using parallel processing."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        work_items = []
        
        logger.info(f"Generating work items from {len(existing_year_make)} year/make combinations")
        
        def process_year_make(year_make_tuple):
            """Process a single year/make combination to get all work items."""
            year, make = year_make_tuple
            local_work_items = []
            
            try:
                # Get models for this year/make using VehicleDataExtractor
                year_make_extractor = VehicleDataExtractor(year=year, make=make)
                models = year_make_extractor.get_models()
                if not models:
                    logger.warning(f"No models found for {year} {make}, skipping")
                    return local_work_items
                
                logger.debug(f"Processing {len(models)} models for {year} {make}")
                
                # Process models in parallel for this year/make
                with ThreadPoolExecutor(max_workers=min(4, len(models)), thread_name_prefix=f"Model-{year}-{make}") as model_executor:
                    model_futures = {
                        model_executor.submit(self._process_model_for_work_items, year, make, model): model 
                        for model in models
                    }
                    
                    for future in as_completed(model_futures):
                        try:
                            model_work_items = future.result()
                            local_work_items.extend(model_work_items)
                        except Exception as e:
                            model = model_futures[future]
                            logger.error(f"Error processing model {year} {make} {model}: {e}")
                
                return local_work_items
                
            except Exception as e:
                logger.error(f"Error processing year/make {year} {make}: {e}")
                return local_work_items
        
        # Process year/make combinations in parallel
        max_year_make_workers = min(8, len(existing_year_make))  # Limit concurrent year/make processing
        with ThreadPoolExecutor(max_workers=max_year_make_workers, thread_name_prefix="YearMake") as executor:
            future_to_year_make = {
                executor.submit(process_year_make, year_make): year_make 
                for year_make in sorted(existing_year_make)
            }
            
            for future in as_completed(future_to_year_make):
                try:
                    year_make_work_items = future.result()
                    work_items.extend(year_make_work_items)
                except Exception as e:
                    year_make = future_to_year_make[future]
                    logger.error(f"Error processing year/make {year_make}: {e}")
        
        logger.info(f"Generated {len(work_items)} work items")
        return work_items
    
    def _process_model_for_work_items(self, year: str, make: str, model: str) -> List[WorkItem]:
        """Process a single model to generate work items for all its trim/drive combinations."""
        work_items = []
        
        try:
            # Get trims for this year/make/model using VehicleDataExtractor
            year_make_model_extractor = VehicleDataExtractor(year=year, make=make, model=model)
            trims = year_make_model_extractor.get_trims()
            if not trims:
                logger.warning(f"No trims found for {year} {make} {model}, skipping")
                return work_items
            
            # Process trims in parallel for this model
            with ThreadPoolExecutor(max_workers=min(3, len(trims)), thread_name_prefix=f"Trim-{year}-{make}-{model}") as trim_executor:
                trim_futures = {
                    trim_executor.submit(self._process_trim_for_work_items, year, make, model, trim): trim 
                    for trim in trims
                }
                
                for future in as_completed(trim_futures):
                    try:
                        trim_work_items = future.result()
                        work_items.extend(trim_work_items)
                    except Exception as e:
                        trim = trim_futures[future]
                        logger.error(f"Error processing trim {year} {make} {model} {trim}: {e}")
            
            return work_items
            
        except Exception as e:
            logger.error(f"Error processing model {year} {make} {model}: {e}")
            return work_items
    
    def _process_trim_for_work_items(self, year: str, make: str, model: str, trim: str) -> List[WorkItem]:
        """Process a single trim to generate work items for all its drive types."""
        work_items = []
        
        try:
            # Get drives for this year/make/model/trim using VehicleDataExtractor
            year_make_model_trim_extractor = VehicleDataExtractor(year=year, make=make, model=model, trim=trim)
            drives = year_make_model_trim_extractor.get_drives()
            if not drives:
                logger.warning(f"No drives found for {year} {make} {model} {trim}, skipping")
                return work_items
            
            for drive in drives:
                work_item = WorkItem(
                    year=year,
                    make=make,
                    model=model,
                    trim=trim,
                    drive=drive
                )
                work_items.append(work_item)
            
            return work_items
            
        except Exception as e:
            logger.error(f"Error processing trim {year} {make} {model} {trim}: {e}")
            return work_items

    def _filter_existing_combinations(self, work_items: List[WorkItem]) -> List[WorkItem]:
        """Filter out work items that already exist in the database using optimized operations."""
        logger.info("Loading existing combinations from database with optimized operations...")
        
        # Use optimized database operations
        from .optimized_cache_ops import load_full_cache_from_db_optimized
        
        # Load existing combinations from database
        db_cache = load_full_cache_from_db_optimized()
        full_combos = db_cache.get("combinations", {})
        
        if not isinstance(full_combos, dict):
            full_combos = {}
        
        # Create a set of base keys (5-part keys) from existing full combinations
        existing_base_keys = set()
        for full_key in full_combos.keys():
            parts = full_key.split("__")
            if len(parts) >= 5:
                base_key = "__".join(parts[:5])  # year__make__model__trim__drive
                existing_base_keys.add(base_key)
        
        logger.info(f"Found {len(existing_base_keys)} existing base combinations in database")
        
        # Filter work items
        filtered_items = []
        for item in work_items:
            # Create base key to check if combination already exists
            from .key_utils import make_base_key
            base_key = make_base_key(item.year, item.make, item.model, item.trim, item.drive)
            
            if base_key not in existing_base_keys:
                filtered_items.append(item)
        
        logger.info(f"Filtered to {len(filtered_items)} new combinations to process")
        return filtered_items
    
    def _process_work_item(self, work_item: WorkItem) -> int:
        """Process a single work item with performance monitoring."""
        import threading
        thread_id = threading.current_thread().ident
        
        try:
            # Use optimized database operations for better performance
            from .optimized_cache_ops import load_full_cache_from_db_optimized
            from .optimized_combination_processor import process_single_vehicle_combination_optimized
            from .optimized_network_manager import optimized_network_manager
            from .performance_monitor import performance_monitor, MetricTracker
            
            # Start performance tracking for this thread if not already started
            if thread_id not in performance_monitor._thread_metrics:
                performance_monitor.start_thread_tracking(thread_id)
            
            with MetricTracker("process_work_item", {"combination": f"{work_item.year} {work_item.make} {work_item.model} {work_item.trim} {work_item.drive}"}):
                # Load database cache once per thread (cached in thread-local storage)
                with MetricTracker("load_database_cache"):
                    db_cache = load_full_cache_from_db_optimized()
                    full_combos = db_cache.get("combinations", {})
                    performance_monitor.increment_counter("database_operations")
                
                if not isinstance(full_combos, dict):
                    full_combos = {}
                
                existing_full_keys = set(full_combos.keys())
                
                # Process the combination using optimized processor
                with MetricTracker("process_combination"):
                    added = process_single_vehicle_combination_optimized(
                        work_item.year,
                        work_item.make,
                        work_item.model,
                        work_item.trim,
                        work_item.drive,
                        full_combos,
                        existing_full_keys,
                        self.config
                    )
                
                if added > 0:
                    performance_monitor.increment_counter("items_processed")
                    logger.info(f"[Thread-{thread_id}] Thread processed {work_item.year} {work_item.make} {work_item.model} {work_item.trim} {work_item.drive}: +{added}")
                else:
                    performance_monitor.increment_counter("no_items_added")
                
                return added
            
        except Exception as e:
            performance_monitor.increment_counter("errors")
            logger.error(f"[Thread-{thread_id}] Error processing {work_item.year} {work_item.make} {work_item.model} {work_item.trim} {work_item.drive}: {e}")
            
            # Log processing error to database
            try:
                context = {
                    'year': work_item.year,
                    'make': work_item.make,
                    'model': work_item.model,
                    'trim': work_item.trim,
                    'drive': work_item.drive,
                    'retry_count': work_item.retry_count,
                    'error_type': type(e).__name__,
                    'processing_stage': 'work_item_processing'
                }
                insert_error_log('custom_wheel_offset_processor', context, str(e))
                logger.debug(f"[MultithreadedProcessor] Processing error logged to database for {work_item.year} {work_item.make} {work_item.model}")
            except Exception as db_error:
                logger.error(f"[MultithreadedProcessor] Failed to log processing error to database: {db_error}")
            
            raise
    
    def process_combinations_multithreaded(self, existing_year_make: Set[Tuple[str, str]]) -> int:
        """
        Process combinations using multiple threads with performance monitoring.
        Can use either the traditional work-item approach, the real-time approach, or the new integrated approach.
        Returns the total number of new entries added.
        """
        logger.info("Starting multithreaded processing for stage 2")
        start_time = time.time()
        
        # Check processing approach configuration
        use_realtime = self.config.get('realtime_processing', False)
        use_integrated = self.config.get('integrated_processing', False)
        
        if use_integrated:
            logger.info("Using integrated processing approach")
            return self._process_integrated_approach(existing_year_make)
        elif use_realtime:
            logger.info("Using real-time processing approach")
            return self._process_realtime_approach(existing_year_make)
        else:
            logger.info("Using traditional work-item approach")
            return self._process_traditional_approach(existing_year_make)
    
    def _process_integrated_approach(self, existing_year_make: Set[Tuple[str, str]]) -> int:
        """Process combinations using the new integrated approach."""
        with MetricTracker("process_combinations_integrated"):
            try:
                # Initialize the integrated processor
                integrated_processor = IntegratedProcessor(self.config)
                
                # Use the integrated processor to process all year/make combinations
                added = integrated_processor.process_year_make_integrated(existing_year_make)
                
                logger.info(f"Integrated processing completed: {added} new entries added")
                return added
                
            except Exception as e:
                performance_monitor.increment_counter("fatal_errors")
                logger.error(f"Fatal error during integrated processing: {e}")
                
                # Log fatal processing error to database
                try:
                    context = {
                        'error_type': type(e).__name__,
                        'processing_stage': 'integrated_multithreaded_processing',
                        'fatal_error': True
                    }
                    insert_error_log('custom_wheel_offset_integrated_fatal', context, str(e))
                    logger.debug("[MultithreadedProcessor] Fatal integrated processing error logged to database")
                except Exception as db_error:
                    logger.error(f"[MultithreadedProcessor] Failed to log fatal error to database: {db_error}")
                
                raise
            finally:
                # Clean up integrated processor
                if 'integrated_processor' in locals():
                    integrated_processor.shutdown()
    
    def _process_realtime_approach(self, existing_year_make: Set[Tuple[str, str]]) -> int:
        """Process combinations using the new real-time approach."""
        with MetricTracker("process_combinations_realtime"):
            try:
                # Use the real-time processor
                added = realtime_processor.process_year_make_realtime(existing_year_make)
                
                logger.info(f"Real-time processing completed: {added} new entries added")
                return added
                
            except Exception as e:
                performance_monitor.increment_counter("fatal_errors")
                logger.error(f"Fatal error during real-time processing: {e}")
                
                # Log fatal processing error to database
                try:
                    context = {
                        'error_type': type(e).__name__,
                        'processing_stage': 'realtime_multithreaded_processing',
                        'fatal_error': True
                    }
                    insert_error_log('custom_wheel_offset_realtime_fatal', context, str(e))
                    logger.debug("[MultithreadedProcessor] Fatal real-time processing error logged to database")
                except Exception as db_error:
                    logger.error(f"[MultithreadedProcessor] Failed to log fatal error to database: {db_error}")
                
                raise
            finally:
                # Clean up real-time processor
                realtime_processor.shutdown()
    
    def _process_traditional_approach(self, existing_year_make: Set[Tuple[str, str]]) -> int:
        """Process combinations using the traditional work-item approach."""
        
        # Record start time for performance tracking
        start_time = time.time()
        
        # Start overall performance tracking
        with MetricTracker("process_combinations_multithreaded"):
            try:
                # Generate work items
                with MetricTracker("generate_work_items"):
                    work_items = self._generate_work_items(existing_year_make)
                
                if not work_items:
                    logger.info("No work items to process")
                    return 0
                
                # Filter existing combinations to avoid duplicates
                if self.config.get('skip_existing', True):
                    with MetricTracker("filter_existing_combinations"):
                        work_items = self._filter_existing_combinations(work_items)
                
                if not work_items:
                    logger.info("All combinations already exist in database")
                    return 0
                
                logger.info(f"Processing {len(work_items)} work items with {self.thread_manager.max_workers} threads")
                
                # Add work items to thread manager
                self.thread_manager.add_work_items(work_items)
                
                # Start processing with performance monitoring
                self.thread_manager.start_processing(self._process_work_item)
                
                # Monitor progress with performance tracking
                self._monitor_progress_with_performance()
                
                # Wait for completion
                success = self.thread_manager.wait_for_completion(timeout=3600)  # 1 hour timeout
                
                if not success:
                    logger.warning("Processing timed out")
                    performance_monitor.increment_counter("timeouts")
                
                # Get final statistics
                stats = self.thread_manager.get_statistics()
                total_processed = stats['total_processed']
                total_errors = stats['total_errors']
                
                elapsed_time = time.time() - start_time
                
                logger.info(f"Processing completed:")
                logger.info(f"  Total processed: {total_processed}")
                logger.info(f"  Total errors: {total_errors}")
                logger.info(f"  Time elapsed: {elapsed_time:.2f} seconds")
                logger.info(f"  Average rate: {total_processed / elapsed_time:.2f} items/second")
                
                # End thread tracking for all threads
                for thread_id in list(performance_monitor._thread_metrics.keys()):
                    if performance_monitor._thread_metrics[thread_id].end_time is None:
                        performance_monitor.end_thread_tracking(thread_id)
                
                # Print comprehensive performance report
                performance_monitor.print_performance_report()
                
                # Export metrics for analysis
                metrics_file = performance_monitor.export_metrics()
                if metrics_file:
                    logger.info(f"Performance metrics exported to: {metrics_file}")
                
                return total_processed
                
            except Exception as e:
                performance_monitor.increment_counter("fatal_errors")
                logger.error(f"Fatal error during processing: {e}")
                
                # Log fatal processing error to database
                try:
                    context = {
                        'error_type': type(e).__name__,
                        'processing_stage': 'multithreaded_processing',
                        'fatal_error': True
                    }
                    insert_error_log('custom_wheel_offset_fatal_error', context, str(e))
                    logger.debug("[MultithreadedProcessor] Fatal processing error logged to database")
                except Exception as db_error:
                    logger.error(f"[MultithreadedProcessor] Failed to log fatal error to database: {db_error}")
                
                raise
            finally:
                # Always shutdown thread manager and clean up resources
                self.thread_manager.shutdown(wait=True)
                optimized_network_manager.close_all_sessions()
    
    def _monitor_progress_with_performance(self) -> None:
        """Monitor processing progress with performance metrics."""
        logger.info("Starting progress monitoring with performance tracking...")
        
        last_processed = 0
        stall_count = 0
        
        while True:
            time.sleep(10)  # Check every 10 seconds
            
            stats = self.thread_manager.get_statistics()
            current_processed = stats['total_processed']
            current_errors = stats['total_errors']
            active_threads = stats['active_threads']
            
            # Calculate processing rate
            processed_since_last = current_processed - last_processed
            rate = processed_since_last / 10.0  # items per second
            
            # Get performance metrics
            overall_stats = performance_monitor.get_overall_stats()
            
            logger.info(f"Progress: {current_processed} processed, {current_errors} errors, "
                       f"{active_threads} active threads, {rate:.2f} items/sec, "
                       f"overall throughput: {overall_stats.get('overall_throughput', 0):.2f} items/sec")
            
            # Check for stalls
            if processed_since_last == 0:
                stall_count += 1
                if stall_count >= 6:  # 1 minute of no progress
                    logger.warning(f"Processing appears stalled (no progress for {stall_count * 10} seconds)")
                    performance_monitor.increment_counter("stalls")
            else:
                stall_count = 0
            
            last_processed = current_processed
            
            # Check if processing is complete
            stats = self.thread_manager.get_statistics()
            queue_empty = stats['queue_size'] == 0
            working_threads = len([
                t for t in stats['threads'].values() 
                if t['state'] == 'working'
            ])
            
            if queue_empty and working_threads == 0:
                logger.info("Processing completed - stopping progress monitoring")
                break
    
    def get_statistics(self) -> Dict:
        """Get processing statistics."""
        return self.thread_manager.get_statistics()
    
    def shutdown(self) -> None:
        """Shutdown the processor and all threads."""
        logger.info("Shutting down MultithreadedProcessor...")
        self.thread_manager.shutdown(wait=True)
        logger.info("MultithreadedProcessor shutdown complete")