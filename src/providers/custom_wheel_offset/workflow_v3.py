#!/usr/bin/env python3
# Refactored workflow_v3.py - Main orchestrator with multi-threading support for stage 2
import os
import sys
from pathlib import Path
from typing import Dict, Set, Tuple

# Ensure `src` is on sys.path for absolute imports
SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from .workflow_session_manager import initialize_session
from .year_make_collector import collect_year_make
from .vehicle_data_processor import backfill_vehicle_types
from .preference_processor import backfill_preferences
from .combination_processor import process_new_full_combinations
from .multithreaded_processor import MultithreadedProcessor
from .config_manager import get_config
from .cache_ops import load_full_cache_from_db
from .optimized_cache_ops import load_full_cache_from_db_optimized
from .logging_config import init_module_logger
from .realtime_processor import realtime_processor

# Initialize logger for this module
logger = init_module_logger("workflow_v3")


def run() -> None:
    """Main workflow orchestrator with multi-threading support for stage 2."""
    config = get_config()
    use_multithreading = config.get('multithreading', True)
    use_realtime = config.get('realtime_processing', False)
    max_workers = config.get('workers', 200)
    
    logger.info(
        f"Config: fast={config['fast']}, pref_fetch={config['pref_fetch']}, "
        f"fetch_vehicle_data={config['fetch_vehicle_data']}, multithreading={use_multithreading}, "
        f"realtime_processing={use_realtime}, workers={max_workers}"
    )
    
    # STAGE 1: Single-threaded initialization and data collection
    logger.info("Starting Stage 1: Single-threaded initialization")
    
    # Initialize performance monitoring for the entire workflow
    from .performance_monitor import performance_monitor, MetricTracker
    
    with MetricTracker("workflow_v3_complete"):
        # Initialize session
        initialize_session()
    
    # Collect year/make combinations
        with MetricTracker("collect_year_make"):
            existing = collect_year_make(config)
        
        # Load from database using optimized operations
        with MetricTracker("load_database_cache"):
            db_cache = load_full_cache_from_db_optimized()
            full_combos = db_cache.get("combinations", {})
        
        if not isinstance(full_combos, dict):
            full_combos = {}
            
        existing_full_keys = set(full_combos.keys())
        logger.info(f"Full-cache loaded with {len(existing_full_keys)} combinations")
        
        # Pass empty dict for backward compatibility, but functions will use database
        full_cache = {"combinations": {}, "total_combinations": len(existing_full_keys)}
        
        # Process backfills (single-threaded)
        with MetricTracker("backfill_vehicle_types"):
            backfill_vehicle_types(full_cache, config)
        with MetricTracker("backfill_preferences"):
            backfill_preferences(full_cache, config)
        
        logger.info("Stage 1 completed")
        
        # STAGE 2: Multi-threaded or single-threaded combination processing
        logger.info(f"Starting Stage 2: {'Real-time' if use_realtime else 'Multi-threaded' if use_multithreading else 'Single-threaded'} combination processing")
        
        if use_realtime:
            # Use real-time processor directly
            logger.info("Using real-time processing for immediate memory optimization")
            try:
                with MetricTracker("realtime_processing"):
                    added_full = realtime_processor.process_year_make_realtime(existing)
            finally:
                # Ensure real-time processor is properly shut down
                realtime_processor.shutdown()
                logger.info("Real-time processor terminated after Stage 2")
        elif use_multithreading:
            # Use multi-threaded processor (can use real-time or traditional approach)
            processor = MultithreadedProcessor(max_workers=max_workers)
            try:
                with MetricTracker("multithreaded_processing"):
                    added_full = processor.process_combinations_multithreaded(existing)
            finally:
                # Ensure threads are properly terminated
                processor.shutdown()
                logger.info("All threads terminated after Stage 2")
        else:
            # Use single-threaded processor (original implementation)
            with MetricTracker("single_threaded_processing"):
                added_full = process_new_full_combinations(existing, full_cache, config)
        
        logger.info(f"Stage 2 completed. Added {added_full} new preference entries.")
        
        # Print comprehensive performance report
        performance_monitor.print_performance_report()
        
        # Export metrics for analysis
        metrics_file = performance_monitor.export_metrics()
        if metrics_file:
            logger.info(f"Workflow performance metrics exported to: {metrics_file}")
        
        # FUTURE STAGES: Will be single-threaded
        # Any additional stages added here will run single-threaded
        # This ensures that multi-threading is contained only to Stage 2


if __name__ == "__main__":
    run()