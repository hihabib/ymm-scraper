#!/usr/bin/env python3
"""
Custom Wheel Offset scraper for vehicle data.
Iterates through all years, makes, models, trims, and drive types.
"""

import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote
import sys
import os
from pathlib import Path

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
    
    # Direct imports for script mode
    from src.providers.custom_wheel_offset.utils import get_years, get_makes, get_models, get_trims, get_drive_types, get_vehicle_data
    from src.providers.custom_wheel_offset.fitment_preferences import get_fitment_preferences
    from src.providers.custom_wheel_offset.wheel_size import get_parsed_data_with_saved_token
    from src.providers.custom_wheel_offset.resolve_captcha import ensure_token_by_visiting_homepage
    
    try:
        from session_restart import handle_session_expired_error
    except ImportError:
        def handle_session_expired_error(context="unknown"):
            print(f"[custom_wheel_offset] Session expired in {context}, but restart functionality not available")
            return
    
    # Import src modules with absolute imports
    from src.services.repository_optimized import (
        insert_custom_wheel_offset_ymm, 
        insert_custom_wheel_offset_data, 
        insert_error_log,
        get_last_custom_wheel_offset_ymm,
        batch_insert_custom_wheel_offset_ymm,
        batch_insert_custom_wheel_offset_data,
        batch_insert_error_logs,
        close_thread_session
    )
    from src.db.migrate import run_migrations
    from src.core.errors import DataSplicingError
    from src.config.worker import CUSTOM_WHEEL_OFFSET_WORKERS
    
else:
    # Running as module - use relative imports
    try:
        from .vehicle_data_extractor import VehicleDataExtractor
        from .fitment_preferences import get_fitment_preferences
        from .wheel_size import get_parsed_data_with_saved_token
        from .session_restart import handle_session_expired_error
        from .resolve_captcha import ensure_token_by_visiting_homepage
        from ...services.repository import insert_custom_wheel_offset_ymm, insert_custom_wheel_offset_data, get_last_custom_wheel_offset_ymm, insert_error_log
        from ...db.migrate import run_migrations
        from ...core.errors import DataSplicingError
        from ...config.worker import CUSTOM_WHEEL_OFFSET_WORKERS
    except ImportError as e:
        print(f"[custom_wheel_offset] Relative import failed: {e}")
        raise


# Global thread lock for database operations to prevent duplicates
    # Remove the global db_lock as it's no longer needed with optimized repository
    # db_lock = threading.Lock()

# Thread coordination for error handling and restart scenarios
stop_all_threads = threading.Event()  # Signal to stop all threads
restart_required = threading.Event()  # Signal that system restart is required
restart_lock = threading.Lock()  # Ensure only one thread handles restart

# Thread pool size from configuration
THREAD_POOL_SIZE = CUSTOM_WHEEL_OFFSET_WORKERS


def is_restart_required_error(error_msg: str, error_type: str = "") -> bool:
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
        # SQLAlchemy session binding errors
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


def handle_restart_scenario(context: str = "unknown") -> bool:
    """
    Handle system restart scenario by stopping all threads and restarting.
    Only one thread will perform the restart operation.
    
    Args:
        context: Context where restart was triggered
        
    Returns:
        bool: True if this thread performed the restart, False if another thread already did
    """
    # First check without lock for performance
    if restart_required.is_set():
        print(f"[RESTART] Restart already in progress, thread {threading.current_thread().ident} skipping")
        return False
    
    with restart_lock:
        # Double-check pattern to prevent race condition
        if restart_required.is_set():
            print(f"[RESTART] Restart already initiated by another thread, thread {threading.current_thread().ident} skipping")
            return False
            
        print(f"\n[RESTART] Thread {threading.current_thread().ident} initiating system restart from context: {context}")
        restart_required.set()
        stop_all_threads.set()
        
        # Give threads time to stop gracefully
        print("[RESTART] Signaling all threads to stop...")
        time.sleep(2)
        
        # Perform the actual restart
        print("[RESTART] Initiating system restart...")
        try:
            handle_session_expired_error(context)
            print(f"[RESTART] System restart completed by thread {threading.current_thread().ident}")
            return True
        except Exception as restart_error:
            print(f"[RESTART] Failed to restart system: {restart_error}")
            raise


def process_fitment_preference(args):
    """
    Thread-safe function to process a single fitment preference combination.
    
    Args:
        args: Tuple containing (year, make, model, trim, drive, vehicle_type, dr_chassis_id, preference, last_row_data)
    
    Returns:
        dict: Processing result with success status and data
    """
    year, make, model, trim, drive, vehicle_type, dr_chassis_id, preference, last_row_data = args
    
    # Check if we should stop processing
    if stop_all_threads.is_set():
        return {"success": False, "stopped": True, "reason": "thread_stop_signal"}
    
    # Helper for case-insensitive, trimmed comparison
    def _norm(s: str | None) -> str:
        return (s or "").strip().lower()
    
    try:
        suspension = preference.get("suspension", "")
        modification = preference.get("modification", "")
        rubbing = preference.get("rubbing", "")
        
        # Guard: never re-insert the exact last row (thread-safe check)
        if last_row_data:
            last_year_norm = _norm(last_row_data.get("year"))
            last_make_norm = _norm(last_row_data.get("make"))
            last_model_norm = _norm(last_row_data.get("model"))
            last_trim_norm = _norm(last_row_data.get("trim"))
            last_drive_norm = _norm(last_row_data.get("drive"))
            last_suspension_norm = _norm(last_row_data.get("suspension"))
            last_modification_norm = _norm(last_row_data.get("modification"))
            last_rubbing_norm = _norm(last_row_data.get("rubbing"))
            
            if (_norm(year) == last_year_norm and _norm(make) == last_make_norm and 
                _norm(model) == last_model_norm and _norm(trim) == last_trim_norm and 
                _norm(drive) == last_drive_norm and _norm(suspension) == last_suspension_norm and 
                _norm(modification) == last_modification_norm and _norm(rubbing) == last_rubbing_norm):
                return {"success": True, "skipped": True, "reason": "duplicate_last_row"}
        
        # Check again if we should stop (before making network request)
        if stop_all_threads.is_set():
            return {"success": False, "stopped": True, "reason": "thread_stop_signal"}

        # Construct the URL with all parameters
        base_url = "https://www.customwheeloffset.com/store/wheels"
        params = [
            f"sort=instock",
            f"year={year}",
            f"make={make}",
            f"model={model}",
            f"trim={trim}",
            f"drive={drive}",
            f"DRChassisID={dr_chassis_id}",
            f"vehicle_type={vehicle_type}",
            f"suspension={suspension}",
            f"modification={modification}",
            f"rubbing={rubbing}",
            f"saleToggle=0",
            f"qdToggle=0"
        ]
        
        # Join parameters with & and URL encode them
        encoded_params = []
        for param in params:
            key, value = param.split("=", 1)
            encoded_params.append(f"{key}={quote(value)}")
        
        full_url = f"{base_url}?{'&'.join(encoded_params)}"
        thread_id = threading.current_thread().ident
        print(f"        [Thread-{thread_id}] URL: {full_url}")
        
        # Call get_parsed_data_with_saved_token (uses shared session)
        parsed_data = get_parsed_data_with_saved_token(full_url)
        print(f"        [Thread-{thread_id}] Response: {parsed_data}")
        
        # Check if we should stop (after network request)
        if stop_all_threads.is_set():
            return {"success": False, "stopped": True, "reason": "thread_stop_signal"}
        
        # Save data to database if parsed_data is valid (OPTIMIZED: batch database operations)
        if parsed_data and isinstance(parsed_data, dict) and ('front' in parsed_data or 'rear' in parsed_data):
            # Prepare database operation data for batching
            db_operation_data = {
                "year": year,
                "make": make,
                "model": model,
                "trim": trim,
                "drive": drive,
                "vehicle_type": vehicle_type,
                "dr_chassis_id": dr_chassis_id,
                "suspension": suspension,
                "modification": modification,
                "rubbing": rubbing,
                "bolt_pattern": parsed_data.get('bolt_pattern'),
                "parsed_data": parsed_data,
                "thread_id": thread_id
            }
            
            # Return data for batch processing instead of immediate database write
            return {
                "success": True, 
                "db_operation_data": db_operation_data,
                "combination": f"{year}-{make}-{model}-{trim}-{drive}-{suspension}-{modification}-{rubbing}"
            }
        else:
            print(f"        [Thread-{thread_id}] No valid data to save")
            return {"success": True, "skipped": True, "reason": "no_valid_data"}
            
    except Exception as e:
        thread_id = threading.current_thread().ident
        error_msg = str(e)
        print(f"        [Thread-{thread_id}] Processing error: {error_msg}")
        
        # Check if this error requires restart
        if is_restart_required_error(error_msg, "processing"):
            print(f"        [Thread-{thread_id}] Processing error requires restart")
            restart_performed = handle_restart_scenario(f"processing_error_{thread_id}")
            return {"success": False, "restart_triggered": restart_performed, "error": error_msg}
        
        return {"success": False, "error": error_msg, "error_type": "processing"}


def run_scrape() -> None:
    print("custom wheel offset ymm scraper started", flush=True)
    
    # Run migrations to ensure database tables exist
    print("Running database migrations...")
    run_migrations()
    print("Database migrations completed")
    
    # Helper for case-insensitive, trimmed comparison
    def _norm(s: str | None) -> str:
        return (s or "").strip().lower()
    
    # Determine resume point from the last inserted row
    last = get_last_custom_wheel_offset_ymm()
    if last:
        print(
            "resuming after last row:",
            last.id,
            last.year,
            last.make,
            last.model,
            last.trim,
            last.drive,
            last.vehicle_type,
            last.dr_chassis_id,
            (last.suspension or ""),
            (last.modification or ""),
            (last.rubbing or ""),
            getattr(last, "created_at", None),
        )
    else:
        print("no previous rows found; starting from beginning")
    
    # Precompute normalized last values
    last_year_norm = _norm(getattr(last, "year", None))
    last_make_norm = _norm(getattr(last, "make", None))
    last_model_norm = _norm(getattr(last, "model", None))
    last_trim_norm = _norm(getattr(last, "trim", None))
    last_drive_norm = _norm(getattr(last, "drive", None))
    last_suspension_norm = _norm(getattr(last, "suspension", None))
    last_modification_norm = _norm(getattr(last, "modification", None))
    last_rubbing_norm = _norm(getattr(last, "rubbing", None))
    
    try:
        # First, get PHPSESSID from API to ensure we have it before other operations
        print("Getting PHPSESSID from ymm-temp API...")
        from src.providers.custom_wheel_offset.resolve_captcha import get_phpsessid_from_api
        # Use default parameters since we don't have specific vehicle context here
        phpsessid = get_phpsessid_from_api()
        if phpsessid:
            print(f"Successfully obtained PHPSESSID: {phpsessid}")
        else:
            print("Warning: Could not obtain PHPSESSID from API, will try to use saved one")
        
        # Ensure captcha is solved early by visiting homepage and solving any captcha
        print("Ensuring captcha is solved by visiting homepage...")
        ensure_token_by_visiting_homepage(max_attempts=20)
        print("Captcha check completed")
        
        # Get all available years
        print("Fetching all years...")
        years = get_years()
        print(f"Found {len(years)} years to process")
        
        # Guard: if resuming but the last year isn't present, raise splicing error
        if last:
            if not any(_norm(y) == last_year_norm for y in years):
                insert_error_log(
                    source="custom_wheel_offset",
                    context={"op": "resume_year_check", "last_year": last.year, "years_count": len(years)},
                    message="Last year not found in fetched years"
                )
                raise DataSplicingError("Resume year not found in years list")
        
        # Determine starting year index
        if last:
            try:
                years_start = next(i for i, y in enumerate(years) if _norm(y) == last_year_norm)
            except StopIteration:
                years_start = 0
        else:
            years_start = 0
        
        # Loop through each year
        for year in years[years_start:]:
            print(f"\nProcessing year: {year}")
            
            # Get makes for this year
            makes = get_makes(year)
            print(f"Found {len(makes)} makes for {year}")
            
            # Determine starting make index only for the matching year
            if last and _norm(year) == last_year_norm:
                # Guard: last make must be present for resume
                if not any(_norm(m) == last_make_norm for m in makes):
                    insert_error_log(
                        source="custom_wheel_offset",
                        context={"op": "resume_make_check", "last_make": last.make, "year": year, "makes_count": len(makes)},
                        message="Last make not found in fetched makes"
                    )
                    raise DataSplicingError("Resume make not found in makes list")
                try:
                    makes_start = next(i for i, m in enumerate(makes) if _norm(m) == last_make_norm)
                except StopIteration:
                    makes_start = 0
            else:
                makes_start = 0
            
            # Loop through each make
            for make in makes[makes_start:]:
                print(f"  Processing {year} {make}")
                
                # Get models for this year/make using VehicleDataExtractor
                year_make_extractor = VehicleDataExtractor(year=year, make=make)
                models = year_make_extractor.get_models()
                print(f"    Found {len(models)} models for {year} {make}")
                
                # Determine starting model index only for the matching year/make
                if last and _norm(year) == last_year_norm and _norm(make) == last_make_norm:
                    # Guard: last model must be present for resume
                    if not any(_norm(mdl) == last_model_norm for mdl in models):
                        insert_error_log(
                            source="custom_wheel_offset",
                            context={"op": "resume_model_check", "last_model": last.model, "year": year, "make": make, "models_count": len(models)},
                            message="Last model not found in fetched models"
                        )
                        raise DataSplicingError("Resume model not found in models list")
                    try:
                        models_start = next(i for i, mdl in enumerate(models) if _norm(mdl) == last_model_norm)
                    except StopIteration:
                        models_start = 0
                else:
                    models_start = 0
                
                # Loop through each model
                for model in models[models_start:]:
                    print(f"    Processing {year} {make} {model}")
                    
                    # Get trims for this year/make/model using VehicleDataExtractor
                    year_make_model_extractor = VehicleDataExtractor(year=year, make=make, model=model)
                    trims = year_make_model_extractor.get_trims()
                    print(f"      Found {len(trims)} trims for {year} {make} {model}")
                    
                    # Determine starting trim index only for the matching year/make/model
                    if last and _norm(year) == last_year_norm and _norm(make) == last_make_norm and _norm(model) == last_model_norm:
                        # Guard: last trim must be present for resume
                        if not any(_norm(t) == last_trim_norm for t in trims):
                            insert_error_log(
                                source="custom_wheel_offset",
                                context={"op": "resume_trim_check", "last_trim": last.trim, "year": year, "make": make, "model": model, "trims_count": len(trims)},
                                message="Last trim not found in fetched trims"
                            )
                            raise DataSplicingError("Resume trim not found in trims list")
                        try:
                            trims_start = next(i for i, t in enumerate(trims) if _norm(t) == last_trim_norm)
                        except StopIteration:
                            trims_start = 0
                    else:
                        trims_start = 0
                    
                    # Loop through each trim
                    for trim in trims[trims_start:]:
                        print(f"      Processing {year} {make} {model} {trim}")
                        
                        # Get drive types for this year/make/model/trim using VehicleDataExtractor
                        year_make_model_trim_extractor = VehicleDataExtractor(year=year, make=make, model=model, trim=trim)
                        drive_types = year_make_model_trim_extractor.get_drives()
                        print(f"        Found {len(drive_types)} drive types for {year} {make} {model} {trim}")
                        
                        # Determine starting drive index only for the matching year/make/model/trim
                        if last and _norm(year) == last_year_norm and _norm(make) == last_make_norm and _norm(model) == last_model_norm and _norm(trim) == last_trim_norm:
                            # Guard: last drive must be present for resume
                            if not any(_norm(d) == last_drive_norm for d in drive_types):
                                insert_error_log(
                                    source="custom_wheel_offset",
                                    context={"op": "resume_drive_check", "last_drive": last.drive, "year": year, "make": make, "model": model, "trim": trim, "drives_count": len(drive_types)},
                                    message="Last drive not found in fetched drives"
                                )
                                raise DataSplicingError("Resume drive not found in drives list")
                            try:
                                drives_start = next(i for i, d in enumerate(drive_types) if _norm(d) == last_drive_norm)
                            except StopIteration:
                                drives_start = 0
                        else:
                            drives_start = 0
                        
                        # Loop through each drive type
                        for drive in drive_types[drives_start:]:
                            # Print the complete combination
                            vehicle_data = get_vehicle_data(year, make, model, trim, drive)
                            vehicle_type = vehicle_data['vehicleType']
                            dr_chassis_id = vehicle_data['drchassisid']
                            print(f"        COMBINATION: ({year}, {make}, {model}, {trim}, {drive}, {vehicle_type}, {dr_chassis_id})")
                            
                            # Get fitment preferences and construct URLs for each combination
                            fitment_preferences = get_fitment_preferences(vehicle_type, "wheels")
                            print(f"        Found {len(fitment_preferences)} fitment preference combinations")
                            
                            # Determine starting preference index only for the matching year/make/model/trim/drive
                            preferences_start = 0
                            if (last and _norm(year) == last_year_norm and _norm(make) == last_make_norm and 
                                _norm(model) == last_model_norm and _norm(trim) == last_trim_norm and _norm(drive) == last_drive_norm):
                                # Find the matching preference combination
                                for i, preference in enumerate(fitment_preferences):
                                    suspension = preference.get("suspension", "")
                                    modification = preference.get("modification", "")
                                    rubbing = preference.get("rubbing", "")
                                    
                                    if (_norm(suspension) == last_suspension_norm and 
                                        _norm(modification) == last_modification_norm and 
                                        _norm(rubbing) == last_rubbing_norm):
                                        preferences_start = i + 1  # Start from next preference
                                        break
                            
                            # Multi-threaded processing of fitment preference combinations
                            print(f"        Processing {len(fitment_preferences[preferences_start:])} fitment preferences with {THREAD_POOL_SIZE} threads")
                            
                            # Prepare arguments for thread pool
                            thread_args = []
                            last_row_data = None
                            if last:
                                last_row_data = {
                                    "year": last.year,
                                    "make": last.make,
                                    "model": last.model,
                                    "trim": last.trim,
                                    "drive": last.drive,
                                    "suspension": last.suspension,
                                    "modification": last.modification,
                                    "rubbing": last.rubbing
                                }
                            
                            for preference in fitment_preferences[preferences_start:]:
                                thread_args.append((
                                    year, make, model, trim, drive, vehicle_type, 
                                    dr_chassis_id, preference, last_row_data
                                ))
                            
                            # Process with thread pool
                            successful_count = 0
                            skipped_count = 0
                            error_count = 0
                            stopped_count = 0
                            restart_triggered = False
                            batch_db_operations = []  # Collect database operations for batch processing
                            
                            with ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE) as executor:
                                # Submit all tasks
                                future_to_args = {executor.submit(process_fitment_preference, args): args for args in thread_args}
                                
                                # Process completed tasks
                                for future in as_completed(future_to_args):
                                    # Check if restart was triggered by any thread
                                    if restart_required.is_set():
                                        print("        [MAIN] Restart signal detected, cancelling remaining tasks...")
                                        # Cancel remaining futures
                                        for remaining_future in future_to_args:
                                            if not remaining_future.done():
                                                remaining_future.cancel()
                                        restart_triggered = True
                                        break
                                    
                                    args = future_to_args[future]
                                    try:
                                        result = future.result()
                                        
                                        # Handle restart triggered by thread
                                        if result.get("restart_triggered"):
                                            restart_triggered = True
                                            print(f"        [MAIN] Thread triggered restart: {result.get('error', 'unknown')}")
                                            break
                                        
                                        # Handle thread stopped by signal
                                        if result.get("stopped"):
                                            stopped_count += 1
                                            print(f"        Stopped: {result.get('reason', 'unknown')}")
                                            continue
                                        
                                        # Handle normal results
                                        if result["success"]:
                                            if result.get("skipped"):
                                                skipped_count += 1
                                                print(f"        Skipped: {result.get('reason', 'unknown')}")
                                            else:
                                                # Collect database operations for batch processing
                                                if result.get("db_operation_data"):
                                                    batch_db_operations.append(result["db_operation_data"])
                                                successful_count += 1
                                                print(f"        Success: {result.get('combination', 'unknown')}")
                                        else:
                                            error_count += 1
                                            print(f"        Error ({result.get('error_type', 'unknown')}): {result.get('error', 'unknown')}")
                                            
                                    except Exception as exc:
                                        error_count += 1
                                        error_msg = str(exc)
                                        print(f"        Thread execution error: {error_msg}")
                                        
                                        # Check if this exception requires restart
                                        if is_restart_required_error(error_msg, "thread_execution"):
                                            print(f"        Thread execution error requires restart")
                                            restart_performed = handle_restart_scenario(f"thread_execution_error")
                                            if restart_performed:
                                                restart_triggered = True
                                                break
                            
                            # Batch process database operations (single-threaded, optimized)
                            if batch_db_operations and not restart_triggered:
                                print(f"        Processing {len(batch_db_operations)} database operations in batch...")
                                
                                try:
                                    # Prepare data for batch operations
                                    ymm_data_list = []
                                    data_list = []
                                    
                                    for db_op in batch_db_operations:
                                        ymm_data_list.append({
                                            "year": db_op["year"],
                                            "make": db_op["make"],
                                            "model": db_op["model"],
                                            "trim": db_op["trim"],
                                            "drive": db_op["drive"],
                                            "vehicle_type": db_op["vehicle_type"],
                                            "dr_chassis_id": db_op["dr_chassis_id"],
                                            "suspension": db_op["suspension"],
                                            "modification": db_op["modification"],
                                            "rubbing": db_op["rubbing"],
                                            "bolt_pattern": db_op["bolt_pattern"]
                                        })
                                    
                                    # Batch insert YMM records
                                    ymm_ids = batch_insert_custom_wheel_offset_ymm(ymm_data_list)
                                    print(f"        [BATCH] Inserted {len(ymm_ids)} YMM records")
                                    
                                    # Prepare wheel offset data with corresponding YMM IDs
                                    for i, db_op in enumerate(batch_db_operations):
                                        if i < len(ymm_ids):
                                            data_list.append({
                                                "ymm_id": ymm_ids[i],
                                                "parsed_data": db_op["parsed_data"]
                                            })
                                    
                                    # Batch insert wheel offset data
                                    data_count = batch_insert_custom_wheel_offset_data(data_list)
                                    print(f"        [BATCH] Inserted {data_count} wheel offset records")
                                    
                                    print(f"        Batch processing completed successfully: {len(ymm_ids)} YMM records, {data_count} wheel records")
                                    
                                except Exception as db_error:
                                    error_msg = str(db_error)
                                    print(f"        [BATCH] Database error: {error_msg}")
                                    
                                    # Check if this error requires restart
                                    if is_restart_required_error(error_msg, "batch_database"):
                                        print(f"        [BATCH] Database error requires restart")
                                        restart_performed = handle_restart_scenario(f"batch_database_error")
                                        if restart_performed:
                                            restart_triggered = True
                            
                            print(f"        Thread pool completed: {successful_count} successful, {skipped_count} skipped, {error_count} errors, {stopped_count} stopped")
                            
                            # If restart was triggered, exit the processing loops
                            if restart_triggered:
                                print("        [MAIN] Exiting processing due to restart trigger")
                                return
                            
                            # Clean up thread-local database sessions after processing
                            close_thread_session()
                            
    except KeyboardInterrupt:
        print("\nScraper interrupted by user")
    except Exception as e:
        print(f"Error during scraping: {e}")
        error_msg = str(e)
        
        # Check if this error requires restart
        if is_restart_required_error(error_msg, "main_scraping"):
            print(f"Main scraping error requires restart")
            restart_performed = handle_restart_scenario(f"main_scraping_error")
            if not restart_performed:
                print("Another thread already handled the restart, exiting gracefully")
        else:
            # For non-restart errors, just log and continue
            print(f"Non-critical error, continuing: {error_msg}")
            try:
                while True:
                    time.sleep(60)
            except KeyboardInterrupt:
                pass


if __name__ == "__main__":
    run_scrape()