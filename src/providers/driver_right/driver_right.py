#!/usr/bin/env python3
"""
Driver Right scraper for vehicle data.
Iterates through all years, makes, models, body types, and sub models.
Console logs final vehicle information instead of saving to database.
"""

import json
import time
import threading
from datetime import datetime
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from .session_manager import SessionManager
from .utils import (
    get_all_years,
    get_all_makes,
    get_all_models,
    get_all_body_types,
    get_all_sub_models,
    get_vehicle_data_from_drd_na
)
from ...services.repository import (
    insert_driver_right_ymm,
    insert_driver_right_vehicle_spec,
    insert_driver_right_tire_options,
    insert_error_log,
    get_last_driver_right_ymm
)
from ...core.errors import DataSplicingError
from ...config.driver_right import DRIVER_RIGHT_DEFAULT_REGION_ID

# Thread-safe lock for database operations
db_lock = threading.Lock()

# Global restart lock to prevent multiple threads from restarting simultaneously
restart_lock = threading.Lock()
restart_in_progress = False

def handle_process_restart(error_context: Dict[str, Any], error_message: str) -> bool:
    """
    Thread-safe process restart handler.
    Only allows one thread to restart the process at a time.
    
    Args:
        error_context: Context information about the error
        error_message: The error message
        
    Returns:
        bool: True if this thread initiated the restart, False if another thread is handling it
    """
    global restart_in_progress
    
    with restart_lock:
        if restart_in_progress:
            print(f"      [Thread-{threading.current_thread().ident}] Another thread is already handling restart, skipping...")
            return False
        
        # Mark restart as in progress
        restart_in_progress = True
        print(f"      [Thread-{threading.current_thread().ident}] Initiating process restart due to error: {error_message}")
        
        try:
            # Log the error that caused the restart
            insert_error_log('driver_right', error_context, f"Process restart triggered: {error_message}")
            
            # Here you would implement the actual restart logic
            # For now, we'll just log and exit - the external process manager should restart
            print(f"      [Thread-{threading.current_thread().ident}] Process restart required. Exiting...")
            
            # Exit the process - external process manager should restart it
            import sys
            sys.exit(1)
            
        except Exception as restart_error:
            print(f"      [Thread-{threading.current_thread().ident}] Error during restart: {restart_error}")
            restart_in_progress = False  # Reset flag on restart failure
            return False
    
    return True


def format_vehicle_info(vehicle_data: Dict[str, Any], context: Dict[str, Any]) -> str:
    """
    Format vehicle information for console output.
    
    Args:
        vehicle_data: Vehicle data from API
        context: Context information (year, make, model, etc.)
        
    Returns:
        Formatted string for console output
    """
    formatted_info = {
        "context": context,
        "vehicle_data": vehicle_data,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    return json.dumps(formatted_info, indent=2, ensure_ascii=False)


def process_sub_model(sub_model_data: Dict[str, Any], year: str, make: str, model: str, 
                     body_type: str, last_data: Dict[str, Any], thread_id: int = 0) -> Dict[str, Any]:
    """
    Process a single sub-model in a thread-safe manner.
    
    Args:
        sub_model_data: Sub-model data from API
        year, make, model, body_type: Vehicle identifiers
        last_data: Resume data for comparison
        thread_id: Thread identifier for logging
        
    Returns:
        Dict with processing results
    """
    def _norm(s: str | None) -> str:
        return (s or "").strip().lower()
    
    sub_model_name = sub_model_data.get('SubModel', 'Unknown')
    print(f"        [Thread-{thread_id}] Processing sub model: {sub_model_name}")
    
    # Guard: never re-insert the exact last row
    if (last_data and _norm(year) == last_data['year_norm'] and _norm(make) == last_data['make_norm'] and 
        _norm(model) == last_data['model_norm'] and _norm(body_type) == last_data['body_type_norm'] and 
        _norm(sub_model_name) == last_data['sub_model_norm']):
        return {"skipped": True, "reason": "duplicate_last_row"}
    
    # Extract DRD IDs
    drd_model_id = sub_model_data.get('DRModelID') or sub_model_data.get('DRDModelID')
    drd_chassis_id = sub_model_data.get('DRChassisID') or sub_model_data.get('DRDChassisID')
    
    if not drd_model_id or not drd_chassis_id:
        print(f"          [Thread-{thread_id}] Missing DRD IDs in sub model data")
        return {"error": "missing_drd_ids"}
    
    print(f"          [Thread-{thread_id}] DRD IDs found - ModelID: {drd_model_id}, ChassisID: {drd_chassis_id}")
    
    try:
        # Get vehicle data from DRD API
        vehicle_data = get_vehicle_data_from_drd_na(drd_model_id, drd_chassis_id)
        
        if not vehicle_data:
            print(f"          [Thread-{thread_id}] No vehicle data returned for ModelID: {drd_model_id}, ChassisID: {drd_chassis_id}")
            return {"error": "no_vehicle_data"}
        
        # Handle different response structures
        if 'data' in vehicle_data:
            data = vehicle_data['data']
        else:
            data = vehicle_data
        
        # Thread-safe database operations
        with db_lock:
            # Insert YMM record
            ymm_id = insert_driver_right_ymm(
                year=year,
                make=make,
                model=model,
                body_type=body_type,
                sub_model=sub_model_name,
                drd_model_id=str(drd_model_id),
                drd_chassis_id=str(drd_chassis_id)
            )
            print(f"          [Thread-{thread_id}] Inserted YMM record with ID: {ymm_id}")
            
            # Insert vehicle specifications if available
            spec_id = None
            if 'DRDChassisReturn' in data:
                spec_id = insert_driver_right_vehicle_spec(ymm_id, data['DRDChassisReturn'])
                print(f"          [Thread-{thread_id}] Inserted vehicle spec with ID: {spec_id}")
            elif 'DRDChassisReturn_NA' in data:
                spec_id = insert_driver_right_vehicle_spec(ymm_id, data['DRDChassisReturn_NA'])
                print(f"          [Thread-{thread_id}] Inserted vehicle spec with ID: {spec_id}")
            
            # Insert tire options if available
            tire_count = 0
            if 'DRDModelReturn' in data:
                model_return = data['DRDModelReturn']
                primary_option = model_return.get('PrimaryOption')
                options = model_return.get('Options', [])
                
                tire_count = insert_driver_right_tire_options(ymm_id, primary_option, options)
                print(f"          [Thread-{thread_id}] Inserted {tire_count} tire options")
            
            print(f"          [Thread-{thread_id}] Successfully saved vehicle data for {year} {make} {model} {body_type} {sub_model_name}")
            
            return {
                "success": True,
                "ymm_id": ymm_id,
                "spec_id": spec_id,
                "tire_count": tire_count,
                "combination": f"{year}-{make}-{model}-{body_type}-{sub_model_name}"
            }
            
    except Exception as e:
        error_context = {
            'year': year,
            'make': make,
            'model': model,
            'body_type': body_type,
            'sub_model': sub_model_name,
            'drd_model_id': drd_model_id,
            'drd_chassis_id': drd_chassis_id
        }
        insert_error_log('driver_right', error_context, str(e))
        print(f"          [Thread-{thread_id}] Error processing vehicle data: {e}")
        
        # Check if this is a critical error that requires restart
        if "connection" in str(e).lower() or "timeout" in str(e).lower() or "network" in str(e).lower() or "database" in str(e).lower():
            handle_process_restart(error_context, f"Critical error in sub-model processing: {e}")
        
        return {"error": str(e)}


def process_body_type(body_type: str, year: str, make: str, model: str, 
                     last_data: Dict[str, Any], thread_id: int = 0) -> Dict[str, Any]:
    """
    Process a single body type and all its sub-models using threading.
    
    Args:
        body_type: Body type to process
        year, make, model: Vehicle identifiers
        last_data: Resume data for comparison
        thread_id: Thread identifier for logging
        
    Returns:
        Dict with processing results
    """
    def _norm(s: str | None) -> str:
        return (s or "").strip().lower()
    
    print(f"      [Thread-{thread_id}] Processing {year} {make} {model} {body_type}")
    
    # Get all sub models for this year/make/model/body_type
    sub_models = get_all_sub_models(year, make, model, body_type, DRIVER_RIGHT_DEFAULT_REGION_ID)
    print(f"        [Thread-{thread_id}] Found {len(sub_models)} sub models for {year} {make} {model} {body_type}")
    
    # Determine starting sub model index only for the matching year/make/model/body_type
    if (last_data and _norm(year) == last_data['year_norm'] and _norm(make) == last_data['make_norm'] and 
        _norm(model) == last_data['model_norm'] and _norm(body_type) == last_data['body_type_norm']):
        # Find the sub model that matches the last one
        sub_models_start = 0
        for i, sub_model_data in enumerate(sub_models):
            sub_model_name = sub_model_data.get('SubModel', 'Unknown')
            if _norm(sub_model_name) == last_data['sub_model_norm']:
                sub_models_start = i + 1  # Start from the next sub model
                break
    else:
        sub_models_start = 0
    
    results = []
    
    # Process sub-models with threading (max 4 concurrent sub-models per body type)
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix=f"SubModel-{thread_id}") as executor:
        future_to_submodel = {}
        
        for i, sub_model_data in enumerate(sub_models[sub_models_start:], sub_models_start):
            future = executor.submit(
                process_sub_model, 
                sub_model_data, 
                year, make, model, body_type, 
                last_data, 
                i
            )
            future_to_submodel[future] = sub_model_data
        
        for future in as_completed(future_to_submodel):
            sub_model_data = future_to_submodel[future]
            try:
                result = future.result()
                results.append(result)
                
                # Small delay to avoid overwhelming the API
                time.sleep(0.05)
                
            except Exception as e:
                sub_model_name = sub_model_data.get('SubModel', 'Unknown')
                print(f"        [Thread-{thread_id}] Exception in sub-model {sub_model_name}: {e}")
                results.append({"error": str(e), "sub_model": sub_model_name})
    
    successful_results = [r for r in results if r.get("success")]
    print(f"      [Thread-{thread_id}] Completed {body_type}: {len(successful_results)}/{len(results)} successful")
    
    return {
        "success": True,
        "body_type": body_type,
        "total_processed": len(results),
        "successful": len(successful_results),
        "results": results
    }


def main():
    """Main function to scrape Driver Right data and save to database."""
    print("Starting Driver Right scraper...")
    
    # Initialize session
    session_manager = SessionManager()
    session = session_manager.session
    
    # Helper for case-insensitive, trimmed comparison
    def _norm(s: str | None) -> str:
        return (s or "").strip().lower()
    
    # Determine resume point from the last inserted row
    last = get_last_driver_right_ymm()
    if last:
        print(
            "resuming after last row:",
            last.id,
            last.year,
            last.make,
            last.model,
            last.body_type,
            last.sub_model,
            getattr(last, "created_at", None),
        )
    else:
        print("no previous rows found; starting from beginning")
    
    # Precompute normalized last values
    last_year_norm = _norm(getattr(last, "year", None))
    last_make_norm = _norm(getattr(last, "make", None))
    last_model_norm = _norm(getattr(last, "model", None))
    last_body_type_norm = _norm(getattr(last, "body_type", None))
    last_sub_model_norm = _norm(getattr(last, "sub_model", None))
    
    try:
        # Get all years
        years = get_all_years()
        print(f"Found {len(years)} years to process")
        
        # Guard: if resuming but the last year isn't present, raise splicing error
        if last:
            if not any(_norm(y) == last_year_norm for y in years):
                insert_error_log(
                    source="driver_right",
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
        
        for year in years[years_start:]:
            print(f"Processing {year}")
            
            # Get all makes for this year
            makes = get_all_makes(year, DRIVER_RIGHT_DEFAULT_REGION_ID)
            print(f"  Found {len(makes)} makes for {year}")
            
            # Determine starting make index only for the matching year
            if last and _norm(year) == last_year_norm:
                # Guard: last make must be present for resume
                if not any(_norm(m) == last_make_norm for m in makes):
                    insert_error_log(
                        source="driver_right",
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
            
            for make in makes[makes_start:]:
                print(f"  Processing {year} {make}")
                
                # Get all models for this year/make
                models = get_all_models(year, make, DRIVER_RIGHT_DEFAULT_REGION_ID)
                print(f"    Found {len(models)} models for {year} {make}")
                
                # Determine starting model index only for the matching year/make
                if last and _norm(year) == last_year_norm and _norm(make) == last_make_norm:
                    # Guard: last model must be present for resume
                    if not any(_norm(mdl) == last_model_norm for mdl in models):
                        insert_error_log(
                            source="driver_right",
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
                
                for model in models[models_start:]:
                    print(f"    Processing {year} {make} {model}")
                    
                    # Get all body types for this year/make/model
                    body_types = get_all_body_types(year, make, model, DRIVER_RIGHT_DEFAULT_REGION_ID)
                    print(f"      Found {len(body_types)} body types for {year} {make} {model}")
                    
                    # Determine starting body type index only for the matching year/make/model
                    if last and _norm(year) == last_year_norm and _norm(make) == last_make_norm and _norm(model) == last_model_norm:
                        # Guard: last body type must be present for resume
                        if not any(_norm(bt) == last_body_type_norm for bt in body_types):
                            insert_error_log(
                                source="driver_right",
                                context={"op": "resume_body_type_check", "last_body_type": last.body_type, "year": year, "make": make, "model": model, "body_types_count": len(body_types)},
                                message="Last body type not found in fetched body types"
                            )
                            raise DataSplicingError("Resume body type not found in body types list")
                        try:
                            body_types_start = next(i for i, bt in enumerate(body_types) if _norm(bt) == last_body_type_norm)
                        except StopIteration:
                            body_types_start = 0
                    else:
                        body_types_start = 0
                    
                    # Prepare last data for threading
                    last_data = None
                    if last:
                        last_data = {
                            'year_norm': last_year_norm,
                            'make_norm': last_make_norm,
                            'model_norm': last_model_norm,
                            'body_type_norm': last_body_type_norm,
                            'sub_model_norm': last_sub_model_norm
                        }
                    
                    # Process body types with threading (max 3 concurrent body types per model)
                    with ThreadPoolExecutor(max_workers=3, thread_name_prefix=f"BodyType-{year}-{make}-{model}") as executor:
                        future_to_bodytype = {}
                        
                        for i, body_type in enumerate(body_types[body_types_start:], body_types_start):
                            future = executor.submit(
                                process_body_type,
                                body_type,
                                year, make, model,
                                last_data,
                                i
                            )
                            future_to_bodytype[future] = body_type
                        
                        for future in as_completed(future_to_bodytype):
                            body_type = future_to_bodytype[future]
                            try:
                                result = future.result()
                                if result.get("success"):
                                    print(f"      Completed {body_type}: {result['successful']}/{result['total_processed']} successful")
                                else:
                                    print(f"      Failed to process {body_type}")
                                    
                            except Exception as e:
                                print(f"      Exception in body type {body_type}: {e}")
                                insert_error_log('driver_right', {'year': year, 'make': make, 'model': model, 'body_type': body_type}, str(e))
                                # Check if this is a critical error that requires restart
                                if "connection" in str(e).lower() or "timeout" in str(e).lower() or "network" in str(e).lower():
                                    error_context = {'year': year, 'make': make, 'model': model, 'body_type': body_type}
                                    handle_process_restart(error_context, f"Critical network error in body type processing: {e}")
    
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        insert_error_log('driver_right', {'stage': 'main_loop'}, str(e))
        # Handle critical error with restart mechanism
        error_context = {'stage': 'main_loop'}
        handle_process_restart(error_context, f"Fatal error in main loop: {e}")
    finally:
        print("Driver Right scraper finished")


if __name__ == "__main__":
    main()