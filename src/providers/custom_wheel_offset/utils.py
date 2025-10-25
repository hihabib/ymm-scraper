"""
Custom Wheel Offset API utilities for vehicle data retrieval.
Provides functions to fetch years, makes, models, trims, drive types, and complete vehicle data.
"""
import requests
import json
import logging
from typing import List, Dict, Any, Optional
from urllib.parse import urlencode

try:
    # Try relative import first (when run as module)
    from .session_manager_threaded import threaded_session_manager
    from .session_restart import handle_session_expired_error
except ImportError:
    # Fallback to direct import (when run directly)
    from session_manager_threaded import threaded_session_manager
    # Import session restart with fallback
    try:
        from session_restart import handle_session_expired_error
    except ImportError:
        # If session_restart is not available, create a dummy function
        def handle_session_expired_error(context="unknown"):
            print(f"[utils] Session expired in {context}, but restart functionality not available")
            return

import time
import random
from urllib.parse import quote

# Handle imports - always use absolute imports to avoid issues
import sys
import inspect
from pathlib import Path

# Always use absolute imports
SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
from config.proxy import (
    PROXY_DNS1, PROXY_USER, PROXY_PASS,
    get_dns_rotation_iterator, get_proxy_config_with_dns, TOTAL_MAX_RETRIES
)

# Import database logging function
try:
    from services.repository_optimized import insert_error_log
except ImportError:
    try:
        from services.repository import insert_error_log
    except ImportError:
        # Fallback function if database logging is not available
        def insert_error_log(source, context, message):
            print(f"[utils] Database logging not available - {source}: {message}")
            pass

# Base URL for Custom Wheel Offset API
BASE_URL = "https://www.enthusiastenterprises.us/fitment/vehicle/co"

# Request timeout in seconds
TIMEOUT = 30

# Get the threaded session instance for current thread
session = threaded_session_manager.get_session()

# Enhanced headers to better mimic real browser requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Connection": "keep-alive",
    "Referer": "https://www.customwheeloffset.com/",
    "Origin": "https://www.customwheeloffset.com",
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-Dest": "empty",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "DNT": "1",
}


def _get_proxy_config() -> Dict[str, str]:
    """Get proxy configuration for requests using first DNS for backward compatibility."""
    return {
        "http": f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_DNS1}",
        "https": f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_DNS1}"
    }


def _make_request(url: str, add_delay: bool = True, max_retries: int = 3) -> Optional[Any]:
    """Make a GET request with DNS rotation, session management, error handling, and retry logic."""
    last_error = None
    last_status_code = None
    
    # Use DNS rotation for improved reliability
    dns_iterator = get_dns_rotation_iterator()
    
    for attempt in range(TOTAL_MAX_RETRIES):
        try:
            # Add random delay to mimic human behavior and avoid rate limiting
            if add_delay:
                delay = random.uniform(0.5, 2.0)  # Random delay between 0.5-2 seconds
                time.sleep(delay)
            
            # Get next DNS server from rotation
            dns_server = next(dns_iterator)
            proxies = get_proxy_config_with_dns(dns_server)
            
            # Get fresh session for each attempt (reset session after failures)
            if attempt > 0:
                print(f"[utils] Resetting session for retry attempt {attempt + 1}/{TOTAL_MAX_RETRIES}")
                threaded_session_manager.reset_session()
            
            # Get current session (fresh or existing)
            current_session = threaded_session_manager.get_session()
            
            print(f"[utils] DNS Rotation - Attempt {attempt + 1}/{TOTAL_MAX_RETRIES} using DNS: {dns_server}")
            
            # Use session for connection pooling and cookie management
            response = current_session.get(
                url,
                headers=HEADERS,
                proxies=proxies,
                timeout=TIMEOUT,
                verify=True,
                allow_redirects=True
            )
            response.raise_for_status()
            print(f"[utils] DNS Rotation - Success with DNS: {dns_server}")
            return response.json()
        except requests.exceptions.Timeout as e:
            print(f"[utils] Timeout on attempt {attempt + 1}/{TOTAL_MAX_RETRIES} for {url} with DNS {dns_server}: {e}")
            last_error = str(e)
            if attempt == TOTAL_MAX_RETRIES - 1:
                print(f"[utils] All {TOTAL_MAX_RETRIES} attempts failed due to timeout for {url}")
                _log_failed_request(url, "timeout", last_error, TOTAL_MAX_RETRIES)
                return None
            # Wait longer before retry
            time.sleep(1.0)  # Fixed delay for DNS rotation
        except requests.exceptions.RequestException as e:
            print(f"[utils] Request failed on attempt {attempt + 1}/{TOTAL_MAX_RETRIES} for {url} with DNS {dns_server}: {e}")
            last_error = str(e)
            
            # Store status code if available
            if hasattr(e, 'response') and e.response is not None:
                last_status_code = e.response.status_code
                
            if attempt == TOTAL_MAX_RETRIES - 1:
                # Log failed request to database after all retries exhausted
                error_type = f"http_{last_status_code}" if last_status_code else "request_exception"
                _log_failed_request(url, error_type, last_error, TOTAL_MAX_RETRIES, last_status_code)
                return None
            # Wait before retry
            time.sleep(1.0)  # Fixed delay for DNS rotation
        except ValueError as e:
            print(f"[utils] JSON decode failed for {url} with DNS {dns_server}: {e}")
            last_error = str(e)
            if attempt == TOTAL_MAX_RETRIES - 1:
                _log_failed_request(url, "json_decode_error", last_error, TOTAL_MAX_RETRIES)
                return None
    
    return None


def _log_failed_request(url: str, error_type: str, error_message: str, max_retries: int, status_code: Optional[int] = None) -> None:
    """Log failed request to database after all retries exhausted."""
    try:
        context = {
            "url": url,
            "error_type": error_type,
            "max_retries": max_retries,
            "final_attempt": True
        }
        
        if status_code:
            context["status_code"] = status_code
            
        insert_error_log(
            source="utils_request_failure",
            context=context,
            message=f"Request failed after {max_retries} attempts: {error_message}"
        )
        print(f"[utils] Logged failed request to database: {url} ({error_type})")
        
    except Exception as log_error:
        print(f"[utils] Failed to log request failure to database: {log_error}")


def _initialize_session():
    """Light session initialization: set headers only, no homepage visit.
    This avoids blocking human verification flows. PHPSESSID can be set via
    resolve_captcha.get_phpsessid_from_api() before requests.
    """
    try:
        # Set session headers
        session.headers.update(HEADERS)
        print("[utils] Light session init: headers set; no homepage visit")
        return True
    except Exception as e:
        print(f"[utils] Failed to initialize session: {e}")
        # Trigger session restart on initialization failure
        try:
            handle_session_expired_error("session_initialization_failed")
        except Exception as restart_error:
            print(f"[utils] Failed to restart session after initialization error: {restart_error}")
        return False


# Initialize session on module load
_session_initialized = _initialize_session()


def get_years() -> List[str]:
    """
    Get available years for Custom Wheel Offset vehicles.
    
    Returns:
        List of year strings (e.g., ["1948", "1949", ...])
    """
    url = f"{BASE_URL}/"
    print(f"[utils] Fetching years from {url}")
    
    result = _make_request(url)
    if result is None:
        print("[utils] Failed to fetch years")
        return []
    
    if isinstance(result, list):
        years = [str(year) for year in result if year]
        # Sort years numerically in descending order
        years.sort(key=int, reverse=True)
        print(f"[utils] Found {len(years)} years")
        return years
    
    print(f"[utils] Unexpected response format for years: {type(result)}")
    return []


def get_makes(year: str) -> List[str]:
    """
    Get available makes for a specific year.
    
    Args:
        year: Year string (e.g., "2025")
        
    Returns:
        List of make strings (e.g., ["Mazda", "Hyundai", ...])
    """
    url = f"{BASE_URL}/{quote(str(year))}"
    print(f"[utils] Fetching makes for year {year} from {url}")
    
    result = _make_request(url)
    if result is None:
        print(f"[utils] Failed to fetch makes for year {year}")
        return []
    
    if isinstance(result, list):
        makes = [str(make) for make in result if make]
        # Sort makes alphabetically in ascending order
        makes.sort()
        print(f"[utils] Found {len(makes)} makes for year {year}")
        return makes
    
    print(f"[utils] Unexpected response format for makes: {type(result)}")
    return []


def get_models(year: str, make: str) -> List[str]:
    """
    Get available models for a specific year and make.
    
    Args:
        year: Year string (e.g., "2025")
        make: Make string (e.g., "Acura")
        
    Returns:
        List of model strings (e.g., ["TLX", "Integra", ...])
    """
    url = f"{BASE_URL}/{quote(str(year))}/{quote(str(make))}"
    print(f"[utils] Fetching models for {year} {make} from {url}")
    
    result = _make_request(url)
    if result is None:
        print(f"[utils] Failed to fetch models for {year} {make}")
        return []
    
    if isinstance(result, list):
        models = [str(model) for model in result if model]
        # Sort models alphabetically in ascending order
        models.sort()
        print(f"[utils] Found {len(models)} models for {year} {make}")
        return models
    
    print(f"[utils] Unexpected response format for models: {type(result)}")
    return []


def get_ymm_info(year: str, make: str, model: str) -> Dict[str, List[str]]:
    """
    Get vehicle information (vehicle type, drive, trim) for year/make/model.
    
    Args:
        year: Year string (e.g., "2025")
        make: Make string (e.g., "Acura")
        model: Model string (e.g., "RDX")
        
    Returns:
        Dictionary with keys: vehicleType, drive, trim
        Example: {"vehicleType":["Truck"],"drive":["AWD"],"trim":["SH-AWD"]}
    """
    url = f"{BASE_URL}/{quote(str(year))}/{quote(str(make))}/{quote(str(model))}"
    print(f"[utils] Fetching YMM info for {year} {make} {model} from {url}")
    
    result = _make_request(url)
    if result is None:
        print(f"[utils] Failed to fetch YMM info for {year} {make} {model}")
        return {"vehicleType": [], "drive": [], "trim": []}
    
    if isinstance(result, dict):
        # Ensure all expected keys exist with list values
        ymm_info = {
            "vehicleType": result.get("vehicleType", []),
            "drive": result.get("drive", []),
            "trim": result.get("trim", [])
        }
        
        # Convert to lists if they're not already
        for key in ymm_info:
            if not isinstance(ymm_info[key], list):
                ymm_info[key] = [ymm_info[key]] if ymm_info[key] else []
        
        print(f"[utils] Found YMM info for {year} {make} {model}: "
              f"{len(ymm_info['trim'])} trims, {len(ymm_info['drive'])} drives")
        return ymm_info
    
    print(f"[utils] Unexpected response format for YMM info: {type(result)}")
    return {"vehicleType": [], "drive": [], "trim": []}


def get_trims(year: str, make: str, model: str) -> List[str]:
    """
    Get available trims for a specific year, make, and model.
    
    Args:
        year: Year string (e.g., "2025")
        make: Make string (e.g., "Acura")
        model: Model string (e.g., "RDX")
        
    Returns:
        List of trim strings (e.g., ["SH-AWD"])
    """
    ymm_info = get_ymm_info(year, make, model)
    trims = ymm_info.get("trim", [])
    # Sort trims alphabetically in ascending order
    trims.sort()
    print(f"[utils] Found {len(trims)} trims for {year} {make} {model}")
    return trims


def get_vehicle_types(year: str, make: str, model: str) -> List[str]:
    """
    Get available vehicle types for a specific year, make, and model.
    
    Args:
        year: Year string (e.g., "2025")
        make: Make string (e.g., "Acura")
        model: Model string (e.g., "RDX")
        
    Returns:
        List of vehicle type strings (e.g., ["Truck", "Car"])
    """
    ymm_info = get_ymm_info(year, make, model)
    vehicle_types = ymm_info.get("vehicleType", [])
    # Sort vehicle types alphabetically in ascending order
    vehicle_types.sort()
    print(f"[utils] Found {len(vehicle_types)} vehicle types for {year} {make} {model}")
    return vehicle_types


def get_drive_types(year: str, make: str, model: str, trim: str) -> List[str]:
    """
    Get available drive types for a specific year, make, model, and trim.
    
    Args:
        year: Year string (e.g., "2025")
        make: Make string (e.g., "Acura")
        model: Model string (e.g., "RDX")
        trim: Trim string (e.g., "SH-AWD")
        
    Returns:
        List of drive type strings (e.g., ["AWD"])
    """
    url = f"{BASE_URL}/offsetguide/{quote(str(year))}/{quote(str(make))}/{quote(str(model))}/trim/{quote(str(trim))}/drives"
    print(f"[utils] Fetching drive types for {year} {make} {model} {trim} from {url}")
    
    result = _make_request(url)
    if result is None:
        print(f"[utils] Failed to fetch drive types for {year} {make} {model} {trim}")
        return []
    
    if isinstance(result, list):
        drives = [str(drive) for drive in result if drive]
        # Sort drive types alphabetically in ascending order
        drives.sort()
        print(f"[utils] Found {len(drives)} drive types for {year} {make} {model} {trim}")
        return drives
    
    print(f"[utils] Unexpected response format for drive types: {type(result)}")
    return []


def get_vehicle_data(year: str, make: str, model: str, trim: str, drive: str) -> Optional[Dict[str, Any]]:
    """
    Get complete vehicle data for Custom Wheel Offset.
    
    Args:
        year: Year string (e.g., "2025")
        make: Make string (e.g., "Acura")
        model: Model string (e.g., "RDX")
        trim: Trim string (e.g., "SH-AWD")
        drive: Drive type string (e.g., "AWD")
        
    Returns:
        Dictionary with complete vehicle data including wheel specs, bolt pattern, etc.
        Returns None if request fails.
    """
    url = f"{BASE_URL}/{quote(str(year))}/{quote(str(make))}/{quote(str(model))}/{quote(str(trim))}/{quote(str(drive))}"
    print(f"[utils] Fetching vehicle data for {year} {make} {model} {trim} {drive} from {url}")
    
    result = _make_request(url)
    if result is None:
        print(f"[utils] Failed to fetch vehicle data for {year} {make} {model} {trim} {drive}")
        return None
    
    if isinstance(result, dict):
        print(f"[utils] Successfully fetched vehicle data for {year} {make} {model} {trim} {drive}")
        return result
    
    print(f"[utils] Unexpected response format for vehicle data: {type(result)}")
    return None


if __name__ == "__main__":
    # Test the functions
    print("Testing Custom Wheel Offset API functions...")
    
    # Test get_years
    years = get_years()
    print(f"Years: {years[:5]}..." if len(years) > 5 else f"Years: {years}")
    
    if years:
        # Test get_makes with the latest year
        latest_year = years[-1] if years else "2025"
        makes = get_makes(latest_year)
        print(f"Makes for {latest_year}: {makes[:5]}..." if len(makes) > 5 else f"Makes for {latest_year}: {makes}")
        
        if makes:
            # Test get_models
            first_make = makes[0]
            models = get_models(latest_year, first_make)
            print(f"Models for {latest_year} {first_make}: {models}")
            
            if models:
                # Test get_ymm_info and get_trims
                first_model = models[0]
                ymm_info = get_ymm_info(latest_year, first_make, first_model)
                print(f"YMM info for {latest_year} {first_make} {first_model}: {ymm_info}")
                
                trims = get_trims(latest_year, first_make, first_model)
                print(f"Trims for {latest_year} {first_make} {first_model}: {trims}")
                
                if trims:
                    # Test get_drive_types
                    first_trim = trims[0]
                    drives = get_drive_types(latest_year, first_make, first_model, first_trim)
                    print(f"Drive types for {latest_year} {first_make} {first_model} {first_trim}: {drives}")
                    
                    if drives:
                        # Test get_vehicle_data
                        first_drive = drives[0]
                        vehicle_data = get_vehicle_data(latest_year, first_make, first_model, first_trim, first_drive)
                        if vehicle_data:
                            print(f"Vehicle data : {vehicle_data}")
                            print(f"Stock wheel: {vehicle_data.get('stockWheelWidth')}x{vehicle_data.get('stockWheelDiameter')} "
                                  f"offset {vehicle_data.get('stockOffset')}")


__all__ = [
    "get_years",
    "get_makes", 
    "get_models",
    "get_ymm_info",
    "get_vehicle_types",
    "get_trims",
    "get_drive_types",
    "get_vehicle_data",
]