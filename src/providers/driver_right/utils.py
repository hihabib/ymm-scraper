#!/usr/bin/env python3
"""
Driver Right API utility functions.
Converted from JavaScript to Python with session management and retry logic.
"""

import time
import random
from typing import List, Dict, Any, Optional
import requests
from urllib.parse import urlencode

# Import configuration and session manager
try:
    from ...config.driver_right import (
        USERNAME, SECURITY_TOKEN, BASE_URL, DEFAULT_REGION_ID,
        REQUEST_TIMEOUT, MAX_RETRIES
    )
    from .session_manager import get_shared_session
except ImportError:
    # Fallback for direct execution
    import sys
    from pathlib import Path
    SRC_DIR = Path(__file__).resolve().parents[2]
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    from config.driver_right import (
        USERNAME, SECURITY_TOKEN, BASE_URL, DEFAULT_REGION_ID,
        REQUEST_TIMEOUT, MAX_RETRIES
    )
    from providers.driver_right.session_manager import get_shared_session


def fetch_with_retry(url: str, max_retries: int = None) -> Any:
    """
    Fetch data from URL with retry logic.
    
    Args:
        url: The URL to fetch
        max_retries: Maximum number of retry attempts (None for infinite retries)
        
    Returns:
        JSON response data
        
    Raises:
        Exception: If all retry attempts fail (only when max_retries is set)
    """
    session = get_shared_session()
    last_exception = None
    attempt = 0
    
    # If max_retries is None, retry infinitely
    while max_retries is None or attempt < max_retries:
        try:
            # Add random delay to mimic human behavior
            if attempt > 0:
                delay = random.uniform(1.0, 3.0)
                time.sleep(delay)
            
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            # Try to parse JSON
            return response.json()
            
        except requests.exceptions.RequestException as e:
            last_exception = e
            retry_info = f"attempt {attempt + 1}" + (f"/{max_retries}" if max_retries else " (infinite retries)")
            print(f"[driver_right] Request failed ({retry_info}): {e}")
            
            # For infinite retries, continue indefinitely
            if max_retries is None:
                attempt += 1
                continue
            # For limited retries, check if we should continue
            elif attempt < max_retries - 1:
                attempt += 1
                continue
        except ValueError as e:
            last_exception = e
            retry_info = f"attempt {attempt + 1}" + (f"/{max_retries}" if max_retries else " (infinite retries)")
            print(f"[driver_right] JSON decode failed ({retry_info}): {e}")
            
            # For infinite retries, continue indefinitely
            if max_retries is None:
                attempt += 1
                continue
            # For limited retries, check if we should continue
            elif attempt < max_retries - 1:
                attempt += 1
                continue
        
        # If we reach here with limited retries, we've exhausted all attempts
        if max_retries is not None:
            break
        
        attempt += 1
    
    # Only raise exception if we had limited retries
    if max_retries is not None:
        raise Exception(f"Failed to fetch data after {max_retries} attempts: {last_exception}")
    
    # This should never be reached for infinite retries
    raise Exception(f"Unexpected error in infinite retry loop: {last_exception}")


def get_all_years() -> List[int]:
    """
    Get all available years from the Driver Right API.
    
    Returns:
        List of years as integers
        
    Raises:
        Exception: If required parameters are missing or API call fails
    """
    if not USERNAME or not SECURITY_TOKEN or not BASE_URL:
        raise Exception("Missing required configuration: USERNAME, SECURITY_TOKEN, or BASE_URL")
    
    params = {
        'username': USERNAME,
        'securityToken': SECURITY_TOKEN
    }
    
    url = f"{BASE_URL}/aaia/GetAAIAYears?{urlencode(params)}"
    data = fetch_with_retry(url)
    
    return [item['Year'] for item in data]


def get_all_makes(year: int, region_id: int = DEFAULT_REGION_ID) -> List[str]:
    """
    Get all available makes for a given year.
    
    Args:
        year: The year to get makes for
        region_id: The region ID (defaults to 1)
        
    Returns:
        List of manufacturer names
        
    Raises:
        Exception: If year parameter is missing or API call fails
    """
    if not year:
        raise Exception("Parameter 'year' is required.")
    
    params = {
        'username': USERNAME,
        'securityToken': SECURITY_TOKEN,
        'year': year,
        'regionID': region_id
    }
    
    url = f"{BASE_URL}/aaia/GetAAIAManufacturers?{urlencode(params)}"
    data = fetch_with_retry(url)
    
    return [item['Manufacturer'] for item in data]


def get_all_models(year: int, make: str, region_id: int = DEFAULT_REGION_ID) -> List[str]:
    """
    Get all available models for a given year and make.
    
    Args:
        year: The year
        make: The manufacturer name
        region_id: The region ID (defaults to 1)
        
    Returns:
        List of model names
        
    Raises:
        Exception: If required parameters are missing or API call fails
    """
    if not year or not make:
        raise Exception("Parameters 'year' and 'make' are required.")
    
    params = {
        'username': USERNAME,
        'securityToken': SECURITY_TOKEN,
        'year': year,
        'regionID': region_id,
        'manufacturer': make
    }
    
    url = f"{BASE_URL}/aaia/GetAAIAModels?{urlencode(params)}"
    data = fetch_with_retry(url)
    
    return [item['Model'] for item in data]


def get_all_body_types(year: int, make: str, model: str, region_id: int = DEFAULT_REGION_ID) -> List[str]:
    """
    Get all available body types for a given year, make, and model.
    
    Args:
        year: The year
        make: The manufacturer name
        model: The model name
        region_id: The region ID (defaults to 1)
        
    Returns:
        List of body type names
        
    Raises:
        Exception: If required parameters are missing or API call fails
    """
    if not year or not make or not model:
        raise Exception("Parameters 'year', 'make', and 'model' are required.")
    
    params = {
        'username': USERNAME,
        'securityToken': SECURITY_TOKEN,
        'year': year,
        'regionID': region_id,
        'manufacturer': make,
        'model': model
    }
    
    url = f"{BASE_URL}/aaia/GetAAIABodyTypes?{urlencode(params)}"
    data = fetch_with_retry(url)
    
    return [item['BodyType'] for item in data]


def get_all_sub_models(year: int, make: str, model: str, body_type: str, region_id: int = DEFAULT_REGION_ID) -> List[Dict[str, Any]]:
    """
    Get all available sub models for a given year, make, model, and body type.
    
    Args:
        year: The year
        make: The manufacturer name
        model: The model name
        body_type: The body type
        region_id: The region ID (defaults to 1)
        
    Returns:
        List of sub model data (full objects, not just names)
        
    Raises:
        Exception: If required parameters are missing or API call fails
    """
    if not year or not make or not model or not body_type:
        raise Exception("Parameters 'year', 'make', 'model', and 'body_type' are required.")
    
    params = {
        'username': USERNAME,
        'securityToken': SECURITY_TOKEN,
        'year': year,
        'regionID': region_id,
        'manufacturer': make,
        'model': model,
        'bodyType': body_type
    }
    
    url = f"{BASE_URL}/aaia/GetAAIASubModelsWheels?{urlencode(params)}"
    data = fetch_with_retry(url)
    
    return data


def get_vehicle_data_from_drd_na(drd_model_id: str, drd_chassis_id: str) -> Dict[str, Any]:
    """
    Get vehicle data from DRD_NA using model ID and chassis ID.
    
    Args:
        drd_model_id: The DRD Model ID
        drd_chassis_id: The DRD Chassis ID
        
    Returns:
        Vehicle data dictionary
        
    Raises:
        Exception: If required parameters are missing or API call fails
    """
    if not drd_model_id or not drd_chassis_id:
        raise Exception("Parameters 'drd_model_id' and 'drd_chassis_id' are required.")
    
    params = {
        'username': USERNAME,
        'securityToken': SECURITY_TOKEN,
        'DRDModelID': drd_model_id,
        'DRDChassisID': drd_chassis_id
    }
    
    url = f"{BASE_URL}/vehicle-info/GetVehicleDataFromDRD_NA?{urlencode(params)}"
    return fetch_with_retry(url)


# Export all functions
__all__ = [
    'fetch_with_retry',
    'get_all_years',
    'get_all_makes',
    'get_all_models',
    'get_all_body_types',
    'get_all_sub_models',
    'get_vehicle_data_from_drd_na'
]