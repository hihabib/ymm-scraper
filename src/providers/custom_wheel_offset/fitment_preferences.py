"""
Fitment preferences extractor for Custom Wheel Offset.
Extracts suspension, modification, and rubbing options from the API endpoints.
"""
import requests
import time
import random
import json
import hashlib
from typing import List, Dict, Optional, Any
from urllib.parse import urlencode
from pathlib import Path

# Import centralized logging
from .logging_config import init_module_logger

# Initialize logger for this module
logger = init_module_logger("fitment_preferences")

# Cache directory for storing fitment preferences
CACHE_DIR = Path(__file__).parent.parent.parent.parent / "data" / "fitment_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _get_cache_key(vehicle_type: str, store: str) -> str:
    """Generate a unique cache key for the given parameters."""
    params_str = f"{vehicle_type.lower()}_{store.lower()}"
    return hashlib.md5(params_str.encode()).hexdigest()

def _get_cache_file_path(cache_key: str) -> Path:
    """Get the cache file path for the given cache key."""
    return CACHE_DIR / f"fitment_preferences_{cache_key}.json"

def _load_from_cache(vehicle_type: str, store: str) -> Optional[List[Dict[str, str]]]:
    """Load fitment preferences from cache if available."""
    cache_key = _get_cache_key(vehicle_type, store)
    cache_file = _get_cache_file_path(cache_key)
    
    if cache_file.exists():
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                
            # Validate cache structure
            if (isinstance(cache_data, dict) and 
                'vehicle_type' in cache_data and 
                'store' in cache_data and 
                'combinations' in cache_data and
                cache_data['vehicle_type'] == vehicle_type.lower() and
                cache_data['store'] == store.lower()):
                
                logger.info(f"Loaded {len(cache_data['combinations'])} combinations from cache for vehicle_type='{vehicle_type}', store='{store}'")
                return cache_data['combinations']
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"Invalid cache file {cache_file}: {e}")
            # Remove invalid cache file
            try:
                cache_file.unlink()
            except OSError:
                pass
    
    return None

def _save_to_cache(vehicle_type: str, store: str, combinations: List[Dict[str, str]]) -> None:
    """Save fitment preferences to cache."""
    cache_key = _get_cache_key(vehicle_type, store)
    cache_file = _get_cache_file_path(cache_key)
    
    cache_data = {
        'vehicle_type': vehicle_type.lower(),
        'store': store.lower(),
        'timestamp': time.time(),
        'combinations': combinations
    }
    
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(combinations)} combinations to cache file: {cache_file}")
    except (OSError, TypeError) as e:
        logger.error(f"Failed to save cache to {cache_file}: {e}")

try:
    # Prefer package-relative import
    from .session_restart import handle_session_expired_error
    from ...config.proxy import PROXY_DNS1, PROXY_USER, PROXY_PASS
    from . import resolve_captcha as rc
    from .session_manager_threaded import threaded_session_manager
except ImportError:
    # Fallback to absolute import
    import sys
    from pathlib import Path
    SRC_DIR = Path(__file__).resolve().parents[2]
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    
    # Always use absolute imports
    SRC_DIR = Path(__file__).resolve().parents[2]
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    
    # Import database logging function
    try:
        from services.repository_optimized import insert_error_log
    except ImportError:
        try:
            from services.repository import insert_error_log
        except ImportError:
            # Fallback function if database logging is not available
            def insert_error_log(source, context, message):
                logger.warning(f"[fitment_preferences] Database logging not available - {source}: {message}")
                pass
    
    # Import session restart with fallback
    try:
        from session_restart import handle_session_expired_error
    except ImportError:
        # If session_restart is not available, create a dummy function
        def handle_session_expired_error(context="unknown"):
            logger.warning(f"Session expired in {context}, but restart functionality not available")
            return
    
    # Import using the full package path to ensure same module instance
    from providers.custom_wheel_offset.session_manager_threaded import threaded_session_manager
    from config.proxy import (
    PROXY_DNS1, PROXY_USER, PROXY_PASS,
    get_dns_rotation_iterator, get_proxy_config_with_dns, TOTAL_MAX_RETRIES
)
    # Import resolve_captcha module
    from importlib.util import spec_from_file_location, module_from_spec
    rc_path = SRC_DIR / "providers" / "custom_wheel_offset" / "resolve_captcha.py"
    spec = spec_from_file_location("cwo_resolve_captcha_mod", str(rc_path))
    mod = module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    rc = mod

# Base URL for fitment preferences API
BASE_API_URL = "https://www.customwheeloffset.com/api/ymm-temp.php"

# Request timeout in seconds
TIMEOUT = 30

# Enhanced headers to match the curl request format exactly
HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Priority": "u=1, i",
    "Referer": "https://www.customwheeloffset.com/store/wheels",
    "Sec-Ch-Ua": '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
}


def get_session():
    """Get the shared session instance."""
    return threaded_session_manager.get_session()


def _load_aws_waf_token() -> Optional[str]:
    """Load AWS WAF token from JSON file."""
    try:
        token_file = Path(__file__).resolve().parents[3] / "data" / "custom_wheel_offset_temp.json"
        if not token_file.exists():
            logger.warning(f"Token file not found: {token_file}")
            return None
        
        with open(token_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            token = data.get('aws_waf_token')
            if token:
                logger.info(f"Loaded AWS WAF token: {token[:20]}...")
                return token
            else:
                logger.warning("No aws_waf_token found in file")
                return None
    except Exception as e:
        logger.error(f"Error loading AWS WAF token: {e}")
        return None


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
                logger.info(f"Resetting session for retry attempt {attempt + 1}/{TOTAL_MAX_RETRIES}")
                # Reset the session to get a fresh one
                from .session_manager_threaded import threaded_session_manager
                threaded_session_manager.reset_session()
            
            # Get current session (fresh or existing)
            session = get_session()
            
            # Load AWS WAF token and set cookies
            aws_waf_token = _load_aws_waf_token()
            if aws_waf_token:
                # Set the aws-waf-token cookie
                session.cookies.set("aws-waf-token", aws_waf_token, domain="www.customwheeloffset.com", path="/")
                logger.debug(f"Using AWS WAF token: {aws_waf_token[:20]}...")
            else:
                logger.warning("No AWS WAF token available")
            
            # Print current cookies for debugging
            logger.debug(f"Making request to {url}")
            logger.debug(f"Current cookies: {len(session.cookies)}")
            for cookie in session.cookies:
                logger.debug(f"Cookie: {cookie.name}={cookie.value[:20]}...")
            
            logger.info(f"[fitment_preferences] DNS Rotation - Attempt {attempt + 1}/{TOTAL_MAX_RETRIES} using DNS: {dns_server}")
            
            # Use session for connection pooling and cookie management
            response = session.get(
                url,
                headers=HEADERS,
                proxies=proxies,
                timeout=TIMEOUT,
                verify=True,
                allow_redirects=True
            )
            
            logger.debug(f"Response status: {response.status_code}")
            logger.debug(f"Response headers: {dict(response.headers)}")
            
            response.raise_for_status()
            
            logger.info(f"[fitment_preferences] DNS Rotation - Success with DNS: {dns_server}")
            
            # Try to parse JSON
            try:
                json_data = response.json()
                logger.debug(f"Raw response: {response.text}")
                logger.debug(f"JSON response type: {type(json_data)}, length: {len(json_data) if isinstance(json_data, (list, dict)) else 'N/A'}")
                return json_data
            except ValueError as json_error:
                logger.error(f"JSON decode failed on attempt {attempt + 1}/{max_retries}, response text: {response.text[:200]}...")
                last_error = str(json_error)
                if attempt == max_retries - 1:
                    _log_failed_request(url, "json_decode_error", last_error, max_retries)
                    return None
                
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout on attempt {attempt + 1}/{TOTAL_MAX_RETRIES} for {url} using DNS {dns_server}: {e}")
            last_error = str(e)
            if attempt == TOTAL_MAX_RETRIES - 1:
                logger.error(f"All {TOTAL_MAX_RETRIES} attempts failed due to timeout for {url}")
                _log_failed_request(url, "timeout", last_error, TOTAL_MAX_RETRIES)
                return None
            # Fixed delay for DNS rotation
            time.sleep(1.0)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed on attempt {attempt + 1}/{TOTAL_MAX_RETRIES} for {url} using DNS {dns_server}: {e}")
            last_error = str(e)
            
            # Store status code if available
            if hasattr(e, 'response') and e.response is not None:
                last_status_code = e.response.status_code
                
            if attempt == TOTAL_MAX_RETRIES - 1:
                # Log failed request to database after all retries exhausted
                error_type = f"http_{last_status_code}" if last_status_code else "request_exception"
                _log_failed_request(url, error_type, last_error, TOTAL_MAX_RETRIES, last_status_code)
                return None
            # Fixed delay for DNS rotation
            time.sleep(1.0)
            
    return None


def _log_failed_request(url: str, error_type: str, error_message: str, max_retries: int, status_code: Optional[int] = None) -> None:
    """Log failed request to database after all retries exhausted."""
    try:
        context = {
            "url": url,
            "error_type": error_type,
            "max_retries": max_retries,
            "final_attempt": True,
            "module": "fitment_preferences"
        }
        
        if status_code:
            context["status_code"] = status_code
            
        insert_error_log(
            source="fitment_preferences_request_failure",
            context=context,
            message=f"Request failed after {max_retries} attempts: {error_message}"
        )
        logger.info(f"[fitment_preferences] Logged failed request to database: {url} ({error_type})")
        
    except Exception as log_error:
        logger.error(f"[fitment_preferences] Failed to log request failure to database: {log_error}")


def _set_additional_cookies():
    """Set additional cookies that are present in the working curl command."""
    session = get_session()
    
    # Do NOT load PHPSESSID from file - it should remain thread-isolated
    # Each thread will get its own PHPSESSID through get_phpsessid_from_api or ensure_token_by_visiting_homepage
    logger.debug("Skipping PHPSESSID loading from file to maintain thread isolation")
    
    # Load AWS WAF token from JSON file (this is shared across threads)
    aws_waf_token = _load_aws_waf_token()
    
    # Only set AWS WAF token if available
    if aws_waf_token:
        try:
            session.cookies.set("aws-waf-token", aws_waf_token, domain="www.customwheeloffset.com", path="/")
            logger.debug(f"Set cookie: aws-waf-token={aws_waf_token[:20]}...")
        except Exception as e:
            logger.error(f"Failed to set cookie aws-waf-token: {e}")
    
    # Print current cookie status
    logger.debug(f"Current cookies after setting: {len(session.cookies)}")
    for cookie in session.cookies:
        logger.debug(f"Cookie: {cookie.name}={cookie.value[:20]}...")


def _initialize_session():
    """Initialize session with proper configuration and set exact cookies from working curl command."""
    try:
        session = get_session()
        # Set session headers
        session.headers.update(HEADERS)
        
        # Set exact cookies from the working curl command without visiting pages first
        logger.info("Setting exact cookies from working curl command...")
        _set_additional_cookies()
        
        # Print all cookies for debugging
        logger.debug(f"Final cookie count: {len(session.cookies)}")
        for cookie in session.cookies:
            logger.debug(f"Cookie: {cookie.name}={cookie.value[:20]}...")
        
        logger.info(f"Session initialized successfully with {len(session.cookies)} cookies")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize session: {e}")
        return False


# Initialize session on module load
_session_initialized = _initialize_session()


def getSuspension(vehicle_type: str = "car", store: str = "wheels") -> List[str]:
    """
    Get available suspension options.
    
    Args:
        vehicle_type: Type of vehicle (default: "car")
        store: Store type (default: "wheels")
        
    Returns:
        List of suspension option strings
    """
    params = {
        "type": "set",
        "vehicle_type": vehicle_type,
        "store": store,
        "getSuspension": "true"
    }
    
    url = f"{BASE_API_URL}?{urlencode(params)}"
    logger.info(f"Fetching suspension options from {url}")
    
    result = _make_request(url)
    logger.info(f"result of suspension request: {result}")
    if result is None:
        logger.error("Failed to fetch suspension options after retries - skipping")
        return []
    
    if isinstance(result, list):
        suspension_options = [str(option) for option in result if option]
        logger.info(f"Found {len(suspension_options)} suspension options")
        return suspension_options
    
    logger.warning(f"Unexpected response format for suspension: {type(result)}")
    return []


def getTrimming(vehicle_type: str = "car", store: str = "wheels") -> List[str]:
    """
    Get available trimming options.
    
    Args:
        vehicle_type: Type of vehicle (default: "car")
        store: Store type (default: "wheels")
        
    Returns:
        List of trimming option strings
    """
    params = {
        "type": "set",
        "vehicle_type": vehicle_type,
        "store": store,
        "getTrimming": "true"
    }
    
    url = f"{BASE_API_URL}?{urlencode(params)}"
    logger.info(f"Fetching trimming options from {url}")
    
    result = _make_request(url)
    if result is None:
        logger.error("Failed to fetch trimming options after retries - skipping")
        return []
    
    if isinstance(result, list):
        trimming_options = [str(option) for option in result if option]
        logger.info(f"Found {len(trimming_options)} trimming options")
        return trimming_options
    
    logger.warning(f"Unexpected response format for trimming: {type(result)}")
    return []


def getRubbing(vehicle_type: str = "car", store: str = "wheels") -> List[str]:
    """
    Get available rubbing options.
    
    Args:
        vehicle_type: Type of vehicle (default: "car")
        store: Store type (default: "wheels")
        
    Returns:
        List of rubbing option strings
    """
    params = {
        "type": "set",
        "vehicle_type": vehicle_type,
        "store": store,
        "getRubbing": "true"
    }
    
    url = f"{BASE_API_URL}?{urlencode(params)}"
    logger.info(f"Fetching rubbing options from {url}")
    
    result = _make_request(url)
    if result is None:
        logger.error("Failed to fetch rubbing options after retries - skipping")
        return []
    
    if isinstance(result, list):
        rubbing_options = [str(option) for option in result if option]
        logger.info(f"Found {len(rubbing_options)} rubbing options")
        return rubbing_options
    
    logger.warning(f"Unexpected response format for rubbing: {type(result)}")
    return []


def get_fitment_preferences(vehicle_type: str = "car", store: str = "wheels") -> List[Dict[str, str]]:
    """
    Get fitment preferences as all possible combinations.
    
    Args:
        vehicle_type: Type of vehicle (default: "car")
        store: Store type (default: "wheels")
    
    Returns:
        List of dictionaries, each containing one combination of:
        - suspension: suspension option
        - modification: trimming option  
        - rubbing: rubbing option
        
        Total combinations = len(suspension) * len(trimming) * len(rubbing)
    """
    vehicle_type = vehicle_type.lower()
    logger.info(f"Getting fitment preferences for vehicle_type='{vehicle_type}', store='{store}'")
    
    # Check cache first
    cached_combinations = _load_from_cache(vehicle_type, store)
    if cached_combinations is not None:
        logger.info(f"Returning {len(cached_combinations)} combinations from cache")
        return cached_combinations
    
    logger.info("Cache miss - fetching from API")
    
    # Step 1: Ensure token is updated by calling the resolve_captcha function
    # logger.info("Updating AWS WAF token...")
    # try:
    #     rc.ensure_token_by_visiting_homepage(max_attempts=20)
    #     logger.info("Token update completed")
    # except Exception as e:
    #     logger.error(f"Token update failed: {e}")
    
    # Step 2: Re-initialize session to ensure we have fresh cookies after token update
    logger.info("Re-initializing session after token update...")
    # Don't reassign session, just reinitialize the existing shared session
    _initialize_session()  # Re-initialize with fresh cookies
    
    # Step 3: Get suspension options
    suspension_options = getSuspension(vehicle_type, store)
    
    # Step 4: Get trimming options
    trimming_options = getTrimming(vehicle_type, store)
    
    # Step 5: Get rubbing options
    rubbing_options = getRubbing(vehicle_type, store)
    
    # Construct all combinations
    combinations = []
    for suspension in suspension_options:
        for trimming in trimming_options:
            for rubbing in rubbing_options:
                combinations.append({
                    "suspension": suspension,
                    "modification": trimming,
                    "rubbing": rubbing
                })
    
    logger.info(f"Generated {len(combinations)} total combinations from "
          f"{len(suspension_options)} suspension, {len(trimming_options)} trimming, "
          f"and {len(rubbing_options)} rubbing options")
    
    # Only log first combination if combinations exist
    if combinations:
        logger.debug(f"First combination: {combinations[0]}")
        # Save to cache for future use
        _save_to_cache(vehicle_type, store, combinations)
    else:
        logger.warning("No combinations generated - all API calls may have failed")
    
    return combinations


# if __name__ == "__main__":
    # Test the functions
    # print("Testing fitment preferences functions...")
    
    # # Test individual functions
    # suspension = getSuspension()
    # print(f"Suspension options: {suspension}")
    
    # trimming = getTrimming()
    # print(f"Trimming options: {trimming}")
    
    # rubbing = getRubbing()
    # print(f"Rubbing options: {rubbing}")
    
    # Test main function
    # preferences = get_fitment_preferences()
    # print(f"Total combinations: {len(preferences)}")
    # if preferences:
    #     print(f"First combination: {preferences[0]}")


__all__ = [
    "getSuspension",
    "getTrimming", 
    "getRubbing",
    "get_fitment_preferences",
]