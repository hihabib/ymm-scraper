#!/usr/bin/env python3
"""
Optimized network manager for Custom Wheel Offset scraper.
Implements request batching, connection pooling, and optimized retry logic.
"""

import time
import threading
import asyncio
import aiohttp
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .session_manager_threaded import threaded_session_manager
from .logging_config import init_module_logger

try:
    from config.proxy import (
        PROXY_DNS1, PROXY_USER, PROXY_PASS,
        get_dns_rotation_iterator, get_proxy_config_with_dns, TOTAL_MAX_RETRIES
    )
except ImportError:
    # Set default proxy values if import fails
    PROXY_DNS1 = ""
    PROXY_USER = ""
    PROXY_PASS = ""
    TOTAL_MAX_RETRIES = 18
    
    def get_dns_rotation_iterator():
        return iter([PROXY_DNS1] * TOTAL_MAX_RETRIES)
    
    def get_proxy_config_with_dns(dns_server):
        return {
            "http": f"http://{PROXY_USER}:{PROXY_PASS}@{dns_server}",
            "https": f"http://{PROXY_USER}:{PROXY_PASS}@{dns_server}"
        }

logger = init_module_logger(__name__)

def _get_proxy_config() -> dict:
    """Get proxy configuration for requests using first DNS for backward compatibility."""
    return {
        "http": f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_DNS1}",
        "https": f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_DNS1}"
    }

class OptimizedNetworkManager:
    """Optimized network manager with connection pooling and request batching."""
    
    def __init__(self):
        self._session_pool = {}
        self._pool_lock = threading.Lock()
        self._request_cache = {}
        self._cache_lock = threading.Lock()
        self._cache_ttl = 300  # 5 minutes cache TTL
        
    def _get_optimized_session(self) -> requests.Session:
        """Get an optimized session with connection pooling and retry strategy."""
        thread_id = threading.current_thread().ident
        
        with self._pool_lock:
            if thread_id not in self._session_pool:
                session = requests.Session()
                
                # Configure retry strategy
                retry_strategy = Retry(
                    total=3,
                    backoff_factor=0.5,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
                )
                
                # Configure HTTP adapter with connection pooling
                adapter = HTTPAdapter(
                    max_retries=retry_strategy,
                    pool_connections=10,
                    pool_maxsize=20,
                    pool_block=False
                )
                
                session.mount("http://", adapter)
                session.mount("https://", adapter)
                
                # Set optimized headers
                session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'application/json, text/html, */*',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive'
                })
                
                self._session_pool[thread_id] = session
                logger.info(f"[OptimizedNetwork] Created optimized session for thread {thread_id}")
            
            return self._session_pool[thread_id]
    
    def _get_cache_key(self, url: str, params: Dict = None, data: Dict = None) -> str:
        """Generate cache key for request."""
        import hashlib
        key_data = f"{url}_{params}_{data}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _is_cache_valid(self, timestamp: float) -> bool:
        """Check if cache entry is still valid."""
        return time.time() - timestamp < self._cache_ttl
    
    def make_request_optimized(self, url: str, method: str = "GET", params: Dict = None, 
                             data: Dict = None, headers: Dict = None, timeout: int = 30,
                             use_cache: bool = True) -> Optional[requests.Response]:
        """Make an optimized HTTP request with DNS rotation, caching and connection pooling."""
        
        # Check cache first for GET requests
        if method.upper() == "GET" and use_cache:
            cache_key = self._get_cache_key(url, params, data)
            with self._cache_lock:
                if cache_key in self._request_cache:
                    cached_response, timestamp = self._request_cache[cache_key]
                    if self._is_cache_valid(timestamp):
                        logger.debug(f"[OptimizedNetwork] Cache hit for {url}")
                        return cached_response
        
        session = self._get_optimized_session()
        
        # Merge headers
        request_headers = session.headers.copy()
        if headers:
            request_headers.update(headers)
        
        # Use DNS rotation for improved reliability
        dns_iterator = get_dns_rotation_iterator()
        last_error = None
        
        for attempt in range(TOTAL_MAX_RETRIES):
            try:
                # Get next DNS server from rotation
                dns_server = next(dns_iterator)
                proxies = get_proxy_config_with_dns(dns_server)
                
                logger.info(f"[OptimizedNetwork] DNS Rotation - Attempt {attempt + 1}/{TOTAL_MAX_RETRIES} using DNS: {dns_server}")
                
                # Make request with optimized session
                response = session.request(
                    method=method.upper(),
                    url=url,
                    params=params,
                    data=data,
                    headers=request_headers,
                    timeout=timeout,
                    proxies=proxies
                )
                
                response.raise_for_status()
                
                logger.info(f"[OptimizedNetwork] DNS Rotation - Success with DNS: {dns_server}")
                
                # Cache successful GET requests
                if method.upper() == "GET" and use_cache and response.status_code == 200:
                    cache_key = self._get_cache_key(url, params, data)
                    with self._cache_lock:
                        self._request_cache[cache_key] = (response, time.time())
                        logger.debug(f"[OptimizedNetwork] Cached response for {url}")
                
                return response
                
            except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
                logger.error(f"[OptimizedNetwork] Request failed on attempt {attempt + 1}/{TOTAL_MAX_RETRIES} for {url} using DNS {dns_server}: {e}")
                last_error = str(e)
                
                if attempt == TOTAL_MAX_RETRIES - 1:
                    logger.error(f"[OptimizedNetwork] All {TOTAL_MAX_RETRIES} attempts failed for {url}")
                    return None
                
                # Fixed delay for DNS rotation
                time.sleep(1.0)
            
            except Exception as e:
                logger.error(f"[OptimizedNetwork] Unexpected error on attempt {attempt + 1}/{TOTAL_MAX_RETRIES} for {url} using DNS {dns_server}: {e}")
                last_error = str(e)
                
                if attempt == TOTAL_MAX_RETRIES - 1:
                    logger.error(f"[OptimizedNetwork] All {TOTAL_MAX_RETRIES} attempts failed for {url}")
                    return None
                
                # Fixed delay for DNS rotation
                time.sleep(1.0)
        
        return None
    
    def _cleanup_cache(self):
        """Clean up expired cache entries."""
        current_time = time.time()
        expired_keys = [
            key for key, (_, timestamp) in self._request_cache.items()
            if current_time - timestamp > self._cache_ttl
        ]
        
        for key in expired_keys:
            del self._request_cache[key]
        
        logger.info(f"[OptimizedNetwork] Cleaned up {len(expired_keys)} expired cache entries")
    
    def batch_requests(self, requests_data: List[Dict]) -> List[Tuple[Dict, Optional[requests.Response]]]:
        """Execute multiple requests in parallel with optimized threading."""
        if not requests_data:
            return []
        
        results = []
        
        # Use ThreadPoolExecutor for parallel requests
        with ThreadPoolExecutor(max_workers=min(10, len(requests_data)), thread_name_prefix="BatchReq") as executor:
            # Submit all requests
            future_to_request = {}
            for request_data in requests_data:
                future = executor.submit(
                    self.make_request_optimized,
                    request_data.get('url'),
                    request_data.get('method', 'GET'),
                    request_data.get('params'),
                    request_data.get('data'),
                    request_data.get('headers'),
                    request_data.get('timeout', 30),
                    request_data.get('use_cache', True)
                )
                future_to_request[future] = request_data
            
            # Collect results as they complete
            for future in as_completed(future_to_request):
                request_data = future_to_request[future]
                try:
                    response = future.result()
                    results.append((request_data, response))
                except Exception as e:
                    logger.error(f"[OptimizedNetwork] Batch request failed: {request_data.get('url')} - {e}")
                    results.append((request_data, None))
        
        logger.info(f"[OptimizedNetwork] Completed batch of {len(requests_data)} requests")
        return results
    
    def get_vehicle_data_batch(self, vehicle_combinations: List[Tuple[str, str, str, str, str]]) -> Dict[Tuple, Dict]:
        """Batch fetch vehicle data for multiple combinations."""
        if not vehicle_combinations:
            return {}
        
        # Prepare batch requests
        requests_data = []
        for year, make, model, trim, drive in vehicle_combinations:
            url = f"https://customwheeloffset.com/api/vehicle-data"
            params = {
                'year': year,
                'make': make,
                'model': model,
                'trim': trim,
                'drive': drive
            }
            requests_data.append({
                'url': url,
                'method': 'GET',
                'params': params,
                'combination': (year, make, model, trim, drive)
            })
        
        # Execute batch requests
        batch_results = self.batch_requests(requests_data)
        
        # Process results
        vehicle_data_results = {}
        for request_data, response in batch_results:
            combination = request_data['combination']
            if response and response.status_code == 200:
                try:
                    data = response.json()
                    vehicle_data_results[combination] = data
                except Exception as e:
                    logger.error(f"[OptimizedNetwork] Failed to parse vehicle data for {combination}: {e}")
                    vehicle_data_results[combination] = {}
            else:
                vehicle_data_results[combination] = {}
        
        logger.info(f"[OptimizedNetwork] Batch fetched vehicle data for {len(vehicle_combinations)} combinations")
        return vehicle_data_results
    
    def close_all_sessions(self):
        """Close all sessions and clean up resources."""
        with self._pool_lock:
            for thread_id, session in self._session_pool.items():
                try:
                    session.close()
                    logger.info(f"[OptimizedNetwork] Closed session for thread {thread_id}")
                except Exception as e:
                    logger.error(f"[OptimizedNetwork] Error closing session for thread {thread_id}: {e}")
            
            self._session_pool.clear()
        
        # Clear cache
        with self._cache_lock:
            self._request_cache.clear()
        
        logger.info("[OptimizedNetwork] All sessions closed and cache cleared")

# Global optimized network manager
optimized_network_manager = OptimizedNetworkManager()

# Optimized functions for backward compatibility
def make_request_optimized(url: str, method: str = "GET", params: Dict = None, 
                         data: Dict = None, headers: Dict = None, timeout: int = 30) -> Optional[requests.Response]:
    """Make an optimized HTTP request."""
    return optimized_network_manager.make_request_optimized(url, method, params, data, headers, timeout)

def batch_requests_optimized(requests_data: List[Dict]) -> List[Tuple[Dict, Optional[requests.Response]]]:
    """Execute multiple requests in parallel."""
    return optimized_network_manager.batch_requests(requests_data)

def get_vehicle_data_batch_optimized(vehicle_combinations: List[Tuple[str, str, str, str, str]]) -> Dict[Tuple, Dict]:
    """Batch fetch vehicle data for multiple combinations."""
    return optimized_network_manager.get_vehicle_data_batch(vehicle_combinations)