"""
Proxy configuration. Update these values as needed.
Keep credentials out of scraper code; import from here.
"""

import itertools
import logging
from typing import Dict, List, Optional

# You can switch to environment variables or a secrets manager later.
# For now, edit these constants or replace with a local, untracked file.

PROXY_DNS1: str = "101.32.255.125:2333"
PROXY_DNS2: str = "170.106.118.114:2333"
PROXY_DNS3: str = "118.193.58.115:2333"
PROXY_DNS4: str = "43.159.28.126:2333"
PROXY_DNS5: str = "165.154.179.147:2333"
PROXY_DNS6: str = "156.229.16.93:2333"
PROXY_USER: str = "ub3b25e2656da05c8-zone-custom"
PROXY_PASS: str = "test"

# Optional cookie string if target requires authentication or special headers
COOKIE_STRING: str = ""

# List of all DNS servers for rotation
PROXY_DNS_LIST: List[str] = [
    PROXY_DNS1,
    PROXY_DNS2,
    PROXY_DNS3,
    PROXY_DNS4,
    PROXY_DNS5,
    PROXY_DNS6,
]

# Maximum retries per DNS (3 times each DNS = 18 total retries)
MAX_RETRIES_PER_DNS: int = 3
TOTAL_MAX_RETRIES: int = len(PROXY_DNS_LIST) * MAX_RETRIES_PER_DNS

def get_proxy_config_with_dns(dns_server: str) -> Dict[str, str]:
    """
    Get proxy configuration for a specific DNS server.
    
    Args:
        dns_server: The DNS server address to use
        
    Returns:
        Dictionary containing HTTP and HTTPS proxy configurations
    """
    if not PROXY_USER or not PROXY_PASS:
        logging.warning("Proxy credentials not configured")
        return {}
    
    proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{dns_server}"
    return {
        "http": proxy_url,
        "https": proxy_url,
    }

def get_dns_rotation_iterator():
    """
    Get an iterator that cycles through each DNS server 3 times.
    This provides 18 total retry attempts (6 DNS × 3 retries each).
    
    Returns:
        Iterator that yields DNS servers in rotation
    """
    # Create a list where each DNS appears 3 times consecutively
    dns_with_retries = []
    for dns in PROXY_DNS_LIST:
        dns_with_retries.extend([dns] * MAX_RETRIES_PER_DNS)
    
    return iter(dns_with_retries)

def get_all_proxy_configs() -> List[Dict[str, str]]:
    """
    Get all proxy configurations for DNS rotation.
    
    Returns:
        List of proxy configuration dictionaries
    """
    configs = []
    for dns in PROXY_DNS_LIST:
        config = get_proxy_config_with_dns(dns)
        if config:  # Only add if credentials are configured
            configs.append(config)
    return configs

__all__ = [
    "PROXY_DNS1",
    "PROXY_DNS2",
    "PROXY_DNS3",
    "PROXY_DNS4",
    "PROXY_DNS5",
    "PROXY_DNS6",
    "PROXY_USER",
    "PROXY_PASS",
    "COOKIE_STRING",
    "PROXY_DNS_LIST",
    "MAX_RETRIES_PER_DNS",
    "TOTAL_MAX_RETRIES",
    "get_proxy_config_with_dns",
    "get_dns_rotation_iterator",
    "get_all_proxy_configs",
]