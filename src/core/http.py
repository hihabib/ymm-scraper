#!/usr/bin/env python3
"""
Core HTTP utilities for making requests through an optional authenticated proxy.
This module exposes minimal, reusable helpers and hides implementation details.
"""

from typing import Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import logging
import time

# ---------- defaults you can override ----------
DEFAULT_HEADERS = {
    "Accept": "application/xml, text/xml, */*; q=0.01",
    "Accept-Language": "en-BD,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,bn;q=0.6",
    "Referer": "https://www.tirerack.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
}

# ---------- helpers ----------
def cookie_dict_from_header(s: str) -> dict:
    d = {}
    for part in s.split(';'):
        part = part.strip()
        if not part:
            continue
        if '=' in part:
            k, v = part.split('=', 1)
            d[k] = v
    return d


def build_proxy_url(proxy_dns: str, username: Optional[str] = None, password: Optional[str] = None) -> str:
    """
    proxy_dns: "host:port" or full "http://host:port"
    username/password: optional; if provided they will be included in the URL for proxy auth
    returns a proxy URL like "http://user:pass@host:port"
    """
    if proxy_dns.startswith("http://") or proxy_dns.startswith("https://"):
        base = proxy_dns
    else:
        base = f"http://{proxy_dns}"
    if username and password:
        # insert credentials after scheme
        scheme, rest = base.split("://", 1)
        return f"{scheme}://{username}:{password}@{rest}"
    return base


def make_session(
    proxy_dns: Optional[str] = None,
    proxy_user: Optional[str] = None,
    proxy_pass: Optional[str] = None,
    cookie_string: Optional[str] = None,
    headers: Optional[dict] = None,
    retries: int = 5,
    backoff_factor: float = 1.0,
) -> requests.Session:
    """
    Build and return a configured requests.Session.
    - If proxy_dns provided, it will be used for BOTH http and https.
    - cookie_string is optional; pass the full cookie header string if you have one.
    """
    session = requests.Session()
    session.trust_env = False  # do not inherit OS proxy/env settings

    # headers
    session.headers.update(headers or DEFAULT_HEADERS)

    # cookies
    if cookie_string:
        session.cookies.update(cookie_dict_from_header(cookie_string))

    # proxy
    if proxy_dns:
        proxy_url = build_proxy_url(proxy_dns, proxy_user, proxy_pass)
        session.proxies.update({"http": proxy_url, "https": proxy_url})

    # retries/backoff
    retry_kwargs = dict(total=retries, backoff_factor=backoff_factor, status_forcelist=(429, 500, 502, 503, 504))
    try:
        retry = Retry(**retry_kwargs, allowed_methods=["GET", "POST", "HEAD"])
    except TypeError:
        retry = Retry(**retry_kwargs, method_whitelist=["GET", "POST", "HEAD"])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def fetch_with_dns_rotation(
    full_url: str,
    timeout: Tuple[int, int] = (10, 60),
    allow_redirects: bool = True,
    verify: bool = True,
    cookie_string: Optional[str] = None,
    headers: Optional[dict] = None,
) -> Tuple[int, str]:
    """
    Perform a GET request with DNS rotation and retry logic.
    Tries each DNS server 3 times for a total of 18 retry attempts.
    
    Args:
        full_url: The URL to fetch
        timeout: (connect_timeout, read_timeout)
        allow_redirects: Whether to follow redirects
        verify: Whether to verify SSL certificates
        cookie_string: Optional cookie string
        headers: Optional headers dictionary
        
    Returns:
        Tuple of (status_code, response_text)
        
    Raises:
        Exception: If all DNS servers and retries are exhausted
    """
    from config.proxy import (
        get_dns_rotation_iterator, 
        get_proxy_config_with_dns, 
        PROXY_USER, 
        PROXY_PASS,
        TOTAL_MAX_RETRIES
    )
    
    dns_iterator = get_dns_rotation_iterator()
    last_exception = None
    
    for attempt in range(TOTAL_MAX_RETRIES):
        try:
            # Get next DNS server from rotation
            dns_server = next(dns_iterator)
            
            # Create session with current DNS
            session = make_session(
                proxy_dns=dns_server,
                proxy_user=PROXY_USER,
                proxy_pass=PROXY_PASS,
                cookie_string=cookie_string,
                headers=headers,
                retries=0,  # We handle retries manually
                backoff_factor=1.0,
            )
            
            logging.info(f"[DNS Rotation] Attempt {attempt + 1}/{TOTAL_MAX_RETRIES} using DNS: {dns_server}")
            
            # Make the request
            resp = session.get(full_url, timeout=timeout, allow_redirects=allow_redirects, verify=verify)
            resp.raise_for_status()
            
            logging.info(f"[DNS Rotation] Success with DNS: {dns_server}")
            return resp.status_code, resp.text
            
        except Exception as e:
            last_exception = e
            logging.warning(f"[DNS Rotation] Attempt {attempt + 1} failed with DNS {dns_server}: {str(e)}")
            
            # Add delay between retries (except for the last attempt)
            if attempt < TOTAL_MAX_RETRIES - 1:
                time.sleep(1.0)
    
    # All attempts failed
    logging.error(f"[DNS Rotation] All {TOTAL_MAX_RETRIES} attempts failed")
    raise Exception(f"All DNS rotation attempts failed. Last error: {str(last_exception)}")


def fetch(
    session: requests.Session,
    full_url: str,
    timeout: Tuple[int, int] = (10, 60),
    allow_redirects: bool = True,
    verify: bool = True,
) -> Tuple[int, str]:
    """
    Perform a GET request for a full URL (including query string).
    timeout: (connect_timeout, read_timeout)
    Returns (status_code, response_text). Exceptions are raised so caller can decide what to do.
    """
    resp = session.get(full_url, timeout=timeout, allow_redirects=allow_redirects, verify=verify)
    resp.raise_for_status()
    return resp.status_code, resp.text


__all__ = [
    "DEFAULT_HEADERS",
    "cookie_dict_from_header",
    "build_proxy_url",
    "make_session",
    "fetch",
    "fetch_with_dns_rotation",
]