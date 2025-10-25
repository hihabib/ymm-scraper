"""
High-level API client exposing a simple GET interface:
    get(url: str) -> str
    get_with_dns_rotation(url: str) -> str
It reads proxy and cookie configuration from config, builds a session, and returns response text.
"""

from typing import Optional
import requests

from core.http import make_session, fetch, fetch_with_dns_rotation
from config.proxy import PROXY_DNS1, PROXY_USER, PROXY_PASS, COOKIE_STRING


def get(url: str, *, timeout=(10, 60), allow_redirects=True, verify: bool = True) -> str:
    """
    Perform a GET request for the given URL and return raw response text.
    Uses the first DNS server for backward compatibility.
    """
    session = make_session(
        proxy_dns=PROXY_DNS1 or None,
        proxy_user=PROXY_USER or None,
        proxy_pass=PROXY_PASS or None,
        cookie_string=COOKIE_STRING or None,
    )
    status, body = fetch(session, url, timeout=timeout, allow_redirects=allow_redirects, verify=verify)
    return body


def get_with_dns_rotation(url: str, *, timeout=(10, 60), allow_redirects=True, verify: bool = True) -> str:
    """
    Perform a GET request with DNS rotation and retry logic.
    Tries each DNS server 3 times for a total of 18 retry attempts.
    """
    status, body = fetch_with_dns_rotation(
        url, 
        timeout=timeout, 
        allow_redirects=allow_redirects, 
        verify=verify,
        cookie_string=COOKIE_STRING or None
    )
    return body


__all__ = ["get", "get_with_dns_rotation"]