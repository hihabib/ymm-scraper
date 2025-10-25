"""
Wheel size page fetcher for Custom Wheel Offset.
Performs a simple GET request and validates the HTML against Human Verification gate.
"""
import requests
from urllib.parse import urlparse
from typing import Optional, Union
from pathlib import Path
import re
import json
from bs4 import BeautifulSoup
from .logging_config import init_module_logger

logger = init_module_logger(__name__)

try:
    # Prefer package-relative import of module to access live globals
    from . import resolve_captcha as rc
    from .session_manager_threaded import threaded_session_manager
    from .session_restart import handle_session_expired_error
    from config.proxy import (
        PROXY_DNS1, PROXY_USER, PROXY_PASS,
        get_dns_rotation_iterator, get_proxy_config_with_dns, TOTAL_MAX_RETRIES
    )
    ensure_not_human_verification = rc.ensure_not_human_verification
except Exception:
    # Fallback to absolute path import via importlib if package context is missing
    import sys
    from pathlib import Path
    from importlib.util import spec_from_file_location, module_from_spec
    from session_manager_threaded import threaded_session_manager
    SRC_DIR = Path(__file__).resolve().parents[2]
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    rc_path = SRC_DIR \
        / "providers" \
        / "custom_wheel_offset" \
        / "resolve_captcha.py"
    spec = spec_from_file_location("cwo_resolve_captcha_mod", str(rc_path))
    mod = module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    rc = mod
    ensure_not_human_verification = getattr(mod, "ensure_not_human_verification")
    
    try:
        from session_restart import handle_session_expired_error
        from config.proxy import (
            PROXY_DNS1, PROXY_USER, PROXY_PASS,
            get_dns_rotation_iterator, get_proxy_config_with_dns, TOTAL_MAX_RETRIES
        )
    except ImportError:
        # If session_restart is not available, create a dummy function
        def handle_session_expired_error(context="unknown"):
            logger.info(f"[wheel_size] Session expired in {context}, but restart functionality not available")
            return
        
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

# Browser-like headers to closely mimic Chrome navigation
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://www.customwheeloffset.com/store/wheels?",
    "sec-ch-ua": "\"Chromium\";v=\"141\", \"Not(A:Brand\";v=\"24\", \"Google Chrome\";v=\"141\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-ch-ua-full-version-list": "\"Chromium\";v=\"141.0.0.0\", \"Not(A:Brand\";v=\"24.0.0.0\", \"Google Chrome\";v=\"141.0.0.0\"",
    "sec-ch-ua-arch": "\"x86\"",
    "sec-ch-ua-bitness": "\"64\"",
    "sec-ch-ua-model": "\"\"",
    "sec-ch-ua-platform-version": "\"15.0.0\"",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document",
    "Viewport-Width": "1920",
    "DPR": "1.0",
    "Device-Memory": "8",
    "RTT": "100",
    "Downlink": "10",
    "Save-Data": "?0",
}

def _make_headers(url: str) -> dict:
    h = dict(HEADERS)
    # Decide same-origin vs cross-site for Sec-Fetch-Site
    try:
        h["Sec-Fetch-Site"] = "same-origin" if url.startswith("https://www.customwheeloffset.com") else "cross-site"
    except Exception:
        h["Sec-Fetch-Site"] = "none"
    # Cookie is handled via session jar to mimic browser behavior
    return h


def _get_proxy_config() -> dict:
    """Get proxy configuration for requests using first DNS for backward compatibility."""
    return {
        "http": f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_DNS1}",
        "https": f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_DNS1}"
    }

def _token_file_path() -> Path:
    """Get the path to the custom wheel offset temp JSON file."""
    return Path(__file__).resolve().parents[3] / "data" / "custom_wheel_offset_temp.json"


def _load_saved_token() -> Optional[str]:
    try:
        p = _token_file_path()
        if not p.exists():
            return None
        with p.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        tok = obj.get("aws_waf_token")
        return tok if isinstance(tok, str) and tok else None
    except Exception:
        return None


def _save_token(token: str) -> None:
    try:
        if not token:
            return
        p = _token_file_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w", encoding="utf-8") as f:
            json.dump({"aws_waf_token": token}, f)
    except Exception:
        # Persistence should not break scraping flow
        pass


def _parse_numbers(text: str) -> list[float]:
    nums = re.findall(r"([0-9]+(?:\.[0-9]+)?)", text)
    return [float(n) for n in nums]


def _parse_range(text: str) -> tuple[float, float]:
    nums = _parse_numbers(text)
    if len(nums) >= 2:
        return nums[0], nums[1]
    if len(nums) == 1:
        return nums[0], nums[0]
    return 0.0, 0.0


def _section_to_ranges(div) -> dict:
    data = {
        "diameter": {"min": None, "max": None},
        "width": {"min": None, "max": None},
        "offset": {"min": None, "max": None},
    }
    for sp in div.select("span.store-conf-range"):
        txt = sp.get_text(strip=True)
        bold = sp.find("b")
        val_str = bold.get_text(strip=True) if bold else txt
        if "Diameter:" in txt:
            dmin, dmax = _parse_range(val_str)
            data["diameter"] = {"min": int(round(dmin)), "max": int(round(dmax))}
        elif "Width:" in txt:
            wmin, wmax = _parse_range(val_str)
            data["width"] = {"min": wmin, "max": wmax}
        elif "Offset:" in txt:
            omin, omax = _parse_range(val_str)
            data["offset"] = {"min": int(round(omin)), "max": int(round(omax))}
    
    return data


def parse_fit_ranges(html_text: str) -> dict:
    soup = BeautifulSoup(html_text or "", "html.parser")
    blocks = soup.select("div.store-ymm-fitrange")
    
    # Extract bolt pattern at vehicle level (not position-specific)
    bolt_pattern = None
    bolt_pattern_span = soup.find("span", class_="bolt_patterns store-bp")
    if bolt_pattern_span:
        bolt_bold = bolt_pattern_span.find("b")
        if bolt_bold:
            bolt_pattern = bolt_bold.get_text(strip=True)
    
    if not blocks:
        # Sometimes class may include full-size; the selector above already matches both.
        return {
            "front": {"diameter": {"min": 0, "max": 0}, "width": {"min": 0.0, "max": 0.0}, "offset": {"min": 0, "max": 0}},
            "rear": {"diameter": {"min": 0, "max": 0}, "width": {"min": 0.0, "max": 0.0}, "offset": {"min": 0, "max": 0}},
            "bolt_pattern": bolt_pattern,
        }
    # Case 1: Two sections with headers Front/Rear
    if len(blocks) >= 2:
        front = None
        rear = None
        for div in blocks:
            header = div.find("span", class_="store-conf-header")
            header_txt = header.get_text(strip=True) if header and header.get_text() else ""
            if "Front Sizes:" in header_txt:
                front = _section_to_ranges(div)
            elif "Rear Sizes:" in header_txt:
                rear = _section_to_ranges(div)
        # Fallback: if headers missing, use first two in order
        if front is None or rear is None:
            parsed = [_section_to_ranges(b) for b in blocks[:2]]
            if front is None:
                front = parsed[0]
            if rear is None:
                rear = parsed[1]
        return {"front": front, "rear": rear, "bolt_pattern": bolt_pattern}
    # Case 2: Single block (full-size), mirror to front and rear
    single = _section_to_ranges(blocks[0])
    return {"front": single, "rear": single, "bolt_pattern": bolt_pattern}


def fetch_wheel_size_page(url: str, *, timeout_secs: int = 30, max_attempts: int = 5) -> str:
    """
    Fetch the wheel size page with DNS rotation, retrying when a Human Verification wall is detected.
    - On detection, solve and store AWS WAF token globally, then retry with cookie.
    - Returns the final HTML once no human verification is detected, or last HTML
      after max_attempts.
    """
    attempt = 0
    last_html = ""
    session = threaded_session_manager.get_session()
    
    # Seed token from global or previously saved JSON
    tok_init = getattr(rc, "AWS_WAF_TOKEN", None)
    if not tok_init:
        tok_saved = _load_saved_token()
        if tok_saved:
            rc.AWS_WAF_TOKEN = tok_saved
            tok_init = tok_saved
    
    # Seed cookie jar with token, if any
    if tok_init:
        dom = urlparse(url).netloc
        try:
            session.cookies.set("aws-waf-token", tok_init, domain=dom, path="/")
        except Exception:
            session.cookies.set("aws-waf-token", tok_init)
    
    # Use DNS rotation for improved reliability
    dns_iterator = get_dns_rotation_iterator()
    
    while True:
        attempt += 1
        headers = _make_headers(url)
        
        # Get next DNS server from rotation
        dns_server = next(dns_iterator)
        proxies = get_proxy_config_with_dns(dns_server)
        
        token_val = getattr(rc, 'AWS_WAF_TOKEN', None)
        logger.info(f"[wheel_size] DNS Rotation - GET attempt {attempt}/{TOTAL_MAX_RETRIES}, DNS: {dns_server}, token={token_val if token_val else 'none'}")
        
        try:
            resp = session.get(url, timeout=timeout_secs, headers=headers, allow_redirects=True, proxies=proxies)
            html_text = resp.text
            last_html = html_text
            
            # Let resolver parse and potentially set token
            ensure_not_human_verification(html_text, url=url)
            
            # If resolver acquired a token, ensure it's in the jar for next attempt
            new_token = getattr(rc, "AWS_WAF_TOKEN", None)
            if new_token and new_token != token_val:
                dom = urlparse(url).netloc
                try:
                    session.cookies.set("aws-waf-token", new_token, domain=dom, path="/")
                except Exception:
                    session.cookies.set("aws-waf-token", new_token)
                _save_token(new_token)
            
            # Detect Human Verification via page title
            try:
                _soup = BeautifulSoup(html_text or "", "html.parser")
                _title = _soup.title.string.strip() if _soup.title and _soup.title.string else ""
                is_hv = _title.lower() == "human verification"
            except Exception:
                is_hv = False
            if not is_hv:
                logger.info("[wheel_size] Human verification not detected; returning HTML.")
                return html_text
            if attempt >= max_attempts:
                logger.warning(f"[wheel_size] Human verification persists after {max_attempts} attempts; returning last HTML.")
                return last_html
                
        except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
            logger.warning(f"[wheel_size] DNS Rotation - Request failed on attempt {attempt}/{TOTAL_MAX_RETRIES} with DNS {dns_server}: {e}")
            if attempt >= TOTAL_MAX_RETRIES:
                logger.error(f"[wheel_size] DNS Rotation - All {TOTAL_MAX_RETRIES} attempts failed")
                return last_html
            # Continue to next DNS server
            import time
            time.sleep(1.0)




def get_parsed_data_with_saved_token(url: str, *, timeout_secs: int = 30) -> Optional[dict]:
    """Fetch the URL once using the saved token and return parsed data.
    - Uses token from JSON (fallback to global if present) without solving.
    - If Human Verification is detected, returns None without retrying.
    """
    session = threaded_session_manager.get_session()
    tok = _load_saved_token()
    if not tok:
        tok = getattr(rc, "AWS_WAF_TOKEN", None)
    if tok:
        dom = urlparse(url).netloc
        try:
            session.cookies.set("aws-waf-token", tok, domain=dom, path="/")
        except Exception:
            session.cookies.set("aws-waf-token", tok)
    headers = _make_headers(url)
    proxies = _get_proxy_config()
    logger.info(f"[wheel_size] Single GET with saved token, token={'set' if tok else 'none'}")
    resp = session.get(url, timeout=timeout_secs, headers=headers, allow_redirects=True, proxies=proxies)
    html_text = resp.text
    # Detect Human Verification without solving or retrying
    try:
        _soup = BeautifulSoup(html_text or "", "html.parser")
        _title = _soup.title.string.strip() if _soup.title and _soup.title.string else ""
        is_hv = _title.lower() == "human verification"
    except Exception:
        is_hv = False
    if is_hv:
        logger.warning("[wheel_size] Human verification detected; returning null without retry.")
        handle_session_expired_error("human_verification_detected")
        return None
    return parse_fit_ranges(html_text)


# if __name__ == "__main__":
#     # Step 1: Solve captcha on homepage and persist token
#     rc.ensure_token_by_visiting_homepage(max_attempts=20)
#     _URL = "https://www.customwheeloffset.com/store/wheels?sort=instock&year=2025&make=Chevrolet&model=Equinox%20EV&trim=LT&drive=FWD&DRChassisID=92916&vehicle_type=Truck&suspension=Suspension%20Lift%202.5%22&modification=Minor%20Plastic%20Trimming&rubbing=Slight%20rub%20at%20full%20turn&saleToggle=0&qdToggle=0"
#     # Step 2: Use saved token to fetch and parse directly (no retries)
#     parsed = get_parsed_data_with_saved_token(_URL)
#     print(json.dumps(parsed, indent=2) if parsed is not None else "null")


__all__ = [
    "fetch_wheel_size_page",
    "parse_fit_ranges",
    "get_parsed_data_with_saved_token",
]