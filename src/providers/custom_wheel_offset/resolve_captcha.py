"""
Resolve captcha / human verification gate for Custom Wheel Offset.
Given an HTML string, detect if the page is a Human Verification wall.
- If title is exactly "Human Verification" (case-insensitive), parse props and scripts,
  acquire a WAF token via 2Captcha and store it globally at AWS_WAF_TOKEN.
- Always return the original HTML string unchanged.
"""
from bs4 import BeautifulSoup
import re
from typing import Optional, Union
import os
import json
import time
import requests
import sys
from pathlib import Path
from twocaptcha import TwoCaptcha
from urllib.parse import urlparse

# Ensure `src` is on sys.path for absolute imports
SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from providers.custom_wheel_offset.logging_config import init_module_logger

logger = init_module_logger(__name__)

try:
    from core.errors import ParsingError
    from providers.custom_wheel_offset.session_manager_threaded import threaded_session_manager
    from config.proxy import PROXY_DNS1, PROXY_USER, PROXY_PASS
except ImportError:
    import sys
    from pathlib import Path
    SRC_DIR = Path(__file__).resolve().parents[2]
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    
    from providers.custom_wheel_offset.session_manager_threaded import threaded_session_manager
    from core.errors import ParsingError
    from config.proxy import PROXY_DNS1, PROXY_USER, PROXY_PASS

def _extract_goku_props_from_head(html_text: str) -> Optional[dict]:
    soup = BeautifulSoup(html_text or "", "html.parser")
    head = soup.head
    if not head:
        return None
    # Look for the first inline <script> in <head> that contains window.gokuProps
    for script in head.find_all("script"):
        # Prefer inline content
        content = script.string or script.get_text() or ""
        if not content:
            continue
        if "window.gokuProps" in content:
            # Extract key, iv, context regardless of order
            key_m = re.search(r'"key"\s*:\s*"([^"]+)"', content)
            iv_m = re.search(r'"iv"\s*:\s*"([^"]+)"', content)
            ctx_m = re.search(r'"context"\s*:\s*"([^"]+)"', content)
            if key_m and iv_m and ctx_m:
                return {"key": key_m.group(1), "iv": iv_m.group(1), "context": ctx_m.group(1)}
            # Fallback: try to parse the JSON block if braces are present
            obj_m = re.search(r'window\.gokuProps\s*=\s*(\{.*?\})', content, flags=re.DOTALL)
            if obj_m:
                block = obj_m.group(1)
                key_m = re.search(r'"key"\s*:\s*"([^"]+)"', block)
                iv_m = re.search(r'"iv"\s*:\s*"([^"]+)"', block)
                ctx_m = re.search(r'"context"\s*:\s*"([^"]+)"', block)
                if key_m and iv_m and ctx_m:
                    return {"key": key_m.group(1), "iv": iv_m.group(1), "context": ctx_m.group(1)}
    return None

def _extract_awswaf_scripts_from_head(html_text: str) -> dict:
    soup = BeautifulSoup(html_text or "", "html.parser")
    head = soup.head
    if not head:
        return {"challengeScript": None, "captchaScript": None}
    challenge_url = None
    captcha_url = None
    for script in head.find_all("script"):
        src = script.get("src")
        if not src:
            continue
        if "challenge.js" in src and challenge_url is None:
            challenge_url = src
        if "captcha.js" in src and captcha_url is None:
            captcha_url = src
        # Early exit if both found
        if challenge_url and captcha_url:
            break
    return {"challengeScript": challenge_url, "captchaScript": captcha_url}

AWS_WAF_TOKEN: Optional[str] = None
def ensure_not_human_verification(html_text: str, url: Optional[str] = None) -> str:
    """
    Inspect the HTML. If a Human Verification page is detected, parse props and
    AWS WAF script URLs, acquire a token via get_aws_waf_token, and store it in
    the global AWS_WAF_TOKEN. Always return the original html_text unchanged.
    """
    try:
        soup = BeautifulSoup(html_text or "", "html.parser")
        title = soup.title.string.strip() if soup.title and soup.title.string else ""
        if title.lower() == "human verification":
            logger.warning("captcha detected")
            props = _extract_goku_props_from_head(html_text) or {"key": None, "iv": None, "context": None}
            scripts = _extract_awswaf_scripts_from_head(html_text)
            props.update(scripts)
            try:
                token = get_aws_waf_token(
                    sitekey=props.get("key"),
                    iv=props.get("iv"),
                    context=props.get("context"),
                    url=url or "https://www.customwheeloffset.com/",
                    challengeScript=props.get("challengeScript"),
                    captchaScript=props.get("captchaScript"),
                )
                if token:
                    global AWS_WAF_TOKEN
                    logger.info(f"Acquired WAF token: {token}")
                    AWS_WAF_TOKEN = token
            except Exception:
                pass
            return html_text
        logger.info("No captcha detected")
        return html_text
    except Exception as e:
        # Any unexpected issues should surface as ParsingError for consistency
        try:
            from core.errors import ParsingError
        except ImportError:
            import sys
            from pathlib import Path
            SRC_DIR = Path(__file__).resolve().parents[2]
            if str(SRC_DIR) not in sys.path:
                sys.path.insert(0, str(SRC_DIR))
            from core.errors import ParsingError
        raise ParsingError(f"HTML parsing error: {type(e).__name__}: {e}")

def get_aws_waf_token(
    sitekey: str,
    iv: str,
    context: str,
    url: str,
    challengeScript: str,
    captchaScript: str,
    solver_api_key: str | None = None,
    task_type: str = "AmazonTaskProxyless",
) -> str:
    """
    Solve AWS WAF via 2Captcha's amazon_waf, submit the voucher, print debug output, and return the new token.

    Parameters mirror the amazon_waf call plus challenge/captcha script URLs.
    Returns the WAF token string if successful, otherwise an empty string.
    """
    api_key = (
        solver_api_key
        or os.getenv("TWOCAPTCHA_API_KEY")
        or os.getenv("TWO_CAPTCHA_API_KEY")
        or "7cf41cbcc8fe7efec88f40317bb92f6f"
    )
    solver = TwoCaptcha(api_key)

    result = solver.amazon_waf(
        sitekey=sitekey,
        iv=iv,
        context=context,
        url=url,
        type=task_type,
        challengeScript=challengeScript,
        captchaScript=captchaScript,
    )
    logger.info("solved:", result)

    code_val = result.get("code")
    if isinstance(code_val, str):
        try:
            code_obj = json.loads(code_val)
        except Exception:
            code_obj = {}
    elif isinstance(code_val, dict):
        code_obj = code_val
    else:
        code_obj = {}

    captcha_voucher = code_obj.get("captcha_voucher")
    existing_token = code_obj.get("existing_token")

    voucher_endpoint = (challengeScript or "").strip().strip("`").replace("challenge.js", "voucher")

    headers = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "text/plain;charset=UTF-8",
        "Origin": "https://www.customwheeloffset.com",
        "Referer": "https://www.customwheeloffset.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    }
    body = json.dumps({"captcha_voucher": captcha_voucher, "existing_token": existing_token})

    resp = requests.post(voucher_endpoint, headers=headers, data=body, timeout=20)
    logger.info("voucher_response:", resp.status_code, resp.text)
    token = ""
    try:
        data = resp.json()
        token = data.get("token") or ""
    except Exception:
        token = ""

    return token


# Token persistence helpers
def _token_file_path() -> Path:
    """Get the path to the custom wheel offset temp JSON file."""
    return Path(__file__).resolve().parents[3] / "data" / "custom_wheel_offset_temp.json"


def _load_saved_token() -> Optional[str]:
    """Load the saved AWS WAF token from JSON file."""
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


def _load_saved_phpsessid() -> Optional[str]:
    """Load the saved PHPSESSID from JSON file."""
    try:
        p = _token_file_path()
        if not p.exists():
            return None
        with p.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        phpsessid = obj.get("PHPSESSID")
        return phpsessid if isinstance(phpsessid, str) and phpsessid else None
    except Exception:
        return None


def _save_token(token: str) -> None:
    """Save the AWS WAF token to JSON file, preserving existing PHPSESSID if present."""
    try:
        if not token:
            return
        p = _token_file_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing data to preserve PHPSESSID
        existing_data = {}
        if p.exists():
            try:
                with p.open("r", encoding="utf-8") as f:
                    existing_data = json.load(f)
            except Exception:
                existing_data = {}
        
        # Update with new token while preserving other data
        existing_data["aws_waf_token"] = token
        
        with p.open("w", encoding="utf-8") as f:
            json.dump(existing_data, f)
    except Exception:
        # Persistence should not break scraping flow
        pass


def _save_phpsessid(phpsessid: str) -> None:
    """Save the PHPSESSID to JSON file, preserving existing aws_waf_token if present."""
    try:
        if not phpsessid:
            return
        p = _token_file_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing data to preserve aws_waf_token
        existing_data = {}
        if p.exists():
            try:
                with p.open("r", encoding="utf-8") as f:
                    existing_data = json.load(f)
            except Exception:
                existing_data = {}
        
        # Update with new PHPSESSID while preserving other data
        existing_data["PHPSESSID"] = phpsessid
        
        with p.open("w", encoding="utf-8") as f:
            json.dump(existing_data, f)
    except Exception:
        # Persistence should not break scraping flow
        pass


# Browser-like headers for navigation
HEADERS_NAV = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    "sec-ch-ua": "\"Chromium\";v=\"141\", \"Not(A:Brand\";v=\"24\", \"Google Chrome\";v=\"141\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document",
}


def _browser_headers(url: str) -> dict:
    """Generate browser-like headers for the given URL."""
    h = dict(HEADERS_NAV)
    try:
        h["Sec-Fetch-Site"] = "same-origin" if url.startswith("https://www.customwheeloffset.com") else "cross-site"
    except Exception:
        h["Sec-Fetch-Site"] = "none"
    return h


def _get_proxy_config() -> dict:
    """Get proxy configuration for requests."""
    return {
        "http": f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_DNS1}",
        "https": f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_DNS1}"
    }


def get_phpsessid_from_api(vehicle_type: str, year: str, make: str, 
                          model: str, trim: str , drive: str, 
                          chassis_id: str) -> Optional[str]:
    """
    Call the ymm-temp.php API to get a fresh PHPSESSID.
    
    Args:
        vehicle_type: Vehicle type (Car, Truck, etc.)
        year: Vehicle year
        make: Vehicle make
        model: Vehicle model
        trim: Vehicle trim
        drive: Drive type (RWD, FWD, AWD, etc.)
        chassis_id: Chassis ID
    
    Returns:
        PHPSESSID if successful, None otherwise.
        Note: Does NOT set the PHPSESSID in session to avoid race conditions in multi-threading.
    """
    api_url = (f"https://www.customwheeloffset.com/api/ymm-temp.php?"
               f"store=wheels&type=set&vehicle_type={vehicle_type}&year={year}&make={make}"
               f"&model={model}&trim={trim}&drive={drive}&chassis={chassis_id}")
    session = threaded_session_manager.get_session()
    
    logger.info(f"[api] Calling ymm-temp.php API to get PHPSESSID for {year} {make} {model} {trim} {drive}...")
    try:
        api_headers = _browser_headers(api_url)
        proxies = _get_proxy_config()
        
        # Create a temporary session to avoid automatic cookie storage
        import requests
        temp_session = requests.Session()
        temp_session.headers.update(api_headers)
        
        api_resp = temp_session.get(api_url, timeout=30, allow_redirects=True, proxies=proxies)
        
        # Extract PHPSESSID from Set-Cookie header
        for cookie in api_resp.cookies:
            if cookie.name == "PHPSESSID":
                phpsessid = cookie.value
                logger.info(f"[api] Got PHPSESSID from API: {phpsessid}")
                
                # Note: PHPSESSID is NOT saved to file as each thread needs its own unique PHPSESSID
                # The calling code is responsible for setting it in the thread-local session
                return phpsessid
        
        logger.warning("[api] No PHPSESSID found in API response")
        return None
        
    except Exception as e:
        logger.error(f"[api] Error calling API: {e}")
        return None


def ensure_token_by_visiting_homepage(max_attempts: int = 20) -> None:
    """
    Visit the homepage, solve captchas, and persist the AWS WAF token.
    Only calls ymm-temp.php API if PHPSESSID is not available or invalid.
    Retries up to max_attempts times until human verification is no longer detected
    and PHPSESSID is found in response cookies.
    """
    homepage_url = "https://www.customwheeloffset.com"
    attempt = 0
    session = threaded_session_manager.get_session()
    
    # Load existing tokens if available
    tok_init = _load_saved_token()
    if tok_init:
        global AWS_WAF_TOKEN
        AWS_WAF_TOKEN = tok_init
        dom = urlparse(homepage_url).netloc
        try:
            session.cookies.set("aws-waf-token", tok_init, domain=dom, path="/")
        except Exception:
            session.cookies.set("aws-waf-token", tok_init)
    
    # Check if we already have a valid PHPSESSID in current session
    current_phpsessid = None
    
    # Check session cookies only (no file loading)
    for cookie in session.cookies:
        if cookie.name == "PHPSESSID":
            current_phpsessid = cookie.value
            break
    
    if current_phpsessid:
        logger.info(f"[homepage] Using existing PHPSESSID from session: {current_phpsessid}")
    else:
        logger.info("[homepage] No PHPSESSID found in session - will be obtained during homepage visit or API calls")
    
    # Always visit homepage first, regardless of existing tokens
    print("[homepage] Always visiting homepage to ensure fresh session state...")
    
    while attempt < max_attempts:
        attempt += 1
        headers = _browser_headers(homepage_url)
        token_val = AWS_WAF_TOKEN
        
        # Show current PHPSESSID being used
        current_phpsessid = None
        for cookie in session.cookies:
            if cookie.name == "PHPSESSID":
                current_phpsessid = cookie.value
                break
        
        logger.info(f"[homepage] GET attempt {attempt}, token={token_val if token_val else 'none'}, PHPSESSID={current_phpsessid if current_phpsessid else 'none'}")
        
        try:
            proxies = _get_proxy_config()
            resp = session.get(homepage_url, timeout=30, headers=headers, allow_redirects=True, proxies=proxies)
            html_text = resp.text
            
            # Let resolver parse and potentially set token
            ensure_not_human_verification(html_text, url=homepage_url)
            
            # If resolver acquired a token, ensure it's in the jar and saved
            new_token = AWS_WAF_TOKEN
            if new_token and new_token != token_val:
                dom = urlparse(homepage_url).netloc
                try:
                    session.cookies.set("aws-waf-token", new_token, domain=dom, path="/")
                except Exception:
                    session.cookies.set("aws-waf-token", new_token)
                _save_token(new_token)
            
            # Check if human verification is still detected
            try:
                soup = BeautifulSoup(html_text or "", "html.parser")
                title = soup.title.string.strip() if soup.title and soup.title.string else ""
                is_hv = title.lower() == "human verification"
            except Exception:
                is_hv = False
            
            if not is_hv:
                logger.info("[homepage] Human verification not detected; token ready.")
                
                # Extract PHPSESSID from response cookies (no longer saving to file)
                phpsessid_found = False
                for cookie in resp.cookies:
                    if cookie.name == "PHPSESSID":
                        logger.info(f"[homepage] Found PHPSESSID: {cookie.value}")
                        # Note: PHPSESSID is NOT saved to file as each thread needs its own unique PHPSESSID
                        phpsessid_found = True
                        break
                
                if phpsessid_found:
                    return
                else:
                    logger.info("[homepage] PHPSESSID not found in response cookies, checking session...")
                    # Check if we already have a PHPSESSID in the current session
                    existing_phpsessid = None
                    for cookie in session.cookies:
                        if cookie.name == "PHPSESSID":
                            existing_phpsessid = cookie.value
                            break
                    
                    if existing_phpsessid:
                        logger.info(f"[homepage] Using existing session PHPSESSID: {existing_phpsessid}")
                        # Note: PHPSESSID is NOT saved to file as each thread needs its own unique PHPSESSID
                        return
                    else:
                        logger.info("[homepage] No PHPSESSID in session either, will retry...")
                        # Add a small delay before retrying to avoid overwhelming the server
                        if attempt < max_attempts:
                            time.sleep(2)
                        # Continue the loop to make another request
                
        except Exception as e:
            logger.error(f"[homepage] Attempt {attempt} failed: {e}")
        
        if attempt >= max_attempts:
            logger.warning(f"[homepage] Maximum attempts ({max_attempts}) reached. Human verification may persist or PHPSESSID not available.")
            return


# if __name__ == "__main__":
#     challenge_script = "https://79d55c0b354e.cbfcebb4.us-east-1.token.awswaf.com/79d55c0b354e/adabdc5b589f/42ddb7530b15/challenge.js"
#     captcha_script = "https://79d55c0b354e.cbfcebb4.us-east-1.captcha.awswaf.com/79d55c0b354e/adabdc5b589f/42ddb7530b15/captcha.js"
#     target_url = "https://www.customwheeloffset.com/store/wheels?sort=instock&year=2025&make=Chevrolet&model=Equinox%20EV&trim=LT&drive=FWD&DRChassisID=92916&vehicle_type=Truck&suspension=Suspension%20Lift%202.5%22&modification=Minor%20Plastic%20Trimming&rubbing=Slight%20rub%20at%20full%20turn&saleToggle=0&qdToggle=0"
#     sitekey = "AQIDAHjcYu/GjX+QlghicBgQ/7bFaQZ+m5FKCMDnO+vTbNg96AEWPRo91eiSvCJ3zDXsN5ZKAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMlu6ghSg678M75xu9AgEQgDvhq1eXWbulQzDiRyoOVVdNWhvYOT814LWr4orxd29Hok636/gi7LLWty1+TIhN5S/QJaIcbuAJhcLVjg=="
#     iv = "CgAFCDKA7gAABcrX"
#     context = "VFd3I6EcKZd3g51tcg2kMIbV00Go1l941ByzuCX5X343AaT8XOVJbVi4P61CsI3u4dn4j+LZvj6KPg5CWZcP3lDnh6mti0FthEFDfovO5lvW/pjd0/Is32ud5fw95ZxoCFNQsmR7e5QCzezyS3+i/JyOvMVjM6r7WiIq16OioK9V2mBlIzv6u04Ye0eXgr3byyAYCa2OlBYviIck3Lp3cdfxuIoblU35NF7U/35t8T+266vsOYRuy3LMGSYbK5dqOsYJ0P74QWUVMsEabpUjpvXRZ0bYvEIL5gujb0L5F8GkqYHUCUXGv3rGORujUXe2IHQrXytdeTs="
#     token = get_aws_waf_token(sitekey, iv, context, target_url, challenge_script, captcha_script)
#     print("token:", token)

__all__ = [
    "ensure_not_human_verification",
    "get_aws_waf_token",
    "AWS_WAF_TOKEN",
    "ensure_token_by_visiting_homepage",
]