import requests
import json
import time
import random
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import brotli  # Optional, used to decode 'br' Content-Encoding if needed
except Exception:
    brotli = None

username = "ub3b25e2656da05c8-zone-custom-region-us"
password = "test"
PROXY_DNS = "43.159.28.126:2334"

def api_call(endpoint: str, params: dict = None, headers: dict = None, use_proxy: bool = True) -> tuple[dict, dict]:
    # Build proxy only when explicitly enabled and available
    proxies = None
    if use_proxy and PROXY_DNS:
        proxy_auth = "http://{}:{}@{}".format(username, password, PROXY_DNS)
        proxies = {"http": proxy_auth, "https": proxy_auth}

    # Derive origin and referer from target URL to mimic real navigation
    parsed = urlparse(endpoint)
    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else None
    referer = origin + "/" if origin else None

    # Chromium-like headers for top-level navigation
    default_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/109.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "sec-ch-ua": '"Google Chrome";v="109", "Chromium";v="109", "Not=A?Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }
    if origin:
        # Chrome does not send Origin on top-level navigations; keep Referer only
        default_headers["Referer"] = referer
    if headers:
        # Allow caller to override/extend defaults (e.g., Cookie, custom headers)
        default_headers.update(headers)

    # We'll try multiple times with exponential backoff and targeted fallbacks.
    # Each attempt creates a brand-new Session to avoid connection reuse.
    max_attempts = 8
    proxies_in_use = proxies
    for attempt in range(max_attempts):
        session = requests.Session()
        retry_strategy = Retry(
            total=2,
            connect=2,
            read=2,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods={"GET"},
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        try:
            response = session.get(
                endpoint,
                params=params,
                proxies=proxies_in_use,
                headers=default_headers,
                timeout=(10, 30),
                allow_redirects=True,
            )

            # Always return the body even for non-2xx to let caller handle WAF/CAPTCHA flows
            resp_headers = dict(response.headers)

            try:
                json_data = response.json()
                # If server returned JSON (e.g., API endpoints), handle Forbidden by retrying with a new session
                if isinstance(json_data, dict) and (json_data.get("message") == "Forbidden" or response.status_code in (401, 403)):
                    print(f"Attempt {attempt + 1}: Server responded 'Forbidden'. Recreating session and retrying.")
                    # Encourage closing connections on next attempt
                    default_headers["Connection"] = "close"
                    # Keep proxy as-is unless we see SSL/proxy issues; rely on next attempt's fresh session
                    raise requests.exceptions.RequestException("Forbidden response")
                # Normal JSON response
                return json_data, resp_headers
            except json.JSONDecodeError:
                # Fallback to text; if server replied with Brotli and requests didn't decode,
                # attempt manual decompression if brotli is available.
                text = response.text
                enc = (response.headers.get("Content-Encoding") or "").lower()
                if enc == "br" and brotli:
                    try:
                        text = brotli.decompress(response.content).decode(response.encoding or "utf-8", errors="replace")
                    except Exception:
                        # Keep original text on failure
                        pass
                # Log non-200 to aid debugging but still return HTML for handling
                if response.status_code >= 400:
                    print(f"Attempt {attempt + 1}: Non-200 status ({response.status_code}). Returning body for handling.")
                return text, resp_headers

        except requests.exceptions.SSLError as e:
            print(f"Attempt {attempt + 1}: SSL Error: {e}")
            # Targeted fallback: disable proxy and close connection for next attempts
            proxies_in_use = None
            default_headers["Connection"] = "close"
            # Simplify encodings in case intermediaries mishandle 'br'
            default_headers["Accept-Encoding"] = "gzip, deflate"
        except requests.exceptions.ConnectionError as e:
            print(f"Attempt {attempt + 1}: Connection Error: {e}")
            # If using proxy, try direct on next attempt; also close connection
            proxies_in_use = None
            default_headers["Connection"] = "close"
        except requests.exceptions.Timeout as e:
            print(f"Attempt {attempt + 1}: Timeout Error: {e}")
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1}: An unexpected request error occurred: {e}")
        finally:
            try:
                session.close()
            except Exception:
                pass
        
        # Exponential backoff with jitter before next attempt
        if attempt < max_attempts - 1:
            delay = min(3.0, 0.3 * (2 ** attempt)) + random.uniform(0, 0.25)
            time.sleep(delay)
    
    print("All retry attempts failed.")
    return None, None