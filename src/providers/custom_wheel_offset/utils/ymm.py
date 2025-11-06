# Robust import: prefer relative when running as a package; fallback to absolute via sys.path injection
try:
    from .request import api_call
except Exception:
    import sys
    from pathlib import Path
    SRC_DIR = Path(__file__).resolve().parents[3]  # points to .../src
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    from providers.custom_wheel_offset.utils.request import api_call
from bs4 import BeautifulSoup
from urllib.parse import quote
import json
import re

year_url = "https://www.customwheeloffset.com/makemodel/bp.php"

def get_years() -> list[str]:
    data, _ = api_call(year_url)
    if data is None:
        print("API call for years returned None.")
        return []
    if isinstance(data, str):
        soup = BeautifulSoup(data, 'html.parser')
        year_select = soup.find('select', {'name': 'year'})
        if year_select:
            years = [option['value'] for option in year_select.find_all('option') if option['value']]
            return years
    print(f"Unexpected data type for years: {type(data)}. Expected str.")
    return []

def get_trimming(vehicle_type: str, phpsessid: str) -> list[str]:
    """
    Retrieves trimming data for a given vehicle type.
    """
    url = f"https://www.customwheeloffset.com/api/ymm-temp.php?type=set&vehicle_type={vehicle_type}&store=wheels&getTrimming=true"
    headers = {"Cookie": f"PHPSESSID={phpsessid}"}
    data, _ = api_call(url, headers=headers)
    if data is None:
        print(f"API call for trimming data for vehicle type {vehicle_type} with PHPSESSID {phpsessid} returned None.")
        return []
    if isinstance(data, list):
        return data
    print(f"Unexpected data type for trimming data: {type(data)}. Expected list.")
    return []

def get_rubbing(vehicle_type: str, phpsessid: str) -> list[str]:
    """
    Retrieves rubbing data for a given vehicle type.
    """
    url = f"https://www.customwheeloffset.com/api/ymm-temp.php?type=set&vehicle_type={vehicle_type}&store=wheels&getRubbing=true"
    headers = {"Cookie": f"PHPSESSID={phpsessid}"}
    data, _ = api_call(url, headers=headers)
    if data is None:
        print(f"API call for rubbing data for vehicle type {vehicle_type} with PHPSESSID {phpsessid} returned None.")
        return []
    if isinstance(data, list):
        return data
    print(f"Unexpected data type for rubbing data: {type(data)}. Expected list.")
    return []

def get_makes(year: str) -> list[str]:
    params = {'year': year}
    data, _ = api_call(year_url, params=params)
    if data is None:
        print(f"API call for makes for year {year} returned None.")
        return []
    if isinstance(data, str):
        soup = BeautifulSoup(data, 'html.parser')
        make_select = soup.find('select', {'name': 'make'})
        if make_select:
            makes = [option['value'] for option in make_select.find_all('option') if option['value']]
            return makes
    print(f"Unexpected data type for makes: {type(data)}. Expected str.")
    return []

def get_models(year: str, make: str) -> list[str]:
    params = {'year': year, 'make': make}
    data, _ = api_call(year_url, params=params)
    if data is None:
        print(f"API call for models for year {year}, make {make} returned None.")
        return []
    if isinstance(data, str):
        soup = BeautifulSoup(data, 'html.parser')
        model_select = soup.find('select', {'name': 'model'})
        if model_select:
            models = [option['value'] for option in model_select.find_all('option') if option['value']]
            return models
    print(f"Unexpected data type for models: {type(data)}. Expected str.")
    return []

def get_trims(year: str, make: str, model: str) -> list[str]:
    params = {'year': year, 'make': make, 'model': model}
    data, _ = api_call(year_url, params=params)
    if data is None:
        print(f"API call for trims for year {year}, make {make}, model {model} returned None.")
        return []
    if isinstance(data, str):
        soup = BeautifulSoup(data, 'html.parser')
        trim_select = soup.find('select', {'name': 'trim'})
        if trim_select:
            trims = [option['value'] for option in trim_select.find_all('option') if option['value']]
            return trims
    print(f"Unexpected data type for trims: {type(data)}. Expected str.")
    return []

def get_drives(year: str, make: str, model: str, trim: str) -> list[str]:
    params = {'year': year, 'make': make, 'model': model, 'trim': trim}
    data, _ = api_call(year_url, params=params)
    if data is None:
        print(f"API call for drives for year {year}, make {make}, model {model}, trim {trim} returned None.")
        return []
    if isinstance(data, str):
        soup = BeautifulSoup(data, 'html.parser')
        drive_select = soup.find('select', {'name': 'drive'})
        if drive_select:
            drives = [option['value'] for option in drive_select.find_all('option') if option['value']]
            return drives
    print(f"Unexpected data type for drives: {type(data)}. Expected str.")
    return []

def get_vehicle_info(year: str, make: str, model: str, trim: str, drive: str) -> dict:
    base_url = "https://www.enthusiastenterprises.us/fitment/vehicle/co"
    encoded_make = quote(make)
    encoded_model = quote(model)
    encoded_trim = quote(trim)
    encoded_drive = quote(drive)
    
    url = f"{base_url}/{year}/{encoded_make}/{encoded_model}/{encoded_trim}/{encoded_drive}"
    data, _ = api_call(url)
    if data is None:
        print(f"API call for vehicle info for {year} {make} {model} {trim} {drive} returned None.")
        return {}
    if isinstance(data, dict):
        return data
    print(f"Unexpected data type for vehicle info: {type(data)}. Expected dict.")
    return {}

def get_phpsessid(vehicle_type: str, year: str, make: str, model: str, trim: str, drive: str, chassis: str) -> str | None:
    base_url = "https://www.customwheeloffset.com/api/ymm-temp.php"
    params = {
        "store": "wheels",
        "type": "set",
        "vehicle_type": vehicle_type,
        "year": year,
        "make": make,
        "model": model,
        "trim": trim,
        "drive": drive,
        "chassis": chassis
    }
    
    # Construct the URL with query parameters
    from urllib.parse import urlencode
    query_string = urlencode(params)
    url = f"{base_url}?{query_string}"
    
    _, headers = api_call(url)
    if headers is None:
        print(f"API call for PHPSESSID for {year} {make} {model} {trim} {drive} {chassis} returned None headers.")
        return None
    if headers and "Set-Cookie" in headers:
        for cookie in headers["Set-Cookie"].split(';'):
            if "PHPSESSID=" in cookie:
                return cookie.split('=')[1].split(';')[0].strip()
    print(f"Failed to extract PHPSESSID from headers: {headers}")
    return None

def get_suspension_data(vehicle_type: str, phpsessid: str) -> list[str]:
    base_url = "https://www.customwheeloffset.com/api/ymm-temp.php"
    params = {
        "type": "set",
        "store": "wheels",
        "vehicle_type": vehicle_type,
        "getSuspension": "true"
    }
    
    from urllib.parse import urlencode
    query_string = urlencode(params)
    url = f"{base_url}?{query_string}"
    
    headers = {"Cookie": f"PHPSESSID={phpsessid}"}
    data, _ = api_call(url, headers=headers)
    if data is None:
        print(f"API call for suspension data for vehicle type {vehicle_type} with PHPSESSID {phpsessid} returned None.")
        return []
    if isinstance(data, list):
        return [str(item) for item in data]
    print(f"Unexpected data type for suspension data: {type(data)}. Expected list.", data)
    return []

def get_fitment_preferences(vehicle_type: str, phpsessid: str) -> list[dict]:
    """
    Return all combinations of suspension, trimming, and rubbing preferences for a vehicle.

    Parameters:
      - vehicle_type: The type of vehicle (e.g., car, truck).
      - phpsessid: PHP session id cookie value.

    Returns:
      A list of dicts, each dict has keys: 'suspension', 'trimming', 'rubbing'.
      The number of objects equals len(suspension) * len(trimming) * len(rubbing).
    """
    suspensions = get_suspension_data(vehicle_type, phpsessid) or []
    trimmings = get_trimming(vehicle_type, phpsessid) or []
    rubbings = get_rubbing(vehicle_type, phpsessid) or []

    combinations: list[dict] = []
    for s in suspensions:
        for t in trimmings:
            for r in rubbings:
                combinations.append({
                    "suspension": str(s),
                    "trimming": str(t),
                    "rubbing": str(r),
                })
    return combinations

def get_fitment_from_store(params: dict) -> dict:
    """
    Fetches fitment data from the CWO store page using the provided params, includes cookies,
    and parses the HTML to return a Python dictionary matching extractFitment.js output.

    Expected params keys:
      - year, make, model, trim, drive, DRChassisID, vehicle_type, suspension, modification, rubbing

    Returns:
      dict with keys 'front' and 'rear' containing diameter, width, offset ranges and boltPattern.
    """
    base_url = "https://www.customwheeloffset.com/store/wheels"

    # Defaults present in example URL
    defaults = {
        "sort": "instock",
        "saleToggle": "0",
        "qdToggle": "0",
    }

    # Required keys to include from params
    keys = [
        "year",
        "make",
        "model",
        "trim",
        "drive",
        "DRChassisID",
        "vehicle_type",
        "suspension",
        "modification",
        "rubbing",
    ]

    # Encode each value with quote (spaces -> %20) and build query string
    def _encode(v: object) -> str:
        return quote(str(v), safe="")

    query_parts = [f"{k}={_encode(v)}" for k, v in defaults.items()]
    for k in keys:
        if k in params and params[k] is not None:
            query_parts.append(f"{k}={_encode(params[k])}")

    url = f"{base_url}?" + "&".join(query_parts)

    # Load cookies and construct Cookie header
    cookies_path = r"e:\scraper\src\providers\custom_wheel_offset\cookies.json"
    cookie_header = ""
    try:
        with open(cookies_path, "r", encoding="utf-8") as f:
            cookies = json.load(f)
        cookie_pairs = [
            f"{c.get('name')}={c.get('value')}"
            for c in cookies
            if isinstance(c, dict) and "customwheeloffset" in (c.get("domain") or "")
        ]
        cookie_header = "; ".join(cookie_pairs)
    except FileNotFoundError:
        print(f"Cookies file not found at {cookies_path}. Proceeding without cookies.")
        cookie_pairs = []
    except Exception as e:
        print(f"Failed to load cookies from {cookies_path}: {e}")
        cookie_pairs = []

    headers = {"Cookie": cookie_header} if cookie_header else None

    data, _ = api_call(url, headers=headers)
    if data is None:
        print("API call for fitment page returned None.")
        return {}
    if not isinstance(data, str):
        print(f"Unexpected data type for fitment page: {type(data)}. Expected str (HTML).")
        return {}

    soup = BeautifulSoup(data, "html.parser")

    # If the page is gated by Human Verification, signal upstream to pause and solve CAPTCHA.
    page_title = (soup.title.get_text(strip=True) if soup.title else "")
    if page_title == "Human Verification":
        print("Detected 'Human Verification' page. Signaling abort to solve CAPTCHA...")
        try:
            from core.errors import HumanVerificationError
        except Exception:
            # Robust import if running directly
            import sys
            from pathlib import Path
            SRC_DIR = Path(__file__).resolve().parents[3]
            if str(SRC_DIR) not in sys.path:
                sys.path.insert(0, str(SRC_DIR))
            from core.errors import HumanVerificationError
        raise HumanVerificationError("Human Verification encountered at fitment store page")

    def parse_range(text: str) -> dict:
        if not text or "to" not in text:
            return {"min": None, "max": None}
        m = re.match(r"^\s*(.+?)\s+to\s+(.+?)\s*$", text.strip())
        if not m:
            return {"min": None, "max": None}
        min_v = m.group(1).strip()
        max_v = m.group(2).strip()
        if not min_v or not max_v or min_v in ('"', 'mm') or max_v in ('"', 'mm'):
            return {"min": None, "max": None}
        return {"min": min_v, "max": max_v}

    def parse_bolt_pattern() -> dict:
        el = soup.select_one(".store-bp")
        if not el:
            return {"inch": None, "mm": None}
        raw = (el.get("data-bp") or "").strip()
        if raw:
            parts = [p.strip() for p in raw.split(",")]
            inch_raw = parts[0] if len(parts) > 0 else None
            mm_raw = parts[1] if len(parts) > 1 else None
            return {
                "inch": (inch_raw + '"') if inch_raw else None,
                "mm": (mm_raw + "mm") if mm_raw else None,
            }
        text = el.get_text(" ", strip=True)
        mm_match = re.search(r"(\d+x[\d\.]+)\s*mm", text, re.I)
        inch_match = re.search(r"\((\d+x[\d\.]+)[\"']?\)", text, re.I)
        mm = (mm_match.group(1) + "mm") if mm_match else None
        inch = (inch_match.group(1) + '"') if inch_match else None
        return {"inch": inch, "mm": mm}

    def extract_from_element(section) -> dict:
        def get_b_text(label: str) -> str:
            for span in section.select(".store-conf-range"):
                txt = span.get_text(strip=True)
                if txt.startswith(label):
                    b_tag = span.find("b")
                    return b_tag.get_text(strip=True) if b_tag else ""
            return ""
        return {
            "diameter": parse_range(get_b_text("Diameter:")),
            "width": parse_range(get_b_text("Width:")),
            "offset": parse_range(get_b_text("Offset:")),
            "boltPattern": parse_bolt_pattern(),
        }

    # Detect merged or separate sections
    merged_el = soup.select_one(".store-ymm-fitrange.full-size")
    if merged_el:
        shared = extract_from_element(merged_el)
        return {"front": shared, "rear": shared}

    def find_section(header_text: str):
        for el in soup.select(".store-ymm-fitrange"):
            nobr = el.find("nobr")
            if nobr and header_text in nobr.get_text(strip=True):
                return el
        return None

    default_values = {
        "diameter": {"min": None, "max": None},
        "width": {"min": None, "max": None},
        "offset": {"min": None, "max": None},
        "boltPattern": {"inch": None, "mm": None},
    }

    front_el = find_section("Front")
    rear_el = find_section("Rear")

    front = extract_from_element(front_el) if front_el else default_values
    rear = extract_from_element(rear_el) if rear_el else default_values

    return {"front": front, "rear": rear}

