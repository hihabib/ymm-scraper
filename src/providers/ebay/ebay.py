#!/usr/bin/env python3
"""
eBay YMM scraper (Python port of demo-js).

Implements dynamic selection flow: Year -> Make -> Model -> Trim -> Submodel -> Engine -> Engine_Liter_Display.
The API is stateful and returns the next property to choose along with already-selected properties.

This implementation does not include any resume behavior; it always starts fresh.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from urllib.parse import urlencode

# PYTHONPATH fallback for direct execution
SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import requests

EBAY_YMM_API_BASE = (
    "https://www.ebay.com/g/api/fitment?vehicle_type=CAR_AND_TRUCK&part_type="
    "&query_type=BY_VEHICLE&vehicle_marketplaceId=EBAY-US&finder=NEW_FINDER"
)
EBAY_VEHICLE_INFORMATION_API_BASE = (
    "https://www.ebay.com/g/api/confirm?referrer=BROWSE&module_groups=TIRE_FINDER&api=null"
)

# Logging controls
PRINT_NO_ENGINE = False  # set True to log branches that have no engine

property_orders: Dict[str, int] = {
    "Year": 1,
    "Make": 2,
    "Model": 3,
    "Trim": 4,
    "Submodel": 5,
    "Engine": 6,
    "Engine_Liter_Display": 7,
}


def _fetch_json(url: str, method: str = "GET", payload: Optional[dict] = None, timeout: tuple[int, int] = (10, 30)) -> dict:
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.ebay.com/",
        "Origin": "https://www.ebay.com",
    }
    for attempt in range(3):
        try:
            if method.upper() == "POST":
                resp = session.post(url, json=payload or {}, headers=headers, timeout=timeout)
            else:
                resp = session.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt == 2:
                raise
            time.sleep(0.5 + attempt * 0.5)
    return {}


def _get_next_property_name(res: dict) -> Optional[str]:
    npc = res.get("nextPropertyChoice")
    return npc.get("name") if isinstance(npc, dict) else None


def _get_next_possible_values(res: dict) -> Optional[List[Any]]:
    npc = res.get("nextPropertyChoice")
    vals = npc.get("possibleValues") if isinstance(npc, dict) else None
    return vals if isinstance(vals, list) else None


def _get_selected_properties(res: dict) -> List[dict]:
    sp = res.get("selectedProperties")
    return sp if isinstance(sp, list) else []


def _get_selected_property_values_by_name(res: dict, name: str) -> Optional[List[Any]]:
    for p in _get_selected_properties(res):
        if p.get("name") == name:
            vals = p.get("possibleValues")
            return vals if isinstance(vals, list) else None
    return None


def _has_next_search_indexed_values(res: dict) -> bool:
    npc = res.get("nextPropertyChoice")
    return isinstance(npc, dict) and isinstance(npc.get("searchIndexedValues"), dict)


def _get_next_search_index_keys(res: dict) -> List[str]:
    npc = res.get("nextPropertyChoice")
    siv = npc.get("searchIndexedValues") if isinstance(npc, dict) else None
    return list(siv.keys()) if isinstance(siv, dict) else []


def _get_next_search_index_values(res: dict, key: str) -> List[Any]:
    npc = res.get("nextPropertyChoice")
    siv = npc.get("searchIndexedValues") if isinstance(npc, dict) else None
    vals = (siv or {}).get(key)
    return vals if isinstance(vals, list) else []


def _has_selected_search_indexed_by_index(res: dict, i: int) -> bool:
    selected = _get_selected_properties(res)
    if 0 <= i < len(selected):
        return isinstance(selected[i].get("searchIndexedValues"), dict)
    return False


def _get_selected_search_index_keys(res: dict, i: int) -> List[str]:
    selected = _get_selected_properties(res)
    if 0 <= i < len(selected):
        siv = selected[i].get("searchIndexedValues")
        return list(siv.keys()) if isinstance(siv, dict) else []
    return []


def _get_selected_search_index_values(res: dict, i: int, key: str) -> List[Any]:
    selected = _get_selected_properties(res)
    if 0 <= i < len(selected):
        siv = selected[i].get("searchIndexedValues")
        vals = (siv or {}).get(key)
        return vals if isinstance(vals, list) else []
    return []


def _concat_param(existing: str, key: str, value: Any) -> str:
    # Build query string by setting or appending key=value
    qs = dict((k, v[0] if isinstance(v, list) else v) for k, v in ([]) )
    # Simpler: use URLSearchParams-like behavior
    from urllib.parse import parse_qsl
    params = dict(parse_qsl(existing, keep_blank_values=True)) if existing else {}
    params[str(key)] = str(value)
    return urlencode(params)


def _parse_params_map(qs: str) -> Dict[str, str]:
    """Parse query string into a simple dict."""
    from urllib.parse import parse_qsl
    return dict(parse_qsl(qs or "", keep_blank_values=True))


def _normalize_value(v: Any) -> str:
    """Normalize a possibleValues entry into a comparable string."""
    if isinstance(v, dict):
        cand = v.get("value") or v.get("name") or v.get("displayValue") or v.get("label")
    else:
        cand = str(v) if v is not None else None
    if not cand:
        return ""
    return str(cand).strip().replace("+", " ").casefold()


def _remove_first_n(arr: List[Any], n: int) -> List[Any]:
    if not isinstance(arr, list):
        return []
    if n <= 0:
        return arr
    return arr[n:] if n < len(arr) else []


def _get_vehicle_information(ymm: List[dict]) -> dict:
    payload = {
        "globalContext": {
            "keyword": "",
            "useFetch": True,
            "basePageURL": "https://www.ebay.com/b/179680",
        },
        "scopedContext": {
            "catalogDetails": {
                "type": "CAR_AND_TRUCK",
                "marketplaceId": "EBAY-US",
                "itemId": "",
                "categoryId": "179680",
            },
            "fitmentProduct": {
                "properties": ymm,
            },
        },
    }
    return _fetch_json(EBAY_VEHICLE_INFORMATION_API_BASE, method="POST", payload=payload)


def _extract_convenience_fields(ymm_result: List[dict]) -> Dict[str, Optional[str]]:
    # Map array of {name, value} into convenience dict
    mapping = {i.get("name"): i.get("value") for i in ymm_result if isinstance(i, dict)}
    return {
        "year": mapping.get("Year"),
        "make": mapping.get("Make"),
        "model": mapping.get("Model"),
        "trim": mapping.get("Trim"),
        "submodel": mapping.get("Submodel"),
        "engine": mapping.get("Engine"),
        "engine_liter_display": mapping.get("Engine_Liter_Display"),
    }


def _parse_engine_liter_display(engine: Optional[str], fallback: Optional[str]) -> Optional[str]:
    """Return engine_liter_display, preferring explicit value; else parse from engine string."""
    if fallback:
        return fallback
    if not engine:
        return None
    import re
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*L", engine)
    return (m.group(1) + "L") if m else None


def _extract_tire_sizes(vehicle_info: dict) -> List[str]:
    """Parse vehicle information JSON to extract tire sizes from the dialog structure."""
    sizes: List[str] = []
    modules = vehicle_info.get("modules") if isinstance(vehicle_info, dict) else None
    dialog = (modules or {}).get("VEHICLE_CONFIRMATION_DIALOG") if isinstance(modules, dict) else None
    tires = (dialog or {}).get("tires") if isinstance(dialog, dict) else None
    if isinstance(tires, list):
        for t in tires:
            details = t.get("tireDetails") if isinstance(t, dict) else None
            if isinstance(details, list):
                for d in details:
                    all_around = d.get("allAround") if isinstance(d, dict) else None
                    spans = (all_around or {}).get("textSpans") if isinstance(all_around, dict) else None
                    if isinstance(spans, list):
                        for s in spans:
                            text = s.get("text") if isinstance(s, dict) else None
                            if isinstance(text, str) and text.strip():
                                sizes.append(text.strip())
    # Deduplicate while preserving order
    seen = set()
    uniq: List[str] = []
    for s in sizes:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


def run() -> None:
    """Main entry point to scrape eBay YMM and save results with resume support."""
    # Lazy import to avoid circular dependencies on module import
    try:
        from services.repository_optimized import (
            insert_ebay_ymm_result,
            batch_insert_ebay_tire_sizes,
            find_ebay_ymm_result,
            get_tire_sizes_for_ymm,
        )
    except Exception:
        # Fallback for direct execution if PYTHONPATH not set
        import importlib
        repo_mod = importlib.import_module("services.repository_optimized")
        insert_ebay_ymm_result = getattr(repo_mod, "insert_ebay_ymm_result")
        batch_insert_ebay_tire_sizes = getattr(repo_mod, "batch_insert_ebay_tire_sizes")
        find_ebay_ymm_result = getattr(repo_mod, "find_ebay_ymm_result")
        get_tire_sizes_for_ymm = getattr(repo_mod, "get_tire_sizes_for_ymm")

    # Worker count configuration
    EBAY_WORKERS = 5
    try:
        from config.worker import EBAY_WORKERS as _EBAY_WORKERS
        EBAY_WORKERS = _EBAY_WORKERS
    except Exception:
        # Fallback dynamic import
        import importlib.util
        wk_file = Path(__file__).resolve().parents[2] / "config" / "worker.py"
        spec_wk = importlib.util.spec_from_file_location("worker_config_module", str(wk_file))
        if spec_wk and spec_wk.loader:
            wmod = importlib.util.module_from_spec(spec_wk)
            spec_wk.loader.exec_module(wmod)
            EBAY_WORKERS = getattr(wmod, "EBAY_WORKERS", EBAY_WORKERS)

    # No resume helpers; selection always proceeds from the beginning

    visited: Set[str] = set()
    visited_lock = Lock()
    pending_count: int = 0
    pending_lock = Lock()

    def _inc_pending() -> None:
        nonlocal pending_count
        with pending_lock:
            pending_count += 1

    def _dec_pending() -> None:
        nonlocal pending_count
        with pending_lock:
            pending_count -= 1

    def _mark_visited(params: str) -> bool:
        with visited_lock:
            if params in visited:
                return False
            visited.add(params)
            return True

    def _process_params(params: str = "") -> None:
        try:
            url = EBAY_YMM_API_BASE + ("&" + params if params and not params.startswith("&") else params)
            result = _fetch_json(url)
            next_property_name = _get_next_property_name(result)
            selected_properties = _get_selected_properties(result)
            selected_properties_names = [p.get("name") for p in selected_properties if isinstance(p, dict)]

            if next_property_name:
                # Carry forward already-selected params exactly as provided.
                new_params = params

                next_possible_values = _get_next_possible_values(result) or []
                if not next_possible_values:
                    print(f"[ebay] No values for next property '{next_property_name}' with params='{params}'")

                for value in next_possible_values:
                    if _has_next_search_indexed_values(result):
                        for key in _get_next_search_index_keys(result):
                            idx_vals = _get_next_search_index_values(result, key)
                            if idx_vals:
                                new_params = _concat_param(new_params, key, idx_vals[0])

                    new_params = _concat_param(new_params, next_property_name, value)
                    if _mark_visited(new_params):
                        _inc_pending()
                        executor.submit(_process_params, new_params)

            else:
                if "Engine" in (selected_properties_names or []):
                    ymm_result: List[dict] = []
                    for i, selected_name in enumerate(selected_properties_names or []):
                        if _has_selected_search_indexed_by_index(result, i):
                            for key in _get_selected_search_index_keys(result, i):
                                vals = _get_selected_search_index_values(result, i, key)
                                ymm_result.append({"name": key, "value": (vals[0] if vals else None)})
                        vals = _get_selected_property_values_by_name(result, selected_name) or []
                        ymm_result.append({"name": selected_name, "value": (vals[0] if vals else None)})

                    convenience = _extract_convenience_fields(ymm_result)
                    engine_val = convenience.get("engine")
                    if not engine_val:
                        if PRINT_NO_ENGINE:
                            print(f"[ebay] Skipped branch (no engine): params='{params}'")
                        return

                    eld = _parse_engine_liter_display(engine_val, convenience.get("engine_liter_display"))

                    vehicle_information = _get_vehicle_information(ymm_result)

                    existing = find_ebay_ymm_result(
                        convenience.get("year"),
                        convenience.get("make"),
                        convenience.get("model"),
                        convenience.get("trim"),
                        convenience.get("submodel"),
                        engine_val,
                    )
                    if existing:
                        ymm_id = existing.id
                        print(
                            f"[ebay] Existing YMM result id={ymm_id}: "
                            f"{convenience.get('year')} {convenience.get('make')} {convenience.get('model')} "
                            f"{convenience.get('trim')} {convenience.get('submodel')} {engine_val} "
                            f"ELD={eld}"
                        )
                    else:
                        ymm_id = insert_ebay_ymm_result(
                            year=convenience.get("year"),
                            make=convenience.get("make"),
                            model=convenience.get("model"),
                            trim=convenience.get("trim"),
                            submodel=convenience.get("submodel"),
                            engine=engine_val,
                            engine_liter_display=eld,
                        )
                        print(
                            f"[ebay] Saved YMM result id={ymm_id}: "
                            f"{convenience.get('year')} {convenience.get('make')} {convenience.get('model')} "
                            f"{convenience.get('trim')} {convenience.get('submodel')} {engine_val} "
                            f"ELD={eld}"
                        )

                    sizes = _extract_tire_sizes(vehicle_information)
                    if sizes:
                        existing_sizes = set(get_tire_sizes_for_ymm(ymm_id))
                        new_sizes = [s for s in sizes if s not in existing_sizes]
                    else:
                        new_sizes = []
                    if new_sizes:
                        batch_insert_ebay_tire_sizes(
                            ymm_id,
                            new_sizes,
                            {
                                "year": convenience.get("year"),
                                "make": convenience.get("make"),
                                "model": convenience.get("model"),
                                "trim": convenience.get("trim"),
                                "submodel": convenience.get("submodel"),
                                "engine": engine_val,
                            },
                        )
                    if sizes:
                        print(f"[ebay] Found {len(sizes)} tire sizes; inserted {len(new_sizes)} new for YMM id={ymm_id}: {new_sizes}")
                    else:
                        print(
                            f"[ebay] No tire sizes found for "
                            f"{convenience.get('year')} {convenience.get('make')} {convenience.get('model')} "
                            f"{convenience.get('trim')} {convenience.get('submodel')} {engine_val}"
                        )
                else:
                    if PRINT_NO_ENGINE:
                        print(f"[ebay] Skipped branch (no engine): params='{params}'")
        except Exception as e:
            print(f"[ebay] Error while processing params='{params}': {type(e).__name__}: {e}")
        finally:
            _dec_pending()

    # Kick off from the beginning with thread pool
    with ThreadPoolExecutor(max_workers=EBAY_WORKERS) as executor:
        # seed root task
        visited.clear()
        _inc_pending()
        executor.submit(_process_params, "")
        # wait until all tasks complete
        while True:
            with pending_lock:
                if pending_count == 0:
                    break
            time.sleep(0.05)


if __name__ == "__main__":
    run()