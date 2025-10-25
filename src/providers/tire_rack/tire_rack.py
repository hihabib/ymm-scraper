#!/usr/bin/env python3
"""
Tire Rack scraper main.
Calls the high-level API client with a URL and prints the response.
Core HTTP and proxy details are hidden behind the API client.
"""
from pathlib import Path
import sys
from urllib.parse import urlencode
import requests


# Support running as a standalone script without setting PYTHONPATH
try:
    from core.client import get
except ImportError:
    SRC_DIR = Path(__file__).resolve().parents[2]  # .../src
    sys.path.insert(0, str(SRC_DIR))
    from core.client import get
from core.http import make_session, fetch
from config.proxy import (
    PROXY_DNS1, PROXY_USER, PROXY_PASS, COOKIE_STRING,
    get_dns_rotation_iterator, get_proxy_config_with_dns, TOTAL_MAX_RETRIES
)


# Import utils in a way that avoids hyphenated package names
try:
    if __package__:
        from .utils import extract_option_values, extract_xml_values  # type: ignore
    else:
        from utils import extract_option_values, extract_xml_values
except Exception:
    # Fallback: load utils by file path
    import importlib.util
    import os
    utils_path = os.path.join(os.path.dirname(__file__), "utils.py")
    spec = importlib.util.spec_from_file_location("tire_rack_utils", utils_path)
    module = importlib.util.module_from_spec(spec)  # type: ignore
    assert spec and spec.loader
    spec.loader.exec_module(module)  # type: ignore
    extract_option_values = module.extract_option_values
    extract_xml_values = module.extract_xml_values
from src.db.migrate import run_migrations
from src.services.repository import insert_ymm, get_last_ymm, insert_error_log, insert_tire_sizes_for_ymm
from src.core.errors import ApiError, ParsingError, DataSplicingError
import time
import shutil
from typing import Callable, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import importlib.util
import os
import threading

# One-time browser pool state (prepared before starting scraping loops)
_BROWSERS_PREPARED: bool = False
BROWSER_MODULES: list[Any] = []
_THREAD_TO_BROWSER_INDEX: dict[int, int] = {}
_BROWSER_ASSIGN_LOCK = threading.Lock()
_NEXT_BROWSER_INDEX = 0

def _load_tire_module_instance(module_name: str) -> Any:
    """Load a fresh instance of the Playwright tire-size module by file path.
    A unique module name ensures separate module state per browser instance.
    """
    script_path = os.path.join(os.path.dirname(__file__), "tire_size.py")
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore
    assert spec and spec.loader
    spec.loader.exec_module(mod)  # type: ignore
    return mod

def prepare_browsers(count: int) -> None:
    """Open `count` browser instances and wait for script loading in each.
    Runs only once per process; subsequent calls are no-ops.
    """
    global _BROWSERS_PREPARED, BROWSER_MODULES
    if _BROWSERS_PREPARED:
        return
    print(f"Preparing {count} browser instance(s) for workers...")
    for i in range(count):
        mod = _load_tire_module_instance(f"tire_size_worker_{i}")
        # Use a unique persistent profile dir per worker to avoid profile locking
        base_data_dir = Path(__file__).resolve().parents[2] / "data"
        profile_dir = base_data_dir / f"chromium_profile_worker_{i}"
        # Tell module to use our profile dir
        try:
            mod.set_profile_dir_override(str(profile_dir))
        except Exception:
            pass

        # Retry with cleanup on transient Chromium launch issues
        max_attempts = 2
        for attempt in range(1, max_attempts + 1):
            try:
                # Blocks until window.runScript is available in the page for this module
                mod.wait_for_script_loaded_sync()
                BROWSER_MODULES.append(mod)
                print(f"Browser {i + 1}/{count} ready: script loaded")
                break
            except Exception as e:
                print(
                    f"[prepare_browsers] worker={i} attempt={attempt} failed: {type(e).__name__}: {e}"
                )
                if attempt < max_attempts:
                    # Clean up profile dir and retry once after a short backoff
                    try:
                        shutil.rmtree(profile_dir, ignore_errors=True)
                    except Exception as cleanup_err:
                        print(f"[prepare_browsers] worker={i} cleanup error: {cleanup_err}")
                    time.sleep(1.5)
                    # Re-apply profile dir override in case module cached state
                    try:
                        mod.set_profile_dir_override(str(profile_dir))
                    except Exception:
                        pass
                    continue
                else:
                    print(f"[prepare_browsers] worker={i} skipped after {max_attempts} failures")
                    # Do not append this mod; continue with remaining workers
                    break
    _BROWSERS_PREPARED = True

def _get_browser_for_current_thread() -> tuple[Any, int]:
    """Return (module, browser_index) mapped to the current thread, assigning round-robin."""
    ident = threading.get_ident()
    with _BROWSER_ASSIGN_LOCK:
        idx = _THREAD_TO_BROWSER_INDEX.get(ident)
        if idx is None:
            # Assign round-robin among prepared browsers
            global _NEXT_BROWSER_INDEX
            if not BROWSER_MODULES:
                raise RuntimeError("Browser modules are not prepared")
            idx = _NEXT_BROWSER_INDEX % len(BROWSER_MODULES)
            _THREAD_TO_BROWSER_INDEX[ident] = idx
            _NEXT_BROWSER_INDEX += 1
            print(f"[thread {ident}] assigned to browser worker {idx}")
    return BROWSER_MODULES[idx], idx

def _retry_call(fn: Callable[[], Any], *, attempts: int = 5, per_call_timeout_secs: int = 40, sleep_between_secs: float = 1.0) -> Any:
    """Run `fn` up to `attempts` times. Each attempt should respect `per_call_timeout_secs` via the underlying client.
    Returns fn() result on success, raises last exception on failure.
    """
    last_exc: Optional[Exception] = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            if i < attempts - 1:
                time.sleep(sleep_between_secs)
    assert last_exc
    raise ApiError(f"API call failed after {attempts} attempts: {type(last_exc).__name__}: {last_exc}")

def get_makes():
    target_url = (
        "https://www.tirerack.com/modalPopups/changeSearchLayer.jsp?shoppingFor=tires"
    )
    print("makes url", target_url)
    try:
        body = _retry_call(lambda: get(target_url, timeout=(10, per_call_timeout := 40)))
        return extract_option_values(body)
    except Exception as e:
        insert_error_log(
            source="tire_rack",
            context={"op": "get_makes", "url": target_url},
            message=f"{type(e).__name__}: {e}"
        )
        raise

def get_years(make: str):
    target_url = (
        "https://www.tirerack.com/survey/ValidationServlet?autoMake={}&autoYearsNeeded=true".format(make)
    )

    try:
        years_xml_str = _retry_call(lambda: get(target_url, timeout=(10, 40)))
        return extract_xml_values(years_xml_str, "year")
    except Exception as e:
        insert_error_log(
            source="tire_rack",
            context={"op": "get_years", "url": target_url, "make": make},
            message=f"{type(e).__name__}: {e}"
        )
        raise

def get_models(year: str, make:str):
    target_url = (
        f"https://www.tirerack.com/survey/ValidationServlet?autoYear={year}&autoMake={make}"
    )
    print("models url", target_url)
    try:
        models_xml_str = _retry_call(lambda: get(target_url, timeout=(10, 40)))
        return extract_xml_values(models_xml_str, "model")
    except Exception as e:
        insert_error_log(
            source="tire_rack",
            context={"op": "get_models", "url": target_url, "make": make, "year": year},
            message=f"{type(e).__name__}: {e}"
        )
        raise

def get_clarifiers(year: str, make: str, model: str):
    target_url = f"https://www.tirerack.com/survey/ValidationServlet?autoYear={year}&autoMake={make}&autoModel={model}&newDesktop=true&includeClarType=true"
    print("clarifiers url", target_url)
    try:
        clarifiers_xml_str = _retry_call(lambda: get(target_url, timeout=(10, 40)))
        return extract_xml_values(clarifiers_xml_str, "clar")
    except Exception as e:
        insert_error_log(
            source="tire_rack",
            context={"op": "get_clarifiers", "url": target_url, "make": make, "year": year, "model": model},
            message=f"{type(e).__name__}: {e}"
        )
        raise

# def get_tire_size(year: str, make: str, model: str, clarifier: str):
#     base_url = "https://www.tirerack.com/tires/SelectTireSize.jsp"
#     params = {
#         "autoMake": make,
#         "autoModel": model,
#         "autoYear": year,
#         "autoModClar": clarifier,
#         "cameFrom": "vehicleSelector",
#         "perfCat": "ALL",
#     }
#     target_url = f"{base_url}?{urlencode(params)}"
#     print("tire size url: ", target_url)
#     try:
#         tire_size_xml_str = new_get(target_url)
#         print("got size html, now extracting...")
#         return extract_xml_values(tire_size_xml_str, "tireSize")
#     except Exception as e:
#         print("Unhandled error:", type(e).__name__, e)
#         return []



def run_scrape(max_workers: int = 8):
    # Prepare browsers for workers (open and load scripts once, before any loops)
    prepare_browsers(max_workers)

    # Ensure database tables exist
    run_migrations()

  
    # Prepare browser pool; guard against transient failures
    try:
        prepare_browsers(max_workers)
    except Exception as e:
        insert_error_log(
            source="tire_rack",
            context={"op": "prepare_browsers", "max_workers": max_workers},
            message=f"{type(e).__name__}: {e}"
        )
        print(f"[run_scrape] prepare_browsers error: {e}. Continuing with available browsers: {len(BROWSER_MODULES)}")

    # Determine resume point from the last inserted row
    last = get_last_ymm()
    if last:
        print(
            "resuming after last row:",
            last.id,
            last.year,
            last.make,
            last.model,
            (last.clarifier or ""),
            getattr(last, "created_at", None),
        )
    else:
        print("no previous rows found; starting from beginning")
    # Helper for case-insensitive, trimmed comparison
    def _norm(s: str | None) -> str:
        return (s or "").strip().lower()
    # Precompute normalized last values
    last_make_norm = _norm(getattr(last, "make", None))
    last_year_norm = _norm(getattr(last, "year", None))
    last_model_norm = _norm(getattr(last, "model", None))
    last_clar_norm = _norm(getattr(last, "clarifier", None))
    print("getting makes...")
    makes = get_makes()
    # Guard: if resuming but the last make isn't present, raise splicing error
    if last:
        if not any(_norm(m) == last_make_norm for m in makes):
            insert_error_log(
                source="tire_rack",
                context={"op": "resume_make_check", "last_make": last.make, "makes_count": len(makes)},
                message="Last make not found in fetched makes"
            )
            raise DataSplicingError("Resume make not found in makes list")
    # Determine starting make index
    if last:
        try:
            makes_start = next(i for i, m in enumerate(makes) if _norm(m) == last_make_norm)
        except StopIteration:
            makes_start = 0
    else:
        makes_start = 0

    # Pre-compute model and clarifier starting positions for the first year
    models_start_first_year = 0
    clar_start_first_model: int | None = None
    if last:
        # get models for the last make/year and find last model's index
        try:
            last_year_models = get_models(last.year, last.make)
        except Exception:
            last_year_models = []
        try:
            m_idx = next(i for i, mdl in enumerate(last_year_models) if _norm(mdl) == last_model_norm)
        except StopIteration:
            m_idx = 0
        # get clarifiers for the last triple and find last clarifier's index
        try:
            last_model_clars = get_clarifiers(last.year, last.make, last.model)
        except Exception:
            last_model_clars = []
        # find clarifier position
        clar_pos = None
        if last_model_clars:
            for i, c in enumerate(last_model_clars):
                if _norm(c) == last_clar_norm:
                    clar_pos = i
                    break
        # Decide where to start:
        if clar_pos is not None:
            if clar_pos < len(last_model_clars) - 1:
                # resume within the same model, starting at next clarifier
                models_start_first_year = m_idx
                clar_start_first_model = clar_pos + 1
            else:
                # last clarifier was the final one; move to next model
                models_start_first_year = m_idx + 1
                clar_start_first_model = None
        else:
            if last_clar_norm == "" and last_model_clars:
                # last row had no clarifier, but model has clarifiers → start same model at clarifier 0
                models_start_first_year = m_idx
                clar_start_first_model = 0
            else:
                # clarifiers empty or mismatch → move to next model
                models_start_first_year = m_idx + 1
                clar_start_first_model = None

    # Create a single ThreadPoolExecutor for the entire scraping process
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        
        def _process_year_make_model(make: str, year: str, model: str, clar_start_opt: Optional[int] = None) -> None:
            """Process a single year/make/model combination with all its clarifiers."""
            try:
                print(f"[thread {threading.get_ident()}] getting clarifiers for {year} {make} {model}")
                clarifiers = get_clarifiers(year, make, model)
                print(f"[thread {threading.get_ident()}] saving YMM data for {year} {make} {model}")
                
                if len(clarifiers) != 0:
                    clar_start = clar_start_opt if clar_start_opt is not None else 0
                    for clarifier in clarifiers[clar_start:]:
                        # Guard: never re-insert the exact last row
                        if last and _norm(make) == last_make_norm and _norm(year) == last_year_norm and _norm(model) == last_model_norm and _norm(clarifier) == last_clar_norm:
                            continue
                        # Call browser script (do not modify DB logic), print result with worker/browser info
                        result = None
                        try:
                            mod, browser_idx = _get_browser_for_current_thread()
                            datum = {"make": make, "year": year, "model": model, "clarifair": clarifier}
                            print(f"[thread {threading.get_ident()}] dispatching browser call on worker {browser_idx} datum={datum}")
                            result = mod.call_run_script_sync(datum)
                            print(f"[thread {threading.get_ident()}] [worker {browser_idx}] [browser {browser_idx}] result: {result}")
                        except Exception as e:
                            print(f"[worker/browser call error] make={make} year={year} model={model} clarifier={clarifier}: {type(e).__name__}: {e}")
                        # Insert YMM and then persist tire sizes if available
                        row_id = insert_ymm(year, make, model, clarifier)
                        print(f"[thread {threading.get_ident()}] saved {row_id} {year} {make} {model} {clarifier}")
                        try:
                            orig = (result or {}).get("originalSizes") or []
                            opt = (result or {}).get("optionalSizes") or []
                            count = insert_tire_sizes_for_ymm(row_id, orig, opt)
                            print(f"[thread {threading.get_ident()}] saved {count} tire sizes for ymm_id={row_id}")
                        except Exception as sz_err:
                            print(f"[sizes save error] ymm_id={row_id}: {type(sz_err).__name__}: {sz_err}")
                else:
                    # No clarifier; treat clarifier as None for comparison
                    if last and _norm(make) == last_make_norm and _norm(year) == last_year_norm and _norm(model) == last_model_norm and last_clar_norm == "":
                        return
                    # Call browser script with empty clarifair (do not modify DB logic)
                    result = None
                    try:
                        mod, browser_idx = _get_browser_for_current_thread()
                        datum = {"make": make, "year": year, "model": model, "clarifair": ""}
                        print(f"[thread {threading.get_ident()}] dispatching browser call on worker {browser_idx} datum={datum}")
                        result = mod.call_run_script_sync(datum)
                        print(f"[thread {threading.get_ident()}] [worker {browser_idx}] [browser {browser_idx}] result: {result}")
                    except Exception as e:
                        print(f"[worker/browser call error] make={make} year={year} model={model} clarifier=<none>: {type(e).__name__}: {e}")
                    # Insert YMM and persist tire sizes if available
                    row_id = insert_ymm(year, make, model, None)
                    print(f"[thread {threading.get_ident()}] saved {row_id} {year} {make} {model} ")
                    try:
                        orig = (result or {}).get("originalSizes") or []
                        opt = (result or {}).get("optionalSizes") or []
                        count = insert_tire_sizes_for_ymm(row_id, orig, opt)
                        print(f"[thread {threading.get_ident()}] saved {count} tire sizes for ymm_id={row_id}")
                    except Exception as sz_err:
                        print(f"[sizes save error] ymm_id={row_id}: {type(sz_err).__name__}: {sz_err}")
            except Exception as e:
                print(f"[_process_year_make_model error] {year} {make} {model}: {type(e).__name__}: {e}")
                insert_error_log(
                    source="tire_rack",
                    context={"op": "_process_year_make_model", "make": make, "year": year, "model": model},
                    message=f"{type(e).__name__}: {e}"
                )

        def _process_year_make(make: str, year: str, models_start: int = 0, first_model_clar_start: Optional[int] = None) -> None:
            """Process a single year/make combination by getting models and submitting them to the thread pool."""
            try:
                print(f"[thread {threading.get_ident()}] getting models for {year} {make}")
                models = get_models(year, make)
                print(f"[thread {threading.get_ident()}] found {len(models)} models for {year} {make}: {models}")
                
                # Submit model processing tasks to the thread pool
                model_futures = []
                sliced_models = models[models_start:]
                print(f"[thread {threading.get_ident()}] processing {len(sliced_models)} models for {year} {make} (starting from index {models_start})")
                
                for idx, model in enumerate(sliced_models):
                    if (
                        last
                        and _norm(make) == last_make_norm
                        and _norm(year) == last_year_norm
                        and idx == 0
                        and _norm(model) == last_model_norm
                    ):
                        # Use the clarifier start offset for the first model
                        clar_start = first_model_clar_start if first_model_clar_start is not None else 0
                        print(f"[thread {threading.get_ident()}] submitting model task (RESUME): {year} {make} {model} (clar_start={clar_start})")
                        model_futures.append(executor.submit(_process_year_make_model, make, year, model, clar_start))
                    else:
                        print(f"[thread {threading.get_ident()}] submitting model task: {year} {make} {model}")
                        model_futures.append(executor.submit(_process_year_make_model, make, year, model, None))
                
                # Wait for all model processing to complete for this year/make
                completed_count = 0
                total_models = len(model_futures)
                print(f"[thread {threading.get_ident()}] waiting for {total_models} model tasks to complete for {year} {make}")
                
                for fut in as_completed(model_futures):
                    try:
                        fut.result()
                        completed_count += 1
                        print(f"[thread {threading.get_ident()}] completed model {completed_count}/{total_models} for {year} {make}")
                    except Exception as e:
                        completed_count += 1
                        print(f"[model processing error] {year} {make} (model {completed_count}/{total_models}): {type(e).__name__}: {e}")
                        
                print(f"[thread {threading.get_ident()}] FINISHED processing all {total_models} models for {year} {make}")
                        
            except Exception as e:
                print(f"[_process_year_make error] {year} {make}: {type(e).__name__}: {e}")
                insert_error_log(
                    source="tire_rack",
                    context={"op": "_process_year_make", "make": make, "year": year},
                    message=f"{type(e).__name__}: {e}"
                )

        # Main processing loop - now with flattened threading to avoid deadlock
        all_model_futures = []
        total_combinations = 0
        total_models_estimated = 0
        
        # First pass: collect all year/make combinations and submit model tasks directly
        print("=== PHASE 1: Collecting all year/make combinations ===")
        year_make_combinations = []
        
        for make in makes[makes_start:]:
            print(f"getting years for make: {make}")
            years = get_years(make)
            print(f"found {len(years)} years for make {make}: {years}")
            
            # Determine starting year index only for the matching make
            if last and _norm(make) == last_make_norm:
                # Guard: last year must be present for resume
                if not any(_norm(y) == last_year_norm for y in years):
                    insert_error_log(
                        source="tire_rack",
                        context={"op": "resume_year_check", "last_year": last.year, "make": make, "years_count": len(years)},
                        message="Last year not found in fetched years"
                    )
                    raise DataSplicingError("Resume year not found in years list")
                try:
                    years_start = next(i for i, y in enumerate(years) if _norm(y) == last_year_norm)
                    print(f"RESUME: starting from year index {years_start} for make {make}")
                except StopIteration:
                    years_start = 0
            else:
                years_start = 0
                
            years_to_process = years[years_start:]
            print(f"will process {len(years_to_process)} years for make {make}")
            total_combinations += len(years_to_process)
                
            for year in years_to_process:
                # Determine starting model index only for the matching make/year
                if last and _norm(make) == last_make_norm and _norm(year) == last_year_norm:
                    # use pre-computed model start for the first year
                    models_start = models_start_first_year
                    first_model_clar_start = clar_start_first_model
                    print(f"RESUME: starting from model index {models_start} for {year} {make}")
                    # Reset for subsequent iterations
                    models_start_first_year = 0
                    clar_start_first_model = None
                else:
                    models_start = 0
                    first_model_clar_start = None
                
                year_make_combinations.append((make, year, models_start, first_model_clar_start))
        
        print(f"=== PHASE 2: Getting models and submitting tasks for {len(year_make_combinations)} year/make combinations ===")
        
        # Second pass: get models and submit all model tasks directly to thread pool
        for combo_idx, (make, year, models_start, first_model_clar_start) in enumerate(year_make_combinations):
            print(f"[{combo_idx+1}/{len(year_make_combinations)}] getting models for {year} {make}")
            
            try:
                models = get_models(year, make)
                print(f"[{combo_idx+1}/{len(year_make_combinations)}] found {len(models)} models for {year} {make}: {models}")
                
                sliced_models = models[models_start:]
                print(f"[{combo_idx+1}/{len(year_make_combinations)}] submitting {len(sliced_models)} model tasks for {year} {make} (starting from index {models_start})")
                total_models_estimated += len(sliced_models)
                
                for idx, model in enumerate(sliced_models):
                    if (
                        last
                        and _norm(make) == last_make_norm
                        and _norm(year) == last_year_norm
                        and idx == 0
                        and _norm(model) == last_model_norm
                    ):
                        # Use the clarifier start offset for the first model
                        clar_start = first_model_clar_start if first_model_clar_start is not None else 0
                        print(f"[{combo_idx+1}/{len(year_make_combinations)}] submitting model task (RESUME): {year} {make} {model} (clar_start={clar_start})")
                        future = executor.submit(_process_year_make_model, make, year, model, clar_start)
                        all_model_futures.append((future, make, year, model))
                    else:
                        print(f"[{combo_idx+1}/{len(year_make_combinations)}] submitting model task: {year} {make} {model}")
                        future = executor.submit(_process_year_make_model, make, year, model, None)
                        all_model_futures.append((future, make, year, model))
                        
            except Exception as e:
                print(f"[get_models error] {year} {make}: {type(e).__name__}: {e}")
                insert_error_log(
                    source="tire_rack",
                    context={"op": "get_models_in_main_loop", "make": make, "year": year},
                    message=f"{type(e).__name__}: {e}"
                )
        
        print(f"=== PHASE 3: Processing {len(all_model_futures)} model tasks across {max_workers} threads ===")
        
        # Wait for all model processing to complete
        completed_models = 0
        
        for future, make, year, model in all_model_futures:
            try:
                future.result()
                completed_models += 1
                progress_pct = (completed_models / len(all_model_futures)) * 100
                print(f"[PROGRESS] Completed {completed_models}/{len(all_model_futures)} models ({progress_pct:.1f}%) - Latest: {year} {make} {model}")
            except Exception as e:
                completed_models += 1
                progress_pct = (completed_models / len(all_model_futures)) * 100
                print(f"[model processing error] ({completed_models}/{len(all_model_futures)}, {progress_pct:.1f}%) {year} {make} {model}: {type(e).__name__}: {e}")
        
        print(f"[COMPLETE] Finished processing all {len(all_model_futures)} model tasks!")

if __name__ == "__main__":
    try:
        run_scrape()
    except Exception as e:
        # Top-level fallback: log and re-raise for visibility
        insert_error_log(
            source="tire_rack",
            context={"op": "__main__"},
            message=f"{type(e).__name__}: {e}"
        )
        raise
