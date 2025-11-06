# Robust imports to support both direct script execution and module execution
try:
    from utils.ymm import (
        get_fitment_from_store,
        get_years,
        get_makes,
        get_models,
        get_trims,
        get_drives,
        get_vehicle_info,
        get_phpsessid,
        get_fitment_preferences,
    )
    from utils.db_operations import (
        save_custom_wheel_offset_ymm,
        save_fitment_data_rows,
        mark_custom_wheel_offset_ymm_processed,
        build_bolt_pattern_string,
        update_custom_wheel_offset_ymm,
        delete_fitment_rows_for_ymm,
        upsert_custom_wheel_offset_ymm,
    )
    from utils.lib import (
        get_resume_state,
        slice_years,
        slice_makes,
        slice_models,
        slice_trims,
        slice_drives,
        compute_pending_fitment_prefs,
    )
except Exception:
    from .utils.ymm import (
        get_fitment_from_store,
        get_years,
        get_makes,
        get_models,
        get_trims,
        get_drives,
        get_vehicle_info,
        get_phpsessid,
        get_fitment_preferences,
    )
    from .utils.db_operations import (
        save_custom_wheel_offset_ymm,
        save_fitment_data_rows,
        mark_custom_wheel_offset_ymm_processed,
        build_bolt_pattern_string,
        update_custom_wheel_offset_ymm,
        delete_fitment_rows_for_ymm,
        upsert_custom_wheel_offset_ymm,
    )
    from .utils.lib import (
        get_resume_state,
        slice_years,
        slice_makes,
        slice_models,
        slice_trims,
        slice_drives,
        compute_pending_fitment_prefs,
    )

# Concurrency and process handling imports
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import subprocess
import sys
import os
import json
import time
from pathlib import Path
from config.worker import CUSTOM_WHEEL_OFFSET_WORKERS
from core.errors import HumanVerificationError

# Comprehensive loop for vehicle data
resume_state = get_resume_state()
years = get_years()
if resume_state and resume_state.get("year"):
    years = slice_years(years, resume_state.get("year"))
if not years:
    print("No years found. Exiting.")
else:
    for year in years:
        makes = get_makes(year)
        if resume_state and year == resume_state.get("year"):
            makes = slice_makes(makes, resume_state.get("make"))
        if not makes:
            print(f"No makes found for year {year}. Skipping.")
            continue
        for make in makes:
            models = get_models(year, make)
            if resume_state and year == resume_state.get("year") and make == resume_state.get("make"):
                models = slice_models(models, resume_state.get("model"))
            if not models:
                print(f"No models found for year {year}, make {make}. Skipping.")
                continue
            for model in models:
                trims = get_trims(year, make, model)
                if (
                    resume_state
                    and year == resume_state.get("year")
                    and make == resume_state.get("make")
                    and model == resume_state.get("model")
                ):
                    trims = slice_trims(trims, resume_state.get("trim"))
                if not trims:
                    print(f"No trims found for year {year}, make {make}, model {model}. Skipping.")
                    continue
                for trim in trims:
                    drives = get_drives(year, make, model, trim)
                    if (
                        resume_state
                        and year == resume_state.get("year")
                        and make == resume_state.get("make")
                        and model == resume_state.get("model")
                        and trim == resume_state.get("trim")
                    ):
                        drives = slice_drives(drives, resume_state.get("drive"))
                    if not drives:
                        print(f"No drives found for year {year}, make {make}, model {model}, trim {trim}. Skipping.")
                        continue
                    for drive in drives:
                        vehicle_info = get_vehicle_info(year, make, model, trim, drive)
                        if vehicle_info:
                            vehicle_type = vehicle_info.get("vehicleType")
                            drchassisid = vehicle_info.get("drchassisid")
                            boltpatternMm = vehicle_info.get("boltpatternMm")
                            print(f"Vehicle Type: {vehicle_type}, DRChassisID: {drchassisid}, BoltpatternMm: {boltpatternMm}")

                            # Get PHPSESSID
                            phpsessid = get_phpsessid(vehicle_type, year, make, model, trim, drive, drchassisid)
                            print(f"PHPSESSID for {year} {make} {model} {trim} {drive} {drchassisid}:", phpsessid)

                            if phpsessid:
                                # Get all fitment preferences combinations
                                all_fitment_prefs = get_fitment_preferences(vehicle_type, phpsessid)
                                # Compute deterministic pending list based on DB state (thread-safe resume)
                                fitment_prefs = compute_pending_fitment_prefs(
                                    all_fitment_prefs or [],
                                    resume_state,
                                    year=year,
                                    make=make,
                                    model=model,
                                    trim=trim,
                                    drive=drive,
                                    vehicle_type=vehicle_type,
                                    dr_chassis_id=drchassisid,
                                )

                                # Multithread the final loop: process each fitment preference concurrently
                                abort_event = threading.Event()

                                def handle_hv_and_restart():
                                    try:
                                        # Stop current process tasks and solve CAPTCHA in a separate process
                                        print("[HV] Stopping current scraping and launching CAPTCHA solver...")
                                        repo_root = Path(__file__).resolve().parents[3]
                                        src_dir = repo_root / "src"
                                        solve_path = src_dir / "providers" / "custom_wheel_offset" / "utils" / "solve_captcha.py"
                                        subprocess.run([sys.executable, str(solve_path)], cwd=str(src_dir), check=True)
                                        print("[HV] CAPTCHA solved. Restarting scraper as a new process...")
                                        # Start a new process to resume scraping using module execution from repo root
                                        env = os.environ.copy()
                                        existing_py_path = env.get("PYTHONPATH", "")
                                        if str(src_dir) not in existing_py_path.split(os.pathsep):
                                            env["PYTHONPATH"] = (
                                                f"{str(src_dir)}{os.pathsep}{existing_py_path}" if existing_py_path else str(src_dir)
                                            )
                                        cmd_list = [sys.executable, "-m", "src.providers.custom_wheel_offset.custom_wheel_offset"]
                                        proc = subprocess.Popen(cmd_list, cwd=str(repo_root), env=env)

                                        # Record new PID into both registry files without removing other providers
                                        provider_key = "custom-wheel-offset"
                                        cmd_str = "python -m src.providers.custom_wheel_offset.custom_wheel_offset"
                                        updated_at = time.time()
                                        for registry_path in [repo_root / "data" / "process_registry.json", src_dir / "data" / "process_registry.json"]:
                                            try:
                                                # Load existing registry if present
                                                try:
                                                    with open(registry_path, "r", encoding="utf-8") as f:
                                                        reg = json.load(f)
                                                    if not isinstance(reg, dict):
                                                        reg = {}
                                                except Exception:
                                                    reg = {}
                                                # Update only the custom-wheel-offset entry
                                                reg[provider_key] = {
                                                    "pid": proc.pid,
                                                    "cmd": cmd_str,
                                                    "updated_at": updated_at,
                                                }
                                                # Ensure directory exists and save
                                                registry_path.parent.mkdir(parents=True, exist_ok=True)
                                                with open(registry_path, "w", encoding="utf-8") as f:
                                                    json.dump(reg, f)
                                            except Exception as e:
                                                try:
                                                    print(f"[HV] Failed to update process registry at {registry_path}: {e}")
                                                except Exception:
                                                    pass
                                    finally:
                                        # Exit current process immediately
                                        sys.exit(0)

                                def worker_task(pref: dict, update_existing: bool):
                                    if abort_event.is_set():
                                        return None
                                    params = {
                                        "year": year,
                                        "make": make,
                                        "model": model,
                                        "trim": trim,
                                        "drive": drive,
                                        "suspension": pref["suspension"],
                                        "modification": pref["trimming"],
                                        "rubbing": pref["rubbing"],
                                        "vehicle_type": vehicle_type,
                                        "DRChassisID": drchassisid,
                                    }
                                    fitment_data = get_fitment_from_store(params)  # may raise HumanVerificationError
                                    bolt_pattern = build_bolt_pattern_string(fitment_data, fallback_mm=boltpatternMm)
                                    if update_existing:
                                        ymm_id = resume_state["id"]
                                        update_custom_wheel_offset_ymm(
                                            ymm_id,
                                            suspension=pref.get("suspension"),
                                            modification=pref.get("trimming"),
                                            rubbing=pref.get("rubbing"),
                                            bolt_pattern=bolt_pattern,
                                            processed=0,
                                        )
                                        delete_fitment_rows_for_ymm(ymm_id)
                                        save_fitment_data_rows(ymm_id, fitment_data)
                                        mark_custom_wheel_offset_ymm_processed(ymm_id)
                                    else:
                                        ymm_id, existed = upsert_custom_wheel_offset_ymm(
                                            year=year,
                                            make=make,
                                            model=model,
                                            trim=trim,
                                            drive=drive,
                                            vehicle_type=vehicle_type,
                                            dr_chassis_id=drchassisid,
                                            suspension=pref.get("suspension"),
                                            modification=pref.get("trimming"),
                                            rubbing=pref.get("rubbing"),
                                            bolt_pattern=bolt_pattern,
                                            processed=0,
                                        )
                                        # Replace fitment rows to avoid duplicates/outdated values
                                        delete_fitment_rows_for_ymm(ymm_id)
                                        save_fitment_data_rows(ymm_id, fitment_data)
                                        mark_custom_wheel_offset_ymm_processed(ymm_id)
                                    print(fitment_data)
                                    return True

                                resume_match = (
                                    resume_state
                                    and year == resume_state.get("year")
                                    and make == resume_state.get("make")
                                    and model == resume_state.get("model")
                                    and trim == resume_state.get("trim")
                                    and drive == resume_state.get("drive")
                                )

                                with ThreadPoolExecutor(max_workers=CUSTOM_WHEEL_OFFSET_WORKERS) as executor:
                                    futures = []
                                    for i, pref in enumerate(fitment_prefs):
                                        update_existing = bool(resume_match and i == 0)
                                        futures.append(executor.submit(worker_task, pref, update_existing))

                                    for fut in as_completed(futures):
                                        try:
                                            fut.result()
                                        except HumanVerificationError:
                                            abort_event.set()
                                            # Cancel pending tasks and handle HV
                                            executor.shutdown(wait=False, cancel_futures=True)
                                            handle_hv_and_restart()
                                        except Exception as e:
                                            # Log and continue other futures; do not alter API mechanisms
                                            print(f"[Worker Error] {e}")
                            else:
                                print(f"Skipping suspension, trimming, and rubbing data retrieval due to missing PHPSESSID for {year} {make} {model} {trim} {drive} {drchassisid}.")
                            print("-" * 40, "\n\n")
