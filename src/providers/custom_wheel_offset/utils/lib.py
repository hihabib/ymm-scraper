from typing import List, Dict, Any, Optional

# Robust imports to work whether running as a module or direct script
try:
    from .db_operations import (
        get_last_custom_wheel_offset_ymm,
        list_prefs_status_for_path,
    )
except Exception:
    import sys
    from pathlib import Path
    SRC_DIR = Path(__file__).resolve().parents[3]  # points to .../src
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    from providers.custom_wheel_offset.utils.db_operations import (
        get_last_custom_wheel_offset_ymm,
        list_prefs_status_for_path,
    )


def get_resume_state() -> Optional[Dict[str, Any]]:
    """Return last inserted YMM row as a dict, or None if not found."""
    return get_last_custom_wheel_offset_ymm()


def _slice_inclusive(items: List[str], target: Optional[str]) -> List[str]:
    """Return list sliced starting at the target (inclusive). If target not present or None, return original list."""
    if not target:
        return items
    try:
        idx = items.index(target)
        return items[idx:]
    except ValueError:
        return items


def slice_years(years: List[str], last_year: Optional[str]) -> List[str]:
    return _slice_inclusive(years, last_year)


def slice_makes(makes: List[str], last_make: Optional[str]) -> List[str]:
    return _slice_inclusive(makes, last_make)


def slice_models(models: List[str], last_model: Optional[str]) -> List[str]:
    return _slice_inclusive(models, last_model)


def slice_trims(trims: List[str], last_trim: Optional[str]) -> List[str]:
    return _slice_inclusive(trims, last_trim)


def slice_drives(drives: List[str], last_drive: Optional[str]) -> List[str]:
    return _slice_inclusive(drives, last_drive)


def slice_fitment_prefs(
    prefs: List[Dict[str, Any]],
    last_suspension: Optional[str],
    last_modification: Optional[str],
    last_rubbing: Optional[str],
) -> List[Dict[str, Any]]:
    """Slice fitment preference list starting at the last inserted combination (inclusive)."""
    if not prefs:
        return prefs
    # If any of the keys are None, do not slice to avoid skipping unintended items
    if last_suspension is None or last_modification is None or last_rubbing is None:
        return prefs
    for i, p in enumerate(prefs):
        if (
            p.get("suspension") == last_suspension
            and p.get("trimming") == last_modification
            and p.get("rubbing") == last_rubbing
        ):
            return prefs[i:]
    return prefs


__all__ = [
    "get_resume_state",
    "slice_years",
    "slice_makes",
    "slice_models",
    "slice_trims",
    "slice_drives",
    "slice_fitment_prefs",
]
def slice_trims(trims: List[str], last_trim: Optional[str]) -> List[str]:
    return _slice_inclusive(trims, last_trim)


def slice_drives(drives: List[str], last_drive: Optional[str]) -> List[str]:
    return _slice_inclusive(drives, last_drive)


def slice_fitment_prefs(
    prefs: List[Dict[str, Any]],
    last_suspension: Optional[str],
    last_modification: Optional[str],
    last_rubbing: Optional[str],
) -> List[Dict[str, Any]]:
    """
    Legacy inclusive slice for preferences. Kept for backward compatibility, but
    under threading, prefer compute_pending_fitment_prefs for deterministic resume.
    """
    if not prefs:
        return []
    target = {
        "suspension": str(last_suspension or ""),
        "trimming": str(last_modification or ""),
        "rubbing": str(last_rubbing or ""),
    }
    try:
        idx = next(
            i for i, p in enumerate(prefs)
            if str(p.get("suspension", "")) == target["suspension"]
            and str(p.get("trimming", "")) == target["trimming"]
            and str(p.get("rubbing", "")) == target["rubbing"]
        )
        return prefs[idx:]
    except StopIteration:
        return prefs


def compute_pending_fitment_prefs(
    fitment_prefs: List[Dict[str, Any]],
    resume_state: Optional[Dict[str, Any]],
    *,
    year: str,
    make: str,
    model: str,
    trim: str,
    drive: str,
    vehicle_type: str,
    dr_chassis_id: str,
) -> List[Dict[str, Any]]:
    """
    Build a deterministic pending preference list for a given path:
    - Exclude preferences already persisted with processed=1
    - Include any unprocessed combos (processed=0) to finish them
    - Force-include the last inserted combo (from resume_state) at the front for update
      if present in the same path

    Preserves API order of `fitment_prefs`.
    """
    status = list_prefs_status_for_path(year, make, model, trim, drive, vehicle_type, dr_chassis_id)
    processed_set = status.get("processed", set())
    unprocessed_set = status.get("unprocessed", set())

    def key_for(p: Dict[str, Any]) -> tuple:
        return (
            str(p.get("suspension", "")),
            str(p.get("trimming", "")),
            str(p.get("rubbing", "")),
        )

    # Filter out already processed combos; include unprocessed ones regardless
    pending: List[Dict[str, Any]] = []
    for p in fitment_prefs:
        k = key_for(p)
        if k in processed_set:
            continue
        pending.append(p)

    # Ensure unprocessed combos are present (if not captured above)
    # (they should be, unless API changed options)
    for k in unprocessed_set:
        if not any(key_for(p) == k for p in pending):
            # fabricate entry if API no longer presents the combo; still try to finish it
            pending.insert(0, {
                "suspension": k[0],
                "trimming": k[1],
                "rubbing": k[2],
            })

    # Force-include last inserted combo for update at the front if path matches
    if resume_state and (
        str(resume_state.get("year")) == str(year) and
        str(resume_state.get("make")) == str(make) and
        str(resume_state.get("model")) == str(model) and
        str(resume_state.get("trim")) == str(trim) and
        str(resume_state.get("drive")) == str(drive)
    ):
        rk = (
            str(resume_state.get("suspension", "")),
            str(resume_state.get("modification", "")),
            str(resume_state.get("rubbing", "")),
        )
        if not any(key_for(p) == rk for p in pending):
            # prefer canonical dict from original API list if available
            rp = next((p for p in fitment_prefs if key_for(p) == rk), None)
            rp = rp or {"suspension": rk[0], "trimming": rk[1], "rubbing": rk[2]}
            pending.insert(0, rp)

    return pending