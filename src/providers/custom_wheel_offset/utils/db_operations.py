"""
Database operations for Custom Wheel Offset provider.

Provides helpers to:
- Insert YMM records into `custom_wheel_offset_ymm`.
- Insert fitment data rows into `custom_wheel_offset_data`.
- Mark YMM records as processed.

All functions manage their own SQLAlchemy session lifecycle.
"""

from typing import Optional, Dict, Any
from typing import Tuple, Set

from sqlalchemy.orm import Session

# Robust imports to work whether running as a module or direct script
try:
    from db.db import SessionLocal
    from core.models import CustomWheelOffsetYMM, CustomWheelOffsetData
except ImportError:
    import sys
    from pathlib import Path
    SRC_DIR = Path(__file__).resolve().parents[3]  # points to .../src
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    from db.db import SessionLocal
    from core.models import CustomWheelOffsetYMM, CustomWheelOffsetData


def save_custom_wheel_offset_ymm(
    year: Optional[str],
    make: Optional[str],
    model: Optional[str],
    trim: Optional[str],
    drive: Optional[str],
    vehicle_type: Optional[str],
    dr_chassis_id: Optional[str],
    suspension: Optional[str],
    modification: Optional[str],
    rubbing: Optional[str],
    bolt_pattern: Optional[str],
    processed: int = 0,
) -> int:
    """
    Insert a record into `custom_wheel_offset_ymm` and return its ID.

    All fields are nullable to accommodate incomplete data.
    The `processed` flag defaults to 0.
    """
    session: Session = SessionLocal()
    try:
        ymm = CustomWheelOffsetYMM(
            year=year,
            make=make,
            model=model,
            trim=trim,
            drive=drive,
            vehicle_type=vehicle_type,
            dr_chassis_id=dr_chassis_id,
            suspension=suspension,
            modification=modification,
            rubbing=rubbing,
            bolt_pattern=bolt_pattern,
            processed=processed,
        )
        session.add(ymm)
        session.commit()
        session.refresh(ymm)
        print(f"Inserted YMM record with ID {ymm.id}")
        return ymm.id
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def save_fitment_data_rows(
    ymm_id: int,
    fitment_data: Optional[Dict[str, Any]],
) -> None:
    """
    Insert fitment rows into `custom_wheel_offset_data` for positions present
    in `fitment_data`.

    Expected `fitment_data` structure:
    {
      "front": {"diameter": {"min": str, "max": str}, "width": {...}, "offset": {...}},
      "rear":  {"diameter": {"min": str, "max": str}, "width": {...}, "offset": {...}}
    }

    Each value may be None; rows will be inserted for positions present.
    """
    session: Session = SessionLocal()
    try:
        for position in ("front", "rear"):
            spec = (fitment_data or {}).get(position)
            if spec is None:
                # If a position is not present, skip inserting a row for it.
                continue

            diameter = spec.get("diameter") or {}
            width = spec.get("width") or {}
            offset = spec.get("offset") or {}

            row = CustomWheelOffsetData(
                ymm_id=ymm_id,
                position=position,
                diameter_min=diameter.get("min"),
                diameter_max=diameter.get("max"),
                width_min=width.get("min"),
                width_max=width.get("max"),
                offset_min=offset.get("min"),
                offset_max=offset.get("max"),
            )
            session.add(row)

        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def mark_custom_wheel_offset_ymm_processed(ymm_id: int) -> None:
    """
    Set `processed = 1` for the given YMM record.
    """
    session: Session = SessionLocal()
    try:
        session.query(CustomWheelOffsetYMM).filter(CustomWheelOffsetYMM.id == ymm_id).update({"processed": 1})
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_last_custom_wheel_offset_ymm() -> Optional[Dict[str, Any]]:
    """Fetch the last inserted YMM row (by highest ID). Returns a dict or None."""
    session: Session = SessionLocal()
    try:
        last = session.query(CustomWheelOffsetYMM).order_by(CustomWheelOffsetYMM.id.desc()).first()
        if not last:
            return None
        return {
            "id": last.id,
            "year": last.year,
            "make": last.make,
            "model": last.model,
            "trim": last.trim,
            "drive": last.drive,
            "vehicle_type": last.vehicle_type,
            "dr_chassis_id": last.dr_chassis_id,
            "suspension": last.suspension,
            "modification": last.modification,
            "rubbing": last.rubbing,
            "bolt_pattern": last.bolt_pattern,
            "processed": last.processed,
        }
    except Exception:
        raise
    finally:
        session.close()


def delete_fitment_rows_for_ymm(ymm_id: int) -> None:
    """Delete all fitment rows for the given YMM ID."""
    session: Session = SessionLocal()
    try:
        session.query(CustomWheelOffsetData).filter(CustomWheelOffsetData.ymm_id == ymm_id).delete(synchronize_session=False)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def update_custom_wheel_offset_ymm(ymm_id: int, **fields: Any) -> None:
    """Update fields on the YMM record (e.g., bolt_pattern, suspension, modification, rubbing, processed)."""
    if not fields:
        return
    session: Session = SessionLocal()
    try:
        session.query(CustomWheelOffsetYMM).filter(CustomWheelOffsetYMM.id == ymm_id).update(fields, synchronize_session=False)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def list_prefs_status_for_path(
    year: Optional[str],
    make: Optional[str],
    model: Optional[str],
    trim: Optional[str],
    drive: Optional[str],
    vehicle_type: Optional[str],
    dr_chassis_id: Optional[str],
) -> Dict[str, Set[Tuple[str, str, str]]]:
    """
    Return preference status sets for a given path.
    - processed: combos where the YMM row has processed=1
    - unprocessed: combos where processed=0 (likely incomplete due to interruption)
    Combos are tuples: (suspension, modification, rubbing).
    """
    session: Session = SessionLocal()
    try:
        q = session.query(CustomWheelOffsetYMM).filter(
            CustomWheelOffsetYMM.year == year,
            CustomWheelOffsetYMM.make == make,
            CustomWheelOffsetYMM.model == model,
            CustomWheelOffsetYMM.trim == trim,
            CustomWheelOffsetYMM.drive == drive,
            CustomWheelOffsetYMM.vehicle_type == vehicle_type,
            CustomWheelOffsetYMM.dr_chassis_id == dr_chassis_id,
        )
        processed: Set[Tuple[str, str, str]] = set()
        unprocessed: Set[Tuple[str, str, str]] = set()
        for row in q.all():
            combo = (
                str(row.suspension or ""),
                str(row.modification or ""),
                str(row.rubbing or ""),
            )
            if int(row.processed or 0) == 1:
                processed.add(combo)
            else:
                unprocessed.add(combo)
        return {"processed": processed, "unprocessed": unprocessed}
    except Exception:
        raise
    finally:
        session.close()


def upsert_custom_wheel_offset_ymm(
    year: Optional[str],
    make: Optional[str],
    model: Optional[str],
    trim: Optional[str],
    drive: Optional[str],
    vehicle_type: Optional[str],
    dr_chassis_id: Optional[str],
    suspension: Optional[str],
    modification: Optional[str],
    rubbing: Optional[str],
    bolt_pattern: Optional[str],
    processed: int = 0,
) -> Tuple[int, bool]:
    """
    Insert or update a YMM record based on the unique combo key.
    Returns (ymm_id, existed) where existed=True if an existing row was updated.

    Unique combo key fields:
      year, make, model, trim, drive, vehicle_type, dr_chassis_id, suspension, modification, rubbing
    """
    session: Session = SessionLocal()
    try:
        existing = (
            session.query(CustomWheelOffsetYMM)
            .filter(
                CustomWheelOffsetYMM.year == year,
                CustomWheelOffsetYMM.make == make,
                CustomWheelOffsetYMM.model == model,
                CustomWheelOffsetYMM.trim == trim,
                CustomWheelOffsetYMM.drive == drive,
                CustomWheelOffsetYMM.vehicle_type == vehicle_type,
                CustomWheelOffsetYMM.dr_chassis_id == dr_chassis_id,
                CustomWheelOffsetYMM.suspension == suspension,
                CustomWheelOffsetYMM.modification == modification,
                CustomWheelOffsetYMM.rubbing == rubbing,
            )
            .first()
        )
        if existing:
            # Update fields; keep processed as provided (typically reset to 0 before rewriting rows)
            existing.bolt_pattern = bolt_pattern
            existing.processed = processed
            session.commit()
            session.refresh(existing)
            return existing.id, True
        else:
            ymm = CustomWheelOffsetYMM(
                year=year,
                make=make,
                model=model,
                trim=trim,
                drive=drive,
                vehicle_type=vehicle_type,
                dr_chassis_id=dr_chassis_id,
                suspension=suspension,
                modification=modification,
                rubbing=rubbing,
                bolt_pattern=bolt_pattern,
                processed=processed,
            )
            session.add(ymm)
            session.commit()
            session.refresh(ymm)
            return ymm.id, False
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

def build_bolt_pattern_string(
    fitment_data: Optional[Dict[str, Any]],
    fallback_mm: Optional[str] = None,
) -> Optional[str]:
    """
    Construct a bolt pattern string like "5x120mm (5x4.72")" using values found
    under `fitment_data[front|rear]['boltPattern']['mm'|'inch']`.

    If fitment_data is missing bolt pattern details, fallback to `fallback_mm`.
    Returns None if both mm and inch are missing.
    """
    mm: Optional[str] = None
    inch: Optional[str] = None

    data = fitment_data or {}
    for pos in ("front", "rear"):
        spec = data.get(pos) or {}
        bp = spec.get("boltPattern") or {}
        mm = mm or bp.get("mm")
        inch = inch or bp.get("inch")
        if mm and inch:
            break

    if not mm and fallback_mm:
        mm = fallback_mm

    if mm and inch:
        return f"{mm} ({inch})"
    if mm:
        return mm
    if inch:
        return inch
    return None


__all__ = [
    "save_custom_wheel_offset_ymm",
    "save_fitment_data_rows",
    "mark_custom_wheel_offset_ymm_processed",
    "build_bolt_pattern_string",
    "get_last_custom_wheel_offset_ymm",
    "delete_fitment_rows_for_ymm",
    "update_custom_wheel_offset_ymm",
]