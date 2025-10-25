from pathlib import Path
import json
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional
from sqlalchemy.orm import Session

# Ensure `src` is on sys.path for absolute imports
SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from .key_utils import make_full_key

# Import using absolute path from the src directory
sys.path.insert(0, str(SRC_DIR))
import db.db as db_module
import core.models as models_module

SessionLocal = db_module.SessionLocal
CustomWheelOffsetYMM = models_module.CustomWheelOffsetYMM

CACHE_PATH = Path(__file__).resolve().parents[3] / "data" / "custom_wheel_offset_combinations_cache.json"
FULL_CACHE_PATH = Path(__file__).resolve().parents[3] / "data" / "custom_wheel_offset_full_combinations_cache.json"


def load_cache() -> Dict:
    """Load year/make cache JSON if present, else initialize a new structure."""
    if CACHE_PATH.exists():
        try:
            with CACHE_PATH.open("r", encoding="utf-8") as f:
                obj = json.load(f)
            combos = obj.get("combinations", [])
            if not isinstance(combos, list):
                combos = []
            obj["combinations"] = combos
            obj["total_combinations"] = len(combos)
            obj.setdefault("created_at", datetime.now(timezone.utc).isoformat())
            return obj
        except Exception:
            pass
    return {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "total_combinations": 0,
        "combinations": [],
    }


def save_cache(cache: Dict) -> None:
    """Persist the year/make cache to disk."""
    cache["total_combinations"] = len(cache.get("combinations", []))
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CACHE_PATH.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def load_full_cache_from_db() -> Dict:
    """Load full combinations from database and return in cache format."""
    with SessionLocal() as session:
        records = session.query(CustomWheelOffsetYMM).all()
        
        combinations = {}
        for record in records:
            key = make_full_key(
                record.year, 
                record.make, 
                record.model, 
                record.trim, 
                record.drive,
                record.vehicle_type,
                record.bolt_pattern or "",
                record.dr_chassis_id
            )
            combinations[key] = {
                "year": record.year,
                "make": record.make,
                "model": record.model,
                "trim": record.trim,
                "drive": record.drive,
                "vehicleType": record.vehicle_type,
                "boltpattern": record.bolt_pattern or "",
                "drchassisid": record.dr_chassis_id,
                "processed": bool(record.processed),
                "suspension": record.suspension,
                "modification": record.modification,
                "rubbing": record.rubbing,
                "id": record.id,
                "created_at": record.created_at.isoformat() if record.created_at else None
            }
        
        return {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "total_combinations": len(combinations),
            "combinations": combinations,
        }


def save_combination_to_db(combination_data: Dict) -> int:
    """Save a single combination to the database and return the ID."""
    with SessionLocal() as session:
        record = CustomWheelOffsetYMM(
            year=combination_data["year"],
            make=combination_data["make"],
            model=combination_data["model"],
            trim=combination_data["trim"],
            drive=combination_data["drive"],
            vehicle_type=combination_data.get("vehicleType", ""),
            dr_chassis_id=combination_data.get("drchassisid", ""),
            suspension=combination_data.get("suspension"),
            modification=combination_data.get("modification"),
            rubbing=combination_data.get("rubbing"),
            bolt_pattern=combination_data.get("boltpattern"),
            processed=int(combination_data.get("processed", False))
        )
        session.add(record)
        session.commit()
        session.refresh(record)
        return record.id


def update_combination_processed_status(combination_key: str, processed: bool = True) -> bool:
    """Update the processed status of a combination in the database."""
    # Parse the key to extract the combination details
    # This is a simplified approach - in practice you might want to store the key directly
    with SessionLocal() as session:
        # For now, we'll need to find the record by matching all fields
        # This could be optimized by storing the key directly in the database
        records = session.query(CustomWheelOffsetYMM).filter(
            CustomWheelOffsetYMM.processed == (0 if not processed else 1)
        ).all()
        
        for record in records:
            record_key = make_full_key(
                record.year, 
                record.make, 
                record.model, 
                record.trim, 
                record.drive,
                record.vehicle_type,
                record.bolt_pattern or "",
                record.dr_chassis_id
            )
            if record_key == combination_key:
                record.processed = int(processed)
                session.commit()
                return True
        
        return False


def check_combination_exists_in_db(year: str, make: str, model: str, trim: str, drive: str, 
                                   vehicle_type: str = "", dr_chassis_id: str = "", 
                                   suspension: str = None, modification: str = None, rubbing: str = None) -> bool:
    """Check if a combination already exists in the database."""
    with SessionLocal() as session:
        query = session.query(CustomWheelOffsetYMM).filter(
            CustomWheelOffsetYMM.year == year,
            CustomWheelOffsetYMM.make == make,
            CustomWheelOffsetYMM.model == model,
            CustomWheelOffsetYMM.trim == trim,
            CustomWheelOffsetYMM.drive == drive,
            CustomWheelOffsetYMM.vehicle_type == vehicle_type,
            CustomWheelOffsetYMM.dr_chassis_id == dr_chassis_id
        )
        
        # Add preference filters if provided
        if suspension is not None:
            query = query.filter(CustomWheelOffsetYMM.suspension == suspension)
        if modification is not None:
            query = query.filter(CustomWheelOffsetYMM.modification == modification)
        if rubbing is not None:
            query = query.filter(CustomWheelOffsetYMM.rubbing == rubbing)
            
        existing = query.first()
        return existing is not None


# Legacy functions for backward compatibility
def load_full_cache() -> Dict:
    """Load the full combinations cache; supports legacy list format and converts to keyed-object."""
    if FULL_CACHE_PATH.exists():
        try:
            with FULL_CACHE_PATH.open("r", encoding="utf-8") as f:
                obj = json.load(f)
            combos = obj.get("combinations", {})
            if isinstance(combos, list):
                mapped = {}
                for item in combos:
                    year = item.get("year")
                    make = item.get("make")
                    model = item.get("model")
                    trim = item.get("trim")
                    drive = item.get("drive")
                    vehicle_type = item.get("vehicleType", "")
                    boltpattern = item.get("boltpattern", "")
                    drchassisid = item.get("drchassisid", "")
                    if year and make and model and trim and drive:
                        key = make_full_key(year, make, model, trim, drive, vehicle_type, boltpattern, drchassisid)
                        mapped[key] = {
                            "year": year,
                            "make": make,
                            "model": model,
                            "trim": trim,
                            "drive": drive,
                            "vehicleType": vehicle_type,
                            "boltpattern": boltpattern,
                            "drchassisid": drchassisid,
                            "processed": bool(item.get("processed", False)),
                        }
                combos = mapped
            elif not isinstance(combos, dict):
                combos = {}
            obj["combinations"] = combos
            obj.setdefault("created_at", datetime.now(timezone.utc).isoformat())
            obj["total_combinations"] = len(combos)
            return obj
        except Exception:
            pass
    return {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "total_combinations": 0,
        "combinations": {},
    }


def save_full_cache(cache: Dict) -> None:
    """Persist full combinations cache as keyed object."""
    combos = cache.get("combinations", {})
    if not isinstance(combos, dict):
        combos = {}
        cache["combinations"] = combos
    cache["total_combinations"] = len(combos)
    FULL_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with FULL_CACHE_PATH.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)