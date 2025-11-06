from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import desc
from datetime import datetime
from db.db import SessionLocal
from core.models import (
    TireRackYMM, 
    ScrapeErrorLog, 
    TireRackTireSize, 
    CustomWheelOffsetYMM, 
    CustomWheelOffsetData,
    DriverRightYMM,
    DriverRightVehicleSpec,
    DriverRightTireOption,
    EbayYMMResult,
    EbayTireSize,
)
import json
import threading
from contextlib import contextmanager

# Thread-local storage for database sessions
_thread_local = threading.local()

@contextmanager
def get_db_session():
    """
    Context manager that provides a database session with proper cleanup.
    Reuses sessions within the same thread to reduce connection overhead.
    """
    if not hasattr(_thread_local, 'session') or _thread_local.session is None:
        _thread_local.session = SessionLocal()
    
    session = _thread_local.session
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    # Note: We don't close the session here to allow reuse within the thread

def close_thread_session():
    """Close the current thread's database session."""
    if hasattr(_thread_local, 'session') and _thread_local.session is not None:
        _thread_local.session.close()
        _thread_local.session = None

# Batch insert functions for CustomWheelOffset operations
def batch_insert_custom_wheel_offset_ymm(ymm_data_list: List[Dict[str, Any]]) -> List[int]:
    """
    Batch insert CustomWheelOffset YMM records.
    
    Args:
        ymm_data_list: List of dicts containing YMM data with keys:
            year, make, model, trim, drive, vehicle_type, dr_chassis_id,
            suspension, modification, rubbing, bolt_pattern
    
    Returns:
        List of inserted record IDs
    """
    if not ymm_data_list:
        return []
    
    inserted_ids = []
    with get_db_session() as session:
        objects_to_add = []
        for data in ymm_data_list:
            obj = CustomWheelOffsetYMM(
                year=data['year'],
                make=data['make'],
                model=data['model'],
                trim=data['trim'],
                drive=data['drive'],
                vehicle_type=data['vehicle_type'],
                dr_chassis_id=data['dr_chassis_id'],
                suspension=data.get('suspension'),
                modification=data.get('modification'),
                rubbing=data.get('rubbing'),
                bolt_pattern=data.get('bolt_pattern')
            )
            objects_to_add.append(obj)
        
        session.add_all(objects_to_add)
        session.flush()  # Flush to get IDs without committing
        
        # Extract IDs while session is still active
        for obj in objects_to_add:
            inserted_ids.append(obj.id)
        
        # Commit the transaction
        session.commit()
    
    return inserted_ids

def batch_insert_custom_wheel_offset_data(data_list: List[Dict[str, Any]]) -> int:
    """
    Batch insert CustomWheelOffset data records.
    
    Args:
        data_list: List of dicts containing data with keys:
            ymm_id, parsed_data (with front/rear position data)
    
    Returns:
        Number of records inserted
    """
    if not data_list:
        return 0
    
    total_inserted = 0
    with get_db_session() as session:
        objects_to_add = []
        
        for item in data_list:
            ymm_id = item['ymm_id']
            parsed_data = item['parsed_data']
            
            for position in ['front', 'rear']:
                if position in parsed_data:
                    pos_data = parsed_data[position]
                    diameter = pos_data.get('diameter', {})
                    width = pos_data.get('width', {})
                    offset = pos_data.get('offset', {})
                    
                    # Ensure all required data is present
                    if (diameter.get('min') is not None and diameter.get('max') is not None and
                        width.get('min') is not None and width.get('max') is not None and
                        offset.get('min') is not None and offset.get('max') is not None):
                        
                        objects_to_add.append(CustomWheelOffsetData(
                            ymm_id=ymm_id,
                            position=position,
                            diameter_min=int(diameter['min']),
                            diameter_max=int(diameter['max']),
                            width_min=str(width['min']),
                            width_max=str(width['max']),
                            offset_min=int(offset['min']),
                            offset_max=int(offset['max'])
                        ))
        
        if objects_to_add:
            session.add_all(objects_to_add)
            session.commit()  # Commit the transaction
            total_inserted = len(objects_to_add)
    
    return total_inserted

def batch_insert_error_logs(error_data_list: List[Dict[str, Any]]) -> List[int]:
    """
    Batch insert error log records.
    
    Args:
        error_data_list: List of dicts containing error data with keys:
            source, context, message
    
    Returns:
        List of inserted record IDs
    """
    if not error_data_list:
        return []
    
    inserted_ids = []
    with get_db_session() as session:
        objects_to_add = []
        for data in error_data_list:
            obj = ScrapeErrorLog(
                source=data['source'],
                context=json.dumps(data['context'], ensure_ascii=False),
                message=data['message']
            )
            objects_to_add.append(obj)
        
        session.add_all(objects_to_add)
        session.flush()  # Flush to get IDs without committing
        
        # Extract IDs while session is still active
        for obj in objects_to_add:
            inserted_ids.append(obj.id)
        
        # Commit the transaction
        session.commit()
    
    return inserted_ids

# Original functions maintained for backward compatibility
def insert_ymm(year: str, make: str, model: str, clarifier: Optional[str] = None) -> int:
    """Insert a YMM row and return its ID."""
    with get_db_session() as session:
        obj = TireRackYMM(year=year, make=make, model=model, clarifier=clarifier)
        session.add(obj)
        session.flush()
        return obj.id

def get_last_ymm() -> Optional[TireRackYMM]:
    """Return the most recently inserted YMM row (by created_at), or None."""
    with get_db_session() as session:
        return (
            session.query(TireRackYMM)
            .order_by(TireRackYMM.created_at.desc(), TireRackYMM.id.desc())
            .first()
        )

def get_last_custom_wheel_offset_ymm() -> Optional[CustomWheelOffsetYMM]:
    """Return the most recently inserted CustomWheelOffset YMM row (by created_at), or None."""
    with get_db_session() as session:
        return (
            session.query(CustomWheelOffsetYMM)
            .order_by(CustomWheelOffsetYMM.created_at.desc(), CustomWheelOffsetYMM.id.desc())
            .first()
        )

def insert_error_log(source: str, context: dict, message: str) -> int:
    """Insert an error log entry and return its ID."""
    with get_db_session() as session:
        obj = ScrapeErrorLog(
            source=source,
            context=json.dumps(context, ensure_ascii=False),
            message=message,
        )
        session.add(obj)
        session.flush()
        return obj.id

def list_ymm(limit: int | None = None, offset: int = 0) -> list[TireRackYMM]:
    """List YMM rows ordered by id ascending."""
    with get_db_session() as session:
        q = session.query(TireRackYMM).order_by(TireRackYMM.id.asc())
        if offset:
            q = q.offset(offset)
        if limit is not None:
            q = q.limit(limit)
        return list(q.all())

def insert_custom_wheel_offset_ymm(year: str, make: str, model: str, trim: str, drive: str, 
                                   vehicle_type: str, dr_chassis_id: str, suspension: str = None, 
                                   modification: str = None, rubbing: str = None, bolt_pattern: str = None) -> int:
    """Insert a CustomWheelOffset YMM row and return its ID."""
    with get_db_session() as session:
        obj = CustomWheelOffsetYMM(
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
            bolt_pattern=bolt_pattern
        )
        session.add(obj)
        session.flush()
        return obj.id

def insert_custom_wheel_offset_data(ymm_id: int, parsed_data: dict) -> int:
    """
    Insert CustomWheelOffset data for both front and rear positions.
    
    Args:
        ymm_id: The ID from custom_wheel_offset_ymm table
        parsed_data: Dict with structure like:
            {
                'front': {'diameter': {'min': 19, 'max': 24}, 'width': {'min': 8.5, 'max': 10.0}, 'offset': {'min': 35, 'max': 60}},
                'rear': {'diameter': {'min': 19, 'max': 24}, 'width': {'min': 8.5, 'max': 10.0}, 'offset': {'min': 35, 'max': 60}}
            }
    
    Returns:
        Number of records inserted (typically 2: front and rear)
    """
    total = 0
    with get_db_session() as session:
        objects_to_add = []
        
        for position in ['front', 'rear']:
            if position in parsed_data:
                pos_data = parsed_data[position]
                diameter = pos_data.get('diameter', {})
                width = pos_data.get('width', {})
                offset = pos_data.get('offset', {})
                
                # Ensure all required data is present
                if (diameter.get('min') is not None and diameter.get('max') is not None and
                    width.get('min') is not None and width.get('max') is not None and
                    offset.get('min') is not None and offset.get('max') is not None):
                    
                    objects_to_add.append(CustomWheelOffsetData(
                        ymm_id=ymm_id,
                        position=position,
                        diameter_min=int(diameter['min']),
                        diameter_max=int(diameter['max']),
                        width_min=str(width['min']),
                        width_max=str(width['max']),
                        offset_min=int(offset['min']),
                        offset_max=int(offset['max'])
                    ))
        
        if objects_to_add:
            session.add_all(objects_to_add)
            total = len(objects_to_add)
    
    return total

# eBay helper functions
def insert_ebay_ymm_result(
    year: str | None = None,
    make: str | None = None,
    model: str | None = None,
    trim: str | None = None,
    submodel: str | None = None,
    engine: str | None = None,
    engine_liter_display: str | None = None,
) -> int:
    """Insert an eBay YMM result and return its ID."""
    with get_db_session() as session:
        obj = EbayYMMResult(
            year=year,
            make=make,
            model=model,
            trim=trim,
            submodel=submodel,
            engine=engine,
            engine_liter_display=engine_liter_display,
        )
        session.add(obj)
        session.flush()
        return obj.id

def batch_insert_ebay_tire_sizes(
    ymm_id: int,
    tire_sizes: List[str],
    convenience_fields: Dict[str, Optional[str]]
) -> int:
    """Batch insert eBay tire sizes tied to a YMM result.

    Args:
        ymm_id: Foreign key id from `ebay_ymm_results`.
        tire_sizes: List of tire size strings like "255/55-19".
        convenience_fields: Dict with keys year/make/model/trim/submodel/engine.

    Returns:
        Number of records inserted.
    """
    if not tire_sizes:
        return 0
    with get_db_session() as session:
        objects = []
        for size in tire_sizes:
            objects.append(EbayTireSize(
                ymm_id=ymm_id,
                year=convenience_fields.get('year'),
                make=convenience_fields.get('make'),
                model=convenience_fields.get('model'),
                trim=convenience_fields.get('trim'),
                submodel=convenience_fields.get('submodel'),
                engine=convenience_fields.get('engine'),
                tire_size=size,
            ))
        session.add_all(objects)
        return len(objects)

def get_last_ebay_ymm_result() -> Optional[EbayYMMResult]:
    """Return the most recently inserted eBay YMM result (by created_at), or None."""
    with get_db_session() as session:
        return (
            session.query(EbayYMMResult)
            .order_by(EbayYMMResult.created_at.desc(), EbayYMMResult.id.desc())
            .first()
        )


def find_ebay_ymm_result(
    year: Optional[str],
    make: Optional[str],
    model: Optional[str],
    trim: Optional[str],
    submodel: Optional[str],
    engine: Optional[str],
) -> Optional[EbayYMMResult]:
    """Find existing EbayYMMResult by key convenience fields."""
    with get_db_session() as session:
        q = session.query(EbayYMMResult)
        if year is not None:
            q = q.filter(EbayYMMResult.year == year)
        else:
            q = q.filter(EbayYMMResult.year.is_(None))
        if make is not None:
            q = q.filter(EbayYMMResult.make == make)
        else:
            q = q.filter(EbayYMMResult.make.is_(None))
        if model is not None:
            q = q.filter(EbayYMMResult.model == model)
        else:
            q = q.filter(EbayYMMResult.model.is_(None))
        if trim is not None:
            q = q.filter(EbayYMMResult.trim == trim)
        else:
            q = q.filter(EbayYMMResult.trim.is_(None))
        if submodel is not None:
            q = q.filter(EbayYMMResult.submodel == submodel)
        else:
            q = q.filter(EbayYMMResult.submodel.is_(None))
        if engine is not None:
            q = q.filter(EbayYMMResult.engine == engine)
        else:
            q = q.filter(EbayYMMResult.engine.is_(None))
        return q.first()


def get_tire_sizes_for_ymm(ymm_id: int) -> List[str]:
    """Return existing tire_size strings for the given ymm_id."""
    with get_db_session() as session:
        rows = (
            session.query(EbayTireSize.tire_size)
            .filter(EbayTireSize.ymm_id == ymm_id)
            .all()
        )
        return [r[0] for r in rows]