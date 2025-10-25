#!/usr/bin/env python3
"""
Optimized cache operations for Custom Wheel Offset scraper.
Implements database connection pooling and batch operations for better multithreading performance.
"""

from pathlib import Path
import json
import sys
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager

# Calculate the path to the src directory
SRC_DIR = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from providers.custom_wheel_offset.key_utils import make_full_key
from providers.custom_wheel_offset.logging_config import init_module_logger

logger = init_module_logger(__name__)

# Import database and model components
from db.db import engine
from core.models import CustomWheelOffsetYMM

# Thread-local storage for database sessions
_thread_local = threading.local()

class OptimizedDatabaseManager:
    """Optimized database manager with connection pooling and batch operations."""
    
    def __init__(self):
        self._engine = None
        self._session_factory = None
        self._lock = threading.Lock()
        self._batch_cache = {}
        self._batch_lock = threading.Lock()
        self._batch_size = 100
        
    def _initialize_engine(self):
        """Initialize the database engine with connection pooling."""
        if self._engine is None:
            with self._lock:
                if self._engine is None:
                    # Use the original engine's URL directly instead of converting to string
                    # to preserve the password (str() masks it with ***)
                    database_url = engine.url
                    
                    # Create optimized engine with connection pooling
                    self._engine = create_engine(
                        database_url,
                        poolclass=QueuePool,
                        pool_size=20,  # Base pool size
                        max_overflow=30,  # Additional connections when needed
                        pool_pre_ping=True,  # Validate connections
                        pool_recycle=3600,  # Recycle connections every hour
                        echo=False
                    )
                    
                    from sqlalchemy.orm import sessionmaker
                    self._session_factory = sessionmaker(bind=self._engine)
                    logger.info("[OptimizedDB] Database engine initialized with connection pooling")
    
    @contextmanager
    def get_session(self):
        """Get a database session with proper cleanup and connection pooling."""
        self._initialize_engine()
        
        # Use thread-local session for better performance
        if not hasattr(_thread_local, 'session') or _thread_local.session is None:
            _thread_local.session = self._session_factory()
        
        session = _thread_local.session
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        # Note: We don't close the session here to allow reuse within the thread
    
    def close_thread_session(self):
        """Close the thread-local session."""
        if hasattr(_thread_local, 'session') and _thread_local.session is not None:
            _thread_local.session.close()
            _thread_local.session = None
    
    def batch_check_combinations_exist(self, combinations: List[Tuple]) -> Set[Tuple]:
        """Batch check if combinations exist in database."""
        if not combinations:
            return set()
        
        existing = set()
        
        with self.get_session() as session:
            # Build a single query to check all combinations
            filters = []
            for year, make, model, trim, drive, vehicle_type, dr_chassis_id, suspension, modification, rubbing in combinations:
                filter_conditions = [
                    CustomWheelOffsetYMM.year == year,
                    CustomWheelOffsetYMM.make == make,
                    CustomWheelOffsetYMM.model == model,
                    CustomWheelOffsetYMM.trim == trim,
                    CustomWheelOffsetYMM.drive == drive,
                    CustomWheelOffsetYMM.vehicle_type == (vehicle_type if vehicle_type else ""),
                    CustomWheelOffsetYMM.dr_chassis_id == (dr_chassis_id if dr_chassis_id else "")
                ]
                
                # Add preference filters if provided
                if suspension is not None:
                    filter_conditions.append(CustomWheelOffsetYMM.suspension == suspension)
                if modification is not None:
                    filter_conditions.append(CustomWheelOffsetYMM.modification == modification)
                if rubbing is not None:
                    filter_conditions.append(CustomWheelOffsetYMM.rubbing == rubbing)
                
                from sqlalchemy import and_
                filters.append(and_(*filter_conditions))
            
            if filters:
                from sqlalchemy import or_
                query = session.query(
                    CustomWheelOffsetYMM.year,
                    CustomWheelOffsetYMM.make,
                    CustomWheelOffsetYMM.model,
                    CustomWheelOffsetYMM.trim,
                    CustomWheelOffsetYMM.drive,
                    CustomWheelOffsetYMM.vehicle_type,
                    CustomWheelOffsetYMM.dr_chassis_id,
                    CustomWheelOffsetYMM.suspension,
                    CustomWheelOffsetYMM.modification,
                    CustomWheelOffsetYMM.rubbing
                ).filter(or_(*filters))
                
                results = query.all()
                for result in results:
                    existing.add((
                        result.year, result.make, result.model, result.trim, result.drive,
                        result.vehicle_type or "", result.dr_chassis_id or "",
                        result.suspension, result.modification, result.rubbing
                    ))
        
        logger.info(f"[OptimizedDB] Batch checked {len(combinations)} combinations, found {len(existing)} existing")
        return existing
    
    def batch_save_combinations(self, combinations_data: List[Dict]) -> List[int]:
        """Batch save combinations to database."""
        if not combinations_data:
            return []
        
        saved_ids = []
        
        with self.get_session() as session:
            records = []
            for combination_data in combinations_data:
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
                records.append(record)
            
            # Batch insert
            session.add_all(records)
            session.commit()
            
            # Get IDs
            for record in records:
                session.refresh(record)
                saved_ids.append(record.id)
        
        logger.info(f"[OptimizedDB] Batch saved {len(combinations_data)} combinations")
        return saved_ids
    
    def load_full_cache_optimized(self) -> Dict:
        """Load full combinations from database with optimized query."""
        with self.get_session() as session:
            # Use a single query to get all records
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
            
            logger.info(f"[OptimizedDB] Loaded {len(combinations)} combinations from database")
            return {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "total_combinations": len(combinations),
                "combinations": combinations,
            }

# Global optimized database manager
optimized_db_manager = OptimizedDatabaseManager()

# Optimized functions for backward compatibility
def load_full_cache_from_db_optimized() -> Dict:
    """Load full combinations from database using optimized operations."""
    return optimized_db_manager.load_full_cache_optimized()

def batch_check_combinations_exist_optimized(combinations: List[Tuple]) -> Set[Tuple]:
    """Batch check if combinations exist in database."""
    return optimized_db_manager.batch_check_combinations_exist(combinations)

def batch_save_combinations_optimized(combinations_data: List[Dict]) -> List[int]:
    """Batch save combinations to database."""
    return optimized_db_manager.batch_save_combinations(combinations_data)

def check_combination_exists_in_db_optimized(year: str, make: str, model: str, trim: str, drive: str, 
                                           vehicle_type: str = "", dr_chassis_id: str = "", 
                                           suspension: str = None, modification: str = None, rubbing: str = None) -> bool:
    """Optimized single combination existence check."""
    combinations = [(year, make, model, trim, drive, vehicle_type, dr_chassis_id, suspension, modification, rubbing)]
    existing = batch_check_combinations_exist_optimized(combinations)
    return len(existing) > 0

def save_combination_to_db_optimized(combination_data: Dict) -> int:
    """Optimized single combination save."""
    ids = batch_save_combinations_optimized([combination_data])
    return ids[0] if ids else None