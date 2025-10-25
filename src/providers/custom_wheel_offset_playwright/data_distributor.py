"""
Data Distribution and URL Building Module for Custom Wheel Offset Playwright Provider.
Handles reading unprocessed records from database, distributing them among browser instances,
and building dynamic URLs for each record.
"""

import logging
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlencode, quote_plus
from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
import sys
from pathlib import Path

# Add the src directory to the path for imports
src_path = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(src_path))

try:
    from db.db import engine
    from core.models import CustomWheelOffsetYMM
except ImportError:
    # Fallback for when running from different contexts
    import os
    import sys
    
    # Get the absolute path to the src directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.join(current_dir, '..', '..', '..')
    src_dir = os.path.normpath(src_dir)
    
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    
    from src.db.db import engine
    from src.core.models import CustomWheelOffsetYMM

logger = logging.getLogger(__name__)


class DataDistributor:
    """Handles data distribution and URL building for browser instances."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    @contextmanager
    def get_db_session(self):
        """Get a database session with proper cleanup."""
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def get_unprocessed_records(self) -> List[CustomWheelOffsetYMM]:
        """
        Read all unprocessed records from the database.
        Returns list of CustomWheelOffsetYMM objects where processed=False (stored as 0).
        """
        with self.get_db_session() as session:
            records = session.query(CustomWheelOffsetYMM).filter(
                CustomWheelOffsetYMM.processed == 0  # Use 0 instead of False since it's stored as Integer
            ).all()
            
            # Detach from session to avoid lazy loading issues
            for record in records:
                session.expunge(record)
            
            self.logger.info(f"Found {len(records)} unprocessed records in database")
            return records
    
    def distribute_records(self, records: List[CustomWheelOffsetYMM], max_workers: int = 5) -> Dict[int, List[CustomWheelOffsetYMM]]:
        """
        Distribute records evenly among browser instances.
        
        Args:
            records: List of unprocessed records
            max_workers: Maximum number of browser instances (default 5)
        
        Returns:
            Dictionary mapping worker_id to list of assigned records
        """
        if not records:
            self.logger.info("No unprocessed records found - no browser instances will be started")
            return {}
        
        num_records = len(records)
        # Don't create more workers than we have records
        num_workers = min(max_workers, num_records)
        
        # Calculate base records per worker and remainder
        base_records_per_worker = num_records // num_workers
        remainder = num_records % num_workers
        
        distribution = {}
        record_index = 0
        
        for worker_id in range(num_workers):
            # Calculate how many records this worker gets
            records_for_worker = base_records_per_worker
            if worker_id == num_workers - 1:  # Last worker gets the remainder
                records_for_worker += remainder
            
            # Assign records to this worker
            worker_records = records[record_index:record_index + records_for_worker]
            distribution[worker_id] = worker_records
            record_index += records_for_worker
            
            record_ids = [record.id for record in worker_records]
            self.logger.info(f"Worker {worker_id} assigned {len(worker_records)} records: IDs {record_ids}")
        
        return distribution
    
    def build_url_from_record(self, record: CustomWheelOffsetYMM) -> str:
        """
        Build a dynamic URL from a database record.
        
        Args:
            record: CustomWheelOffsetYMM database record
        
        Returns:
            Complete URL string with proper encoding
        """
        base_url = "https://www.customwheeloffset.com/store/wheels"
        
        # Required parameters
        params = {
            'year': str(record.year),
            'make': record.make,
            'model': record.model,
            'trim': record.trim,
            'drive': record.drive,
            'vehicle_type': record.vehicle_type,
            'DRChassisID': str(record.dr_chassis_id)
        }
        
        # Optional parameters - only add if they exist and are not None/empty
        optional_params = {
            'suspension': record.suspension,
            'modification': record.modification,
            'rubbing': record.rubbing
        }
        
        for key, value in optional_params.items():
            if value is not None and str(value).strip():
                params[key] = str(value)
        
        # Build query string with proper URL encoding
        query_string = urlencode(params, quote_via=quote_plus)
        full_url = f"{base_url}?{query_string}"
        
        self.logger.debug(f"Built URL for record {record.id}: {full_url}")
        return full_url
    
    def get_worker_assignments(self, max_workers: int = 5) -> Dict[int, List[Tuple[CustomWheelOffsetYMM, str]]]:
        """
        Get complete worker assignments with records and their corresponding URLs.
        
        Args:
            max_workers: Maximum number of browser instances
        
        Returns:
            Dictionary mapping worker_id to list of (record, url) tuples
        """
        # Step 1: Get unprocessed records
        records = self.get_unprocessed_records()
        
        if not records:
            return {}
        
        # Step 2: Distribute records among workers
        distribution = self.distribute_records(records, max_workers)
        
        # Step 3: Build URLs for each record
        assignments = {}
        for worker_id, worker_records in distribution.items():
            worker_assignments = []
            for record in worker_records:
                url = self.build_url_from_record(record)
                worker_assignments.append((record, url))
            assignments[worker_id] = worker_assignments
            
            self.logger.info(f"Worker {worker_id} has {len(worker_assignments)} URL assignments")
        
        return assignments


def get_data_distribution(max_workers: int = 5) -> Dict[int, List[Tuple[CustomWheelOffsetYMM, str]]]:
    """
    Convenience function to get data distribution for browser instances.
    
    Args:
        max_workers: Maximum number of browser instances
    
    Returns:
        Dictionary mapping worker_id to list of (record, url) tuples
    """
    distributor = DataDistributor()
    return distributor.get_worker_assignments(max_workers)