import logging
from contextlib import contextmanager
from typing import Dict, Any, List, Optional
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import sys
from pathlib import Path

# Add the src directory to the path for imports
src_path = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(src_path))

try:
    from core.models import CustomWheelOffsetData, CustomWheelOffsetYMM
    from db.db import engine
except ImportError:
    # Fallback for when running from different contexts
    from src.core.models import CustomWheelOffsetData, CustomWheelOffsetYMM
    from src.db.db import engine


class FitmentDatabaseHandler:
    """Handles database operations for fitment data insertion."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.engine = engine
        self.SessionLocal = sessionmaker(bind=self.engine)
    
    @contextmanager
    def get_db_session(self):
        """Get a database session with proper cleanup."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def insert_fitment_data(self, ymm_id: int, fitment_data: Dict[str, Any], record_id: int) -> int:
        """
        Insert fitment data for a specific YMM record.
        
        Args:
            ymm_id: The ID from custom_wheel_offset_ymm table
            fitment_data: Cleaned fitment data with front/rear positions
            record_id: Original record ID for logging
            
        Returns:
            Number of records inserted
        """
        if not fitment_data:
            self.logger.warning(f"Record {record_id}: No fitment data to insert")
            return 0
        
        inserted_count = 0
        
        try:
            with self.get_db_session() as session:
                for position in ['front', 'rear']:
                    if position in fitment_data:
                        pos_data = fitment_data[position]
                        
                        # Validate required fields
                        required_fields = ['diameter_min', 'diameter_max', 'width_min', 'width_max', 'offset_min', 'offset_max']
                        if not all(field in pos_data and pos_data[field] is not None for field in required_fields):
                            self.logger.warning(f"Record {record_id}: Incomplete {position} data, skipping")
                            continue
                        
                        # Create database record
                        fitment_record = CustomWheelOffsetData(
                            ymm_id=ymm_id,
                            position=position,
                            diameter_min=pos_data['diameter_min'],
                            diameter_max=pos_data['diameter_max'],
                            width_min=str(pos_data['width_min']),  # Store as string to preserve precision
                            width_max=str(pos_data['width_max']),  # Store as string to preserve precision
                            offset_min=pos_data['offset_min'],
                            offset_max=pos_data['offset_max']
                        )
                        
                        session.add(fitment_record)
                        inserted_count += 1
                        
                        self.logger.debug(f"Record {record_id}: Prepared {position} fitment data for insertion")
                
                if inserted_count > 0:
                    self.logger.info(f"Record {record_id}: Successfully inserted {inserted_count} fitment records for YMM ID {ymm_id}")
                else:
                    self.logger.warning(f"Record {record_id}: No valid fitment data to insert")
                    
        except Exception as e:
            self.logger.error(f"Record {record_id}: Database error inserting fitment data: {e}")
            raise
        
        return inserted_count
    
    def batch_insert_fitment_data(self, fitment_batch: List[Dict[str, Any]]) -> int:
        """
        Batch insert multiple fitment data records.
        
        Args:
            fitment_batch: List of dicts with keys: ymm_id, fitment_data, record_id
            
        Returns:
            Total number of records inserted
        """
        if not fitment_batch:
            return 0
        
        total_inserted = 0
        
        try:
            with self.get_db_session() as session:
                records_to_add = []
                
                for batch_item in fitment_batch:
                    ymm_id = batch_item['ymm_id']
                    fitment_data = batch_item['fitment_data']
                    record_id = batch_item['record_id']
                    
                    for position in ['front', 'rear']:
                        if position in fitment_data:
                            pos_data = fitment_data[position]
                            
                            # Validate required fields
                            required_fields = ['diameter_min', 'diameter_max', 'width_min', 'width_max', 'offset_min', 'offset_max']
                            if not all(field in pos_data and pos_data[field] is not None for field in required_fields):
                                self.logger.warning(f"Record {record_id}: Incomplete {position} data in batch, skipping")
                                continue
                            
                            # Create database record
                            fitment_record = CustomWheelOffsetData(
                                ymm_id=ymm_id,
                                position=position,
                                diameter_min=pos_data['diameter_min'],
                                diameter_max=pos_data['diameter_max'],
                                width_min=str(pos_data['width_min']),  # Store as string to preserve precision
                                width_max=str(pos_data['width_max']),  # Store as string to preserve precision
                                offset_min=pos_data['offset_min'],
                                offset_max=pos_data['offset_max']
                            )
                            
                            records_to_add.append(fitment_record)
                            total_inserted += 1
                
                # Bulk insert all records
                if records_to_add:
                    session.add_all(records_to_add)
                    self.logger.info(f"Batch inserted {total_inserted} fitment records")
                else:
                    self.logger.warning("No valid fitment records to insert in batch")
                    
        except Exception as e:
            self.logger.error(f"Database error in batch fitment insertion: {e}")
            raise
        
        return total_inserted
    
    def mark_record_as_processed(self, record_id: int) -> bool:
        """
        Mark a YMM record as processed.
        
        Args:
            record_id: The ID of the record to mark as processed
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.get_db_session() as session:
                try:
                    from ...core.models import CustomWheelOffsetYMM
                except ImportError:
                    # Fallback to absolute import
                    import sys
                    from pathlib import Path
                    src_path = Path(__file__).resolve().parents[2]
                    if str(src_path) not in sys.path:
                        sys.path.insert(0, str(src_path))
                    from core.models import CustomWheelOffsetYMM
                
                record = session.query(CustomWheelOffsetYMM).filter(CustomWheelOffsetYMM.id == record_id).first()
                if record:
                    record.processed = 1
                    self.logger.debug(f"Marked record {record_id} as processed")
                    return True
                else:
                    self.logger.warning(f"Record {record_id} not found for processing update")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Error marking record {record_id} as processed: {e}")
            return False
    
    def get_processing_stats(self) -> Dict[str, int]:
        """
        Get statistics about processing status.
        
        Returns:
            Dict with processing statistics
        """
        try:
            with self.get_db_session() as session:
                try:
                    from ...core.models import CustomWheelOffsetYMM, CustomWheelOffsetData
                except ImportError:
                    # Fallback to absolute import
                    import sys
                    from pathlib import Path
                    src_path = Path(__file__).resolve().parents[2]
                    if str(src_path) not in sys.path:
                        sys.path.insert(0, str(src_path))
                    from core.models import CustomWheelOffsetYMM, CustomWheelOffsetData
                
                total_ymm = session.query(CustomWheelOffsetYMM).count()
                processed_ymm = session.query(CustomWheelOffsetYMM).filter(CustomWheelOffsetYMM.processed == True).count()
                total_fitment_data = session.query(CustomWheelOffsetData).count()
                
                return {
                    'total_ymm_records': total_ymm,
                    'processed_ymm_records': processed_ymm,
                    'unprocessed_ymm_records': total_ymm - processed_ymm,
                    'total_fitment_records': total_fitment_data
                }
                
        except Exception as e:
            self.logger.error(f"Error getting processing stats: {e}")
            return {}