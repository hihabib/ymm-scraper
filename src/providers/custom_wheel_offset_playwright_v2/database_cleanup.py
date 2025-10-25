"""
Database cleanup utility for Custom Wheel Offset scraper.
Identifies and removes incomplete YMM records that don't have corresponding data records.
"""
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

# Add the src directory to the path for imports
src_path = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(src_path))

from services.repository_optimized import get_db_session
from core.models import CustomWheelOffsetYMM, CustomWheelOffsetData


class DatabaseCleanup:
    """Handles database cleanup operations for incomplete records."""
    
    def __init__(self, logger: logging.Logger = None):
        self.logger = logger or logging.getLogger(__name__)
    
    def find_incomplete_ymm_records(self) -> List[Dict[str, Any]]:
        """
        Find YMM records that don't have corresponding data records.
        
        Returns:
            List of dictionaries containing incomplete record information
        """
        incomplete_records = []
        
        try:
            with get_db_session() as session:
                # Query to find YMM records without corresponding data records
                query = session.query(CustomWheelOffsetYMM).outerjoin(
                    CustomWheelOffsetData, 
                    CustomWheelOffsetYMM.id == CustomWheelOffsetData.ymm_id
                ).filter(CustomWheelOffsetData.ymm_id.is_(None))
                
                incomplete_ymm_records = query.all()
                
                for record in incomplete_ymm_records:
                    incomplete_records.append({
                        'id': record.id,
                        'year': record.year,
                        'make': record.make,
                        'model': record.model,
                        'trim': record.trim,
                        'drive': record.drive,
                        'vehicle_type': record.vehicle_type,
                        'dr_chassis_id': record.dr_chassis_id,
                        'created_at': record.created_at
                    })
                
                self.logger.info(f"Found {len(incomplete_records)} incomplete YMM records")
                
        except Exception as e:
            self.logger.error(f"Error finding incomplete YMM records: {e}")
            raise
        
        return incomplete_records
    
    def delete_incomplete_records(self, record_ids: List[int]) -> int:
        """
        Delete YMM records by their IDs.
        
        Args:
            record_ids: List of YMM record IDs to delete
            
        Returns:
            Number of records deleted
        """
        if not record_ids:
            return 0
        
        deleted_count = 0
        
        try:
            with get_db_session() as session:
                # Delete records by IDs
                deleted_count = session.query(CustomWheelOffsetYMM).filter(
                    CustomWheelOffsetYMM.id.in_(record_ids)
                ).delete(synchronize_session=False)
                
                self.logger.info(f"Deleted {deleted_count} incomplete YMM records")
                
        except Exception as e:
            self.logger.error(f"Error deleting incomplete records: {e}")
            raise
        
        return deleted_count
    
    def perform_cleanup(self) -> Dict[str, Any]:
        """
        Perform complete database cleanup operation.
        
        Returns:
            Dictionary containing cleanup results and statistics
        """
        cleanup_results = {
            'incomplete_records': [],
            'deleted_count': 0,
            'cleanup_timestamp': datetime.now().isoformat()
        }
        
        try:
            # Find incomplete records
            incomplete_records = self.find_incomplete_ymm_records()
            cleanup_results['incomplete_records'] = incomplete_records
            
            if incomplete_records:
                # Extract IDs for deletion
                record_ids = [record['id'] for record in incomplete_records]
                
                # Log details about records to be deleted
                self.logger.info("=== DATABASE CLEANUP REPORT ===")
                self.logger.info(f"Found {len(incomplete_records)} incomplete records to delete:")
                
                for record in incomplete_records:
                    created_date = record['created_at'].strftime('%Y-%m-%d %H:%M:%S') if record['created_at'] else 'Unknown'
                    vehicle_info = f"{record['year']} {record['make']} {record['model']} {record['trim']} {record['drive']}"
                    self.logger.info(f"  - ID {record['id']}: {vehicle_info} (Created: {created_date})")
                
                # Delete the records
                deleted_count = self.delete_incomplete_records(record_ids)
                cleanup_results['deleted_count'] = deleted_count
                
                self.logger.info(f"=== CLEANUP SUMMARY ===")
                self.logger.info(f"Total records deleted: {deleted_count}")
                self.logger.info("Database cleanup completed successfully")
                
            else:
                self.logger.info("No incomplete records found - database is clean")
                
        except Exception as e:
            self.logger.error(f"Database cleanup failed: {e}")
            raise
        
        return cleanup_results


def run_database_cleanup(logger: logging.Logger = None) -> Dict[str, Any]:
    """
    Convenience function to run database cleanup.
    
    Args:
        logger: Optional logger instance
        
    Returns:
        Dictionary containing cleanup results
    """
    cleanup = DatabaseCleanup(logger)
    return cleanup.perform_cleanup()


if __name__ == "__main__":
    # Setup basic logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        results = run_database_cleanup(logger)
        print(f"\nCleanup completed. Deleted {results['deleted_count']} records.")
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        sys.exit(1)