#!/usr/bin/env python3
"""
Check final results after running the complete workflow.
"""

import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).resolve().parent / "src"
sys.path.insert(0, str(src_path))

from db.db import SessionLocal
from core.models import CustomWheelOffsetYMM, CustomWheelOffsetData

def check_results():
    """Check the final results of the scraping workflow."""
    with SessionLocal() as session:
        # Count total YMM records
        total_ymm = session.query(CustomWheelOffsetYMM).count()
        
        # Count processed YMM records
        processed_ymm = session.query(CustomWheelOffsetYMM).filter(
            CustomWheelOffsetYMM.processed == 1
        ).count()
        
        # Count unprocessed YMM records
        unprocessed_ymm = session.query(CustomWheelOffsetYMM).filter(
            CustomWheelOffsetYMM.processed == 0
        ).count()
        
        # Count fitment data records
        fitment_data = session.query(CustomWheelOffsetData).count()
        
        print("=== Final Scraping Results ===")
        print(f"Total YMM records: {total_ymm}")
        print(f"Processed YMM records: {processed_ymm}")
        print(f"Unprocessed YMM records: {unprocessed_ymm}")
        print(f"Fitment data records: {fitment_data}")
        print()
        
        if processed_ymm > 0:
            print("✅ SUCCESS: Workers successfully processed records and marked them as complete!")
            print(f"Processing rate: {processed_ymm}/{total_ymm} ({(processed_ymm/total_ymm)*100:.1f}%)")
        else:
            print("❌ No records were marked as processed")
            
        if fitment_data > 0:
            print(f"✅ SUCCESS: {fitment_data} fitment data records were collected!")
        else:
            print("❌ No fitment data was collected")

if __name__ == "__main__":
    check_results()