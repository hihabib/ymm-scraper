"""
Slim wrapper for Custom Wheel Offset Playwright provider.
This module keeps a small surface and delegates implementation to the impl module.
Also supports multi-worker execution when run as a script, using
`src/config/worker.py::CUSTOM_WHEEL_OFFSET_WORKERS`.
"""

import asyncio
import os
import sys
from pathlib import Path
from typing import Optional, List

# Import implementation, support running as script or package
try:
    from .custom_wheel_offset_playwright_impl import CustomWheelOffsetPlaywright as _ImplClass
except ImportError:
    sys.path.append(os.path.dirname(__file__))
    from custom_wheel_offset_playwright_impl import CustomWheelOffsetPlaywright as _ImplClass


# Re-export the class for external users
class CustomWheelOffsetPlaywright(_ImplClass):
    """
    Facade class that inherits the full implementation from impl.
    This file stays small by avoiding heavy inline code.
    """
    pass


async def run_preparation_worker_with_monitoring() -> bool:
    """
    Run the preparation worker that executes JavaScript scraping before main workers.
    Returns True if 'No more data left' message is detected, False otherwise.
    """
    profile_name = "preparation_worker"
    scraper = CustomWheelOffsetPlaywright(profile_name=profile_name)
    try:
        return await scraper.run_preparation_instance_with_monitoring()
    finally:
        try:
            await scraper.cleanup()
        except Exception:
            pass


async def run_preparation_worker() -> None:
    """Run the preparation worker that executes JavaScript scraping before main workers."""
    profile_name = "preparation_worker"
    scraper = CustomWheelOffsetPlaywright(profile_name=profile_name)
    try:
        await scraper.run_preparation_instance()
    finally:
        try:
            await scraper.cleanup()
        except Exception:
            pass


async def run_worker(worker_id: int, assigned_records: List = None) -> None:
    """Run a single worker with its own persistent profile and assigned records."""
    profile_name = f"worker_{worker_id}"
    scraper = CustomWheelOffsetPlaywright(profile_name=profile_name, assigned_records=assigned_records)
    try:
        await scraper.run()
    finally:
        try:
            await scraper.cleanup()
        except Exception:
            pass


async def main(profile_name: Optional[str] = None, workers: Optional[int] = None):
    """
    If `profile_name` is provided, run a single worker using that profile.
    Otherwise, run continuous scraping with smart preparation stage and infinite loop.
    """
    if profile_name:
        scraper = CustomWheelOffsetPlaywright(profile_name=profile_name)
        await scraper.run()
        return

    # Multi-worker mode with continuous operation
    n = None
    if workers is not None:
        n = max(1, int(workers))
    else:
        try:
            # Ensure we can import from the project root when running as script
            script_dir = Path(__file__).resolve().parent
            project_root = script_dir.parent.parent.parent  # Go up to e:\scraper
            src_dir = project_root / "src"
            if str(src_dir) not in sys.path:
                sys.path.insert(0, str(src_dir))
            
            # Also add project root to path for absolute imports
            if str(project_root) not in sys.path:
                sys.path.insert(0, str(project_root))
            
            from src.config.worker import CUSTOM_WHEEL_OFFSET_WORKERS
            n = max(1, int(CUSTOM_WHEEL_OFFSET_WORKERS))
            print(f"Launching {n} workers from config...")
        except Exception as e:
            print(f"Failed to import config: {e}, defaulting to 1 worker")
            n = 1
    
    print(f"Starting continuous scraping system with {n} concurrent workers...")
    
    # Import data distribution functionality
    try:
        from .data_distributor import DataDistributor
    except ImportError:
        # Fallback for when running as script
        sys.path.append(os.path.dirname(__file__))
        from data_distributor import DataDistributor
    
    distributor = DataDistributor()
    
    # Infinite loop until "No more data left" is detected
    cycle_count = 0
    while True:
        cycle_count += 1
        print(f"\n=== Starting Cycle {cycle_count} ===")
        
        # Step 1: Smart Preparation Stage - Check for unprocessed records first
        print("Checking for unprocessed records...")
        unprocessed_records = distributor.get_unprocessed_records()
        
        no_more_data = False
        
        if not unprocessed_records:
            print("No unprocessed records found - running preparation worker...")
            try:
                no_more_data = await run_preparation_worker_with_monitoring()
                if no_more_data:
                    print("Preparation worker detected 'No more data left' - terminating process")
                    break
                print("Preparation worker completed successfully.")
            except Exception as e:
                print(f"Preparation worker failed: {e}")
                print("Continuing with data check...")
        else:
            print(f"Found {len(unprocessed_records)} unprocessed records - skipping preparation stage")
        
        # Step 2: Get data distribution for workers (check again after potential preparation)
        print("Getting data distribution for workers...")
        try:
            from .data_distributor import get_data_distribution
        except ImportError:
            from data_distributor import get_data_distribution
        
        worker_assignments = get_data_distribution(max_workers=n)
        
        if not worker_assignments:
            print("No unprocessed records found after preparation - continuing to next cycle")
            continue
        
        print(f"Data distribution completed. {len(worker_assignments)} workers will be started.")
        
        # Step 3: Run the main workers with their assignments
        print(f"Starting {len(worker_assignments)} main workers...")
        tasks = []
        for worker_id, assignments in worker_assignments.items():
            tasks.append(run_worker(worker_id, assignments))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            print("All workers completed.")
        else:
            print("No workers to start.")
        
        print(f"=== Cycle {cycle_count} Completed ===")
    
    print("Continuous scraping system terminated gracefully.")


if __name__ == "__main__":
    asyncio.run(main())