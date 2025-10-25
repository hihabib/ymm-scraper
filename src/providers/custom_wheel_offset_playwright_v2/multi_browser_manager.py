"""Multi-Browser Manager for Custom Wheel Offset Scraper V2.
Manages multiple browser instances running in parallel for different year ranges.
"""

import asyncio
import logging
import time
from typing import List, Dict, Any, Optional
from custom_wheel_offset import CustomWheelOffsetScraperV2
from config.worker import CUSTOM_WHEEL_OFFSET_FINAL_VERSION_WORKERS
from database_cleanup import run_database_cleanup


class MultiBrowserManager:
    """Manages multiple browser instances for parallel scraping with infinite restart capability."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.workers = CUSTOM_WHEEL_OFFSET_FINAL_VERSION_WORKERS
        self.scrapers: List[CustomWheelOffsetScraperV2] = []
        self.tasks: List[asyncio.Task] = []
        self.worker_restart_counts: Dict[int, int] = {}
        self.worker_last_restart: Dict[int, float] = {}
        self.should_stop = False
        self.monitoring_task: Optional[asyncio.Task] = None
        
    async def initialize_scrapers(self) -> None:
        """Initialize scraper instances for each worker configuration."""
        
        for i, worker_config in enumerate(self.workers):
            start_year = worker_config.get('START_YEAR')
            end_year = worker_config.get('END_YEAR')
            
            # Create scraper instance with unique profile for each worker
            scraper = CustomWheelOffsetScraperV2(
                profile_name=f"custom_wheel_offset_worker_{i+1}",
                worker_id=i+1,
                start_year=start_year,
                end_year=end_year
            )
            
            self.scrapers.append(scraper)
    
    async def start_worker_with_infinite_restart(self, scraper: CustomWheelOffsetScraperV2, worker_id: int) -> None:
        """Start a worker with infinite restart capability - will restart on any error, no limits."""
        restart_count = 0
        
        while not self.should_stop:
            try:
                restart_count += 1
                self.worker_restart_counts[worker_id] = restart_count
                self.worker_last_restart[worker_id] = time.time()
                
                if restart_count == 1:
                    self.logger.info(f"Worker {worker_id}: Starting (Years: {scraper.start_year}-{scraper.end_year})")
                else:
                    self.logger.info(f"Worker {worker_id}: Restarting (Attempt #{restart_count})")
                
                # Initialize the scraper browser
                await scraper.setup_browser()
                
                # Start the scraping workflow
                await scraper.run_continuous_scraping()
                
                # If we reach here, the worker completed normally (shouldn't happen in continuous mode)
                self.logger.warning(f"Worker {worker_id}: Completed unexpectedly, restarting...")
                
            except Exception as e:
                self.logger.error(f"Worker {worker_id}: Failed (Restart #{restart_count}): {e}")
                
                # Clean up the scraper before restart
                try:
                    await scraper.close()
                except Exception as cleanup_error:
                    self.logger.error(f"Worker {worker_id}: Cleanup error: {cleanup_error}")
                
                # Wait a bit before restart to prevent rapid restart loops
                await asyncio.sleep(5)
                
                # Continue the loop to restart
                continue
        
        self.logger.info(f"Worker {worker_id}: Stopped (Total restarts: {restart_count})")
    
    async def start_worker(self, scraper: CustomWheelOffsetScraperV2, worker_id: int) -> None:
        """Start a single worker scraper instance."""
        try:
            self.logger.info(f"Worker {worker_id}: Starting (Years: {scraper.start_year}-{scraper.end_year})")
            
            # Initialize the scraper browser
            await scraper.setup_browser()
            
            # Start the scraping workflow
            await scraper.run_continuous_scraping()
            
        except Exception as e:
            self.logger.error(f"Worker {worker_id}: Failed: {e}")
            raise
    
    async def start_all_workers(self) -> None:
        """Start all worker instances with staggered startup to prevent profile conflicts."""
        if not self.scrapers:
            await self.initialize_scrapers()
        
        # Start workers with delays to prevent profile directory conflicts
        self.tasks = []
        for i, scraper in enumerate(self.scrapers):
            # Add delay between worker startups (5 seconds apart)
            if i > 0:
                await asyncio.sleep(i * 5)
            
            # Create and start task for this worker
            task = asyncio.create_task(self.start_worker(scraper, i + 1))
            self.tasks.append(task)
        
        # Wait for all tasks to complete
        try:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        except Exception as e:
            self.logger.error(f"Error in parallel execution: {e}")
            await self.stop_all_workers()
            raise
    
    async def start_all_workers_with_infinite_restart(self) -> None:
        """Start all worker instances with infinite restart capability and staggered startup."""
        if not self.scrapers:
            await self.initialize_scrapers()
        
        # Initialize restart tracking for all workers
        for i in range(len(self.scrapers)):
            worker_id = i + 1
            self.worker_restart_counts[worker_id] = 0
            self.worker_last_restart[worker_id] = 0
        
        # Start workers with delays to prevent profile directory conflicts
        self.tasks = []
        for i, scraper in enumerate(self.scrapers):
            worker_id = i + 1
            
            # Add delay between worker startups (5 seconds apart)
            if i > 0:
                await asyncio.sleep(i * 5)
            
            # Create and start task for this worker with infinite restart
            task = asyncio.create_task(
                self.start_worker_with_infinite_restart(scraper, worker_id),
                name=f"worker_{worker_id}"
            )
            self.tasks.append(task)
        
        # Start monitoring task
        self.monitoring_task = asyncio.create_task(self.monitor_workers())
        
        # Wait for all tasks to complete (they should run continuously)
        try:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        except Exception as e:
            self.logger.error(f"Error in parallel execution: {e}")
            await self.stop_all_workers()
            raise
    
    async def monitor_workers(self) -> None:
        """Monitor worker status and log restart statistics."""
        while not self.should_stop:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                status = await self.get_worker_status()
                running_count = status["running_workers"]
                total_count = status["total_workers"]
                
                # Only log if there are restarts to report
                restart_info = []
                for worker_info in status["workers"]:
                    worker_id = worker_info["worker_id"]
                    restart_count = self.worker_restart_counts.get(worker_id, 0)
                    if restart_count > 1:
                        restart_info.append(f"Worker {worker_id}: {restart_count} restarts")
                
                if restart_info:
                    self.logger.info(f"Status: {running_count}/{total_count} running | " + " | ".join(restart_info))
                
            except Exception as e:
                self.logger.error(f"Error in worker monitoring: {e}")
                await asyncio.sleep(10)  # Wait before retrying
    
    async def stop_all_workers(self) -> None:
        """Stop all running worker instances."""
        
        # Signal all workers to stop
        self.should_stop = True
        
        # Cancel monitoring task
        if self.monitoring_task and not self.monitoring_task.done():
            self.monitoring_task.cancel()
        
        # Cancel all worker tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to be cancelled
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Close all scrapers
        for scraper in self.scrapers:
            try:
                await scraper.close()
            except Exception as e:
                self.logger.error(f"Error closing scraper: {e}")
    
    async def get_worker_status(self) -> Dict[str, Any]:
        """Get status of all workers with restart information."""
        status = {
            "total_workers": len(self.scrapers),
            "running_workers": sum(1 for task in self.tasks if not task.done()),
            "workers": []
        }
        
        for i, (scraper, task) in enumerate(zip(self.scrapers, self.tasks)):
            worker_id = i + 1
            restart_count = self.worker_restart_counts.get(worker_id, 0)
            last_restart = self.worker_last_restart.get(worker_id, 0)
            
            worker_status = {
                "worker_id": worker_id,
                "year_range": f"{scraper.start_year}-{scraper.end_year}",
                "status": "running" if not task.done() else "stopped",
                "profile_name": scraper.profile_name,
                "restart_count": restart_count,
                "last_restart_time": last_restart
            }
            
            if task.done() and task.exception():
                worker_status["error"] = str(task.exception())
            
            status["workers"].append(worker_status)
        
        return status


async def main():
    """Main function to run the multi-browser manager with infinite restart."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    # Perform database cleanup before starting any workers
    logger.info("=== STARTING DATABASE CLEANUP ===")
    try:
        cleanup_results = run_database_cleanup(logger)
        logger.info("Database cleanup completed successfully")
    except Exception as e:
        logger.error(f"Database cleanup failed: {e}")
        logger.error("Continuing with worker startup despite cleanup failure...")
    
    manager = MultiBrowserManager()
    
    try:
        # Use infinite restart version by default
        await manager.start_all_workers_with_infinite_restart()
    except KeyboardInterrupt:
        logging.info("Received interrupt signal, stopping all workers...")
        await manager.stop_all_workers()
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        await manager.stop_all_workers()
        raise


if __name__ == "__main__":
    asyncio.run(main())