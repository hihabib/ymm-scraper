#!/usr/bin/env python3
"""
Thread manager for Custom Wheel Offset scraper.
Handles multi-threading with race condition protection and error recovery.
"""

import threading
import time
import queue
import traceback
from typing import Dict, List, Tuple, Callable, Any, Optional
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass
from enum import Enum

from .config_manager import get_config
from .logging_config import init_module_logger

# Import error logging functionality
try:
    from services.repository_optimized import insert_error_log
except ImportError:
    try:
        # Fallback to regular repository if optimized version not available
        from services.repository import insert_error_log
    except ImportError:
        # If both fail, define a no-op function to prevent crashes
        def insert_error_log(source, context, message):
            pass

logger = init_module_logger(__name__)


class ThreadState(Enum):
    """Thread states for tracking."""
    IDLE = "idle"
    WORKING = "working"
    ERROR = "error"
    RESTARTING = "restarting"
    TERMINATED = "terminated"


@dataclass
class WorkItem:
    """Work item for thread processing."""
    year: str
    make: str
    model: str
    trim: str
    drive: str
    retry_count: int = 0


@dataclass
class ThreadInfo:
    """Thread information tracking."""
    thread_id: int
    state: ThreadState
    current_work: Optional[WorkItem]
    error_count: int
    last_error: Optional[str]


class ThreadManager:
    """
    Thread manager with race condition handling and error recovery.
    Ensures only one thread can restart the process at a time.
    """
    
    def __init__(self, max_workers: Optional[int] = None):
        """Initialize thread manager."""
        config = get_config()
        self.max_workers = max_workers or config.get('workers', 200)
        
        # Thread synchronization
        self._restart_lock = threading.RLock()  # Reentrant lock for restart operations
        self._work_queue_lock = threading.Lock()
        self._stats_lock = threading.Lock()
        
        # Thread state tracking
        self._threads: Dict[int, ThreadInfo] = {}
        self._restart_in_progress = False
        self._shutdown_requested = False
        
        # Work management
        self._work_queue: queue.Queue = queue.Queue()
        self._completed_work: set = set()  # Track completed work to prevent duplicates
        self._failed_work: List[WorkItem] = []
        
        # Statistics
        self._total_processed = 0
        self._total_errors = 0
        self._restart_count = 0
        
        # Thread pool
        self._executor: Optional[ThreadPoolExecutor] = None
        self._futures: List[Future] = []
        
        logger.info(f"[ThreadManager] Initialized with {self.max_workers} workers")
    
    def add_work_items(self, work_items: List[WorkItem]) -> None:
        """Add work items to the queue, avoiding duplicates."""
        with self._work_queue_lock:
            added_count = 0
            for item in work_items:
                work_key = f"{item.year}|{item.make}|{item.model}|{item.trim}|{item.drive}"
                if work_key not in self._completed_work:
                    self._work_queue.put(item)
                    added_count += 1
            
            logger.info(f"[ThreadManager] Added {added_count} work items to queue (total: {self._work_queue.qsize()})")
    
    def _get_next_work_item(self) -> Optional[WorkItem]:
        """Get next work item from queue."""
        try:
            return self._work_queue.get_nowait()
        except queue.Empty:
            return None
    
    def _mark_work_completed(self, work_item: WorkItem) -> None:
        """Mark work item as completed to prevent duplicates."""
        work_key = f"{work_item.year}|{work_item.make}|{work_item.model}|{work_item.trim}|{work_item.drive}"
        with self._work_queue_lock:
            self._completed_work.add(work_key)
    
    def _handle_thread_error(self, thread_id: int, work_item: WorkItem, error: Exception) -> bool:
        """
        Handle thread error with race condition protection.
        Returns True if restart was initiated, False otherwise.
        """
        with self._restart_lock:
            # Update thread info
            if thread_id in self._threads:
                self._threads[thread_id].state = ThreadState.ERROR
                self._threads[thread_id].error_count += 1
                self._threads[thread_id].last_error = str(error)
            
            with self._stats_lock:
                self._total_errors += 1
            
            error_msg = f"[ThreadManager] Thread {thread_id} error: {error}"
            logger.error(error_msg)
            logger.error(f"[ThreadManager] Error traceback: {traceback.format_exc()}")
            
            # Log error to database
            try:
                context = {
                    'thread_id': thread_id,
                    'year': work_item.year,
                    'make': work_item.make,
                    'model': work_item.model,
                    'trim': work_item.trim,
                    'drive': work_item.drive,
                    'retry_count': work_item.retry_count,
                    'error_type': type(error).__name__
                }
                insert_error_log('custom_wheel_offset_thread', context, str(error))
                logger.debug(f"[ThreadManager] Error logged to database for thread {thread_id}")
            except Exception as db_error:
                logger.error(f"[ThreadManager] Failed to log error to database: {db_error}")
            
            # Check if restart is needed and not already in progress
            if not self._restart_in_progress and self._should_restart_process(error):
                logger.info(f"[ThreadManager] Thread {thread_id} initiating process restart...")
                self._restart_in_progress = True
                self._restart_count += 1
                
                # Put failed work back in queue for retry
                if work_item.retry_count < 3:  # Max 3 retries
                    work_item.retry_count += 1
                    with self._work_queue_lock:
                        self._work_queue.put(work_item)
                else:
                    self._failed_work.append(work_item)
                
                # Set all threads to restarting state
                for tid, thread_info in self._threads.items():
                    if thread_info.state != ThreadState.TERMINATED:
                        thread_info.state = ThreadState.RESTARTING
                
                return True
            else:
                # Just retry the work item if restart not needed
                if work_item.retry_count < 3:
                    work_item.retry_count += 1
                    with self._work_queue_lock:
                        self._work_queue.put(work_item)
                else:
                    self._failed_work.append(work_item)
                
                return False
    
    def _should_restart_process(self, error: Exception) -> bool:
        """Determine if process should be restarted based on error type."""
        error_str = str(error).lower()
        
        # Restart conditions
        restart_keywords = [
            'session', 'captcha', 'authentication', 'login', 'token',
            'connection reset', 'timeout', 'network', 'ssl', 'certificate'
        ]
        
        return any(keyword in error_str for keyword in restart_keywords)
    
    def _wait_for_restart_completion(self, thread_id: int) -> None:
        """Wait for restart to complete."""
        logger.info(f"[ThreadManager] Thread {thread_id} waiting for restart completion...")
        
        while self._restart_in_progress and not self._shutdown_requested:
            time.sleep(1)
        
        if not self._shutdown_requested:
            logger.info(f"[ThreadManager] Thread {thread_id} resuming after restart")
            if thread_id in self._threads:
                self._threads[thread_id].state = ThreadState.IDLE
    
    def _complete_restart(self) -> None:
        """Complete the restart process."""
        with self._restart_lock:
            if self._restart_in_progress:
                logger.info("[ThreadManager] Restart process completed")
                self._restart_in_progress = False
                
                # Reset thread states
                for thread_info in self._threads.values():
                    if thread_info.state == ThreadState.RESTARTING:
                        thread_info.state = ThreadState.IDLE
    
    def _worker_thread(self, thread_id: int, work_function: Callable[[WorkItem], Any]) -> None:
        """Worker thread function."""
        # Register thread
        self._threads[thread_id] = ThreadInfo(
            thread_id=thread_id,
            state=ThreadState.IDLE,
            current_work=None,
            error_count=0,
            last_error=None
        )
        
        logger.info(f"[ThreadManager] Worker thread {thread_id} started")
        
        try:
            consecutive_empty_checks = 0
            max_empty_checks = 50  # 5 seconds of no work (50 * 0.1s)
            
            while not self._shutdown_requested:
                # Wait if restart is in progress
                if self._restart_in_progress:
                    self._wait_for_restart_completion(thread_id)
                    consecutive_empty_checks = 0  # Reset counter after restart
                    continue
                
                # Get next work item
                work_item = self._get_next_work_item()
                if work_item is None:
                    consecutive_empty_checks += 1
                    
                    # If no work for extended period and shutdown not requested, terminate thread
                    if consecutive_empty_checks >= max_empty_checks:
                        logger.info(f"[ThreadManager] Thread {thread_id} terminating due to no work available")
                        break
                    
                    # No more work, wait a bit and check again
                    time.sleep(0.1)
                    continue
                
                # Reset counter when work is found
                consecutive_empty_checks = 0
                
                # Update thread state
                self._threads[thread_id].state = ThreadState.WORKING
                self._threads[thread_id].current_work = work_item
                
                try:
                    # Process work item
                    result = work_function(work_item)
                    
                    # Mark as completed
                    self._mark_work_completed(work_item)
                    
                    with self._stats_lock:
                        self._total_processed += 1
                    
                    # Update thread state
                    self._threads[thread_id].state = ThreadState.IDLE
                    self._threads[thread_id].current_work = None
                    
                except Exception as e:
                    # Handle error
                    restart_initiated = self._handle_thread_error(thread_id, work_item, e)
                    
                    if restart_initiated:
                        # This thread initiated restart, perform restart logic
                        try:
                            self._perform_restart()
                        except Exception as restart_error:
                             logger.error(f"[ThreadManager] Restart failed: {restart_error}")
                             
                             # Log restart failure to database
                             try:
                                 context = {
                                     'thread_id': thread_id,
                                     'error_type': type(restart_error).__name__,
                                     'restart_context': 'thread_initiated_restart'
                                 }
                                 insert_error_log('custom_wheel_offset_restart_failure', context, str(restart_error))
                                 logger.debug(f"[ThreadManager] Restart failure logged to database for thread {thread_id}")
                             except Exception as db_error:
                                 logger.error(f"[ThreadManager] Failed to log restart failure to database: {db_error}")
                        finally:
                            self._complete_restart()
                
        except Exception as e:
             logger.error(f"[ThreadManager] Worker thread {thread_id} crashed: {e}")
             logger.error(f"[ThreadManager] Thread {thread_id} traceback: {traceback.format_exc()}")
             
             # Log thread crash to database
             try:
                 context = {
                     'thread_id': thread_id,
                     'error_type': type(e).__name__,
                     'crash_location': 'worker_thread_main_loop'
                 }
                 insert_error_log('custom_wheel_offset_thread_crash', context, str(e))
                 logger.debug(f"[ThreadManager] Thread crash logged to database for thread {thread_id}")
             except Exception as db_error:
                 logger.error(f"[ThreadManager] Failed to log thread crash to database: {db_error}")
        finally:
            # Mark thread as terminated
            if thread_id in self._threads:
                self._threads[thread_id].state = ThreadState.TERMINATED
            logger.info(f"[ThreadManager] Worker thread {thread_id} terminated")
    
    def _perform_restart(self) -> None:
        """Perform the actual restart logic."""
        logger.info("[ThreadManager] Performing process restart...")
        
        # Import here to avoid circular imports
        from .workflow_session_manager import initialize_session
        
        # Wait for all threads to reach restarting state
        max_wait = 30  # 30 seconds max wait
        wait_count = 0
        
        while wait_count < max_wait:
            working_threads = [
                tid for tid, info in self._threads.items() 
                if info.state == ThreadState.WORKING
            ]
            
            if not working_threads:
                break
                
            logger.info(f"[ThreadManager] Waiting for {len(working_threads)} threads to finish current work...")
            time.sleep(1)
            wait_count += 1
        
        # Reinitialize session
        try:
            initialize_session()
            logger.info("[ThreadManager] Session reinitialized successfully")
        except Exception as e:
            logger.error(f"[ThreadManager] Session reinitialization failed: {e}")
            
            # Log session reinitialization failure to database
            try:
                context = {
                    'error_type': type(e).__name__,
                    'restart_context': 'session_reinitialization'
                }
                insert_error_log('custom_wheel_offset_session_reinit_failure', context, str(e))
                logger.debug("[ThreadManager] Session reinitialization failure logged to database")
            except Exception as db_error:
                logger.error(f"[ThreadManager] Failed to log session reinitialization failure to database: {db_error}")
            
            raise
    
    def start_processing(self, work_function: Callable[[WorkItem], Any]) -> None:
        """Start multi-threaded processing."""
        if self._executor is not None:
            raise RuntimeError("Processing already started")
        
        logger.info(f"[ThreadManager] Starting processing with {self.max_workers} threads")
        
        self._executor = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="CWO-Worker")
        
        # Submit worker threads
        for i in range(self.max_workers):
            future = self._executor.submit(self._worker_thread, i, work_function)
            self._futures.append(future)
    
    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """Wait for all work to complete."""
        if self._executor is None:
            return True
        
        logger.info("[ThreadManager] Waiting for all work to complete...")
        
        start_time = time.time()
        while True:
            # Check if queue is empty and no threads are working
            with self._work_queue_lock:
                queue_empty = self._work_queue.empty()
            
            working_threads = [
                tid for tid, info in self._threads.items() 
                if info.state == ThreadState.WORKING
            ]
            
            if queue_empty and not working_threads:
                logger.info("[ThreadManager] All work completed")
                return True
            
            if timeout and (time.time() - start_time) > timeout:
                 logger.warning(f"[ThreadManager] Timeout reached after {timeout} seconds")
                 return False
            
            time.sleep(1)
    
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown thread manager."""
        logger.info("[ThreadManager] Shutting down...")
        
        self._shutdown_requested = True
        
        if self._executor is not None:
            if wait:
                # Wait for current work to complete
                self.wait_for_completion(timeout=60)
            
            # Shutdown executor
            self._executor.shutdown(wait=wait)
            self._executor = None
        
        self._futures.clear()
        logger.info("[ThreadManager] Shutdown complete")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics."""
        with self._stats_lock:
            active_threads = len([
                t for t in self._threads.values() 
                if t.state in [ThreadState.WORKING, ThreadState.IDLE]
            ])
            
            return {
                "total_processed": self._total_processed,
                "total_errors": self._total_errors,
                "restart_count": self._restart_count,
                "active_threads": active_threads,
                "queue_size": self._work_queue.qsize(),
                "completed_work_count": len(self._completed_work),
                "failed_work_count": len(self._failed_work),
                "threads": {tid: {
                    "state": info.state.value,
                    "error_count": info.error_count,
                    "current_work": f"{info.current_work.year} {info.current_work.make} {info.current_work.model}" if info.current_work else None
                } for tid, info in self._threads.items()}
            }
    
    def print_statistics(self) -> None:
        """Print current statistics."""
        stats = self.get_statistics()
        logger.info(f"[ThreadManager] Statistics:")
        logger.info(f"  Processed: {stats['total_processed']}")
        logger.info(f"  Errors: {stats['total_errors']}")
        logger.info(f"  Restarts: {stats['restart_count']}")
        logger.info(f"  Active threads: {stats['active_threads']}")
        logger.info(f"  Queue size: {stats['queue_size']}")
        logger.info(f"  Failed work: {stats['failed_work_count']}")