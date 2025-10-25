#!/usr/bin/env python3
"""
Thread-safe session manager for custom wheel offset scraper.
Each thread gets its own session instance to prevent cookie conflicts and connection contention.
"""

import threading
import requests
from .logging_config import init_module_logger

logger = init_module_logger(__name__)
from typing import Dict


class ThreadedSessionManager:
    """
    Thread-safe session manager that provides isolated sessions per thread.
    This prevents cookie conflicts and connection contention between threads.
    """
    
    def __init__(self):
        self._sessions: Dict[int, requests.Session] = {}
        self._lock = threading.Lock()
    
    def get_session(self) -> requests.Session:
        """
        Get a session instance for the current thread.
        Creates a new session if one doesn't exist for this thread.
        
        Returns:
            requests.Session: Thread-specific session instance
        """
        thread_id = threading.current_thread().ident
        
        with self._lock:
            if thread_id not in self._sessions:
                session = requests.Session()
                # Set default headers
                session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                })
                self._sessions[thread_id] = session
                logger.info(f"[ThreadedSessionManager] Created new session for thread {thread_id}")
            
            return self._sessions[thread_id]
    
    def reset_session(self) -> None:
        """
        Reset the session for the current thread.
        Creates a fresh session instance.
        """
        thread_id = threading.current_thread().ident
        
        with self._lock:
            if thread_id in self._sessions:
                # Close the existing session
                self._sessions[thread_id].close()
                del self._sessions[thread_id]
                logger.info(f"[ThreadedSessionManager] Reset session for thread {thread_id}")
            
            # Create a new session
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            })
            self._sessions[thread_id] = session
            logger.info(f"[ThreadedSessionManager] Created fresh session for thread {thread_id}")
    
    def reset_all_sessions(self) -> None:
        """
        Reset all sessions across all threads.
        This clears all stored sessions and forces creation of new ones.
        """
        with self._lock:
            # Close all existing sessions
            for session in self._sessions.values():
                try:
                    session.close()
                except Exception:
                    pass  # Ignore errors during cleanup
            
            # Clear the sessions dictionary
            self._sessions.clear()
            logger.info(f"[ThreadedSessionManager] Reset all sessions ({len(self._sessions)} sessions cleared)")
    
    def close_all_sessions(self) -> None:
        """
        Close all sessions and clean up resources.
        Should be called when shutting down the application.
        """
        with self._lock:
            for thread_id, session in self._sessions.items():
                try:
                    session.close()
                    logger.info(f"[ThreadedSessionManager] Closed session for thread {thread_id}")
                except Exception as e:
                    logger.error(f"[ThreadedSessionManager] Error closing session for thread {thread_id}: {e}")
            
            self._sessions.clear()
            logger.info(f"[ThreadedSessionManager] All sessions closed and cleared")


# Global threaded session manager instance
threaded_session_manager = ThreadedSessionManager()