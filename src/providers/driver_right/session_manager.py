#!/usr/bin/env python3
"""
Shared session manager for Driver Right scraper.
Maintains a single requests.Session instance across all modules.
"""

import requests
from typing import Optional

class SessionManager:
    """Singleton class to manage a single requests session."""
    
    _instance: Optional['SessionManager'] = None
    _session: Optional[requests.Session] = None
    _initialized: bool = False
    
    def __new__(cls) -> 'SessionManager':
        if cls._instance is None:
            cls._instance = super(SessionManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not SessionManager._initialized:
            self._session = requests.Session()
            # Set default headers that should be consistent across all requests
            self._session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Content-Type': 'application/json'
            })
            SessionManager._initialized = True

    @property
    def session(self) -> requests.Session:
        """Get the shared session instance."""
        if self._session is None:
            self.__init__()
        return self._session
    
    def reset_session(self) -> None:
        """Reset the session (create a new one)."""
        if self._session:
            # Explicitly clear all cookies before closing
            if hasattr(self._session, 'cookies') and self._session.cookies:
                self._session.cookies.clear()
            self._session.close()
        
        # Create a completely new session
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Content-Type': 'application/json'
        })
        SessionManager._initialized = True


# Global instance for easy access
_session_manager = SessionManager()

def get_shared_session() -> requests.Session:
    """Get the shared session instance."""
    return _session_manager.session

def reset_shared_session() -> None:
    """Reset the shared session."""
    _session_manager.reset_session()