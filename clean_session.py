#!/usr/bin/env python3
"""
Session cleanup utility for Custom Wheel Offset scraper.
Provides methods to clean and reset session data.
"""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def clean_session_data():
    """Clean all session data including cookies and persistent storage."""
    try:
        from providers.custom_wheel_offset.session_manager_threaded import threaded_session_manager
        
        print("=== Cleaning Session Data ===")
        
        # Get current session state
        session = threaded_session_manager.get_session()
        print(f"Current session cookies: {len(session.cookies)}")
        
        # Method 1: Reset all threaded sessions (creates new session instances)
        print("1. Resetting all threaded sessions...")
        threaded_session_manager.reset_all_sessions()
        new_session = threaded_session_manager.get_session()
        print(f"   New session cookies: {len(new_session.cookies)}")
        print(f"   Session reset: {'✓' if session is not new_session else '✗'}")
        
        # Method 2: Clear persistent token storage
        print("2. Clearing persistent token storage...")
        token_file = Path(__file__).parent / "data" / "custom_wheel_offset_temp.json"
        if token_file.exists():
            token_file.write_text("{}")
            print(f"   Cleared token file: {token_file}")
        else:
            print(f"   Token file not found: {token_file}")
        
        print("✓ Session cleanup completed!")
        return True
        
    except Exception as e:
        print(f"✗ Error cleaning session: {e}")
        return False

def show_session_status():
    """Show current session status and stored data."""
    try:
        from providers.custom_wheel_offset.session_manager_threaded import threaded_session_manager
        
        print("=== Session Status ===")
        
        # Show session state
        session = threaded_session_manager.get_session()
        print(f"Session ID: {id(session)}")
        print(f"Session cookies: {len(session.cookies)}")
        
        if session.cookies:
            print("Cookies:")
            for cookie in session.cookies:
                print(f"  - {cookie.name}={cookie.value[:30]}{'...' if len(cookie.value) > 30 else ''}")
        
        # Show persistent storage
        token_file = Path(__file__).parent / "data" / "custom_wheel_offset_temp.json"
        print(f"\nPersistent storage: {token_file}")
        print(f"File exists: {'✓' if token_file.exists() else '✗'}")
        
        if token_file.exists():
            try:
                import json
                with open(token_file, 'r') as f:
                    data = json.load(f)
                print(f"Stored data: {data}")
            except Exception as e:
                print(f"Error reading token file: {e}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error showing session status: {e}")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Session cleanup utility")
    parser.add_argument("--clean", action="store_true", help="Clean all session data")
    parser.add_argument("--status", action="store_true", help="Show session status")
    
    args = parser.parse_args()
    
    if args.clean:
        clean_session_data()
    elif args.status:
        show_session_status()
    else:
        print("Usage:")
        print("  python clean_session.py --clean   # Clean all session data")
        print("  python clean_session.py --status  # Show session status")