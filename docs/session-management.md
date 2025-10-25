# Session Management - Custom Wheel Offset Scraper

## Overview

The Custom Wheel Offset scraper uses a sophisticated session management system to maintain authentication state and optimize performance across multiple requests. This document explains how sessions work and how to manage them effectively.

## Session Architecture

### Singleton Session Manager
The scraper uses a singleton pattern to ensure all modules share the same session instance:
- **Location**: `src/providers/custom_wheel_offset/session_manager.py`
- **Purpose**: Maintains connection pooling, cookies, and headers across requests
- **Shared by**: `utils`, `fitment_preferences`, `wheel_size`, and `resolve_captcha` modules

### Session Storage

#### 1. In-Memory Session
- **Type**: `requests.Session` instance
- **Contains**: 
  - HTTP cookies (PHPSESSID, aws-waf-token)
  - Request headers (User-Agent, etc.)
  - Connection pool for performance
- **Lifetime**: Exists while the scraper process is running

#### 2. Persistent Token Storage
- **File**: `data/custom_wheel_offset_temp.json`
- **Contains**:
  ```json
  {
    "aws_waf_token": "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSYnKCkqKywtLi8wMTIzNDU2Nzg5Ojs8PT4_QA",
    "PHPSESSID": "bh840qimnr6kvbk8m7r1e9lvo4"
  }
  ```
- **Purpose**: Persist authentication tokens between scraper runs

## Session Lifecycle

1. **Initialization**: Session created on first import via singleton pattern
2. **Token Loading**: AWS WAF tokens and PHPSESSID loaded from JSON file
3. **Request Processing**: Session used for all HTTP requests with shared cookies
4. **Token Persistence**: Important tokens saved to JSON file for reuse
5. **Cleanup**: Session can be reset or cleaned when needed

## Session Cleaning Methods

### Method 1: Automated Cleanup Utility

The `clean_session.py` utility provides the easiest way to manage sessions:

```bash
# Check current session status
python clean_session.py --status

# Clean all session data
python clean_session.py --clean
```

**Output Example:**
```
=== Session Status ===
Session ID: 1474887799136
Session cookies: 2
Cookies:
  - PHPSESSID=bh840qimnr6kvbk8m7r1e9lvo4
  - aws-waf-token=AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSYnKCkqKywtLi8wMTIzNDU2Nzg5Ojs8PT4_QA

Persistent storage: E:\scraper\data\custom_wheel_offset_temp.json
File exists: ✓
Stored data: {"aws_waf_token": "...", "PHPSESSID": "..."}
```

### Method 2: Programmatic Cleanup

```python
from providers.custom_wheel_offset.session_manager import reset_shared_session, get_shared_session

# Get current session
session = get_shared_session()

# Option 1: Clear cookies only
session.cookies.clear()

# Option 2: Reset entire session (recommended)
reset_shared_session()
```

### Method 3: Manual Cleanup

1. **Stop the scraper process** (kills in-memory session)
2. **Delete or empty the token file**:
   ```bash
   echo "{}" > data/custom_wheel_offset_temp.json
   ```
3. **Restart the scraper** (creates fresh session)

## When to Clean Sessions

### Required Cleanup Scenarios
- **Authentication failures**: 401/403 HTTP errors
- **Stale cookie errors**: "Invalid session" messages
- **CAPTCHA bypass issues**: AWS WAF token expired
- **Rate limiting**: Too many requests with same session

### Recommended Cleanup Scenarios
- **After long inactivity**: Sessions older than 24 hours
- **Between different targets**: Switching scraping focus
- **Development/testing**: Clean state for debugging
- **Deployment**: Fresh start in production

## Troubleshooting Session Issues

### Common Problems

#### Problem: "Session expired" or 401 errors
**Solution**: Clean session data and restart
```bash
python clean_session.py --clean
```

#### Problem: CAPTCHA challenges appearing frequently
**Solution**: Reset AWS WAF token
```bash
# Clear persistent storage
echo "{}" > data/custom_wheel_offset_temp.json
# Restart scraper to get fresh tokens
```

#### Problem: Inconsistent session state across modules
**Solution**: Verify all modules use shared session
```python
# Check if all modules use same session
from providers.custom_wheel_offset.session_manager import get_shared_session
from providers.custom_wheel_offset import fitment_preferences, utils

session1 = get_shared_session()
session2 = fitment_preferences.get_session()
print(f"Same session: {session1 is session2}")  # Should be True
```

### Debug Session State

```python
# Show detailed session information
session = get_shared_session()
print(f"Session ID: {id(session)}")
print(f"Cookies: {len(session.cookies)}")
print(f"Headers: {dict(session.headers)}")

# Show persistent storage
import json
from pathlib import Path
token_file = Path("data/custom_wheel_offset_temp.json")
if token_file.exists():
    with open(token_file) as f:
        data = json.load(f)
    print(f"Stored tokens: {data}")
```

## Best Practices

1. **Regular Cleanup**: Clean sessions every 24 hours in production
2. **Error Handling**: Always clean sessions after authentication failures
3. **Monitoring**: Log session state changes for debugging
4. **Testing**: Use clean sessions for consistent test results
5. **Security**: Never log or expose session tokens in plain text

## Integration with Scraper Workflow

```python
# Example: Robust scraping with session management
from providers.custom_wheel_offset.session_manager import get_shared_session, reset_shared_session
import requests

def robust_scrape(url):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            session = get_shared_session()
            response = session.get(url)
            
            if response.status_code == 401:
                print(f"Authentication failed, cleaning session (attempt {attempt + 1})")
                reset_shared_session()
                continue
                
            return response
            
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                raise
            print(f"Request failed, retrying with clean session: {e}")
            reset_shared_session()
    
    raise Exception("Max retries exceeded")
```

This comprehensive session management ensures reliable and efficient scraping operations while maintaining proper authentication state.