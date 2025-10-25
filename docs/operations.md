# Operations & Storage

## Behavior and Storage

- Provider Naming: requests use lowercase (e.g., `tire-rack`); env vars use uppercase with `_` (e.g., `SCRAPER_CMD_TIRE_RACK`).
- Process Tracking: The API tracks only processes it starts via `/scraper/start`.
- PID Registry: Process info persists in `e:\scraper\data\process_registry.json`.

## Session Management (Custom Wheel Offset)

The Custom Wheel Offset scraper maintains session state for authentication and performance optimization. Session data is stored in two locations:

### Session Storage Locations
- **In-Memory Session**: Shared `requests.Session` instance with cookies and headers
- **Persistent Storage**: `data/custom_wheel_offset_temp.json` containing:
  - `aws_waf_token`: AWS WAF token for bypassing protection
  - `PHPSESSID`: PHP session ID for server-side session

### Cleaning Session Data

When experiencing authentication issues or stale session problems, clean the session data:

#### Method 1: Using the Cleanup Utility
```bash
# Show current session status
python clean_session.py --status

# Clean all session data
python clean_session.py --clean
```

#### Method 2: Manual Cleanup
1. **Reset in-memory session**: Restart the scraper process
2. **Clear persistent tokens**: Delete or empty `data/custom_wheel_offset_temp.json`
3. **Full cleanup**: Use both methods above

#### Method 3: Programmatic Cleanup
```python
from providers.custom_wheel_offset.session_manager import reset_shared_session
reset_shared_session()  # Creates fresh session instance
```

### When to Clean Sessions
- Authentication failures or 403/401 errors
- Stale cookie issues
- After long periods of inactivity
- When switching between different scraping targets

## Troubleshooting

- "Failed to start <provider>": Ensure the provider env var is set and the command is valid.
- "Failed to stop <provider>": Process may have already exited; check `status` and `exit_code`.
- If your scraper uses browsers, make sure to install them (`playwright install`).
- **Custom Wheel Offset authentication issues**: Clean session data using methods above.

## Example Workflow (Windows, Git Bash)

1. Set provider command (persistent):
   - ``PY="$(python -c 'import sys; print(sys.executable)')"``
   - Tire Rack: ``setx SCRAPER_CMD_TIRE_RACK "\"$PY\" e:\\scraper\\src\\providers\\tire_rack\\index.py"``
   - Custom Wheel Offset: ``setx SCRAPER_CMD_CUSTOM_WHEEL_OFFSET "\"$PY\" e:\\scraper\\src\\providers\\custom_wheel_offset\\index.py"``
2. Start the API:
   - `python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8081`
3. Start a scraper:
   - Tire Rack: `curl "http://localhost:8081/scraper/start?provider=tire-rack"`
   - Custom Wheel Offset: `curl "http://localhost:8081/scraper/start?provider=custom-wheel-offset"`
4. Stop the scraper:
   - Tire Rack: `curl "http://localhost:8081/scraper/stop?provider=tire-rack"`
   - Custom Wheel Offset: `curl "http://localhost:8081/scraper/stop?provider=custom-wheel-offset"`

---

For production, consider adding authentication, rate limiting, and structured logging.