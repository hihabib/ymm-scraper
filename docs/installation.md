# Installation

This guide covers installing dependencies, configuring the Tire Rack provider command, and starting the API.

## Dependencies

- Install Python dependencies:
  - `pip install -r requirements.txt`
- If your scraper uses Playwright (Chromium):
  - `playwright install`
  - `playwright install chromium`

## Configure Provider Command (Windows, Git Bash)

The API starts provider scrapers via environment variables. Set a persistent Windows environment variable using Git Bash for each provider.

- Resolve your Python executable:
  - ``PY="$(python -c 'import sys; print(sys.executable)')"``

### Tire Rack (`tire-rack`)
- Persistent:
  - ``setx SCRAPER_CMD_TIRE_RACK "\"$PY\" e:\\scraper\\src\\providers\\tire_rack\\index.py"``
- Session-only:
  - ``export SCRAPER_CMD_TIRE_RACK="\"$PY\" e:\\scraper\\src\\providers\\tire_rack\\index.py"``

### Custom Wheel Offset (`custom-wheel-offset`)
- Persistent:
  - ``setx SCRAPER_CMD_CUSTOM_WHEEL_OFFSET "\"$PY\" e:\\scraper\\src\\providers\\custom_wheel_offset\\index.py"``
- Session-only:
  - ``export SCRAPER_CMD_CUSTOM_WHEEL_OFFSET="\"$PY\" e:\\scraper\\src\\providers\\custom_wheel_offset\\index.py"``

Notes:
- Provider env var keys are uppercased with hyphens replaced by underscores.
- The value is a full command line. Quoting the Python executable ensures paths with spaces work.

Other accepted variable names (resolved in order):
- For `tire-rack`:
  1. `SCRAPER_CMD_TIRE_RACK`
  2. `APP_SCRAPER_CMD_TIRE_RACK`
  3. `SCRAPER_CMD`
  4. `APP_SCRAPER_CMD`
- For `custom-wheel-offset`:
  1. `SCRAPER_CMD_CUSTOM_WHEEL_OFFSET`
  2. `APP_SCRAPER_CMD_CUSTOM_WHEEL_OFFSET`
  3. `SCRAPER_CMD`
  4. `APP_SCRAPER_CMD`

## Start the API

- Recommended:
  - `python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8081`
- Development with auto-reload:
  - `uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8081`