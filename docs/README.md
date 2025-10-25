# Documentation Overview

This repository contains a lightweight API for managing scraping workers.

- Overview: starts/stops Tire Rack scraping workers and exposes system health.
- Architecture: multi-process workers with headless Chromium via Playwright.
- Responses: standardized JSON envelope for consistency.
- Session Management: Custom Wheel Offset scraper includes session cleaning utilities.

## Documents

- Installation: setup, provider command, start the API
  - [installation.md](./installation.md)
- API Reference: endpoints and response formats
  - [api.md](./api.md)
- Operations & Storage: behavior, troubleshooting, workflow, and session management
  - [operations.md](./operations.md)
- Session Management: comprehensive guide for Custom Wheel Offset scraper sessions
  - [session-management.md](./session-management.md)
- **Optimization Guide: multithreading performance optimizations and usage**
  - [optimization-guide.md](./optimization-guide.md)

## Quick Session Cleanup (Custom Wheel Offset)

For authentication issues with the Custom Wheel Offset scraper:

```bash
# Check session status
python clean_session.py --status

# Clean all session data
python clean_session.py --clean
```

See [operations.md](./operations.md) for detailed session management information.