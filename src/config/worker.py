"""
Worker pool configuration for scrapers.
Adjust these counts to control concurrency per provider.
"""

# Tire Rack scraper worker count. Change as needed.
TIRE_RACK_WORKERS: int = 5

# Custom Wheel Offset scraper worker count.
CUSTOM_WHEEL_OFFSET_WORKERS: int = 200

# Driver Right Data scraper worker count. Change as needed.
DRIVER_RIGHT_WORKERS: int = 50

__all__ = [
    "TIRE_RACK_WORKERS",
    "CUSTOM_WHEEL_OFFSET_FINAL_VERSION_WORKERS",
    "DRIVER_RIGHT_WORKERS",
]