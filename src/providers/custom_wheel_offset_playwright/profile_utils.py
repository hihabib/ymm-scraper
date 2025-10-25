from pathlib import Path
from typing import Optional


def get_profile_dir(profile_name: str) -> Path:
    """Return the profile directory path for a given profile name."""
    return Path(f"E:/scraper/src/data/chromium_profile_{profile_name}")


def check_profile_exists(profile_name: str, logger) -> bool:
    """Check if the profile directory exists and contains meaningful browser data.

    Returns True if profile exists with data, False otherwise.
    """
    profile_dir = get_profile_dir(profile_name)

    if not profile_dir.exists():
        logger.info(f"Profile '{profile_name}' does not exist - new profile")
        return False

    default_dir = profile_dir / "Default"
    if not default_dir.exists():
        logger.info(f"Profile '{profile_name}' exists but no Default directory - new profile")
        return False

    key_files = [
        default_dir / "Cookies",
        default_dir / "History",
        default_dir / "Preferences",
        default_dir / "Web Data",
    ]

    existing_files = [f for f in key_files if f.exists() and f.stat().st_size > 0]

    if existing_files:
        logger.info(
            f"Profile '{profile_name}' exists with {len(existing_files)} data files - existing profile"
        )
        return True

    logger.info(
        f"Profile '{profile_name}' exists but no meaningful data - treating as new profile"
    )
    return False