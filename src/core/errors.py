"""
Custom error types for scraper robustness.
"""

class ScraperError(Exception):
    """Base class for scraper-related errors."""


class ApiError(ScraperError):
    """Raised when API interactions fail after retries."""


class ParsingError(ScraperError):
    """Raised when parsing HTML/XML fails or expected elements are missing."""


class DataSplicingError(ScraperError):
    """Raised when data slicing/splicing logic encounters inconsistencies."""

class HumanVerificationError(ScraperError):
    """Raised when target site returns a Human Verification page.

    This signals that scraping should pause and trigger the external CAPTCHA
    solver process, then restart the scraper as a new process.
    """

__all__ = ["ScraperError", "ApiError", "ParsingError", "DataSplicingError", "HumanVerificationError"]
