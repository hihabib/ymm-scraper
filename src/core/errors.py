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

__all__ = ["ScraperError", "ApiError", "ParsingError", "DataSplicingError"]
