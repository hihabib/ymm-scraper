from fastapi import APIRouter, Query, HTTPException
from typing import Optional

from ..utils.process import start_provider, stop_provider
from ..utils.response import success, error_json

router = APIRouter(prefix="/scraper", tags=["scraper"])


@router.get("/start")
def start_scrapper(provider: str = Query(..., description="Provider to start scraper for")):
    """Start a provider-specific scraper in a separate process using env-configured command."""
    try:
        result = start_provider(provider)
        # Wrap in standardized envelope
        return success(data=result, message="Scraper start processed", status_code=200)
    except Exception as e:
        return error_json(message=f"Failed to start {provider}: {e}", status_code=500)



@router.get("/stop")
def stop_scrapper(provider: str = Query(..., description="Provider to stop scraper for"), timeout: Optional[float] = Query(10.0, ge=0.5, le=60.0)):
    """Stop the running provider-specific scraper process if present."""
    try:
        result = stop_provider(provider, timeout=timeout or 10.0)
        # Wrap in standardized envelope
        return success(data=result, message="Scraper stop processed", status_code=200)
    except Exception as e:
        return error_json(message=f"Failed to stop {provider}: {e}", status_code=500)