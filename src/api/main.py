from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi import HTTPException
from starlette.requests import Request
from fastapi.middleware.cors import CORSMiddleware
from .routes import router as system_router
from .routers.scraper import router as scraper_router
from .utils.response import envelope

app = FastAPI(title="Scraper API")

# Allow all origins (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(system_router)
app.include_router(scraper_router)

# Startup restoration removed per request; app will not auto-restore child processes.

# Global exception handlers (standard envelope)
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    payload = envelope(False, exc.detail if isinstance(exc.detail, str) else "HTTP Error", exc.status_code, None)
    return JSONResponse(content=payload, status_code=exc.status_code)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    payload = envelope(False, "Validation Error", 422, exc.errors())
    return JSONResponse(content=payload, status_code=422)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    payload = envelope(False, "Internal Server Error", 500, None)
    return JSONResponse(content=payload, status_code=500)