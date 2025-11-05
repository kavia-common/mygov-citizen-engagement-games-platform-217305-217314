from typing import Optional

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import sqlite3

from . import db as db_module

# Configure the FastAPI app with metadata for OpenAPI
app = FastAPI(
    title="MyGov Gaming Backend",
    description="Backend APIs and WebSocket services for the MyGov Citizen Engagement Gaming Platform.",
    version="0.1.0",
    openapi_tags=[
        {"name": "Health", "description": "Service health and status endpoints."},
        {"name": "Auth", "description": "Mock authentication endpoints for development."},
        {"name": "Users", "description": "User profile endpoints."},
        {"name": "Games", "description": "Game catalog and scoring endpoints."},
        {"name": "Database", "description": "Database related helpers and diagnostics."},
    ],
)

# CORS - permissive by default; tighten in production environments
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_db_conn: Optional[sqlite3.Connection] = None


# PUBLIC_INTERFACE
def get_db_conn() -> sqlite3.Connection:
    """Dependency to get a live DB connection; raises 503 if unavailable."""
    if _db_conn is None:
        # Defer initialization error as a 503 to be graceful for clients
        raise HTTPException(
            status_code=503,
            detail="Database is not initialized. Check DB_FILE configuration or startup logs.",
        )
    return _db_conn


@app.on_event("startup")
def on_startup() -> None:
    """Initialize the database connection on application startup."""
    global _db_conn
    try:
        _db_conn = db_module.create_connection()
    except db_module.DatabaseConfigError:
        # Keep app running but indicate degraded mode
        _db_conn = None
        # In a real deployment, we would log this error. FastAPI will still start.
        # Clients will receive 503 on DB-dependent endpoints.
    except sqlite3.Error:
        _db_conn = None


@app.on_event("shutdown")
def on_shutdown() -> None:
    """Close the database connection on application shutdown."""
    global _db_conn
    try:
        if _db_conn is not None:
            _db_conn.close()
    finally:
        _db_conn = None


class HealthResponse(BaseModel):
    status: str = Field(..., description="Overall service status.")
    database: str = Field(..., description="Database connectivity status.")
    detail: str = Field(..., description="Additional details about the service.")


# PUBLIC_INTERFACE
@app.get(
    "/",
    summary="Health Check",
    tags=["Health"],
    response_model=HealthResponse,
)
def health_check(db: sqlite3.Connection = Depends(get_db_conn)) -> HealthResponse:
    """Health check endpoint.

    Returns overall service health, including database connectivity verification.
    Uses a lightweight PRAGMA query to verify database access.

    Parameters:
        db: Injected sqlite3 connection (via dependency). Will return 503 if DB is not available.

    Returns:
        HealthResponse: JSON object with service and DB status.
    """
    try:
        # Perform a lightweight check and ensure foreign keys remain ON
        cur = db.cursor()
        cur.execute("PRAGMA foreign_keys;")
        fk_row = cur.fetchone()
        foreign_keys_on = bool(fk_row[0]) if fk_row else False
        if not foreign_keys_on:
            # Re-enable if somehow off (should be on at connection creation)
            cur.execute("PRAGMA foreign_keys = ON;")
        cur.close()
        return HealthResponse(status="ok", database="connected", detail="Service healthy")
    except Exception as e:
        # Gracefully report DB issues
        raise HTTPException(status_code=503, detail=f"Database error: {e}")


# Auxiliary unauthenticated status endpoint that does not require DB
# PUBLIC_INTERFACE
@app.get(
    "/status",
    summary="Service Status (No DB)",
    description="Lightweight status endpoint that does not require database connectivity.",
    tags=["Health"],
)
def service_status() -> JSONResponse:
    """Return a lightweight service status without touching the DB."""
    db_state = "initialized" if _db_conn is not None else "unavailable"
    return JSONResponse({"status": "ok", "database": db_state})


# Register routers
from src.api.routes.auth import router as auth_router  # noqa: E402
from src.api.routes.users import router as users_router  # noqa: E402
from src.api.routes.games import router as games_router  # noqa: E402

app.include_router(auth_router)
app.include_router(users_router)
app.include_router(games_router)
