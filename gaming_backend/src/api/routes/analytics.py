from typing import Any, Dict, Optional

import json
import sqlite3
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from src.api.main import get_db_conn

router = APIRouter(prefix="/analytics", tags=["Analytics"])


# ====== Pydantic Models ======
class AnalyticsEvent(BaseModel):
    user_id: Optional[int] = Field(None, ge=1, description="Optional user ID")
    game_id: Optional[int] = Field(None, ge=1, description="Optional game ID")
    event_name: str = Field(..., min_length=1, max_length=128, description="Event name")
    properties: Optional[Dict[str, Any]] = Field(
        default=None, description="Arbitrary JSON properties for the event"
    )


class AnalyticsIngestResponse(BaseModel):
    id: int = Field(..., description="Inserted analytics event row ID")
    status: str = Field(..., description="Status of ingestion (e.g., accepted)")
    detail: str = Field(..., description="Additional details")


# ====== Internal helpers ======
CREATE_ANALYTICS_SQL = """
CREATE TABLE IF NOT EXISTS analytics_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    game_id INTEGER,
    event_name TEXT NOT NULL,
    properties TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_ANALYTICS_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_analytics_eventname ON analytics_events(event_name);",
    "CREATE INDEX IF NOT EXISTS idx_analytics_game ON analytics_events(game_id, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_analytics_user ON analytics_events(user_id, created_at DESC);",
)


def _ensure_analytics_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(CREATE_ANALYTICS_SQL)
    for stmt in CREATE_ANALYTICS_INDEXES:
        cur.execute(stmt)
    conn.commit()
    cur.close()


def _game_exists(conn: sqlite3.Connection, game_id: int) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM games WHERE id = ? LIMIT 1", (game_id,))
        return cur.fetchone() is not None
    finally:
        cur.close()


# ====== Routes ======

# PUBLIC_INTERFACE
@router.post(
    "/event",
    summary="Record Analytics Event",
    description="Ingests an analytics event with optional user_id and game_id. Ensures schema and uses indexes for later analysis/aggregation.",
    response_model=AnalyticsIngestResponse,
    responses={
        200: {"description": "Event ingested"},
        400: {"description": "Invalid input"},
        503: {"description": "Database unavailable"},
    },
)
def record_event(payload: AnalyticsEvent, db: sqlite3.Connection = Depends(get_db_conn)) -> AnalyticsIngestResponse:
    """Record an analytics event.

    Parameters:
        payload: AnalyticsEvent containing event_name and optional user_id, game_id, properties.
        db: Injected sqlite3 connection.

    Returns:
        AnalyticsIngestResponse with inserted ID and status.

    Raises:
        HTTPException: If validation fails or DB errors occur.
    """
    _ensure_analytics_schema(db)

    # Basic validations
    if payload.user_id is not None and payload.user_id < 1:
        raise HTTPException(status_code=400, detail="user_id must be positive when provided")
    if payload.game_id is not None:
        if payload.game_id < 1:
            raise HTTPException(status_code=400, detail="game_id must be positive when provided")
        # Optionally verify game exists to avoid orphan references
        if not _game_exists(db, payload.game_id):
            raise HTTPException(status_code=400, detail="game_id does not reference an existing game")

    # Serialize properties safely
    properties_str: Optional[str] = None
    if payload.properties is not None:
        try:
            properties_str = json.dumps(payload.properties, separators=(",", ":"), ensure_ascii=False)
        except Exception:
            raise HTTPException(status_code=400, detail="properties must be serializable JSON")

    cur = db.cursor()
    try:
        cur.execute(
            """
            INSERT INTO analytics_events (user_id, game_id, event_name, properties)
            VALUES (?, ?, ?, ?)
            """,
            (payload.user_id, payload.game_id, payload.event_name, properties_str),
        )
        inserted_id = int(cur.lastrowid)
        db.commit()
        return AnalyticsIngestResponse(id=inserted_id, status="accepted", detail="Event recorded")
    except sqlite3.IntegrityError as e:
        raise HTTPException(status_code=400, detail=f"Invalid data: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to record event: {e}")
    finally:
        cur.close()
