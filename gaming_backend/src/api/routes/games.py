from typing import Any, Dict, List, Literal, Optional, Tuple

import sqlite3
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from src.api.main import get_db_conn

router = APIRouter(prefix="/games", tags=["Games"])


# ====== Pydantic Models ======

class Game(BaseModel):
    id: int = Field(..., description="Game ID")
    slug: str = Field(..., description="Unique game slug")
    name: str = Field(..., description="Display name for the game")
    description: Optional[str] = Field(None, description="Game description")
    created_at: Optional[str] = Field(None, description="Creation timestamp")


class GameListResponse(BaseModel):
    items: List[Game] = Field(..., description="List of games in the current page")
    total: int = Field(..., description="Total games count")
    page: int = Field(..., description="Current page (1-based)")
    size: int = Field(..., description="Page size")


class ScoreItem(BaseModel):
    id: int = Field(..., description="Score row ID")
    user_id: int = Field(..., description="User ID who achieved this score")
    score: int = Field(..., description="Score value")
    created_at: Optional[str] = Field(None, description="When the score was recorded")


class ScoresResponse(BaseModel):
    game: Game = Field(..., description="Game info")
    items: List[ScoreItem] = Field(..., description="List of scores")
    mode: Literal["recent", "top"] = Field(..., description="Selected mode")
    limit: int = Field(..., description="Limit used")


class PostScoreRequest(BaseModel):
    user_id: int = Field(..., ge=1, description="User ID")
    score: int = Field(..., ge=0, description="Score to record (non-negative)")

    @field_validator("score")
    @classmethod
    def _score_int(cls, v: int) -> int:
        if int(v) != v:
            raise ValueError("score must be integer")
        return int(v)


class PostScoreResponse(BaseModel):
    game: Game = Field(..., description="Game info")
    inserted: ScoreItem = Field(..., description="Inserted score row")
    stats: Dict[str, Any] = Field(
        ..., description="Updated stats such as top score, total scores count"
    )


# ====== Internal helpers: schema and indices ======

CREATE_GAMES_SQL = """
CREATE TABLE IF NOT EXISTS games (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_SCORES_SQL = """
CREATE TABLE IF NOT EXISTS scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    score INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE
);
"""

# Helpful indices for lookups and leaderboards
CREATE_INDEXES_SQL: Tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_games_slug ON games(slug);",
    "CREATE INDEX IF NOT EXISTS idx_scores_game_created ON scores(game_id, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_scores_game_score ON scores(game_id, score DESC, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_scores_user ON scores(user_id);",
)


def _ensure_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(CREATE_GAMES_SQL)
    cur.execute(CREATE_SCORES_SQL)
    for stmt in CREATE_INDEXES_SQL:
        cur.execute(stmt)
    conn.commit()
    cur.close()


def _row_to_game(row: sqlite3.Row) -> Game:
    return Game(
        id=row["id"],
        slug=row["slug"],
        name=row["name"],
        description=row["description"],
        created_at=row["created_at"],
    )


def _row_to_score(row: sqlite3.Row) -> ScoreItem:
    return ScoreItem(
        id=row["id"],
        user_id=row["user_id"],
        score=row["score"],
        created_at=row["created_at"],
    )


def _get_game_by_id(conn: sqlite3.Connection, game_id: int) -> Optional[Game]:
    _ensure_schema(conn)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, slug, name, description, created_at FROM games WHERE id = ?",
        (game_id,),
    )
    row = cur.fetchone()
    cur.close()
    return _row_to_game(row) if row else None


def _paginate(offset: int, limit: int) -> Tuple[int, int]:
    if limit < 1:
        limit = 10
    if limit > 100:
        limit = 100
    if offset < 0:
        offset = 0
    return offset, limit


# ====== Routes ======

# PUBLIC_INTERFACE
@router.get(
    "",
    summary="List Games",
    description="Returns a paginated list of games. Index on slug helps auxiliary lookups.",
    response_model=GameListResponse,
    responses={
        200: {"description": "List of games with total count"},
    },
)
def list_games(
    page: int = Query(1, ge=1, description="1-based page number"),
    size: int = Query(10, ge=1, le=100, description="Page size"),
    db: sqlite3.Connection = Depends(get_db_conn),
) -> GameListResponse:
    """List games with simple pagination."""
    _ensure_schema(db)
    offset = (page - 1) * size
    offset, size = _paginate(offset, size)

    cur = db.cursor()
    cur.execute("SELECT COUNT(1) AS c FROM games")
    total = int(cur.fetchone()["c"])

    cur.execute(
        """
        SELECT id, slug, name, description, created_at
        FROM games
        ORDER BY created_at DESC, id DESC
        LIMIT ? OFFSET ?
        """,
        (size, offset),
    )
    rows = cur.fetchall()
    cur.close()

    items = [_row_to_game(r) for r in rows]
    return GameListResponse(items=items, total=total, page=page, size=size)


# PUBLIC_INTERFACE
@router.get(
    "/{game_id}",
    summary="Get Game by ID",
    description="Returns game details for the provided ID.",
    response_model=Game,
    responses={
        200: {"description": "Game found"},
        404: {"description": "Game not found"},
    },
)
def get_game(game_id: int, db: sqlite3.Connection = Depends(get_db_conn)) -> Game:
    """Fetch a single game by ID."""
    game = _get_game_by_id(db, game_id)
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    return game


# PUBLIC_INTERFACE
@router.get(
    "/{game_id}/scores",
    summary="Get Scores for a Game",
    description="Returns recent or top scores for a game. Uses indices for efficient ordering.",
    response_model=ScoresResponse,
    responses={
        200: {"description": "Scores returned"},
        404: {"description": "Game not found"},
        400: {"description": "Invalid mode or params"},
    },
)
def get_game_scores(
    game_id: int,
    mode: Literal["recent", "top"] = Query(
        "recent", description="Select 'recent' for latest scores or 'top' for highest"
    ),
    limit: int = Query(10, ge=1, le=100, description="Max number of scores to return"),
    db: sqlite3.Connection = Depends(get_db_conn),
) -> ScoresResponse:
    """Return a list of scores for a game."""
    game = _get_game_by_id(db, game_id)
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    _ensure_schema(db)
    cur = db.cursor()

    if mode == "recent":
        # Uses idx_scores_game_created
        cur.execute(
            """
            SELECT id, user_id, score, created_at
            FROM scores
            WHERE game_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (game_id, limit),
        )
    else:
        # mode == "top" uses idx_scores_game_score
        cur.execute(
            """
            SELECT id, user_id, score, created_at
            FROM scores
            WHERE game_id = ?
            ORDER BY score DESC, created_at DESC, id DESC
            LIMIT ?
            """,
            (game_id, limit),
        )

    rows = cur.fetchall()
    cur.close()
    items = [_row_to_score(r) for r in rows]
    return ScoresResponse(game=game, items=items, mode=mode, limit=limit)


def _insert_score(
    conn: sqlite3.Connection, game_id: int, user_id: int, score: int
) -> ScoreItem:
    _ensure_schema(conn)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO scores (game_id, user_id, score) VALUES (?, ?, ?)",
        (game_id, user_id, score),
    )
    inserted_id = cur.lastrowid
    cur.execute(
        "SELECT id, user_id, score, created_at FROM scores WHERE id = ?",
        (inserted_id,),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    return _row_to_score(row)


def _score_stats(conn: sqlite3.Connection, game_id: int) -> Dict[str, Any]:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) AS c FROM scores WHERE game_id = ?", (game_id,))
    total = int(cur.fetchone()["c"])

    cur.execute(
        """
        SELECT id, user_id, score, created_at
        FROM scores
        WHERE game_id = ?
        ORDER BY score DESC, created_at DESC, id DESC
        LIMIT 1
        """,
        (game_id,),
    )
    best = cur.fetchone()
    cur.close()
    best_item = _row_to_score(best) if best else None
    return {
        "total_scores": total,
        "top_score": best_item.model_dump() if best_item else None,
    }


# PUBLIC_INTERFACE
@router.post(
    "/{game_id}/score",
    summary="Submit Score for a Game",
    description="Inserts a new score row for the given game and returns updated info.",
    response_model=PostScoreResponse,
    responses={
        200: {"description": "Score inserted successfully"},
        400: {"description": "Invalid inputs"},
        404: {"description": "Game not found"},
        503: {"description": "Database unavailable"},
    },
)
def post_game_score(
    game_id: int, payload: PostScoreRequest, db: sqlite3.Connection = Depends(get_db_conn)
) -> PostScoreResponse:
    """Insert a score for the game and return updated stats."""
    if payload.user_id < 1:
        raise HTTPException(status_code=400, detail="user_id must be positive")
    if payload.score < 0:
        raise HTTPException(status_code=400, detail="score must be non-negative")

    game = _get_game_by_id(db, game_id)
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    try:
        inserted = _insert_score(db, game_id, payload.user_id, payload.score)
        stats = _score_stats(db, game_id)
        return PostScoreResponse(game=game, inserted=inserted, stats=stats)
    except sqlite3.IntegrityError as e:
        # likely foreign key or constraints; though users table is not FK here
        raise HTTPException(status_code=400, detail=f"Invalid data: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to record score: {e}")
