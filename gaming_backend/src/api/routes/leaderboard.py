from typing import List, Optional

import sqlite3
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from src.api.main import get_db_conn

router = APIRouter(prefix="/leaderboard", tags=["Leaderboard"])


# ====== Pydantic Models ======
class LeaderboardEntry(BaseModel):
    user_id: int = Field(..., description="User ID")
    best_score: int = Field(..., description="User's best score for the game")
    best_at: Optional[str] = Field(None, description="When the best score was recorded")
    ranks: Optional[int] = Field(None, description="Rank (1-based) when available")


class LeaderboardPage(BaseModel):
    game_id: int = Field(..., description="Game ID")
    items: List[LeaderboardEntry] = Field(..., description="Leaderboard entries")
    total_users: int = Field(..., description="Total number of users on the leaderboard")
    limit: int = Field(..., description="Page size limit used")
    offset: int = Field(..., description="Offset used for pagination")


class UserLeaderboardResponse(BaseModel):
    game_id: int = Field(..., description="Game ID")
    user_id: int = Field(..., description="Requested user ID")
    best_score: Optional[int] = Field(None, description="User best score or None if no scores")
    best_at: Optional[str] = Field(None, description="Timestamp for best score")
    rank: Optional[int] = Field(
        None,
        description="1-based rank of the user among all users for the game by best score. None if no score.",
    )


# ====== Internal helpers ======
def _validate_pagination(limit: int, offset: int) -> tuple[int, int]:
    if limit < 1:
        limit = 10
    if limit > 100:
        limit = 100
    if offset < 0:
        offset = 0
    return limit, offset


def _ensure_scores_indexes(conn: sqlite3.Connection) -> None:
    """Ensure indexes exist to optimize leaderboard queries."""
    cur = conn.cursor()
    # Matches indexes created in games.py to keep consistency
    cur.execute("CREATE INDEX IF NOT EXISTS idx_scores_game_score ON scores(game_id, score DESC, created_at DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_scores_user ON scores(user_id);")
    conn.commit()
    cur.close()


def _game_exists(conn: sqlite3.Connection, game_id: int) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM games WHERE id = ? LIMIT 1", (game_id,))
        row = cur.fetchone()
        return row is not None
    finally:
        cur.close()


# ====== Routes ======

# PUBLIC_INTERFACE
@router.get(
    "/top",
    summary="Top Leaderboard",
    description="Returns a paginated leaderboard for a game, ranking users by their best score. Uses indexes on (game_id, score DESC) for efficiency.",
    response_model=LeaderboardPage,
    responses={
        200: {"description": "Leaderboard returned"},
        404: {"description": "Game not found"},
        400: {"description": "Invalid parameters"},
    },
)
def get_top_leaderboard(
    game_id: int = Query(..., ge=1, description="Game ID"),
    limit: int = Query(10, ge=1, le=100, description="Maximum rows to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: sqlite3.Connection = Depends(get_db_conn),
) -> LeaderboardPage:
    """Return a leaderboard of best scores per user for a given game.

    Parameters:
        game_id: Game identifier.
        limit: Maximum number of rows to return (1-100).
        offset: Number of rows to skip.
        db: Injected sqlite3 connection.

    Returns:
        LeaderboardPage with items and total user count.
    """
    limit, offset = _validate_pagination(limit, offset)
    if not _game_exists(db, game_id):
        raise HTTPException(status_code=404, detail="Game not found")

    _ensure_scores_indexes(db)
    cur = db.cursor()
    try:
        # Total unique users with scores for this game
        cur.execute("SELECT COUNT(DISTINCT user_id) AS c FROM scores WHERE game_id = ?", (game_id,))
        total_users = int(cur.fetchone()["c"])

        # Best score per user with a tie-breaker on created_at (earlier best_at preferred)
        # Utilize index on (game_id, score DESC, created_at DESC)
        # SQLite lacks DISTINCT ON; use correlated subquery to fetch best record per user.
        cur.execute(
            """
            SELECT s.user_id,
                   s.score AS best_score,
                   s.created_at AS best_at
            FROM scores s
            WHERE s.game_id = ?
              AND NOT EXISTS (
                 SELECT 1 FROM scores s2
                 WHERE s2.game_id = s.game_id
                   AND s2.user_id = s.user_id
                   AND (s2.score > s.score OR (s2.score = s.score AND s2.created_at < s.created_at))
              )
            ORDER BY best_score DESC, best_at ASC, user_id ASC
            LIMIT ? OFFSET ?
            """,
            (game_id, limit, offset),
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    items = [
        LeaderboardEntry(
            user_id=row["user_id"],
            best_score=row["best_score"],
            best_at=row["best_at"],
        )
        for row in rows
    ]
    return LeaderboardPage(
        game_id=game_id,
        items=items,
        total_users=total_users,
        limit=limit,
        offset=offset,
    )


# PUBLIC_INTERFACE
@router.get(
    "/user/{user_id}",
    summary="User Leaderboard Entry",
    description="Returns a user's best score and rank for a given game. Efficiently computes rank using windowing emulation.",
    response_model=UserLeaderboardResponse,
    responses={
        200: {"description": "User leaderboard info"},
        404: {"description": "Game not found"},
        400: {"description": "Invalid parameters"},
    },
)
def get_user_leaderboard_entry(
    user_id: int,
    game_id: int = Query(..., ge=1, description="Game ID"),
    db: sqlite3.Connection = Depends(get_db_conn),
) -> UserLeaderboardResponse:
    """Return the requesting user's best score and rank for the specified game.

    Parameters:
        user_id: The user whose rank to fetch.
        game_id: Game ID.
        db: Injected sqlite3 connection.

    Returns:
        UserLeaderboardResponse with best score and rank (or None if no score).
    """
    if user_id < 1:
        raise HTTPException(status_code=400, detail="user_id must be positive")
    if not _game_exists(db, game_id):
        raise HTTPException(status_code=404, detail="Game not found")

    _ensure_scores_indexes(db)
    cur = db.cursor()
    try:
        # First, get user's best score and timestamp
        cur.execute(
            """
            SELECT s.user_id, s.score AS best_score, s.created_at AS best_at
            FROM scores s
            WHERE s.game_id = ? AND s.user_id = ?
              AND NOT EXISTS (
                SELECT 1 FROM scores s2
                WHERE s2.game_id = s.game_id
                  AND s2.user_id = s.user_id
                  AND (s2.score > s.score OR (s2.score = s.score AND s2.created_at < s.created_at))
              )
            LIMIT 1
            """,
            (game_id, user_id),
        )
        best_row = cur.fetchone()
        if not best_row:
            # User has no score; still compute a response with None values
            return UserLeaderboardResponse(
                game_id=game_id, user_id=user_id, best_score=None, best_at=None, rank=None
            )

        best_score = int(best_row["best_score"])
        best_at = best_row["best_at"]

        # Compute rank: number of users with strictly higher best score; ties are ranked by earlier best_at.
        # Rank = higher_count + 1
        # We'll count users where their best score is greater than current OR equal but achieved earlier.
        cur.execute(
            """
            WITH bests AS (
                SELECT s.user_id,
                       s.score,
                       s.created_at
                FROM scores s
                WHERE s.game_id = ?
                AND NOT EXISTS (
                    SELECT 1 FROM scores s2
                    WHERE s2.game_id = s.game_id
                      AND s2.user_id = s.user_id
                      AND (s2.score > s.score OR (s2.score = s.score AND s2.created_at < s.created_at))
                )
            )
            SELECT COUNT(1) AS higher
            FROM bests b
            WHERE (b.score > ?) OR (b.score = ? AND b.created_at < ?)
            """,
            (game_id, best_score, best_score, best_at),
        )
        rank_row = cur.fetchone()
        higher = int(rank_row["higher"]) if rank_row else 0
        rank = higher + 1

        return UserLeaderboardResponse(
            game_id=game_id, user_id=user_id, best_score=best_score, best_at=best_at, rank=rank
        )
    finally:
        cur.close()
