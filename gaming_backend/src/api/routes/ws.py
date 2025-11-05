from __future__ import annotations

import asyncio
from typing import Any, Dict, Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

router = APIRouter(prefix="/ws", tags=["WebSocket"])


class LeaderboardBroadcast(BaseModel):
    """Payload shape for leaderboard update broadcasts sent to clients."""
    event: str = Field(..., description="Event type identifier, e.g., 'leaderboard_update'")
    game_id: int = Field(..., description="Game ID for which the update applies")
    payload: Dict[str, Any] = Field(
        default_factory=dict, description="Arbitrary payload with update details"
    )


# A simple in-memory connection registry per game_id for targeted broadcasts.
# For small scale demos this is acceptable; for production consider a pub/sub layer.
_connections_by_game: Dict[int, Set[WebSocket]] = {}
# A general set of all connections listening to all leaderboard updates if no game filter is provided.
_all_connections: Set[WebSocket] = set()
# Async lock to protect registry updates across tasks
_registry_lock = asyncio.Lock()


async def _register_connection(ws: WebSocket, game_id: int | None) -> None:
    """Register a WebSocket connection, optionally scoped to a game."""
    async with _registry_lock:
        if game_id is None:
            _all_connections.add(ws)
        else:
            _connections_by_game.setdefault(game_id, set()).add(ws)


async def _unregister_connection(ws: WebSocket, game_id: int | None) -> None:
    """Remove a WebSocket connection from registries."""
    async with _registry_lock:
        if game_id is None:
            _all_connections.discard(ws)
        else:
            bucket = _connections_by_game.get(game_id)
            if bucket is not None:
                bucket.discard(ws)
                if not bucket:
                    # Clean up empty bucket
                    _connections_by_game.pop(game_id, None)


async def _safe_send(ws: WebSocket, data: str) -> bool:
    """Attempt to send, returning False if the socket is closed/broken."""
    try:
        await ws.send_text(data)
        return True
    except Exception:
        return False


# PUBLIC_INTERFACE
async def notify_leaderboard_update(game_id: int, update: Dict[str, Any]) -> None:
    """Broadcast a leaderboard update to all interested WebSocket clients.

    Sends to:
      - all connections subscribed to the specific game_id
      - all global connections subscribed to all updates

    Args:
        game_id: The game identifier whose leaderboard changed.
        update: Arbitrary payload with update details (e.g., newly inserted score and stats).
    """
    message = LeaderboardBroadcast(event="leaderboard_update", game_id=game_id, payload=update)
    payload_str = message.model_dump_json()

    # Collect recipients under lock to avoid iterating while mutating
    async with _registry_lock:
        game_specific = set(_connections_by_game.get(game_id, set()))
        global_listeners = set(_all_connections)

    # Send to game-specific first, then globals (avoid duplicate sends)
    delivered: Set[WebSocket] = set()
    for ws in list(game_specific):
        if await _safe_send(ws, payload_str):
            delivered.add(ws)
    for ws in list(global_listeners):
        if ws in delivered:
            continue
        await _safe_send(ws, payload_str)


# PUBLIC_INTERFACE
@router.websocket(
    "/leaderboard",
)
async def leaderboard_ws(websocket: WebSocket) -> None:
    """WebSocket endpoint to receive real-time leaderboard updates.

    Query parameters:
        game_id (optional): If provided, subscribe only to updates for this game.
                            If omitted, receive updates for all games.

    Message protocol:
        - Server sends JSON strings in the shape of LeaderboardBroadcast:
            {
              "event": "leaderboard_update",
              "game_id": 123,
              "payload": { ... update fields ... }
            }
        - Clients are not required to send messages; any text received will be ignored
          except for "ping" which is replied with "pong" to help keepalive.

    Close behavior:
        - When client disconnects, it is unregistered from broadcasts.

    Notes:
        - This in-memory implementation is suitable for single-process deployments. To scale
          horizontally, use a shared pub/sub (e.g., Redis) and a proper connection manager.
    """
    await websocket.accept()
    # Optional filter for a specific game
    raw_game_id = websocket.query_params.get("game_id")
    try:
        game_filter = int(raw_game_id) if raw_game_id is not None else None
    except ValueError:
        # If malformed, default to global subscription
        game_filter = None

    await _register_connection(websocket, game_filter)

    try:
        # Basic keep-alive loop: listen for pings, otherwise ignore
        while True:
            try:
                data = await websocket.receive_text()
                if data.strip().lower() == "ping":
                    await websocket.send_text("pong")
                # Ignore other client messages for now
            except WebSocketDisconnect:
                break
            except Exception:
                # If receive fails for other reasons, break so we unregister cleanly
                break
    finally:
        await _unregister_connection(websocket, game_filter)
