from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException
import sqlite3

from src.api.main import get_db_conn
from src.api.models import UserProfile, get_user_by_id, verify_token

router = APIRouter(prefix="/users", tags=["Users"])


def _extract_bearer(auth_header: Optional[str]) -> str:
    if not auth_header:
        raise HTTPException(status_code=401, detail="Authorization header missing")
    parts = auth_header.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid Authorization header")
    return parts[1]


# PUBLIC_INTERFACE
@router.get(
    "/me",
    summary="Get Current User Profile",
    description="Reads Bearer token, validates signature and expiry, then returns the user's profile.",
    response_model=UserProfile,
    responses={
        200: {"description": "User profile returned"},
        401: {"description": "Unauthorized or invalid token"},
        404: {"description": "User not found"},
    },
)
def get_me(
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    db: sqlite3.Connection = Depends(get_db_conn),
) -> UserProfile:
    """Return the authenticated user's profile.

    Parameters:
        authorization: Bearer token Authorization header.
        db: Injected sqlite3 connection.

    Returns:
        UserProfile: The user's profile.

    Raises:
        HTTPException: For invalid/expired token, missing user, or other errors.
    """
    token = _extract_bearer(authorization)
    try:
        claims = verify_token(token)
        sub = claims.get("sub")
        if sub is None:
            raise HTTPException(status_code=401, detail="Invalid token: missing subject")
        user = get_user_by_id(db, int(sub))
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return user
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Unauthorized: {e}")
