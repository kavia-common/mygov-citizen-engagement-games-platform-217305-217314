from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException
import sqlite3

from src.api.main import get_db_conn
from src.api.models import MockLoginRequest, TokenResponse, sign_token, upsert_user

router = APIRouter(prefix="/auth", tags=["Auth"])


# PUBLIC_INTERFACE
@router.post(
    "/mock-login",
    summary="Mock Login",
    description="Issues an HMAC signed token and upserts the user into the users table. Intended for development/testing.",
    response_model=TokenResponse,
    responses={
        200: {"description": "Successful token issuance"},
        400: {"description": "Invalid request"},
        500: {"description": "Server error"},
    },
)
def mock_login(payload: MockLoginRequest, db: sqlite3.Connection = Depends(get_db_conn)) -> TokenResponse:
    """Mock login endpoint.

    Parameters:
        payload: MockLoginRequest containing email, name, optional avatar_url and locale.
        db: Injected sqlite3 connection.

    Returns:
        TokenResponse with a bearer token.

    Raises:
        HTTPException: If the database operation fails or misconfigured secret.
    """
    try:
        user_id = upsert_user(
            db,
            email=payload.email,
            name=payload.name,
            avatar_url=payload.avatar_url,
            locale=payload.locale,
        )
        claims: Dict[str, Any] = {
            "sub": str(user_id),
            "email": payload.email,
            "name": payload.name,
        }
        token = sign_token(claims)
        return TokenResponse(access_token=token, token_type="bearer")
    except RuntimeError as e:
        # Secret key missing, etc.
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Login failed: {e}")
