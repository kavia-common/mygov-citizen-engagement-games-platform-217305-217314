from __future__ import annotations

import base64
import hmac
import os
import sqlite3
import time
from hashlib import sha256
from typing import Any, Dict, Optional

from pydantic import BaseModel, EmailStr, Field


# PUBLIC_INTERFACE
def get_secret_key(env_var: str = "SECRET_KEY") -> bytes:
    """Fetch the HMAC secret key from environment variables.

    Returns:
        The secret key bytes.

    Raises:
        RuntimeError: If the SECRET_KEY env var is not set.
    """
    secret = os.getenv(env_var)
    if not secret:
        raise RuntimeError(
            f"Environment variable '{env_var}' is not set. Please configure it in your .env"
        )
    # Normalize to bytes
    return secret.encode("utf-8")


def _b64u(data: bytes) -> str:
    """URL-safe base64 without padding, to keep token compact."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64u_decode(s: str) -> bytes:
    """Decode URL-safe base64 without padding."""
    padding = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + padding)


# PUBLIC_INTERFACE
def sign_token(payload: Dict[str, Any], ttl_seconds: int = 60 * 60 * 24) -> str:
    """Create a compact HMAC-SHA256 signed token.

    The token format is: base64url(header).base64url(payload).base64url(signature)
    This is a lightweight JWT-like structure for mock authentication.

    Args:
        payload: dict containing user claims. 'exp' will be added if not present.
        ttl_seconds: time-to-live in seconds for the token.

    Returns:
        Signed token string.
    """
    secret = get_secret_key()
    header = {"alg": "HS256", "typ": "MYGOVMOCK"}
    # Add expiry if missing
    if "exp" not in payload:
        payload = dict(payload)
        payload["exp"] = int(time.time()) + ttl_seconds

    import json

    header_b = json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8")
    payload_b = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")

    header_enc = _b64u(header_b)
    payload_enc = _b64u(payload_b)
    signing_input = f"{header_enc}.{payload_enc}".encode("ascii")
    signature = hmac.new(secret, signing_input, sha256).digest()
    sig_enc = _b64u(signature)
    return f"{header_enc}.{payload_enc}.{sig_enc}"


# PUBLIC_INTERFACE
def verify_token(token: str) -> Dict[str, Any]:
    """Verify a compact HMAC-SHA256 signed token and return the payload.

    Args:
        token: token string.

    Returns:
        Decoded payload dict if signature valid and token not expired.

    Raises:
        ValueError: If token is malformed, signature invalid, or expired.
    """
    secret = get_secret_key()
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("Invalid token format")

    header_enc, payload_enc, sig_enc = parts
    signing_input = f"{header_enc}.{payload_enc}".encode("ascii")
    signature = _b64u_decode(sig_enc)
    expected_sig = hmac.new(secret, signing_input, sha256).digest()
    if not hmac.compare_digest(signature, expected_sig):
        raise ValueError("Invalid token signature")

    import json

    payload = json.loads(_b64u_decode(payload_enc))
    # Expiry check
    exp = payload.get("exp")
    if exp is None or not isinstance(exp, int):
        raise ValueError("Token missing exp")
    if time.time() > exp:
        raise ValueError("Token expired")
    return payload


# Data models for API
class MockLoginRequest(BaseModel):
    email: EmailStr = Field(..., description="User email address for mock login.")
    name: str = Field(..., description="Display name of the user.")
    avatar_url: Optional[str] = Field(
        None, description="Optional avatar URL for the user profile."
    )
    locale: Optional[str] = Field(
        None, description="Preferred locale/language tag (e.g., en-IN)."
    )


class TokenResponse(BaseModel):
    access_token: str = Field(..., description="Signed HMAC token.")
    token_type: str = Field("bearer", description="Token type.")


class UserProfile(BaseModel):
    id: int = Field(..., description="User ID.")
    email: EmailStr = Field(..., description="User email.")
    name: str = Field(..., description="Display name.")
    avatar_url: Optional[str] = Field(None, description="Avatar URL.")
    locale: Optional[str] = Field(None, description="Preferred locale.")


# Simple DAL for users table
CREATE_USERS_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    avatar_url TEXT,
    locale TEXT
);
"""


# PUBLIC_INTERFACE
def ensure_user_table(conn: sqlite3.Connection) -> None:
    """Ensure the users table exists."""
    cur = conn.cursor()
    cur.execute(CREATE_USERS_SQL)
    conn.commit()
    cur.close()


# PUBLIC_INTERFACE
def upsert_user(conn: sqlite3.Connection, email: str, name: str, avatar_url: Optional[str], locale: Optional[str]) -> int:
    """Insert or update a user row, returning the user ID.

    Args:
        conn: sqlite connection
        email: user email (unique)
        name: user name
        avatar_url: optional avatar
        locale: optional locale

    Returns:
        int: user id
    """
    ensure_user_table(conn)
    cur = conn.cursor()
    # Try insert; if exists, update then fetch id
    cur.execute(
        """
        INSERT INTO users (email, name, avatar_url, locale)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(email) DO UPDATE SET
            name=excluded.name,
            avatar_url=excluded.avatar_url,
            locale=excluded.locale
        """,
        (email, name, avatar_url, locale),
    )
    # Fetch id
    cur.execute("SELECT id FROM users WHERE email = ?", (email,))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    return int(row["id"]) if row else -1


# PUBLIC_INTERFACE
def get_user_by_id(conn: sqlite3.Connection, user_id: int) -> Optional[UserProfile]:
    """Fetch a user by id.

    Args:
        conn: sqlite connection
        user_id: user id

    Returns:
        Optional[UserProfile]: profile if found.
    """
    ensure_user_table(conn)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, email, name, avatar_url, locale FROM users WHERE id = ?",
        (user_id,),
    )
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    return UserProfile(
        id=row["id"],
        email=row["email"],
        name=row["name"],
        avatar_url=row["avatar_url"],
        locale=row["locale"],
    )
