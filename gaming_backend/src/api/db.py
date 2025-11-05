import os
import sqlite3
from contextlib import contextmanager
from typing import Generator, Optional


class DatabaseConfigError(Exception):
    """Raised when the database configuration is invalid or missing."""


# PUBLIC_INTERFACE
def get_db_file_from_env(env_var: str = "DB_FILE") -> str:
    """Get the SQLite database file path from environment variable.

    This function reads the DB file path from the provided environment variable.
    It validates that the environment variable is set and that the file exists.

    Args:
        env_var: The environment variable name to read the DB file path from. Defaults to "DB_FILE".

    Returns:
        The path to the SQLite database file.

    Raises:
        DatabaseConfigError: If the environment variable is missing or the file does not exist.
    """
    db_file = os.getenv(env_var)
    if not db_file:
        raise DatabaseConfigError(
            f"Environment variable '{env_var}' is not set. Please configure it in your .env."
        )
    if not os.path.exists(db_file):
        raise DatabaseConfigError(
            f"SQLite database file not found at '{db_file}'. Ensure the file exists or adjust '{env_var}'."
        )
    return db_file


def _connect(db_file: str) -> sqlite3.Connection:
    """Create a SQLite connection with safe defaults and PRAGMAs set."""
    conn = sqlite3.connect(db_file, check_same_thread=False)
    # Row factory to access columns by name
    conn.row_factory = sqlite3.Row
    # Enforce foreign key constraints
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# PUBLIC_INTERFACE
def create_connection(db_file: Optional[str] = None) -> sqlite3.Connection:
    """Create and return a SQLite connection using the configured DB file.

    Args:
        db_file: Optional path to the SQLite DB file. If not provided, it will be
                 read from the DB_FILE environment variable.

    Returns:
        A live sqlite3.Connection instance with foreign keys enforcement enabled.

    Raises:
        DatabaseConfigError: If configuration is missing or the DB file does not exist.
        sqlite3.Error: If the connection cannot be established.
    """
    if db_file is None:
        db_file = get_db_file_from_env()
    return _connect(db_file)


# PUBLIC_INTERFACE
@contextmanager
def db_session(conn: sqlite3.Connection) -> Generator[sqlite3.Cursor, None, None]:
    """Context manager that yields a cursor and safely commits/rolls back.

    Ensures the connection is committed on success or rolled back on failure.

    Args:
        conn: An active sqlite3.Connection.

    Yields:
        sqlite3.Cursor for executing queries.

    Raises:
        sqlite3.Error: If a database error occurs.
    """
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
