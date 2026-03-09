from __future__ import annotations

import secrets
import sqlite3
from dataclasses import dataclass

from radio_app.db import utc_after_hours_iso


@dataclass
class AuthUser:
    id: int
    riro_user_key: str
    display_name: str
    is_admin_approved: bool


def create_or_update_user(conn: sqlite3.Connection, riro_user_key: str, display_name: str) -> AuthUser:
    conn.execute(
        """
        INSERT INTO users(riro_user_key, display_name, created_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(riro_user_key)
        DO UPDATE SET display_name = excluded.display_name
        """,
        (riro_user_key, display_name),
    )
    row = conn.execute(
        "SELECT id, riro_user_key, display_name, is_admin_approved FROM users WHERE riro_user_key = ?",
        (riro_user_key,),
    ).fetchone()
    if row is None:
        raise RuntimeError("failed to load user after upsert")
    return AuthUser(
        id=int(row["id"]),
        riro_user_key=str(row["riro_user_key"]),
        display_name=str(row["display_name"]),
        is_admin_approved=bool(row["is_admin_approved"]),
    )


def issue_session(conn: sqlite3.Connection, user_id: int, ttl_hours: int) -> str:
    token = secrets.token_urlsafe(32)
    conn.execute(
        "INSERT INTO sessions(token, user_id, expires_at) VALUES (?, ?, ?)",
        (token, user_id, utc_after_hours_iso(ttl_hours)),
    )
    return token


def authenticate_session(conn: sqlite3.Connection, token: str | None) -> AuthUser | None:
    if not token:
        return None
    row = conn.execute(
        """
        SELECT u.id, u.riro_user_key, u.display_name, u.is_admin_approved
        FROM sessions s
        JOIN users u ON u.id = s.user_id
        WHERE s.token = ? AND s.expires_at > datetime('now')
        """,
        (token,),
    ).fetchone()
    if row is None:
        return None
    return AuthUser(
        id=int(row["id"]),
        riro_user_key=str(row["riro_user_key"]),
        display_name=str(row["display_name"]),
        is_admin_approved=bool(row["is_admin_approved"]),
    )


def revoke_session(conn: sqlite3.Connection, token: str) -> None:
    conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
