from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterator


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_after_hours_iso(hours: int) -> str:
    return (datetime.now(UTC) + timedelta(hours=hours)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass
class DB:
    path: Path

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @contextmanager
    def session(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_schema(self) -> None:
        with self.session() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    riro_user_key TEXT NOT NULL UNIQUE,
                    display_name TEXT NOT NULL,
                    is_admin_approved INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sessions (
                    token TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    expires_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS rounds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cadence TEXT NOT NULL CHECK (cadence IN ('weekly', 'monthly')),
                    status TEXT NOT NULL CHECK (status IN ('open', 'closing', 'closed')),
                    start_at TEXT NOT NULL,
                    end_at TEXT NOT NULL,
                    playlist_size INTEGER NOT NULL DEFAULT 12,
                    target_seconds INTEGER NOT NULL DEFAULT 2400,
                    loudnorm_enabled INTEGER NOT NULL DEFAULT 1,
                    close_job_key TEXT,
                    created_at TEXT NOT NULL,
                    closed_at TEXT
                );

                CREATE UNIQUE INDEX IF NOT EXISTS uq_rounds_close_job_key
                    ON rounds(close_job_key) WHERE close_job_key IS NOT NULL;

                CREATE TABLE IF NOT EXISTS songs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    spotify_track_id TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    artist TEXT NOT NULL,
                    album_art_url TEXT,
                    external_url TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS submissions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    round_id INTEGER NOT NULL REFERENCES rounds(id) ON DELETE CASCADE,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    song_id INTEGER NOT NULL REFERENCES songs(id) ON DELETE CASCADE,
                    is_hidden INTEGER NOT NULL DEFAULT 0,
                    submitted_at TEXT NOT NULL,
                    UNIQUE(round_id, song_id)
                );

                CREATE TABLE IF NOT EXISTS votes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    round_id INTEGER NOT NULL REFERENCES rounds(id) ON DELETE CASCADE,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    submission_id INTEGER NOT NULL REFERENCES submissions(id) ON DELETE CASCADE,
                    voted_at TEXT NOT NULL,
                    UNIQUE(round_id, user_id, submission_id)
                );

                CREATE TABLE IF NOT EXISTS audio_assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    song_id INTEGER NOT NULL UNIQUE REFERENCES songs(id) ON DELETE CASCADE,
                    file_path TEXT NOT NULL,
                    duration_seconds INTEGER NOT NULL DEFAULT 0,
                    is_valid INTEGER NOT NULL DEFAULT 1,
                    validation_error TEXT,
                    uploaded_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS round_artifacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    round_id INTEGER NOT NULL UNIQUE REFERENCES rounds(id) ON DELETE CASCADE,
                    m3u_path TEXT NOT NULL,
                    mp3_path TEXT NOT NULL,
                    total_seconds INTEGER NOT NULL,
                    generation_log TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    round_id INTEGER REFERENCES rounds(id) ON DELETE SET NULL,
                    actor_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                    action TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS rate_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    action TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            self._ensure_round_close_columns(conn)
            self._seed_defaults(conn)

    def _ensure_round_close_columns(self, conn: sqlite3.Connection) -> None:
        columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(rounds)").fetchall()}
        additions = {
            "close_phase": "ALTER TABLE rounds ADD COLUMN close_phase TEXT",
            "close_message": "ALTER TABLE rounds ADD COLUMN close_message TEXT",
            "close_progress": "ALTER TABLE rounds ADD COLUMN close_progress INTEGER NOT NULL DEFAULT 0",
            "close_started_at": "ALTER TABLE rounds ADD COLUMN close_started_at TEXT",
            "close_finished_at": "ALTER TABLE rounds ADD COLUMN close_finished_at TEXT",
            "close_error": "ALTER TABLE rounds ADD COLUMN close_error TEXT",
        }
        for name, sql in additions.items():
            if name not in columns:
                conn.execute(sql)

    def _seed_defaults(self, conn: sqlite3.Connection) -> None:
        defaults = {
            "round_default_cadence": "monthly",
            "default_playlist_size": "12",
            "default_target_seconds": "2400",
            "default_loudnorm_enabled": "1",
        }
        for key, value in defaults.items():
            conn.execute(
                "INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)",
                (key, value),
            )
