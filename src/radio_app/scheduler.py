from __future__ import annotations

import sqlite3
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

from radio_app.db import DB
from radio_app.services.rounds import close_round


class RoundAutoCloser:
    def __init__(self, db: DB, artifacts_dir: Path, interval_seconds: int = 30, ffmpeg_path: str | None = None, uploads_dir: Path | None = None, yt_dlp_enabled: bool = True) -> None:
        self._db = db
        self._artifacts_dir = artifacts_dir
        self._interval_seconds = max(5, interval_seconds)
        self._ffmpeg_path = ffmpeg_path
        self._uploads_dir = uploads_dir
        self._yt_dlp_enabled = yt_dlp_enabled
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run_loop, name="round-auto-closer", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception:
                # Keep scheduler alive; errors are intentionally swallowed here.
                pass
            self._stop.wait(self._interval_seconds)

    def _tick(self) -> None:
        now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        with self._db.session() as conn:
            due_rounds = conn.execute(
                "SELECT id FROM rounds WHERE status = 'open' AND end_at <= ?",
                (now,),
            ).fetchall()
            for row in due_rounds:
                try:
                    close_round(conn, int(row["id"]), self._artifacts_dir, uploads_dir=self._uploads_dir, ffmpeg_path=self._ffmpeg_path, yt_dlp_enabled=self._yt_dlp_enabled)
                except Exception:
                    conn.execute(
                        """
                        INSERT INTO audit_logs(round_id, action, detail, created_at)
                        VALUES (?, ?, ?, datetime('now'))
                        """,
                        (int(row["id"]), "auto_close_failed", "scheduler-close-error"),
                    )
