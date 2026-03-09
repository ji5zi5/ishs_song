from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from radio_app.db import DB
from radio_app.services.rounds import close_round


class RoundAutoCloser:
    def __init__(
        self,
        db: DB,
        artifacts_dir: Path,
        interval_seconds: int = 30,
        file_retention_seconds: int = 1800,
        ffmpeg_path: str | None = None,
        uploads_dir: Path | None = None,
        yt_dlp_enabled: bool = True,
    ) -> None:
        self._db = db
        self._artifacts_dir = artifacts_dir
        self._interval_seconds = max(5, interval_seconds)
        self._file_retention_seconds = max(0, file_retention_seconds)
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
            self._prune_expired_files(conn)

    def _prune_expired_files(self, conn: sqlite3.Connection) -> None:
        if self._file_retention_seconds <= 0:
            return

        cutoff = (
            datetime.now(UTC) - timedelta(seconds=self._file_retention_seconds)
        ).replace(microsecond=0).isoformat().replace("+00:00", "Z")

        deleted_audio_rows = 0
        deleted_audio_files = 0
        if self._uploads_dir is not None:
            audio_rows = conn.execute(
                """
                SELECT id, file_path
                FROM audio_assets
                WHERE uploaded_at < ?
                """,
                (cutoff,),
            ).fetchall()
            for row in audio_rows:
                if self._safe_unlink(Path(str(row["file_path"])), self._uploads_dir):
                    deleted_audio_files += 1
                conn.execute("DELETE FROM audio_assets WHERE id = ?", (int(row["id"]),))
                deleted_audio_rows += 1

        deleted_artifact_rows = 0
        deleted_artifact_files = 0
        artifact_rows = conn.execute(
            """
            SELECT id, m3u_path, mp3_path
            FROM round_artifacts
            WHERE created_at < ?
            """,
            (cutoff,),
        ).fetchall()
        for row in artifact_rows:
            deleted_artifact_files += int(self._safe_unlink(Path(str(row["m3u_path"])), self._artifacts_dir))
            deleted_artifact_files += int(self._safe_unlink(Path(str(row["mp3_path"])), self._artifacts_dir))
            conn.execute("DELETE FROM round_artifacts WHERE id = ?", (int(row["id"]),))
            deleted_artifact_rows += 1

        if deleted_audio_rows or deleted_artifact_rows:
            conn.execute(
                """
                INSERT INTO audit_logs(action, detail, created_at)
                VALUES (?, ?, datetime('now'))
                """,
                (
                    "retention_cleanup",
                    json.dumps(
                        {
                            "cutoff": cutoff,
                            "audio_assets_deleted": deleted_audio_rows,
                            "audio_files_deleted": deleted_audio_files,
                            "round_artifacts_deleted": deleted_artifact_rows,
                            "artifact_files_deleted": deleted_artifact_files,
                        },
                        ensure_ascii=False,
                    ),
                ),
            )

    @staticmethod
    def _safe_unlink(file_path: Path, root_dir: Path) -> bool:
        try:
            resolved_path = file_path.resolve()
            resolved_root = root_dir.resolve()
            resolved_path.relative_to(resolved_root)
        except (FileNotFoundError, ValueError):
            return False
        try:
            resolved_path.unlink(missing_ok=True)
        except OSError:
            return False
        return True
