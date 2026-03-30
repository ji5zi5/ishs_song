from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from radio_app.db import DB
from radio_app.services.rounds import close_round

LOGGER = logging.getLogger(__name__)


def _is_database_locked(exc: BaseException) -> bool:
    return isinstance(exc, sqlite3.OperationalError) and "database is locked" in str(exc).lower()


class RoundAutoCloser:
    def __init__(
        self,
        db: DB,
        artifacts_dir: Path,
        interval_seconds: int = 30,
        file_retention_seconds: int = 86400,
        audit_log_retention_days: int = 30,
        ffmpeg_path: str | None = None,
        uploads_dir: Path | None = None,
        yt_dlp_enabled: bool = True,
    ) -> None:
        self._db = db
        self._artifacts_dir = artifacts_dir
        self._interval_seconds = max(5, interval_seconds)
        self._file_retention_seconds = max(0, file_retention_seconds)
        self._audit_log_retention_days = max(0, audit_log_retention_days)
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
            except sqlite3.OperationalError as exc:
                if _is_database_locked(exc):
                    LOGGER.warning("Scheduler tick skipped due to database lock")
                else:
                    LOGGER.exception("Scheduler tick failed")
                    self._record_scheduler_event(
                        "scheduler_loop_failed",
                        {
                            "stage": "tick",
                            "error_type": type(exc).__name__,
                            "error": str(exc) or repr(exc),
                        },
                    )
            except Exception as exc:
                LOGGER.exception("Scheduler tick failed")
                self._record_scheduler_event(
                    "scheduler_loop_failed",
                    {
                        "stage": "tick",
                        "error_type": type(exc).__name__,
                        "error": str(exc) or repr(exc),
                    },
                )
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
                except Exception as exc:
                    LOGGER.exception("Automatic round close failed", extra={"round_id": int(row["id"])})
                    conn.execute(
                        """
                        INSERT INTO audit_logs(round_id, action, detail, created_at)
                        VALUES (?, ?, ?, datetime('now'))
                        """,
                        (
                            int(row["id"]),
                            "auto_close_failed",
                            json.dumps(
                                {
                                    "round_id": int(row["id"]),
                                    "stage": "close-round",
                                    "error_type": type(exc).__name__,
                                    "error": str(exc) or repr(exc),
                                },
                                ensure_ascii=False,
                            ),
                        ),
                    )
            self._prune_expired_files(conn)

    def _prune_expired_files(self, conn: sqlite3.Connection) -> None:
        if self._file_retention_seconds <= 0 and self._audit_log_retention_days <= 0:
            return

        latest_artifact_id_row = conn.execute(
            "SELECT id FROM round_artifacts ORDER BY created_at DESC, id DESC LIMIT 1"
        ).fetchone()
        protected_artifact_id = int(latest_artifact_id_row["id"]) if latest_artifact_id_row is not None else None
        protected_song_ids = {
            int(row["song_id"])
            for row in conn.execute(
                "SELECT song_id FROM round_artifact_tracks WHERE artifact_id = ?",
                (protected_artifact_id,),
            ).fetchall()
        } if protected_artifact_id is not None else set()

        file_cutoff = (
            datetime.now(UTC) - timedelta(seconds=self._file_retention_seconds)
        ).replace(microsecond=0).isoformat().replace("+00:00", "Z")

        deleted_audio_rows = 0
        deleted_audio_files = 0
        if self._file_retention_seconds > 0 and self._uploads_dir is not None:
            audio_rows = conn.execute(
                """
                SELECT id, song_id, file_path
                FROM audio_assets
                WHERE uploaded_at < ?
                """,
                (file_cutoff,),
            ).fetchall()
            for row in audio_rows:
                if int(row["song_id"]) in protected_song_ids:
                    continue
                if self._safe_unlink(Path(str(row["file_path"])), self._uploads_dir):
                    deleted_audio_files += 1
                conn.execute("DELETE FROM audio_assets WHERE id = ?", (int(row["id"]),))
                deleted_audio_rows += 1

        deleted_manual_rows = 0
        deleted_manual_files = 0
        if self._file_retention_seconds > 0 and self._uploads_dir is not None:
            manual_rows = conn.execute(
                """
                SELECT id, file_path
                FROM manual_downloads
                WHERE created_at < ?
                """,
                (file_cutoff,),
            ).fetchall()
            for row in manual_rows:
                if self._safe_unlink(Path(str(row["file_path"])), self._uploads_dir):
                    deleted_manual_files += 1
                conn.execute("DELETE FROM manual_downloads WHERE id = ?", (int(row["id"]),))
                deleted_manual_rows += 1

        deleted_artifact_rows = 0
        deleted_artifact_files = 0
        if self._file_retention_seconds > 0:
            artifact_rows = conn.execute(
                """
                SELECT id, m3u_path, mp3_path
                FROM round_artifacts
                WHERE created_at < ?
                """,
                (file_cutoff,),
            ).fetchall()
            for row in artifact_rows:
                if protected_artifact_id is not None and int(row["id"]) == protected_artifact_id:
                    continue
                deleted_artifact_files += int(self._safe_unlink(Path(str(row["m3u_path"])), self._artifacts_dir))
                deleted_artifact_files += int(self._safe_unlink(Path(str(row["mp3_path"])), self._artifacts_dir))
                conn.execute("DELETE FROM round_artifacts WHERE id = ?", (int(row["id"]),))
                deleted_artifact_rows += 1

        deleted_audit_logs = 0
        audit_cutoff = None
        if self._audit_log_retention_days > 0:
            audit_cutoff = (
                datetime.now(UTC) - timedelta(days=self._audit_log_retention_days)
            ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            deleted_audit_logs = int(
                conn.execute("DELETE FROM audit_logs WHERE created_at < ?", (audit_cutoff,)).rowcount or 0
            )

        if deleted_audio_rows or deleted_manual_rows or deleted_artifact_rows or deleted_audit_logs:
            conn.execute(
                """
                INSERT INTO audit_logs(action, detail, created_at)
                VALUES (?, ?, datetime('now'))
                """,
                (
                    "retention_cleanup",
                    json.dumps(
                        {
                            "file_cutoff": file_cutoff if self._file_retention_seconds > 0 else None,
                            "audit_cutoff": audit_cutoff,
                            "deleted_audio_assets": deleted_audio_rows,
                            "deleted_audio_files": deleted_audio_files,
                            "deleted_manual_downloads": deleted_manual_rows,
                            "deleted_manual_files": deleted_manual_files,
                            "deleted_round_artifacts": deleted_artifact_rows,
                            "deleted_artifact_files": deleted_artifact_files,
                            "audit_logs_deleted": deleted_audit_logs,
                        },
                        ensure_ascii=False,
                    ),
                ),
            )

    def _record_scheduler_event(self, action: str, detail: dict[str, object]) -> None:
        try:
            with self._db.session() as conn:
                conn.execute(
                    """
                    INSERT INTO audit_logs(action, detail, created_at)
                    VALUES (?, ?, datetime('now'))
                    """,
                    (action, json.dumps(detail, ensure_ascii=False)),
                )
        except sqlite3.OperationalError as exc:
            if _is_database_locked(exc):
                LOGGER.warning("Skipped scheduler audit log due to database lock", extra={"action": action})
                return
            LOGGER.exception("Failed to write scheduler audit log", extra={"action": action})
        except Exception:
            LOGGER.exception("Failed to write scheduler audit log", extra={"action": action})

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
