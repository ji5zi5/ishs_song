from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
import sys
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from radio_app.db import DB, utc_now_iso
from radio_app.scheduler import RoundAutoCloser


class SchedulerCleanupTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.artifacts_dir = self.root / "artifacts"
        self.uploads_dir = self.root / "uploads"
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.uploads_dir.mkdir(parents=True, exist_ok=True)
        self.db = DB(path=self.root / "test.db")
        self.db.init_schema()
        self.scheduler = RoundAutoCloser(
            db=self.db,
            artifacts_dir=self.artifacts_dir,
            uploads_dir=self.uploads_dir,
            interval_seconds=30,
            file_retention_seconds=3600,
        )

    def _old_iso(self) -> str:
        return (datetime.now(UTC) - timedelta(hours=2)).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def test_tick_deletes_expired_audio_assets_and_files(self) -> None:
        expired_file = self.uploads_dir / "youtube" / "expired.mp3"
        expired_file.parent.mkdir(parents=True, exist_ok=True)
        expired_file.write_bytes(b"expired")

        with self.db.session() as conn:
            conn.execute(
                "INSERT INTO songs(spotify_track_id, title, artist, album_art_url, external_url, created_at) VALUES ('expired-song', 'Expired', 'Artist', '', '', ?)",
                (utc_now_iso(),),
            )
            song_id = int(conn.execute("SELECT id FROM songs WHERE spotify_track_id = 'expired-song'").fetchone()["id"])
            conn.execute(
                "INSERT INTO audio_assets(song_id, file_path, duration_seconds, is_valid, validation_error, uploaded_at) VALUES (?, ?, 30, 1, NULL, ?)",
                (song_id, str(expired_file), self._old_iso()),
            )

        self.scheduler._tick()

        self.assertFalse(expired_file.exists())
        with self.db.session() as conn:
            row = conn.execute("SELECT * FROM audio_assets WHERE song_id = ?", (song_id,)).fetchone()
            self.assertIsNone(row)

    def test_tick_deletes_expired_round_artifacts_and_files(self) -> None:
        m3u_path = self.artifacts_dir / "round-1.m3u"
        mp3_path = self.artifacts_dir / "round-1.mp3"
        m3u_path.write_text("#EXTM3U\n", encoding="utf-8")
        mp3_path.write_bytes(b"merged")
        newer_m3u = self.artifacts_dir / "round-2.m3u"
        newer_mp3 = self.artifacts_dir / "round-2.mp3"
        newer_m3u.write_text("#EXTM3U\n", encoding="utf-8")
        newer_mp3.write_bytes(b"newer")

        with self.db.session() as conn:
            conn.execute(
                "INSERT INTO rounds(cadence, status, start_at, end_at, playlist_size, target_seconds, loudnorm_enabled, created_at, closed_at) VALUES ('monthly', 'closed', ?, ?, 12, 2400, 0, ?, ?)",
                (self._old_iso(), utc_now_iso(), self._old_iso(), self._old_iso()),
            )
            round_id = int(conn.execute("SELECT id FROM rounds ORDER BY id DESC LIMIT 1").fetchone()["id"])
            conn.execute(
                "INSERT INTO round_artifacts(round_id, m3u_path, mp3_path, total_seconds, generation_log, created_at) VALUES (?, ?, ?, 120, 'merged', ?)",
                (round_id, str(m3u_path), str(mp3_path), self._old_iso()),
            )
            conn.execute(
                "INSERT INTO rounds(cadence, status, start_at, end_at, playlist_size, target_seconds, loudnorm_enabled, created_at, closed_at) VALUES ('monthly', 'closed', ?, ?, 12, 2400, 0, ?, ?)",
                (utc_now_iso(), utc_now_iso(), utc_now_iso(), utc_now_iso()),
            )
            newer_round_id = int(conn.execute("SELECT id FROM rounds ORDER BY id DESC LIMIT 1").fetchone()["id"])
            conn.execute(
                "INSERT INTO round_artifacts(round_id, m3u_path, mp3_path, total_seconds, generation_log, created_at) VALUES (?, ?, ?, 120, 'merged', ?)",
                (newer_round_id, str(newer_m3u), str(newer_mp3), utc_now_iso()),
            )

        self.scheduler._tick()

        self.assertFalse(m3u_path.exists())
        self.assertFalse(mp3_path.exists())
        self.assertTrue(newer_m3u.exists())
        self.assertTrue(newer_mp3.exists())
        with self.db.session() as conn:
            row = conn.execute("SELECT * FROM round_artifacts WHERE round_id = ?", (round_id,)).fetchone()
            self.assertIsNone(row)
            self.assertIsNotNone(conn.execute("SELECT * FROM round_artifacts WHERE round_id = ?", (newer_round_id,)).fetchone())

    def test_tick_keeps_recent_files_and_rows(self) -> None:
        upload_path = self.uploads_dir / "youtube" / "recent.mp3"
        artifact_m3u = self.artifacts_dir / "round-2.m3u"
        artifact_mp3 = self.artifacts_dir / "round-2.mp3"
        upload_path.parent.mkdir(parents=True, exist_ok=True)
        upload_path.write_bytes(b"recent")
        artifact_m3u.write_text("#EXTM3U\n", encoding="utf-8")
        artifact_mp3.write_bytes(b"recent-merged")

        with self.db.session() as conn:
            conn.execute(
                "INSERT INTO songs(spotify_track_id, title, artist, album_art_url, external_url, created_at) VALUES ('recent-song', 'Recent', 'Artist', '', '', ?)",
                (utc_now_iso(),),
            )
            song_id = int(conn.execute("SELECT id FROM songs WHERE spotify_track_id = 'recent-song'").fetchone()["id"])
            conn.execute(
                "INSERT INTO audio_assets(song_id, file_path, duration_seconds, is_valid, validation_error, uploaded_at) VALUES (?, ?, 30, 1, NULL, ?)",
                (song_id, str(upload_path), utc_now_iso()),
            )
            conn.execute(
                "INSERT INTO rounds(cadence, status, start_at, end_at, playlist_size, target_seconds, loudnorm_enabled, created_at, closed_at) VALUES ('monthly', 'closed', ?, ?, 12, 2400, 0, ?, ?)",
                (utc_now_iso(), utc_now_iso(), utc_now_iso(), utc_now_iso()),
            )
            round_id = int(conn.execute("SELECT id FROM rounds ORDER BY id DESC LIMIT 1").fetchone()["id"])
            conn.execute(
                "INSERT INTO round_artifacts(round_id, m3u_path, mp3_path, total_seconds, generation_log, created_at) VALUES (?, ?, ?, 120, 'merged', ?)",
                (round_id, str(artifact_m3u), str(artifact_mp3), utc_now_iso()),
            )

        self.scheduler._tick()

        self.assertTrue(upload_path.exists())
        self.assertTrue(artifact_m3u.exists())
        self.assertTrue(artifact_mp3.exists())
        with self.db.session() as conn:
            self.assertIsNotNone(conn.execute("SELECT * FROM audio_assets WHERE song_id = ?", (song_id,)).fetchone())
            self.assertIsNotNone(conn.execute("SELECT * FROM round_artifacts WHERE round_id = ?", (round_id,)).fetchone())

    def test_tick_keeps_latest_artifact_and_selected_track_audio_even_when_expired(self) -> None:
        selected_path = self.uploads_dir / "youtube" / "selected.mp3"
        selected_path.parent.mkdir(parents=True, exist_ok=True)
        selected_path.write_bytes(b"selected")
        artifact_m3u = self.artifacts_dir / "round-latest.m3u"
        artifact_mp3 = self.artifacts_dir / "round-latest.mp3"
        artifact_m3u.write_text("#EXTM3U\n", encoding="utf-8")
        artifact_mp3.write_bytes(b"merged")

        with self.db.session() as conn:
            conn.execute(
                "INSERT INTO users(riro_user_key, display_name, is_admin_approved, created_at) VALUES (?, ?, 0, ?)",
                ("listener", "청취자", utc_now_iso()),
            )
            user_id = int(conn.execute("SELECT id FROM users WHERE riro_user_key = 'listener'").fetchone()["id"])
            conn.execute(
                "INSERT INTO songs(spotify_track_id, title, artist, album_art_url, external_url, created_at) VALUES ('selected-song', 'Selected', 'Artist', '', '', ?)",
                (utc_now_iso(),),
            )
            song_id = int(conn.execute("SELECT id FROM songs WHERE spotify_track_id = 'selected-song'").fetchone()["id"])
            conn.execute(
                "INSERT INTO rounds(cadence, status, start_at, end_at, playlist_size, target_seconds, loudnorm_enabled, created_at, closed_at) VALUES ('monthly', 'closed', ?, ?, 12, 2400, 0, ?, ?)",
                (self._old_iso(), self._old_iso(), self._old_iso(), self._old_iso()),
            )
            round_id = int(conn.execute("SELECT id FROM rounds ORDER BY id DESC LIMIT 1").fetchone()["id"])
            conn.execute(
                "INSERT INTO audio_assets(song_id, file_path, duration_seconds, is_valid, validation_error, uploaded_at) VALUES (?, ?, 30, 1, NULL, ?)",
                (song_id, str(selected_path), self._old_iso()),
            )
            conn.execute(
                "INSERT INTO round_artifacts(round_id, m3u_path, mp3_path, total_seconds, generation_log, created_at) VALUES (?, ?, ?, 120, 'merged', ?)",
                (round_id, str(artifact_m3u), str(artifact_mp3), self._old_iso()),
            )
            artifact_id = int(conn.execute("SELECT id FROM round_artifacts WHERE round_id = ?", (round_id,)).fetchone()["id"])
            conn.execute(
                "INSERT INTO submissions(round_id, user_id, song_id, is_hidden, submitted_at) VALUES (?, ?, ?, 0, ?)",
                (round_id, user_id, song_id, self._old_iso()),
            )
            submission_id = int(conn.execute("SELECT id FROM submissions ORDER BY id DESC LIMIT 1").fetchone()["id"])
            conn.execute(
                """
                INSERT INTO round_artifact_tracks(artifact_id, submission_id, song_id, title, artist, file_path, duration_seconds, track_order, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (artifact_id, submission_id, song_id, "Selected", "Artist", str(selected_path), 30, 1, self._old_iso()),
            )

        self.scheduler._tick()

        self.assertTrue(selected_path.exists())
        self.assertTrue(artifact_m3u.exists())
        self.assertTrue(artifact_mp3.exists())
        with self.db.session() as conn:
            self.assertIsNotNone(conn.execute("SELECT * FROM audio_assets WHERE song_id = ?", (song_id,)).fetchone())
            self.assertIsNotNone(conn.execute("SELECT * FROM round_artifacts WHERE id = ?", (artifact_id,)).fetchone())

    def test_tick_deletes_expired_audit_logs_and_reports_count(self) -> None:
        with self.db.session() as conn:
            conn.execute(
                "INSERT INTO audit_logs(action, detail, created_at) VALUES (?, ?, ?)",
                ("old-log", "expired", (datetime.now(UTC) - timedelta(days=40)).replace(microsecond=0).isoformat().replace("+00:00", "Z")),
            )
            conn.execute(
                "INSERT INTO audit_logs(action, detail, created_at) VALUES (?, ?, ?)",
                ("fresh-log", "recent", utc_now_iso()),
            )

        self.scheduler = RoundAutoCloser(
            db=self.db,
            artifacts_dir=self.artifacts_dir,
            uploads_dir=self.uploads_dir,
            interval_seconds=30,
            audit_log_retention_days=1,
        )

        self.scheduler._tick()

        with self.db.session() as conn:
            actions = [str(row["action"]) for row in conn.execute("SELECT action FROM audit_logs ORDER BY id").fetchall()]
            cleanup = conn.execute(
                "SELECT detail FROM audit_logs WHERE action = 'retention_cleanup' ORDER BY id DESC LIMIT 1"
            ).fetchone()

        self.assertIn("fresh-log", actions)
        self.assertNotIn("old-log", actions)
        self.assertIsNotNone(cleanup)
        detail = json.loads(str(cleanup["detail"]))
        self.assertEqual(detail["audit_logs_deleted"], 1)

    def test_tick_deletes_expired_manual_downloads_and_files(self) -> None:
        expired_file = self.uploads_dir / "manual" / "expired-manual.mp3"
        expired_file.parent.mkdir(parents=True, exist_ok=True)
        expired_file.write_bytes(b"manual")

        with self.db.session() as conn:
            conn.execute(
                "INSERT INTO users(riro_user_key, display_name, is_admin_approved, created_at) VALUES (?, ?, 1, ?)",
                ("manual-admin", "수동다운관리자", utc_now_iso()),
            )
            user_id = int(conn.execute("SELECT id FROM users WHERE riro_user_key = 'manual-admin'").fetchone()["id"])
            conn.execute(
                """
                INSERT INTO manual_downloads(actor_user_id, source_url, video_id, title, uploader, file_path, duration_seconds, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, "https://youtu.be/expired", "expired", "Expired", "Uploader", str(expired_file), 91, self._old_iso()),
            )

        self.scheduler._tick()

        self.assertFalse(expired_file.exists())
        with self.db.session() as conn:
            row = conn.execute("SELECT * FROM manual_downloads WHERE video_id = 'expired'").fetchone()
            self.assertIsNone(row)

    def test_tick_records_structured_auto_close_failure(self) -> None:
        old_end = self._old_iso()
        with self.db.session() as conn:
            conn.execute(
                """
                INSERT INTO rounds(cadence, status, start_at, end_at, playlist_size, target_seconds, loudnorm_enabled, created_at)
                VALUES ('monthly', 'open', ?, ?, 12, 2400, 1, ?)
                """,
                (self._old_iso(), old_end, self._old_iso()),
            )
            round_id = int(conn.execute("SELECT id FROM rounds ORDER BY id DESC LIMIT 1").fetchone()["id"])

        with mock.patch("radio_app.scheduler.close_round", side_effect=RuntimeError("close failed")):
            self.scheduler._tick()

        with self.db.session() as conn:
            row = conn.execute(
                "SELECT detail FROM audit_logs WHERE action = 'auto_close_failed' ORDER BY id DESC LIMIT 1"
            ).fetchone()

        self.assertIsNotNone(row)
        detail = json.loads(str(row["detail"]))
        self.assertEqual(detail["round_id"], round_id)
        self.assertEqual(detail["error_type"], "RuntimeError")
        self.assertEqual(detail["error"], "close failed")

    def test_run_loop_logs_and_audits_unexpected_tick_error(self) -> None:
        def stop_after_wait(_seconds: int) -> bool:
            self.scheduler._stop.set()
            return True

        with (
            self.assertLogs("radio_app.scheduler", level="ERROR") as logs,
            mock.patch.object(self.scheduler, "_tick", side_effect=RuntimeError("loop boom")),
            mock.patch.object(self.scheduler._stop, "wait", side_effect=stop_after_wait),
        ):
            self.scheduler._run_loop()

        self.assertTrue(any("loop boom" in entry for entry in logs.output))
        with self.db.session() as conn:
            row = conn.execute(
                "SELECT detail FROM audit_logs WHERE action = 'scheduler_loop_failed' ORDER BY id DESC LIMIT 1"
            ).fetchone()

        self.assertIsNotNone(row)
        detail = json.loads(str(row["detail"]))
        self.assertEqual(detail["error_type"], "RuntimeError")
        self.assertEqual(detail["error"], "loop boom")

    def test_run_loop_skips_locked_database_without_failure_audit(self) -> None:
        def stop_after_wait(_seconds: int) -> bool:
            self.scheduler._stop.set()
            return True

        with (
            self.assertLogs("radio_app.scheduler", level="WARNING") as logs,
            mock.patch.object(self.scheduler, "_tick", side_effect=sqlite3.OperationalError("database is locked")),
            mock.patch.object(self.scheduler._stop, "wait", side_effect=stop_after_wait),
        ):
            self.scheduler._run_loop()

        self.assertTrue(any("database lock" in entry.lower() for entry in logs.output))
        with self.db.session() as conn:
            row = conn.execute(
                "SELECT detail FROM audit_logs WHERE action = 'scheduler_loop_failed' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertIsNone(row)


if __name__ == "__main__":
    unittest.main()
