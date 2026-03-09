from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
import sys

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

        self.scheduler._tick()

        self.assertFalse(m3u_path.exists())
        self.assertFalse(mp3_path.exists())
        with self.db.session() as conn:
            row = conn.execute("SELECT * FROM round_artifacts WHERE round_id = ?", (round_id,)).fetchone()
            self.assertIsNone(row)

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


if __name__ == "__main__":
    unittest.main()
