from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from radio_app.db import DB, utc_now_iso
from radio_app.services.rounds import close_round


class RoundLogicTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = DB(path=self.root / "test.db")
        self.db.init_schema()
        (self.root / "artifacts").mkdir(parents=True, exist_ok=True)

    def _seed_base(self) -> int:
        with self.db.session() as conn:
            conn.execute("INSERT INTO users(riro_user_key, display_name, is_admin_approved, created_at) VALUES ('u1', 'u1', 1, ?)", (utc_now_iso(),))
            conn.execute("INSERT INTO users(riro_user_key, display_name, is_admin_approved, created_at) VALUES ('u2', 'u2', 1, ?)", (utc_now_iso(),))
            conn.execute(
                """
                INSERT INTO rounds(cadence, status, start_at, end_at, playlist_size, target_seconds, loudnorm_enabled, created_at)
                VALUES ('monthly', 'open', ?, ?, 3, 120, 0, ?)
                """,
                ("2026-03-01T00:00:00Z", "2026-04-01T00:00:00Z", utc_now_iso()),
            )
            return int(conn.execute("SELECT id FROM rounds ORDER BY id DESC LIMIT 1").fetchone()["id"])

    @staticmethod
    def _fake_merge_mp3_files(_file_paths, output_path, loudnorm_enabled, ffmpeg_path=None) -> str:
        del loudnorm_enabled, ffmpeg_path
        Path(output_path).write_bytes(b"merged")
        return "merged-test"

    @staticmethod
    def _fake_validate_mp3(path: Path) -> tuple[int, str | None]:
        name = Path(path).name
        if name.startswith("s"):
            idx = int(name[1])
            return (70, None) if idx == 4 else (50, None)
        if name.startswith("x"):
            return 30, None
        if name == "f1.mp3":
            return 30, None
        return 0, "unable-to-parse-mp3-duration"

    def test_close_round_applies_ranking_and_duration_trim(self) -> None:
        round_id = self._seed_base()
        with self.db.session() as conn:
            for idx in range(1, 5):
                conn.execute(
                    "INSERT INTO songs(spotify_track_id, title, artist, album_art_url, external_url, created_at) VALUES (?, ?, 'a', '', '', ?)",
                    (f"t{idx}", f"Song{idx}", utc_now_iso()),
                )
                song_id = int(conn.execute("SELECT id FROM songs WHERE spotify_track_id = ?", (f"t{idx}",)).fetchone()["id"])
                conn.execute(
                    "INSERT INTO submissions(round_id, user_id, song_id, submitted_at) VALUES (?, ?, ?, ?)",
                    (round_id, 1 if idx < 3 else 2, song_id, f"2026-03-0{idx}T00:00:00Z"),
                )
                sub_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
                if idx == 1:
                    for u in (1, 2):
                        conn.execute("INSERT INTO votes(round_id, user_id, submission_id, voted_at) VALUES (?, ?, ?, ?)", (round_id, u, sub_id, utc_now_iso()))
                if idx == 2:
                    conn.execute("INSERT INTO votes(round_id, user_id, submission_id, voted_at) VALUES (?, ?, ?, ?)", (round_id, 1, sub_id, utc_now_iso()))
                media = self.root / f"s{idx}.mp3"
                media.write_bytes((f"audio{idx}").encode("utf-8") * 10)
                conn.execute(
                    "INSERT INTO audio_assets(song_id, file_path, duration_seconds, is_valid, validation_error, uploaded_at) VALUES (?, ?, ?, 1, NULL, ?)",
                    (song_id, str(media), 50 if idx in (1, 2, 3) else 70, utc_now_iso()),
                )

            with (
                patch("radio_app.services.rounds.validate_mp3_and_get_duration_seconds", side_effect=self._fake_validate_mp3),
                patch("radio_app.services.rounds.merge_mp3_files", side_effect=self._fake_merge_mp3_files),
            ):
                result = close_round(conn, round_id, self.root / "artifacts")
            self.assertEqual(result["status"], "closed")
            # Top 3 candidates would total 150s; must trim to 120s by removing lowest-ranked.
            self.assertEqual(result["selected_count"], 2)
            self.assertLessEqual(result["total_seconds"], 120)
            artifact = conn.execute("SELECT * FROM round_artifacts WHERE round_id = ?", (round_id,)).fetchone()
            self.assertIsNotNone(artifact)
            self.assertTrue(Path(artifact["m3u_path"]).exists())
            self.assertTrue(Path(artifact["mp3_path"]).exists())

    def test_close_round_replaces_invalid_audio_with_next_ranked(self) -> None:
        round_id = self._seed_base()
        with self.db.session() as conn:
            for idx in range(1, 4):
                conn.execute(
                    "INSERT INTO songs(spotify_track_id, title, artist, album_art_url, external_url, created_at) VALUES (?, ?, 'a', '', '', ?)",
                    (f"x{idx}", f"X{idx}", utc_now_iso()),
                )
                song_id = int(conn.execute("SELECT id FROM songs WHERE spotify_track_id = ?", (f"x{idx}",)).fetchone()["id"])
                conn.execute(
                    "INSERT INTO submissions(round_id, user_id, song_id, submitted_at) VALUES (?, 1, ?, ?)",
                    (round_id, song_id, f"2026-03-0{idx}T00:00:00Z"),
                )
                sub_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
                # all tied with one vote for deterministic submitted_at ordering
                conn.execute("INSERT INTO votes(round_id, user_id, submission_id, voted_at) VALUES (?, 1, ?, ?)", (round_id, sub_id, utc_now_iso()))

                media = self.root / f"x{idx}.mp3"
                media.write_bytes(b"audio")
                is_valid = 0 if idx == 1 else 1
                path = str(self.root / "missing.mp3") if idx == 1 else str(media)
                conn.execute(
                    "INSERT INTO audio_assets(song_id, file_path, duration_seconds, is_valid, validation_error, uploaded_at) VALUES (?, ?, 30, ?, NULL, ?)",
                    (song_id, path, is_valid, utc_now_iso()),
                )

            with (
                patch("radio_app.services.rounds.validate_mp3_and_get_duration_seconds", side_effect=self._fake_validate_mp3),
                patch("radio_app.services.rounds.merge_mp3_files", side_effect=self._fake_merge_mp3_files),
            ):
                result = close_round(conn, round_id, self.root / "artifacts")
            self.assertEqual(result["status"], "closed")
            self.assertEqual(result["selected_count"], 2)
            self.assertIn("skip", result["generation_log"])

    @patch("radio_app.services.rounds.validate_mp3_and_get_duration_seconds", side_effect=_fake_validate_mp3)
    @patch("radio_app.services.rounds.merge_mp3_files", side_effect=RuntimeError("merge-failed"))
    def test_close_round_resets_status_when_merge_fails(self, _mock_merge, _mock_validate) -> None:
        round_id = self._seed_base()
        with self.db.session() as conn:
            conn.execute(
                "INSERT INTO songs(spotify_track_id, title, artist, album_art_url, external_url, created_at) VALUES ('f1', 'FailSong', 'a', '', '', ?)",
                (utc_now_iso(),),
            )
            song_id = int(conn.execute("SELECT id FROM songs WHERE spotify_track_id = 'f1'").fetchone()["id"])
            conn.execute(
                "INSERT INTO submissions(round_id, user_id, song_id, submitted_at) VALUES (?, 1, ?, ?)",
                (round_id, song_id, utc_now_iso()),
            )
            media = self.root / "f1.mp3"
            media.write_bytes(b"audio")
            conn.execute(
                "INSERT INTO audio_assets(song_id, file_path, duration_seconds, is_valid, validation_error, uploaded_at) VALUES (?, ?, 30, 1, NULL, ?)",
                (song_id, str(media), utc_now_iso()),
            )

            with self.assertRaises(RuntimeError):
                close_round(conn, round_id, self.root / "artifacts")

            round_row = conn.execute("SELECT status, close_job_key FROM rounds WHERE id = ?", (round_id,)).fetchone()
            self.assertEqual(round_row["status"], "open")
            self.assertIsNone(round_row["close_job_key"])
            artifact = conn.execute("SELECT * FROM round_artifacts WHERE round_id = ?", (round_id,)).fetchone()
            self.assertIsNone(artifact)


if __name__ == "__main__":
    unittest.main()
