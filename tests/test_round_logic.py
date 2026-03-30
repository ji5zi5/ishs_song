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
from radio_app.services.rounds import close_round, format_round_label, ranked_submissions, select_round_for_admin_close


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

    def test_close_round_ignores_playlist_size_when_time_allows(self) -> None:
        round_id = self._seed_base()
        with self.db.session() as conn:
            conn.execute("UPDATE rounds SET playlist_size = 2, target_seconds = 180 WHERE id = ?", (round_id,))
            for idx in range(1, 4):
                conn.execute(
                    "INSERT INTO songs(spotify_track_id, title, artist, album_art_url, external_url, created_at) VALUES (?, ?, 'a', '', '', ?)",
                    (f"g{idx}", f"Group{idx}", utc_now_iso()),
                )
                song_id = int(conn.execute("SELECT id FROM songs WHERE spotify_track_id = ?", (f"g{idx}",)).fetchone()["id"])
                conn.execute(
                    "INSERT INTO submissions(round_id, user_id, song_id, submitted_at) VALUES (?, 1, ?, ?)",
                    (round_id, song_id, f"2026-03-0{idx}T00:00:00Z"),
                )
                media = self.root / f"s{idx}.mp3"
                media.write_bytes(b"audio")
                conn.execute(
                    "INSERT INTO audio_assets(song_id, file_path, duration_seconds, is_valid, validation_error, uploaded_at) VALUES (?, ?, 50, 1, NULL, ?)",
                    (song_id, str(media), utc_now_iso()),
                )

            with (
                patch("radio_app.services.rounds.validate_mp3_and_get_duration_seconds", side_effect=self._fake_validate_mp3),
                patch("radio_app.services.rounds.merge_mp3_files", side_effect=self._fake_merge_mp3_files),
            ):
                result = close_round(conn, round_id, self.root / "artifacts")

        self.assertEqual(result["status"], "closed")
        self.assertEqual(result["selected_count"], 3)
        self.assertEqual(result["total_seconds"], 150)

    def test_close_round_reports_progress_stages_in_order(self) -> None:
        round_id = self._seed_base()
        progress_events: list[tuple[str, str, int]] = []
        with self.db.session() as conn:
            for idx in range(1, 3):
                conn.execute(
                    "INSERT INTO songs(spotify_track_id, title, artist, album_art_url, external_url, created_at) VALUES (?, ?, 'a', '', '', ?)",
                    (f"p{idx}", f"Progress{idx}", utc_now_iso()),
                )
                song_id = int(conn.execute("SELECT id FROM songs WHERE spotify_track_id = ?", (f"p{idx}",)).fetchone()["id"])
                conn.execute(
                    "INSERT INTO submissions(round_id, user_id, song_id, submitted_at) VALUES (?, 1, ?, ?)",
                    (round_id, song_id, f"2026-03-0{idx}T00:00:00Z"),
                )
                media = self.root / f"s{idx}.mp3"
                media.write_bytes(b"audio")
                conn.execute(
                    "INSERT INTO audio_assets(song_id, file_path, duration_seconds, is_valid, validation_error, uploaded_at) VALUES (?, ?, 50, 1, NULL, ?)",
                    (song_id, str(media), utc_now_iso()),
                )

            with (
                patch("radio_app.services.rounds.validate_mp3_and_get_duration_seconds", side_effect=self._fake_validate_mp3),
                patch("radio_app.services.rounds.merge_mp3_files", side_effect=self._fake_merge_mp3_files),
            ):
                close_round(
                    conn,
                    round_id,
                    self.root / "artifacts",
                    progress_callback=lambda stage, message, percent: progress_events.append((stage, message, percent)),
                )

        self.assertEqual(
            [stage for stage, _, _ in progress_events],
            ["preparing", "validating-audio", "trimming-playlist", "writing-m3u", "merging-mp3", "finalizing"],
        )
        self.assertTrue(all(progress_events[idx][2] <= progress_events[idx + 1][2] for idx in range(len(progress_events) - 1)))

    def test_ranked_submissions_can_sort_by_recent(self) -> None:
        round_id = self._seed_base()
        with self.db.session() as conn:
            for idx, submitted_at in ((1, "2026-03-02T00:00:00Z"), (2, "2026-03-04T00:00:00Z"), (3, "2026-03-03T00:00:00Z")):
                conn.execute(
                    "INSERT INTO songs(spotify_track_id, title, artist, album_art_url, external_url, created_at) VALUES (?, ?, 'a', '', '', ?)",
                    (f"r{idx}", f"Recent{idx}", utc_now_iso()),
                )
                song_id = int(conn.execute("SELECT id FROM songs WHERE spotify_track_id = ?", (f"r{idx}",)).fetchone()["id"])
                conn.execute(
                    "INSERT INTO submissions(round_id, user_id, song_id, submitted_at) VALUES (?, 1, ?, ?)",
                    (round_id, song_id, submitted_at),
                )
                sub_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
                if idx == 1:
                    for user_id in (1, 2):
                        conn.execute(
                            "INSERT INTO votes(round_id, user_id, submission_id, voted_at) VALUES (?, ?, ?, ?)",
                            (round_id, user_id, sub_id, utc_now_iso()),
                        )

            popular = ranked_submissions(conn, round_id)
            recent = ranked_submissions(conn, round_id, sort_by="recent")

        self.assertEqual([item["title"] for item in popular[:3]], ["Recent1", "Recent3", "Recent2"])
        self.assertEqual([item["title"] for item in recent[:3]], ["Recent2", "Recent3", "Recent1"])

    def test_format_round_label_uses_local_month_at_utc_boundary(self) -> None:
        round_row = {
            "id": 10,
            "cadence": "monthly",
            "start_at": "2026-02-28T15:00:00Z",
            "created_at": "2026-03-01T00:00:00Z",
        }
        with self.db.session() as conn:
            self.assertEqual(format_round_label(conn, round_row, "Asia/Seoul"), "3월 1회차")

    def test_format_round_label_adds_sequence_for_multiple_monthly_rounds(self) -> None:
        with self.db.session() as conn:
            conn.execute(
                """
                INSERT INTO rounds(cadence, status, start_at, end_at, playlist_size, target_seconds, loudnorm_enabled, created_at)
                VALUES ('monthly', 'closed', '2026-02-28T15:00:00Z', '2026-03-31T15:00:00Z', 12, 2400, 0, '2026-03-10T00:00:00Z')
                """,
            )
            first = conn.execute("SELECT * FROM rounds WHERE id = last_insert_rowid()").fetchone()
            conn.execute(
                """
                INSERT INTO rounds(cadence, status, start_at, end_at, playlist_size, target_seconds, loudnorm_enabled, created_at)
                VALUES ('monthly', 'open', '2026-02-28T15:00:00Z', '2026-03-31T15:00:00Z', 12, 2400, 0, '2026-03-20T00:00:00Z')
                """,
            )
            second = conn.execute("SELECT * FROM rounds WHERE id = last_insert_rowid()").fetchone()

            self.assertEqual(format_round_label(conn, first, "Asia/Seoul"), "3월 1회차")
            self.assertEqual(format_round_label(conn, second, "Asia/Seoul"), "3월 2회차")

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

    def test_select_round_for_admin_close_recovers_stale_closing_round(self) -> None:
        with self.db.session() as conn:
            conn.execute("INSERT INTO users(riro_user_key, display_name, is_admin_approved, created_at) VALUES ('u1', 'u1', 1, ?)", (utc_now_iso(),))
            conn.execute(
                """
                INSERT INTO rounds(cadence, status, start_at, end_at, playlist_size, target_seconds, loudnorm_enabled, close_job_key, created_at)
                VALUES ('monthly', 'closing', ?, ?, 12, 2400, 0, 'stale-job', ?)
                """,
                ("2026-02-28T15:00:00Z", "2026-03-31T15:00:00Z", utc_now_iso()),
            )
            stale_round_id = int(conn.execute("SELECT id FROM rounds ORDER BY id DESC LIMIT 1").fetchone()["id"])
            conn.execute(
                "INSERT INTO songs(spotify_track_id, title, artist, album_art_url, external_url, created_at) VALUES ('stale-song', 'Stale', 'Artist', '', '', ?)",
                (utc_now_iso(),),
            )
            song_id = int(conn.execute("SELECT id FROM songs WHERE spotify_track_id = 'stale-song'").fetchone()["id"])
            conn.execute(
                "INSERT INTO submissions(round_id, user_id, song_id, submitted_at) VALUES (?, 1, ?, ?)",
                (stale_round_id, song_id, utc_now_iso()),
            )
            conn.execute(
                """
                INSERT INTO rounds(cadence, status, start_at, end_at, playlist_size, target_seconds, loudnorm_enabled, created_at)
                VALUES ('monthly', 'open', ?, ?, 12, 2400, 0, ?)
                """,
                ("2026-02-28T15:00:00Z", "2026-03-31T15:00:00Z", utc_now_iso()),
            )

            selected = select_round_for_admin_close(conn, 'Asia/Seoul')

            self.assertEqual(int(selected['id']), stale_round_id)
            refreshed = conn.execute("SELECT status, close_job_key FROM rounds WHERE id = ?", (stale_round_id,)).fetchone()
            self.assertEqual(refreshed['status'], 'open')
            self.assertIsNone(refreshed['close_job_key'])



if __name__ == "__main__":
    unittest.main()
