from __future__ import annotations

import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from radio_app.db import DB, utc_now_iso
from radio_app.services.youtube import (
    DownloadedAudio,
    _rank_candidate,
    _sanitize_filename,
    ensure_audio_for_songs,
    search_and_download,
)


class SanitizeFilenameTest(unittest.TestCase):
    def test_removes_special_characters(self) -> None:
        self.assertEqual(_sanitize_filename('a/b\\c:d*e?"f<g>h|i'), "a_b_c_d_e__f_g_h_i")

    def test_truncates_long_names(self) -> None:
        long_name = "x" * 200
        self.assertLessEqual(len(_sanitize_filename(long_name)), 120)


class CandidateRankingTest(unittest.TestCase):
    def test_topic_candidate_wins_when_title_and_artist_match(self) -> None:
        topic = _rank_candidate(
            {"id": "a1", "title": "Artist - Song", "uploader": "Artist - Topic", "webpage_url": "https://youtu.be/a1"},
            requested_artist="Artist",
            requested_title="Song",
        )
        generic = _rank_candidate(
            {"id": "b2", "title": "Artist - Song lyrics", "uploader": "random uploader", "webpage_url": "https://youtu.be/b2"},
            requested_artist="Artist",
            requested_title="Song",
        )
        self.assertGreater(topic.score, generic.score)
        self.assertIn("topic-hint", topic.reason)

    def test_better_title_match_can_beat_weaker_topic_candidate(self) -> None:
        weak_topic = _rank_candidate(
            {"id": "a1", "title": "Artist - Song live", "uploader": "Artist - Topic", "webpage_url": "https://youtu.be/a1"},
            requested_artist="Artist",
            requested_title="Song",
        )
        better_generic = _rank_candidate(
            {"id": "b2", "title": "Artist - Song", "uploader": "label channel", "webpage_url": "https://youtu.be/b2"},
            requested_artist="Artist",
            requested_title="Song",
        )
        self.assertGreater(better_generic.score, weak_topic.score)


class SearchAndDownloadTest(unittest.TestCase):
    @patch("radio_app.services.youtube.yt_dlp", create=True)
    def test_raises_when_yt_dlp_not_installed(self, _mock: MagicMock) -> None:
        with patch.dict("sys.modules", {"yt_dlp": None}):
            with self.assertRaises(RuntimeError):
                search_and_download("test query", Path("/tmp/test_yt"))

    def test_does_not_reuse_old_mp3_when_download_creates_nothing(self) -> None:
        class FakeDL:
            def __init__(self, _opts) -> None:
                pass

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

            def extract_info(self, _query, download=True):
                if download:
                    return {"id": "dummy"}
                return {"entries": [{"id": "dummy", "title": "new song", "uploader": "artist - topic", "webpage_url": "https://youtu.be/dummy"}]}

        fake_mod = types.SimpleNamespace(YoutubeDL=FakeDL)
        with tempfile.TemporaryDirectory() as td:
            out = Path(td)
            (out / "old.mp3").write_bytes(b"old")
            with patch.dict("sys.modules", {"yt_dlp": fake_mod}):
                with self.assertRaises(RuntimeError):
                    search_and_download("artist - new song", out)

    def test_sets_timeout_and_retry_options_for_yt_dlp(self) -> None:
        captured_opts: dict = {}

        class FakeDL:
            def __init__(self, opts) -> None:
                self.opts = opts
                captured_opts.update(opts)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

            def extract_info(self, query, download=True):
                if not download:
                    return {"entries": [{"id": "dummy", "title": "any", "uploader": "artist - topic", "webpage_url": "https://youtu.be/dummy"}]}
                out = Path(str(self.opts["outtmpl"]).replace("%(ext)s", "mp3"))
                out.write_bytes(b"new")
                return {"id": "dummy"}

        fake_mod = types.SimpleNamespace(YoutubeDL=FakeDL)
        with tempfile.TemporaryDirectory() as td:
            out = Path(td)
            expected = out / "query.mp3"
            with patch("radio_app.services.youtube._sanitize_filename", return_value="query"):
                with patch.dict("sys.modules", {"yt_dlp": fake_mod}):
                    got = search_and_download("Artist - Song", out)

            self.assertEqual(got.path, expected)
            self.assertEqual(captured_opts.get("socket_timeout"), 15)
            self.assertEqual(captured_opts.get("retries"), 2)
            self.assertEqual(captured_opts.get("extractor_retries"), 2)

    def test_ranked_fallback_prefers_better_topic_candidate(self) -> None:
        class FakeDL:
            def __init__(self, opts) -> None:
                self.opts = opts

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

            def extract_info(self, q, download=True):
                if not download:
                    return {
                        "entries": [
                            {"id": "bad1", "title": "Artist - Song cover", "uploader": "random", "webpage_url": "https://youtu.be/bad1"},
                            {"id": "ok2", "title": "Artist - Song", "uploader": "Artist - Topic", "webpage_url": "https://youtu.be/ok2"},
                        ]
                    }
                query = str(q)
                if "ok2" in query:
                    out = Path(str(self.opts["outtmpl"]).replace("%(ext)s", "mp3"))
                    out.write_bytes(b"ok")
                    return {"id": "ok2"}
                raise RuntimeError("unexpected candidate")

        fake_mod = types.SimpleNamespace(YoutubeDL=FakeDL)
        with tempfile.TemporaryDirectory() as td:
            out = Path(td)
            with patch("radio_app.services.youtube._sanitize_filename", return_value="song-query"):
                with patch.dict("sys.modules", {"yt_dlp": fake_mod}):
                    got = search_and_download("Artist - Song", out)

            self.assertEqual(got.path, out / "song-query.mp3")
            self.assertEqual(got.candidate.video_id, "ok2")
            self.assertEqual(got.candidate.confidence, "strong")

    def test_uses_weak_fallback_when_stronger_candidate_download_fails(self) -> None:
        class FakeDL:
            def __init__(self, opts) -> None:
                self.opts = opts

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

            def extract_info(self, q, download=True):
                if not download:
                    return {
                        "entries": [
                            {"id": "strong1", "title": "Artist - Song", "uploader": "Artist - Topic", "webpage_url": "https://youtu.be/strong1"},
                            {"id": "weak2", "title": "Song audio", "uploader": "fan upload", "webpage_url": "https://youtu.be/weak2"},
                        ]
                    }
                query = str(q)
                if "strong1" in query:
                    raise RuntimeError("This video is not available")
                if "weak2" in query:
                    out = Path(str(self.opts["outtmpl"]).replace("%(ext)s", "mp3"))
                    out.write_bytes(b"ok")
                    return {"id": "weak2"}
                raise RuntimeError("unexpected")

        fake_mod = types.SimpleNamespace(YoutubeDL=FakeDL)
        with tempfile.TemporaryDirectory() as td:
            out = Path(td)
            with patch("radio_app.services.youtube._sanitize_filename", return_value="song-query"):
                with patch.dict("sys.modules", {"yt_dlp": fake_mod}):
                    got = search_and_download("Artist - Song", out)

            self.assertEqual(got.candidate.video_id, "weak2")
            self.assertIn(got.candidate.confidence, {"good", "weak"})


class EnsureAudioForSongsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = DB(path=self.root / "test.db")
        self.db.init_schema()

    def _seed_song(self, conn: DB, track_id: str, title: str, artist: str) -> None:
        conn.execute(
            "INSERT INTO users(riro_user_key, display_name, is_admin_approved, created_at) VALUES ('u1', 'u1', 1, ?)",
            (utc_now_iso(),),
        )
        conn.execute(
            "INSERT INTO rounds(cadence, status, start_at, end_at, playlist_size, target_seconds, loudnorm_enabled, created_at) VALUES ('monthly', 'open', ?, ?, 3, 120, 0, ?)",
            ("2026-03-01T00:00:00Z", "2026-04-01T00:00:00Z", utc_now_iso()),
        )
        conn.execute(
            "INSERT INTO songs(spotify_track_id, title, artist, album_art_url, external_url, created_at) VALUES (?, ?, ?, '', '', ?)",
            (track_id, title, artist, utc_now_iso()),
        )
        conn.execute(
            "INSERT INTO submissions(round_id, user_id, song_id, submitted_at) VALUES (1, 1, 1, ?)",
            (utc_now_iso(),),
        )

    def test_skips_songs_with_existing_valid_audio(self) -> None:
        media = self.root / "existing.mp3"
        media.write_bytes(b"fake_audio" * 100)

        with self.db.session() as conn:
            self._seed_song(conn, "t1", "Song1", "Artist1")
            conn.execute(
                "INSERT INTO audio_assets(song_id, file_path, duration_seconds, is_valid, validation_error, uploaded_at) VALUES (1, ?, 30, 1, NULL, ?)",
                (str(media), utc_now_iso()),
            )
            row = conn.execute(
                """
                SELECT s.id AS submission_id, so.title, so.artist, a.file_path, a.is_valid, a.duration_seconds
                FROM submissions s
                JOIN songs so ON so.id = s.song_id
                LEFT JOIN audio_assets a ON a.song_id = s.song_id
                WHERE s.id = 1
                """,
            ).fetchone()

            with patch("radio_app.services.youtube.validate_mp3_and_get_duration_seconds", return_value=(180, None)):
                results = ensure_audio_for_songs(conn, [row], self.root / "uploads")
            self.assertEqual(results[1], "revalidated-existing")

    @patch("radio_app.services.youtube.search_and_download")
    def test_revalidates_existing_file_even_when_asset_marked_invalid(self, mock_dl: MagicMock) -> None:
        media = self.root / "stale-invalid.mp3"
        media.write_bytes(b"fake_audio" * 100)

        with self.db.session() as conn:
            self._seed_song(conn, "t4", "Song4", "Artist4")
            conn.execute(
                "INSERT INTO audio_assets(song_id, file_path, duration_seconds, is_valid, validation_error, uploaded_at) VALUES (1, ?, 0, 0, 'unable-to-parse-mp3-duration', ?)",
                (str(media), utc_now_iso()),
            )
            row = conn.execute(
                """
                SELECT s.id AS submission_id, so.title, so.artist, a.file_path, a.is_valid, a.duration_seconds
                FROM submissions s
                JOIN songs so ON so.id = s.song_id
                LEFT JOIN audio_assets a ON a.song_id = s.song_id
                WHERE s.id = 1
                """,
            ).fetchone()

            with patch("radio_app.services.youtube.validate_mp3_and_get_duration_seconds", return_value=(1791, None)):
                results = ensure_audio_for_songs(conn, [row], self.root / "uploads")

            self.assertEqual(results[1], "revalidated-existing")
            asset = conn.execute("SELECT * FROM audio_assets WHERE song_id = 1").fetchone()
            self.assertEqual(asset["duration_seconds"], 1791)
            self.assertEqual(int(asset["is_valid"]), 1)
            self.assertIsNone(asset["validation_error"])
            mock_dl.assert_not_called()


    @patch("radio_app.services.youtube.search_and_download")
    def test_downloads_when_audio_missing(self, mock_dl: MagicMock) -> None:
        dl_path = self.root / "downloaded.mp3"
        dl_path.write_bytes(b"fake_audio" * 100)
        mock_dl.return_value = DownloadedAudio(
            path=dl_path,
            candidate=_rank_candidate(
                {"id": "vid1", "title": "Artist1 - Song1", "uploader": "Artist1 - Topic", "webpage_url": "https://youtu.be/vid1"},
                requested_artist="Artist1",
                requested_title="Song1",
            ),
        )

        with self.db.session() as conn:
            self._seed_song(conn, "t1", "Song1", "Artist1")
            row = conn.execute(
                """
                SELECT s.id AS submission_id, so.title, so.artist, a.file_path, a.is_valid, a.duration_seconds
                FROM submissions s
                JOIN songs so ON so.id = s.song_id
                LEFT JOIN audio_assets a ON a.song_id = s.song_id
                WHERE s.id = 1
                """,
            ).fetchone()

            with patch("radio_app.services.youtube.validate_mp3_and_get_duration_seconds", return_value=(180, None)):
                results = ensure_audio_for_songs(conn, [row], self.root / "uploads")

            self.assertTrue(results[1].startswith("downloaded:"))
            asset = conn.execute("SELECT * FROM audio_assets WHERE song_id = 1").fetchone()
            self.assertEqual(asset["duration_seconds"], 180)
            audit = conn.execute("SELECT action, detail FROM audit_logs WHERE action = 'youtube_audio_selected' ORDER BY id DESC LIMIT 1").fetchone()
            self.assertIsNotNone(audit)
            self.assertIn('"video_id": "vid1"', audit["detail"])

    @patch("radio_app.services.youtube.search_and_download")
    def test_marks_weak_fallback_downloads(self, mock_dl: MagicMock) -> None:
        dl_path = self.root / "fallback.mp3"
        dl_path.write_bytes(b"fresh_audio" * 100)
        weak_candidate = _rank_candidate(
            {"id": "weak2", "title": "Song2 audio", "uploader": "fan uploader", "webpage_url": "https://youtu.be/weak2"},
            requested_artist="Artist2",
            requested_title="Song2",
        )
        mock_dl.return_value = DownloadedAudio(path=dl_path, candidate=weak_candidate)

        with self.db.session() as conn:
            self._seed_song(conn, "t2", "Song2", "Artist2")
            row = conn.execute(
                """
                SELECT s.id AS submission_id, so.title, so.artist, a.file_path, a.is_valid, a.duration_seconds
                FROM submissions s
                JOIN songs so ON so.id = s.song_id
                LEFT JOIN audio_assets a ON a.song_id = s.song_id
                WHERE s.id = 1
                """,
            ).fetchone()

            with patch("radio_app.services.youtube.validate_mp3_and_get_duration_seconds", return_value=(200, None)):
                results = ensure_audio_for_songs(conn, [row], self.root / "uploads")

            self.assertEqual(results[1], "downloaded:weak-fallback:duration=200s")

    @patch("radio_app.services.youtube.search_and_download")
    def test_invalid_download_file_is_deleted(self, mock_dl: MagicMock) -> None:
        dl_path = self.root / "bad-download.mp3"
        dl_path.write_bytes(b"bad")
        mock_dl.return_value = DownloadedAudio(
            path=dl_path,
            candidate=_rank_candidate(
                {"id": "vid3", "title": "Artist3 - Song3", "uploader": "Artist3 - Topic", "webpage_url": "https://youtu.be/vid3"},
                requested_artist="Artist3",
                requested_title="Song3",
            ),
        )

        with self.db.session() as conn:
            self._seed_song(conn, "t3", "Song3", "Artist3")
            row = conn.execute(
                """
                SELECT s.id AS submission_id, so.title, so.artist, a.file_path, a.is_valid, a.duration_seconds
                FROM submissions s
                JOIN songs so ON so.id = s.song_id
                LEFT JOIN audio_assets a ON a.song_id = s.song_id
                WHERE s.id = 1
                """,
            ).fetchone()

            with patch("radio_app.services.youtube.validate_mp3_and_get_duration_seconds", return_value=(0, "unable-to-parse-mp3-duration")):
                results = ensure_audio_for_songs(conn, [row], self.root / "uploads")

            self.assertTrue(results[1].startswith("invalid-download:"))
            self.assertFalse(dl_path.exists())


if __name__ == "__main__":
    unittest.main()
