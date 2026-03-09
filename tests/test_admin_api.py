from __future__ import annotations

import json
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from radio_app.app import AppContext, make_server
from radio_app.auth import issue_session
from radio_app.config import AppConfig
from radio_app.db import DB, utc_now_iso
from radio_app.services.music_search import ITunesSearchClient
from radio_app.services.rounds import ensure_open_round, set_setting


class AdminApiTest(unittest.TestCase):
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
        self._seed_data()

        self.cfg = AppConfig(
            host="127.0.0.1",
            port=0,
            db_path=self.db.path,
            uploads_dir=self.uploads_dir,
            artifacts_dir=self.artifacts_dir,
            riro_auth_mode="mock",
        )
        self.ctx = AppContext(cfg=self.cfg, db=self.db, song_search=ITunesSearchClient(country="KR"))
        self.server = make_server(self.ctx)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self._stop_server)
        host, port = self.server.server_address
        self.base_url = f"http://{host}:{port}"

    def _stop_server(self) -> None:
        if hasattr(self, "server"):
            self.server.shutdown()
            self.server.server_close()
        if hasattr(self, "thread"):
            self.thread.join(timeout=2)

    def _seed_data(self) -> None:
        with self.db.session() as conn:
            self.admin_user_id = self._insert_user(conn, "admin", "관리자", True)
            self.member_user_id = self._insert_user(conn, "member", "일반유저", False)
            self.other_user_id = self._insert_user(conn, "other", "신청자", False)
            self.admin_cookie = f"session={issue_session(conn, self.admin_user_id, 24)}"
            self.member_cookie = f"session={issue_session(conn, self.member_user_id, 24)}"
            set_setting(conn, "round_default_cadence", "weekly")
            set_setting(conn, "default_playlist_size", "15")
            set_setting(conn, "default_target_seconds", "1800")
            set_setting(conn, "default_loudnorm_enabled", "0")

            round_row = ensure_open_round(conn, "Asia/Seoul")
            self.round_id = int(round_row["id"])
            self.visible_submission_id = self._insert_submission(conn, "itunes:100", "Visible Song", "Visible Artist", self.member_user_id, False)
            self.hidden_submission_id = self._insert_submission(conn, "itunes:200", "Hidden Song", "Hidden Artist", self.other_user_id, True)
            conn.execute(
                "INSERT INTO votes(round_id, user_id, submission_id, voted_at) VALUES (?, ?, ?, ?)",
                (self.round_id, self.admin_user_id, self.visible_submission_id, utc_now_iso()),
            )
            self.m3u_path = self.artifacts_dir / f"round-{self.round_id}.m3u"
            self.mp3_path = self.artifacts_dir / f"round-{self.round_id}.mp3"
            self.m3u_path.write_text("#EXTM3U\n", encoding="utf-8")
            self.mp3_path.write_bytes(b"fake-mp3")
            conn.execute(
                "INSERT INTO round_artifacts(round_id, m3u_path, mp3_path, total_seconds, generation_log, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (self.round_id, str(self.m3u_path), str(self.mp3_path), 1810, "merged\nselected=2", utc_now_iso()),
            )
            self.artifact_id = int(conn.execute("SELECT id FROM round_artifacts WHERE round_id = ?", (self.round_id,)).fetchone()["id"])
            conn.execute(
                "INSERT INTO audit_logs(round_id, actor_user_id, action, detail, created_at) VALUES (?, ?, ?, ?, ?)",
                (self.round_id, self.admin_user_id, "seed", "initial entry", utc_now_iso()),
            )

    def _insert_user(self, conn, riro_user_key: str, display_name: str, is_admin: bool) -> int:
        conn.execute(
            "INSERT INTO users(riro_user_key, display_name, is_admin_approved, created_at) VALUES (?, ?, ?, ?)",
            (riro_user_key, display_name, 1 if is_admin else 0, utc_now_iso()),
        )
        return int(conn.execute("SELECT id FROM users WHERE riro_user_key = ?", (riro_user_key,)).fetchone()["id"])

    def _insert_submission(self, conn, track_id: str, title: str, artist: str, user_id: int, is_hidden: bool) -> int:
        conn.execute(
            "INSERT INTO songs(spotify_track_id, title, artist, album_art_url, external_url, created_at) VALUES (?, ?, ?, '', '', ?)",
            (track_id, title, artist, utc_now_iso()),
        )
        song_id = int(conn.execute("SELECT id FROM songs WHERE spotify_track_id = ?", (track_id,)).fetchone()["id"])
        conn.execute(
            "INSERT INTO submissions(round_id, user_id, song_id, is_hidden, submitted_at) VALUES (?, ?, ?, ?, ?)",
            (self.round_id, user_id, song_id, 1 if is_hidden else 0, utc_now_iso()),
        )
        return int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

    def _request(self, path: str, method: str = "GET", body: dict | None = None, cookie: str | None = None) -> tuple[int, dict, bytes]:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if cookie:
            headers["Cookie"] = cookie
        request = urllib.request.Request(f"{self.base_url}{path}", data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, dict(response.headers), response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers), exc.read()

    def _request_json(self, path: str, method: str = "GET", body: dict | None = None, cookie: str | None = None) -> tuple[int, dict, dict]:
        status, headers, raw = self._request(path, method=method, body=body, cookie=cookie)
        payload = json.loads(raw.decode("utf-8")) if raw else {}
        return status, headers, payload

    def test_admin_settings_current_auth_and_persisted_defaults(self) -> None:
        status, _, payload = self._request_json("/api/admin/settings/current")
        self.assertEqual(status, 401)
        self.assertEqual(payload["error"], "auth-required")

        status, _, payload = self._request_json("/api/admin/settings/current", cookie=self.member_cookie)
        self.assertEqual(status, 403)
        self.assertEqual(payload["error"], "admin-required")

        status, _, payload = self._request_json("/api/admin/settings/current", cookie=self.admin_cookie)
        self.assertEqual(status, 200)
        self.assertEqual(payload["defaults"]["cadence"], "weekly")
        self.assertEqual(payload["defaults"]["playlist_size"], 15)
        self.assertEqual(payload["defaults"]["target_seconds"], 1800)
        self.assertFalse(payload["defaults"]["loudnorm_enabled"])

    def test_admin_users_endpoint_returns_expected_fields(self) -> None:
        status, _, payload = self._request_json("/api/admin/users", cookie=self.admin_cookie)
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(payload["users"]), 3)
        admin_user = next(user for user in payload["users"] if user["id"] == self.admin_user_id)
        self.assertEqual(admin_user["display_name"], "관리자")
        self.assertTrue(admin_user["is_admin_approved"])
        self.assertIn("created_at", admin_user)

    def test_admin_submissions_current_returns_hidden_flag(self) -> None:
        status, _, payload = self._request_json("/api/admin/submissions/current", cookie=self.admin_cookie)
        self.assertEqual(status, 200)
        self.assertEqual(payload["round_id"], self.round_id)
        hidden_item = next(item for item in payload["items"] if item["submission_id"] == self.hidden_submission_id)
        self.assertTrue(hidden_item["is_hidden"])
        visible_item = next(item for item in payload["items"] if item["submission_id"] == self.visible_submission_id)
        self.assertEqual(visible_item["vote_count"], 1)

    def test_admin_audit_logs_invalid_limit_returns_400(self) -> None:
        status, _, payload = self._request_json("/api/admin/audit-logs?limit=abc", cookie=self.admin_cookie)
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"], "invalid-audit-log-limit")

    def test_admin_latest_artifact_returns_summary_without_paths(self) -> None:
        status, _, payload = self._request_json("/api/admin/artifacts/latest", cookie=self.admin_cookie)
        self.assertEqual(status, 200)
        artifact = payload["artifact"]
        self.assertEqual(artifact["id"], self.artifact_id)
        self.assertTrue(artifact["has_m3u"])
        self.assertTrue(artifact["has_mp3"])
        self.assertNotIn("m3u_path", artifact)
        self.assertNotIn("mp3_path", artifact)
        self.assertNotIn("generation_log", artifact)

    def test_admin_approve_user_returns_404_and_409_and_writes_audit(self) -> None:
        status, _, payload = self._request_json(
            "/api/admin/users/approve",
            method="POST",
            body={"user_id": 999999, "approved": True},
            cookie=self.admin_cookie,
        )
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"], "user-not-found")

        status, _, payload = self._request_json(
            "/api/admin/users/approve",
            method="POST",
            body={"user_id": self.admin_user_id, "approved": True},
            cookie=self.admin_cookie,
        )
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"], "admin-state-unchanged")

        status, _, payload = self._request_json(
            "/api/admin/users/approve",
            method="POST",
            body={"user_id": self.member_user_id, "approved": True},
            cookie=self.admin_cookie,
        )
        self.assertEqual(status, 200)
        self.assertTrue(payload["approved"])
        with self.db.session() as conn:
            row = conn.execute("SELECT is_admin_approved FROM users WHERE id = ?", (self.member_user_id,)).fetchone()
            self.assertEqual(int(row["is_admin_approved"]), 1)
            audit = conn.execute("SELECT action, detail FROM audit_logs WHERE action = 'admin_approval_changed' ORDER BY id DESC LIMIT 1").fetchone()
            self.assertIsNotNone(audit)
            self.assertIn(f'"user_id": {self.member_user_id}', audit["detail"])

    def test_admin_hide_submission_is_disabled(self) -> None:
        status, _, payload = self._request_json(
            "/api/admin/submissions/hide",
            method="POST",
            body={"submission_id": self.visible_submission_id, "is_hidden": True},
            cookie=self.admin_cookie,
        )
        self.assertEqual(status, 403)
        self.assertEqual(payload["error"], "submission-hide-disabled")
        with self.db.session() as conn:
            row = conn.execute("SELECT is_hidden FROM submissions WHERE id = ?", (self.visible_submission_id,)).fetchone()
            self.assertEqual(int(row["is_hidden"]), 0)

    def test_admin_artifact_download_validation_and_success(self) -> None:
        status, _, payload = self._request_json("/api/admin/artifacts/download?artifact_id=1&type=mp3")
        self.assertEqual(status, 401)
        self.assertEqual(payload["error"], "auth-required")

        status, _, payload = self._request_json("/api/admin/artifacts/download?artifact_id=bad&type=mp3", cookie=self.admin_cookie)
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"], "invalid-artifact-download-request")

        status, _, payload = self._request_json("/api/admin/artifacts/download?artifact_id=99999&type=mp3", cookie=self.admin_cookie)
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"], "artifact-not-found")

        status, headers, raw = self._request(
            f"/api/admin/artifacts/download?artifact_id={self.artifact_id}&type=m3u",
            cookie=self.admin_cookie,
        )
        self.assertEqual(status, 200)
        self.assertIn('attachment; filename="3_-playlist.m3u"', headers["Content-Disposition"])
        self.assertIn("filename*=UTF-8''3%EC%9B%94-playlist.m3u", headers["Content-Disposition"])
        self.assertIn(b"#EXTM3U", raw)

    def test_admin_artifact_download_rejects_out_of_root_files(self) -> None:
        outside_path = self.root / "outside.mp3"
        outside_path.write_bytes(b"outside")
        with self.db.session() as conn:
            conn.execute(
                "UPDATE round_artifacts SET mp3_path = ? WHERE id = ?",
                (str(outside_path), self.artifact_id),
            )
        status, _, payload = self._request_json(
            f"/api/admin/artifacts/download?artifact_id={self.artifact_id}&type=mp3",
            cookie=self.admin_cookie,
        )
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"], "artifact-file-missing")

    def test_public_results_redacts_private_artifact_fields(self) -> None:
        status, _, payload = self._request_json(f"/api/public/results?round_id={self.round_id}")
        self.assertEqual(status, 200)
        self.assertEqual(payload["artifact"]["total_seconds"], 1810)
        self.assertIn("created_at", payload["artifact"])
        self.assertNotIn("m3u_path", payload["artifact"])
        self.assertNotIn("mp3_path", payload["artifact"])
        self.assertNotIn("generation_log", payload["artifact"])

    def test_unexpected_errors_are_sanitized_for_get_post_and_put(self) -> None:
        with mock.patch("radio_app.app.LOGGER.exception"):
            with mock.patch.object(self.server.RequestHandlerClass, "_handle_public_songs", side_effect=RuntimeError("secret-get")):
                status, _, payload = self._request_json("/api/public/songs")
                self.assertEqual(status, 500)
                self.assertEqual(payload, {"error": "internal-error", "message": "서버 오류가 발생했습니다."})

            with mock.patch.object(self.server.RequestHandlerClass, "_handle_admin_settings", side_effect=RuntimeError("secret-post")):
                status, _, payload = self._request_json(
                    "/api/admin/settings",
                    method="POST",
                    body={"cadence": "monthly", "playlist_size": 12, "target_seconds": 2400, "loudnorm_enabled": True},
                    cookie=self.admin_cookie,
                )
                self.assertEqual(status, 500)
                self.assertNotIn("secret-post", json.dumps(payload, ensure_ascii=False))

            with mock.patch.object(self.server.RequestHandlerClass, "_handle_votes_replace", side_effect=RuntimeError("secret-put")):
                status, _, payload = self._request_json(
                    "/api/votes",
                    method="PUT",
                    body={"submission_ids": []},
                    cookie=self.member_cookie,
                )
                self.assertEqual(status, 500)
                self.assertNotIn("secret-put", json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    unittest.main()
