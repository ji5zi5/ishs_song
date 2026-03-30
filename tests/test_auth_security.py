from __future__ import annotations

import importlib
import json
import os
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
from radio_app.config import AppConfig
from radio_app.db import DB
from radio_app.services.music_search import ITunesSearchClient
from radio_app.services.riro import RiroAuthResult


class LoginSecurityTest(unittest.TestCase):
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
        self.cfg = AppConfig(
            host="127.0.0.1",
            port=0,
            db_path=self.db.path,
            uploads_dir=self.uploads_dir,
            artifacts_dir=self.artifacts_dir,
            riro_auth_mode="riro",
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

    def _request_json(self, path: str, method: str = "GET", body: dict | None = None) -> tuple[int, dict, dict]:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {"Content-Type": "application/json"} if body is not None else {}
        request = urllib.request.Request(f"{self.base_url}{path}", data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8")) if response.length else {}
                return response.status, dict(response.headers), payload
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            payload = json.loads(raw.decode("utf-8")) if raw else {}
            return exc.code, dict(exc.headers), payload

    def test_failed_logins_are_rate_limited_per_login_id_after_ten_failures(self) -> None:
        failing = RiroAuthResult(status="error", message="bad credentials")
        with mock.patch("radio_app.app.check_riro_login", return_value=failing) as mocked:
            for _ in range(10):
                status, _, payload = self._request_json(
                    "/api/auth/login",
                    method="POST",
                    body={"riro_id": "24010001", "riro_pw": "wrong"},
                )
                self.assertEqual(status, 401)
                self.assertEqual(payload["error"], "riro-auth-failed")

            status, _, payload = self._request_json(
                "/api/auth/login",
                method="POST",
                body={"riro_id": "24010001", "riro_pw": "wrong"},
            )
        self.assertEqual(status, 429)
        self.assertEqual(payload["error"], "rate-limit")
        self.assertEqual(mocked.call_count, 10)

    def test_failed_logins_for_other_ids_are_not_blocked_by_shared_ip(self) -> None:
        failing = RiroAuthResult(status="error", message="bad credentials")
        with mock.patch("radio_app.app.check_riro_login", return_value=failing) as mocked:
            for _ in range(10):
                status, _, payload = self._request_json(
                    "/api/auth/login",
                    method="POST",
                    body={"riro_id": "24010001", "riro_pw": "wrong"},
                )
                self.assertEqual(status, 401)
                self.assertEqual(payload["error"], "riro-auth-failed")

            status, _, payload = self._request_json(
                "/api/auth/login",
                method="POST",
                body={"riro_id": "24010002", "riro_pw": "wrong"},
            )
        self.assertEqual(status, 401)
        self.assertEqual(payload["error"], "riro-auth-failed")
        self.assertEqual(mocked.call_count, 11)


class ConfigSecurityTest(unittest.TestCase):
    def tearDown(self) -> None:
        import radio_app.config as config_module

        importlib.reload(config_module)

    def test_session_cookie_secure_defaults_to_true_without_env_override(self) -> None:
        import radio_app.config as config_module

        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("RADIO_SESSION_COOKIE_SECURE", None)
            reloaded = importlib.reload(config_module)
            self.assertTrue(reloaded.AppConfig().session_cookie_secure)

    def test_session_cookie_secure_can_be_disabled_explicitly(self) -> None:
        import radio_app.config as config_module

        with mock.patch.dict(os.environ, {"RADIO_SESSION_COOKIE_SECURE": "0"}, clear=False):
            reloaded = importlib.reload(config_module)
            self.assertFalse(reloaded.AppConfig().session_cookie_secure)

    def test_super_admin_ids_are_loaded_from_env(self) -> None:
        import radio_app.config as config_module

        with mock.patch.dict(os.environ, {"RADIO_SUPER_ADMIN_IDS": "admin, manager , ,lead"}, clear=False):
            reloaded = importlib.reload(config_module)
            self.assertEqual(reloaded.AppConfig().super_admin_ids, ("admin", "manager", "lead"))


if __name__ == "__main__":
    unittest.main()
