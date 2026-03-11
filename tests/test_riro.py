from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

import requests

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from radio_app.services.riro import check_riro_login


class _FakeResponse:
    def __init__(self, *, status_code: int = 200, text: str = "", json_data=None, json_error: Exception | None = None) -> None:
        self.status_code = status_code
        self.text = text
        self._json_data = json_data
        self._json_error = json_error

    def json(self):
        if self._json_error is not None:
            raise self._json_error
        return self._json_data


class RiroAuthTest(unittest.TestCase):
    def test_returns_network_block_message_on_forbidden_html_response(self) -> None:
        fake_session = mock.Mock()
        fake_session.post.side_effect = [
            _FakeResponse(status_code=200),
            _FakeResponse(
                status_code=403,
                text="<html><title>에러페이지</title><img src='403.jpg' alt='페이지를 찾을 수 없습니다.'></html>",
                json_error=ValueError("not json"),
            ),
        ]

        with mock.patch("radio_app.services.riro.requests.Session", return_value=fake_session):
            result = check_riro_login("24010001", "pw", max_retries=1, sleep_seconds=0)

        self.assertEqual(result.status, "error")
        self.assertIn("VPN", result.message or "")

    def test_returns_timeout_message_when_riro_server_times_out(self) -> None:
        fake_session = mock.Mock()
        fake_session.post.side_effect = [
            _FakeResponse(status_code=200),
            requests.Timeout("timed out"),
        ]

        with mock.patch("radio_app.services.riro.requests.Session", return_value=fake_session):
            result = check_riro_login("24010001", "pw", max_retries=1, sleep_seconds=0)

        self.assertEqual(result.status, "error")
        self.assertIn("지연", result.message or "")

    def test_returns_invalid_credentials_message_for_code_902(self) -> None:
        fake_session = mock.Mock()
        fake_session.post.side_effect = [
            _FakeResponse(status_code=200),
            _FakeResponse(status_code=200, json_data={"code": "902"}),
        ]

        with mock.patch("radio_app.services.riro.requests.Session", return_value=fake_session):
            result = check_riro_login("24010001", "pw", max_retries=1, sleep_seconds=0)

        self.assertEqual(result.status, "error")
        self.assertEqual(result.message, "아이디 또는 비밀번호가 틀렸습니다.")


if __name__ == "__main__":
    unittest.main()
