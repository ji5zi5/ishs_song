from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class PublicUiSecurityTest(unittest.TestCase):
    def test_static_pages_do_not_use_inline_event_handlers(self) -> None:
        pages = [
            ROOT / "src" / "radio_app" / "static" / "index.html",
            ROOT / "src" / "radio_app" / "static" / "submit.html",
            ROOT / "src" / "radio_app" / "static" / "vote.html",
            ROOT / "src" / "radio_app" / "static" / "admin.html",
        ]
        for path in pages:
            html = path.read_text(encoding="utf-8")
            self.assertNotIn("onclick=", html, msg=str(path))
            self.assertNotIn("onchange=", html, msg=str(path))
            self.assertIn('<script nonce="__CSP_NONCE__">', html, msg=str(path))

    def test_index_page_does_not_interpolate_song_fields_into_innerhtml(self) -> None:
        html = (ROOT / "src" / "radio_app" / "static" / "index.html").read_text(encoding="utf-8")
        self.assertNotIn("${item.title}", html)
        self.assertNotIn("${item.artist}", html)
        self.assertNotIn("${item.album_art_url", html)

    def test_submit_page_does_not_interpolate_song_fields_into_innerhtml(self) -> None:
        html = (ROOT / "src" / "radio_app" / "static" / "submit.html").read_text(encoding="utf-8")
        self.assertNotIn("${track.title}", html)
        self.assertNotIn("${track.artist}", html)
        self.assertNotIn("${track.album_art_url", html)

    def test_vote_page_does_not_interpolate_song_fields_into_innerhtml(self) -> None:
        html = (ROOT / "src" / "radio_app" / "static" / "vote.html").read_text(encoding="utf-8")
        self.assertNotIn("${item.title}", html)
        self.assertNotIn("${item.artist}", html)
        self.assertNotIn("${item.album_art_url", html)


if __name__ == "__main__":
    unittest.main()
