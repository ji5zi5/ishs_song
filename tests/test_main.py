from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from main import validate_media_toolchain
from radio_app.config import AppConfig


class MediaToolchainValidationTest(unittest.TestCase):
    def test_raises_when_ffmpeg_and_ffprobe_are_missing(self) -> None:
        cfg = AppConfig(ffmpeg_path=None)
        with patch("main.shutil.which", return_value=None):
            with self.assertRaises(RuntimeError) as ctx:
                validate_media_toolchain(cfg)
        self.assertIn("ffmpeg", str(ctx.exception))
        self.assertIn("ffprobe", str(ctx.exception))

    def test_accepts_configured_ffmpeg_and_sibling_ffprobe(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            bin_dir = Path(td)
            ffmpeg = bin_dir / "ffmpeg"
            ffprobe = bin_dir / "ffprobe"
            ffmpeg.write_text("", encoding="utf-8")
            ffprobe.write_text("", encoding="utf-8")
            cfg = AppConfig(ffmpeg_path=str(ffmpeg))

            with patch("main.shutil.which", return_value=None):
                resolved_ffmpeg, resolved_ffprobe = validate_media_toolchain(cfg)

            self.assertEqual(resolved_ffmpeg, ffmpeg)
            self.assertEqual(resolved_ffprobe, ffprobe)


if __name__ == "__main__":
    unittest.main()
