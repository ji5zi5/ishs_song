from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from radio_app.services.audio import merge_mp3_files


class MergeMp3FilesTest(unittest.TestCase):
    def test_concat_temp_file_is_removed_when_ffmpeg_launch_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            src = root / "a.mp3"
            src.write_bytes(b"fake-audio")
            out = root / "out.mp3"
            concat_file = root / "out.concat.txt"

            with self.assertRaises(RuntimeError):
                merge_mp3_files([src], out, loudnorm_enabled=False, ffmpeg_path="/definitely-not-found/ffmpeg")

            self.assertFalse(concat_file.exists())


if __name__ == "__main__":
    unittest.main()

