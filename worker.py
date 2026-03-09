from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from radio_app.config import AppConfig, ensure_directories
from radio_app.db import DB
from radio_app.scheduler import RoundAutoCloser


def main() -> None:
    cfg = AppConfig()
    ensure_directories(cfg)
    db = DB(path=cfg.db_path)
    db.init_schema()
    closer = RoundAutoCloser(
        db=db,
        artifacts_dir=cfg.artifacts_dir,
        interval_seconds=cfg.scheduler_interval_seconds,
        ffmpeg_path=cfg.ffmpeg_path,
        uploads_dir=cfg.uploads_dir,
        yt_dlp_enabled=cfg.yt_dlp_enabled,
    )
    closer.start()
    print("round worker started")
    try:
        while True:
            time.sleep(60)
    finally:
        closer.stop()


if __name__ == "__main__":
    main()
