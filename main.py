from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from radio_app.app import AppContext, make_server
from radio_app.config import AppConfig, ensure_directories
from radio_app.db import DB
from radio_app.scheduler import RoundAutoCloser
from radio_app.services.music_search import ITunesSearchClient


def main() -> None:
    cfg = AppConfig()
    ensure_directories(cfg)

    db = DB(path=cfg.db_path)
    db.init_schema()

    song_search = ITunesSearchClient(country=cfg.search_country)
    ctx = AppContext(cfg=cfg, db=db, song_search=song_search)

    scheduler = RoundAutoCloser(
        db=db,
        artifacts_dir=cfg.artifacts_dir,
        interval_seconds=cfg.scheduler_interval_seconds,
        ffmpeg_path=cfg.ffmpeg_path,
        uploads_dir=cfg.uploads_dir,
        yt_dlp_enabled=cfg.yt_dlp_enabled,
    )
    scheduler.start()

    server = make_server(ctx)
    print(f"radio-app listening on http://{cfg.host}:{cfg.port}")
    try:
        server.serve_forever()
    finally:
        scheduler.stop()


if __name__ == "__main__":
    main()
