from __future__ import annotations

import shutil
import sys
from dataclasses import replace
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


def _resolve_media_binary(binary_name: str, configured_path: str | None = None) -> Path | None:
    if configured_path:
        candidate = Path(configured_path)
        if candidate.is_dir():
            binary_candidate = candidate / binary_name
        elif candidate.name == binary_name:
            binary_candidate = candidate
        else:
            binary_candidate = candidate.parent / binary_name
        if binary_candidate.exists():
            return binary_candidate.resolve()

    discovered = shutil.which(binary_name)
    return Path(discovered).resolve() if discovered else None


def validate_media_toolchain(cfg: AppConfig) -> tuple[Path, Path]:
    ffmpeg = _resolve_media_binary("ffmpeg", cfg.ffmpeg_path)
    ffprobe = _resolve_media_binary("ffprobe", cfg.ffmpeg_path)
    if ffmpeg is None or ffprobe is None:
        raise RuntimeError(
            "ffmpeg and ffprobe are required to run this server. "
            "Install them or set RADIO_FFMPEG_PATH to the directory/binary location."
        )
    return ffmpeg, ffprobe


def main() -> None:
    cfg = AppConfig()
    ffmpeg_path, _ffprobe_path = validate_media_toolchain(cfg)
    cfg = replace(cfg, ffmpeg_path=str(ffmpeg_path))
    ensure_directories(cfg)

    db = DB(
        path=cfg.db_path,
        busy_timeout_ms=cfg.sqlite_busy_timeout_ms,
        journal_mode=cfg.sqlite_journal_mode,
        synchronous=cfg.sqlite_synchronous,
    )
    db.init_schema()

    song_search = ITunesSearchClient(country=cfg.search_country)
    ctx = AppContext(cfg=cfg, db=db, song_search=song_search)

    scheduler = RoundAutoCloser(
        db=db,
        artifacts_dir=cfg.artifacts_dir,
        interval_seconds=cfg.scheduler_interval_seconds,
        file_retention_seconds=cfg.file_retention_seconds,
        audit_log_retention_days=cfg.audit_log_retention_days,
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
