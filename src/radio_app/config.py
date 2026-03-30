from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _csv_env(name: str) -> tuple[str, ...]:
    raw = os.getenv(name, "")
    return tuple(item.strip() for item in raw.split(",") if item.strip())


@dataclass(frozen=True)
class AppConfig:
    host: str = os.getenv("RADIO_HOST", "127.0.0.1")
    port: int = int(os.getenv("RADIO_PORT", "8080"))
    db_path: Path = Path(os.getenv("RADIO_DB_PATH", "data/radio.db"))
    uploads_dir: Path = Path(os.getenv("RADIO_UPLOADS_DIR", "uploads"))
    artifacts_dir: Path = Path(os.getenv("RADIO_ARTIFACTS_DIR", "artifacts"))
    session_ttl_hours: int = int(os.getenv("RADIO_SESSION_TTL_HOURS", "24"))
    timezone: str = os.getenv("RADIO_TIMEZONE", "Asia/Seoul")
    search_country: str = os.getenv("RADIO_SEARCH_COUNTRY", "KR")
    riro_auth_mode: str = os.getenv("RIRO_AUTH_MODE", "riro")
    allow_nonlocal_mock_auth: bool = os.getenv("RADIO_ALLOW_NONLOCAL_MOCK_AUTH", "0").lower() in ("1", "true", "yes")
    ffmpeg_path: str | None = os.getenv("RADIO_FFMPEG_PATH")
    scheduler_interval_seconds: int = int(os.getenv("RADIO_SCHEDULER_INTERVAL_SECONDS", "30"))
    file_retention_seconds: int = int(os.getenv("RADIO_FILE_RETENTION_SECONDS", "86400"))
    audit_log_retention_days: int = int(os.getenv("RADIO_AUDIT_LOG_RETENTION_DAYS", "30"))
    sqlite_busy_timeout_ms: int = int(os.getenv("RADIO_SQLITE_BUSY_TIMEOUT_MS", "5000"))
    sqlite_journal_mode: str = os.getenv("RADIO_SQLITE_JOURNAL_MODE", "WAL")
    sqlite_synchronous: str = os.getenv("RADIO_SQLITE_SYNCHRONOUS", "NORMAL")
    yt_dlp_enabled: bool = os.getenv("RADIO_YT_DLP_ENABLED", "1").lower() in ("1", "true", "yes")
    login_failure_window_seconds: int = int(os.getenv("RADIO_LOGIN_FAILURE_WINDOW_SECONDS", "300"))
    login_failure_limit_per_user: int = int(os.getenv("RADIO_LOGIN_FAILURE_LIMIT_PER_USER", "10"))
    session_cookie_secure: bool = os.getenv("RADIO_SESSION_COOKIE_SECURE", "1").lower() in ("1", "true", "yes")
    super_admin_ids: tuple[str, ...] = _csv_env("RADIO_SUPER_ADMIN_IDS")


def ensure_directories(cfg: AppConfig) -> None:
    cfg.db_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.uploads_dir.mkdir(parents=True, exist_ok=True)
    cfg.artifacts_dir.mkdir(parents=True, exist_ok=True)
