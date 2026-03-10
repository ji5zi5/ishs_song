from __future__ import annotations

import secrets
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from radio_app.db import utc_now_iso
from radio_app.services.audio import merge_mp3_files, validate_mp3_and_get_duration_seconds
from radio_app.services.youtube import ensure_audio_for_songs


@dataclass
class RoundConfig:
    cadence: str
    playlist_size: int
    target_seconds: int
    loudnorm_enabled: bool


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def get_setting(conn: sqlite3.Connection, key: str, default: str) -> str:
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return str(row["value"]) if row else default


def set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO settings(key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def current_defaults(conn: sqlite3.Connection) -> RoundConfig:
    return RoundConfig(
        cadence=get_setting(conn, "round_default_cadence", "monthly"),
        playlist_size=int(get_setting(conn, "default_playlist_size", "12")),
        target_seconds=int(get_setting(conn, "default_target_seconds", "2400")),
        loudnorm_enabled=bool(int(get_setting(conn, "default_loudnorm_enabled", "1"))),
    )


def _window_for_now(cadence: str, timezone_name: str, now_utc: datetime | None = None) -> tuple[str, str]:
    tz = ZoneInfo(timezone_name)
    now = (now_utc or datetime.now(UTC)).astimezone(tz)
    if cadence == "weekly":
        start_local = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        end_local = start_local + timedelta(days=7)
    else:
        start_local = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start_local.month == 12:
            end_local = start_local.replace(year=start_local.year + 1, month=1)
        else:
            end_local = start_local.replace(month=start_local.month + 1)
    return _iso(start_local), _iso(end_local)


def ensure_open_round(conn: sqlite3.Connection, timezone_name: str) -> sqlite3.Row:
    now_iso = utc_now_iso()
    row = conn.execute(
        """
        SELECT * FROM rounds
        WHERE status = 'open' AND start_at <= ? AND end_at > ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (now_iso, now_iso),
    ).fetchone()
    if row:
        return row

    cfg = current_defaults(conn)
    start_at, end_at = _window_for_now(cfg.cadence, timezone_name)
    existing = conn.execute(
        "SELECT * FROM rounds WHERE start_at = ? AND end_at = ? ORDER BY id DESC LIMIT 1",
        (start_at, end_at),
    ).fetchone()
    if existing and existing["status"] == "open":
        return existing
    conn.execute(
        """
        INSERT INTO rounds(cadence, status, start_at, end_at, playlist_size, target_seconds, loudnorm_enabled, created_at)
        VALUES (?, 'open', ?, ?, ?, ?, ?, ?)
        """,
        (
            cfg.cadence,
            start_at,
            end_at,
            cfg.playlist_size,
            cfg.target_seconds,
            1 if cfg.loudnorm_enabled else 0,
            utc_now_iso(),
        ),
    )
    return conn.execute("SELECT * FROM rounds ORDER BY id DESC LIMIT 1").fetchone()


def select_round_for_admin_close(conn: sqlite3.Connection, timezone_name: str) -> sqlite3.Row:
    open_round = ensure_open_round(conn, timezone_name)
    now_iso = utc_now_iso()
    stale_closing_round = conn.execute(
        """
        SELECT r.*
        FROM rounds r
        WHERE r.status = 'closing'
          AND r.start_at <= ?
          AND r.end_at > ?
        ORDER BY r.id DESC
        LIMIT 1
        """,
        (now_iso, now_iso),
    ).fetchone()
    if stale_closing_round is None:
        return open_round

    open_submission_count = int(
        conn.execute(
            "SELECT COUNT(*) AS cnt FROM submissions WHERE round_id = ?",
            (int(open_round["id"]),),
        ).fetchone()["cnt"]
    )
    stale_submission_count = int(
        conn.execute(
            "SELECT COUNT(*) AS cnt FROM submissions WHERE round_id = ?",
            (int(stale_closing_round["id"]),),
        ).fetchone()["cnt"]
    )
    has_artifact = conn.execute(
        "SELECT 1 FROM round_artifacts WHERE round_id = ?",
        (int(stale_closing_round["id"]),),
    ).fetchone()
    if stale_submission_count > 0 and open_submission_count == 0 and has_artifact is None:
        conn.execute(
            "UPDATE rounds SET status = 'open', close_job_key = NULL WHERE id = ?",
            (int(stale_closing_round["id"]),),
        )
        conn.commit()
        reopened = conn.execute(
            "SELECT * FROM rounds WHERE id = ?",
            (int(stale_closing_round["id"]),),
        ).fetchone()
        if reopened is not None:
            return reopened
    return open_round


def enforce_rate_limit(
    conn: sqlite3.Connection,
    user_id: int,
    action: str,
    max_count: int,
    window_seconds: int,
) -> bool:
    now = datetime.now(UTC)
    floor = _iso(now - timedelta(seconds=window_seconds))
    conn.execute(
        "DELETE FROM rate_events WHERE action = ? AND created_at < ?",
        (action, floor),
    )
    count = conn.execute(
        "SELECT COUNT(*) AS cnt FROM rate_events WHERE user_id = ? AND action = ? AND created_at >= ?",
        (user_id, action, floor),
    ).fetchone()["cnt"]
    if int(count) >= max_count:
        return False
    conn.execute(
        "INSERT INTO rate_events(user_id, action, created_at) VALUES (?, ?, ?)",
        (user_id, action, utc_now_iso()),
    )
    return True


def ranked_submissions(conn: sqlite3.Connection, round_id: int) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT
                s.id AS submission_id,
                s.song_id AS song_id,
                s.submitted_at,
                so.spotify_track_id,
                so.title,
                so.artist,
                so.album_art_url,
                so.external_url,
                a.file_path,
                a.duration_seconds,
                a.is_valid,
                COALESCE(v.votes, 0) AS vote_count
            FROM submissions s
            JOIN songs so ON so.id = s.song_id
            LEFT JOIN audio_assets a ON a.song_id = s.song_id
            LEFT JOIN (
                SELECT submission_id, COUNT(*) AS votes
                FROM votes
                WHERE round_id = ?
                GROUP BY submission_id
            ) v ON v.submission_id = s.id
            WHERE s.round_id = ? AND s.is_hidden = 0
            ORDER BY vote_count DESC, s.submitted_at ASC, s.id ASC
            """,
            (round_id, round_id),
        ).fetchall()
    )


def close_round(
    conn: sqlite3.Connection,
    round_id: int,
    artifacts_dir: Path,
    uploads_dir: Path | None = None,
    ffmpeg_path: str | None = None,
    yt_dlp_enabled: bool = True,
) -> dict:
    row = conn.execute("SELECT * FROM rounds WHERE id = ?", (round_id,)).fetchone()
    if row is None:
        raise ValueError("round-not-found")
    if row["status"] == "closed":
        existing = conn.execute("SELECT * FROM round_artifacts WHERE round_id = ?", (round_id,)).fetchone()
        return {
            "status": "already-closed",
            "round_id": round_id,
            "artifact": dict(existing) if existing else None,
        }
    if row["status"] == "closing":
        return {"status": "already-closing", "round_id": round_id}

    job_key = secrets.token_hex(8)
    conn.execute(
        "UPDATE rounds SET status = 'closing', close_job_key = ? WHERE id = ? AND status = 'open'",
        (job_key, round_id),
    )
    conn.commit()

    m3u_path: Path | None = None
    mp3_path: Path | None = None
    try:
        ranked = ranked_submissions(conn, round_id)

        # ── YouTube auto-download for songs without audio ──────────────
        yt_log_lines: list[str] = []
        if yt_dlp_enabled and uploads_dir:
            try:
                yt_results = ensure_audio_for_songs(
                    conn, list(ranked), uploads_dir, ffmpeg_path=ffmpeg_path,
                )
                for sub_id, status in yt_results.items():
                    yt_log_lines.append(f"yt:{sub_id}:{status}")
                # Re-fetch so newly downloaded audio_assets are visible.
                conn.commit()
                ranked = ranked_submissions(conn, round_id)
            except Exception as exc:
                yt_log_lines.append(f"yt:batch-error:{exc}")
        selected: list[dict] = []
        skipped: list[str] = []
        for item in ranked:
            if len(selected) >= int(row["playlist_size"]):
                break
            file_path = item["file_path"]
            duration = int(item["duration_seconds"] or 0)
            is_valid = int(item["is_valid"]) if item["is_valid"] is not None else 0
            if not file_path or not is_valid or duration <= 0 or not Path(file_path).exists():
                skipped.append(f"skip:{item['submission_id']}:missing-or-invalid-audio")
                continue
            checked_duration, validation_error = validate_mp3_and_get_duration_seconds(Path(file_path))
            if validation_error or checked_duration <= 0:
                conn.execute(
                    """
                    UPDATE audio_assets
                    SET is_valid = 0,
                        validation_error = ?,
                        duration_seconds = 0
                    WHERE song_id = ?
                    """,
                    (validation_error or "unable-to-parse-mp3-duration", int(item["song_id"])),
                )
                skipped.append(f"skip:{item['submission_id']}:revalidation-failed")
                continue
            selected_item = dict(item)
            selected_item["duration_seconds"] = checked_duration
            selected.append(selected_item)

        while selected and sum(int(s["duration_seconds"]) for s in selected) > int(row["target_seconds"]):
            dropped = selected.pop()
            skipped.append(f"drop:{dropped['submission_id']}:duration-trim")

        if not selected:
            raise RuntimeError("no-valid-audio-assets")

        artifacts_dir.mkdir(parents=True, exist_ok=True)
        m3u_path = artifacts_dir / f"round-{round_id}.m3u"
        mp3_path = artifacts_dir / f"round-{round_id}.mp3"
        m3u_path.write_text(
            "#EXTM3U\n"
            + "".join(
                f"#EXTINF:{int(item['duration_seconds'])},{item['artist']} - {item['title']}\n{item['file_path']}\n"
                for item in selected
            ),
            encoding="utf-8",
        )

        merge_log = merge_mp3_files(
            [Path(str(item["file_path"])) for item in selected],
            mp3_path,
            loudnorm_enabled=bool(row["loudnorm_enabled"]),
            ffmpeg_path=ffmpeg_path,
        )

        all_log_parts = [merge_log] + yt_log_lines + skipped
        generation_log = "\n".join(all_log_parts)
        total_seconds = sum(int(s["duration_seconds"]) for s in selected)
        conn.execute(
            """
            INSERT INTO round_artifacts(round_id, m3u_path, mp3_path, total_seconds, generation_log, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(round_id)
            DO UPDATE SET
                m3u_path = excluded.m3u_path,
                mp3_path = excluded.mp3_path,
                total_seconds = excluded.total_seconds,
                generation_log = excluded.generation_log,
                created_at = excluded.created_at
            """,
            (round_id, str(m3u_path), str(mp3_path), total_seconds, generation_log, utc_now_iso()),
        )
        conn.execute(
            "UPDATE rounds SET status = 'closed', closed_at = ? WHERE id = ?",
            (utc_now_iso(), round_id),
        )
        conn.execute(
            "INSERT INTO audit_logs(round_id, action, detail, created_at) VALUES (?, ?, ?, ?)",
            (
                round_id,
                "closed",
                f"tracks={len(selected)};total_seconds={total_seconds}",
                utc_now_iso(),
            ),
        )
        conn.commit()
        return {
            "status": "closed",
            "round_id": round_id,
            "selected_count": len(selected),
            "total_seconds": total_seconds,
            "m3u_path": str(m3u_path),
            "mp3_path": str(mp3_path),
            "generation_log": generation_log,
        }
    except Exception as exc:
        if m3u_path:
            m3u_path.unlink(missing_ok=True)
        if mp3_path:
            mp3_path.unlink(missing_ok=True)
        conn.execute(
            "UPDATE rounds SET status = 'open', close_job_key = NULL WHERE id = ?",
            (round_id,),
        )
        conn.execute(
            "INSERT INTO audit_logs(round_id, action, detail, created_at) VALUES (?, ?, ?, ?)",
            (round_id, "close_failed", str(exc), utc_now_iso()),
        )
        conn.commit()
        raise


def get_round_result(conn: sqlite3.Connection, round_id: int) -> dict:
    round_row = conn.execute("SELECT * FROM rounds WHERE id = ?", (round_id,)).fetchone()
    if round_row is None:
        raise ValueError("round-not-found")
    items = ranked_submissions(conn, round_id)
    artifact = conn.execute(
        "SELECT total_seconds, created_at FROM round_artifacts WHERE round_id = ?",
        (round_id,),
    ).fetchone()
    return {
        "round": dict(round_row),
        "items": [dict(i) for i in items],
        "artifact": dict(artifact) if artifact else None,
    }
