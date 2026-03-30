from __future__ import annotations

import importlib.metadata
import json
import logging
import secrets
import sqlite3
import subprocess
import sys
import threading
import uuid
from dataclasses import dataclass
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import urlopen

from radio_app.auth import authenticate_session, create_or_update_user, issue_session, revoke_session
from radio_app.config import AppConfig
from radio_app.db import DB, utc_now_iso
from radio_app.services.audio import validate_mp3_and_get_duration_seconds
from radio_app.services.rounds import (
    check_keyed_rate_limit,
    clear_keyed_rate_events,
    close_round,
    current_defaults,
    enforce_rate_limit,
    ensure_open_round,
    format_round_label,
    get_round_result,
    get_setting,
    ranked_submissions,
    record_keyed_rate_event,
    select_round_for_admin_close,
    set_setting,
)
from radio_app.services.riro import check_riro_login
from radio_app.services.music_search import ITunesSearchClient, SongSearchError
from radio_app.services.youtube import download_youtube_url


@dataclass
class AppContext:
    cfg: AppConfig
    db: DB
    song_search: ITunesSearchClient


LOGGER = logging.getLogger(__name__)

class RadioHTTPRequestHandler(BaseHTTPRequestHandler):
    ctx: AppContext

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path == "/":
                return self._serve_static("index.html", "text/html; charset=utf-8")
            if path == "/submit":
                return self._serve_static("submit.html", "text/html; charset=utf-8")
            if path == "/vote":
                return self._serve_static("vote.html", "text/html; charset=utf-8")
            if path == "/admin":
                return self._serve_static("admin.html", "text/html; charset=utf-8")
            if path.startswith("/static/"):
                return self._serve_static(path.removeprefix("/static/"), self._guess_mime(path))

            if path == "/api/health":
                return self._send_json({"ok": True})
            if path == "/api/me":
                return self._handle_me()
            if path == "/api/me/votes":
                return self._handle_me_votes()
            if path == "/api/public/current-round":
                return self._handle_public_current_round()
            if path == "/api/public/songs":
                return self._handle_public_songs()
            if path == "/api/public/results":
                return self._handle_public_results()
            if path == "/api/admin/settings/current":
                return self._handle_admin_current_settings()
            if path == "/api/admin/users":
                return self._handle_admin_users()
            if path == "/api/admin/submissions/current":
                return self._handle_admin_current_submissions()
            if path == "/api/admin/audit-logs":
                return self._handle_admin_audit_logs()
            if path == "/api/admin/maintenance/yt-dlp":
                return self._handle_admin_yt_dlp_status()
            if path == "/api/admin/manual-downloads":
                return self._handle_admin_manual_downloads()
            if path == "/api/admin/manual-downloads/download":
                return self._handle_admin_manual_download_download()
            if path == "/api/admin/artifacts/latest":
                return self._handle_admin_latest_artifact()
            if path == "/api/admin/artifacts/download":
                return self._handle_admin_artifact_download()
            if path == "/api/admin/artifacts/download-track":
                return self._handle_admin_artifact_track_download()
            if path == "/api/admin/rounds/close-status":
                return self._handle_admin_close_status()
            self._send_json({"error": "not-found"}, status=HTTPStatus.NOT_FOUND)
        except Exception as exc:
            self._handle_unexpected_error(exc)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            if not self._enforce_same_origin():
                return
            if path == "/api/auth/login":
                return self._handle_login()
            if path == "/api/auth/logout":
                return self._handle_logout()
            if path == "/api/songs/search":
                return self._handle_song_search()
            if path == "/api/submissions":
                return self._handle_submit_song()
            if path == "/api/admin/users/approve":
                return self._handle_admin_approve_user()
            if path == "/api/admin/rounds/close":
                return self._handle_admin_close_round()
            if path == "/api/admin/settings":
                return self._handle_admin_settings()
            if path == "/api/admin/submissions/hide":
                return self._handle_admin_hide_submission()
            if path == "/api/admin/maintenance/yt-dlp/update":
                return self._handle_admin_yt_dlp_update()
            if path == "/api/admin/manual-downloads/youtube":
                return self._handle_admin_manual_youtube_download()
            self._send_json({"error": "not-found"}, status=HTTPStatus.NOT_FOUND)
        except Exception as exc:
            self._handle_unexpected_error(exc)

    def do_PUT(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            if not self._enforce_same_origin():
                return
            if path == "/api/votes":
                return self._handle_votes_replace()
            self._send_json({"error": "not-found"}, status=HTTPStatus.NOT_FOUND)
        except Exception as exc:
            self._handle_unexpected_error(exc)

    def _handle_me(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=False)
            if not user:
                return self._send_json({"user": None})
            self._send_json(
                {
                    "user": {
                        "id": user.id,
                        "riro_user_key": user.riro_user_key,
                        "display_name": user.display_name,
                        "is_admin_approved": user.is_admin_approved,
                        "is_super_admin": self._is_super_admin_user(user),
                    }
                }
            )

    def _handle_login(self) -> None:
        body = self._read_json_body()
        mode = self.ctx.cfg.riro_auth_mode.strip().lower()

        if mode == "mock":
            riro_user_key = str(body.get("riro_user_key", body.get("riro_id", ""))).strip()
            display_name = str(body.get("display_name", "")).strip() or riro_user_key
            if not riro_user_key:
                return self._send_json({"error": "riro_user_key-required"}, status=HTTPStatus.BAD_REQUEST)
        else:
            riro_id = str(body.get("riro_id", body.get("id", ""))).strip()
            riro_pw = str(body.get("riro_pw", body.get("password", ""))).strip()
            if not riro_id or not riro_pw:
                return self._send_json({"error": "riro_id-and-riro_pw-required"}, status=HTTPStatus.BAD_REQUEST)

            with self.ctx.db.session() as conn:
                if self._is_login_rate_limited(conn, riro_id):
                    return self._send_json({"error": "rate-limit"}, status=HTTPStatus.TOO_MANY_REQUESTS)

            result = check_riro_login(riro_id, riro_pw)
            if result.status != "success":
                with self.ctx.db.session() as conn:
                    self._record_failed_login_attempt(conn, riro_id)
                return self._send_json(
                    {"error": "riro-auth-failed", "message": result.message or "인증 실패"},
                    status=HTTPStatus.UNAUTHORIZED,
                )

            # teacher/integrated 계정에서 student_number가 비어있거나 0일 수 있어 id를 fallback key로 사용.
            riro_user_key = (result.student_number or "").strip()
            if not riro_user_key or riro_user_key == "0":
                riro_user_key = (result.riro_id or riro_id).strip()
            display_name = (result.name or "").strip() or riro_user_key

        with self.ctx.db.session() as conn:
            if mode != "mock":
                self._clear_failed_login_attempts(conn, riro_id)
            user = create_or_update_user(conn, riro_user_key=riro_user_key, display_name=display_name)
            token = issue_session(conn, user.id, ttl_hours=self.ctx.cfg.session_ttl_hours)
            headers = {"Set-Cookie": self._build_session_cookie(token, max_age=self.ctx.cfg.session_ttl_hours * 3600)}
            self._send_json(
                {
                    "user": {
                        "id": user.id,
                        "riro_user_key": user.riro_user_key,
                        "display_name": user.display_name,
                        "is_admin_approved": user.is_admin_approved,
                    }
                },
                headers=headers,
            )

    def _handle_logout(self) -> None:
        token = self._session_token()
        with self.ctx.db.session() as conn:
            if token:
                revoke_session(conn, token)
        self._send_json({"ok": True}, headers={"Set-Cookie": self._build_session_cookie("", max_age=0)})

    def _handle_me_votes(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=False)
            if not user:
                return self._send_json({"active_votes": []})
            round_row = ensure_open_round(conn, self.ctx.cfg.timezone)
            rows = conn.execute(
                """
                SELECT submission_id
                FROM votes
                WHERE round_id = ? AND user_id = ?
                ORDER BY submission_id
                """,
                (round_row["id"], user.id),
            ).fetchall()
            self._send_json({"round_id": int(round_row["id"]), "active_votes": [int(r["submission_id"]) for r in rows]})

    def _handle_song_search(self) -> None:
        body = self._read_json_body()
        query = str(body.get("query", "")).strip()
        limit = int(body.get("limit", 10))
        try:
            tracks = self.ctx.song_search.search_tracks(query=query, limit=limit)
        except SongSearchError as exc:
            return self._send_json(
                {"error": "song-search-unavailable", "message": str(exc)},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
        self._send_json({"tracks": tracks})

    def _handle_submit_song(self) -> None:
        body = self._read_json_body()
        try:
            track_id = self._validated_text(body.get("track_id", body.get("spotify_track_id", "")), max_length=255)
            title = self._validated_text(body.get("title", ""), max_length=255)
            artist = self._validated_text(body.get("artist", ""), max_length=255)
            album_art_url = self._validated_optional_url(body.get("album_art_url", ""))
            external_url = self._validated_optional_url(body.get("external_url", ""))
        except ValueError:
            return self._send_json({"error": "invalid-song-payload"}, status=HTTPStatus.BAD_REQUEST)

        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True)
            if not user:
                return
            if not enforce_rate_limit(conn, user.id, "submit", max_count=20, window_seconds=60):
                return self._send_json({"error": "rate-limit"}, status=HTTPStatus.TOO_MANY_REQUESTS)
            round_row = ensure_open_round(conn, self.ctx.cfg.timezone)
            if round_row["status"] != "open":
                return self._send_json({"error": "round-not-open"}, status=HTTPStatus.CONFLICT)

            own_count = conn.execute(
                "SELECT COUNT(*) AS cnt FROM submissions WHERE round_id = ? AND user_id = ?",
                (round_row["id"], user.id),
            ).fetchone()["cnt"]
            if int(own_count) >= 3:
                return self._send_json({"error": "submission-limit-reached", "limit": 3}, status=HTTPStatus.CONFLICT)

            conn.execute(
                """
                INSERT INTO songs(spotify_track_id, title, artist, album_art_url, external_url, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(spotify_track_id)
                DO UPDATE SET
                    title = excluded.title,
                    artist = excluded.artist,
                    album_art_url = excluded.album_art_url,
                    external_url = excluded.external_url
                """,
                (track_id, title, artist, album_art_url, external_url, utc_now_iso()),
            )
            song = conn.execute("SELECT id FROM songs WHERE spotify_track_id = ?", (track_id,)).fetchone()
            try:
                conn.execute(
                    "INSERT INTO submissions(round_id, user_id, song_id, submitted_at) VALUES (?, ?, ?, ?)",
                    (round_row["id"], user.id, song["id"], utc_now_iso()),
                )
            except sqlite3.IntegrityError:
                existing = conn.execute(
                    """
                    SELECT s.id AS submission_id, s.round_id
                    FROM submissions s
                    WHERE s.round_id = ? AND s.song_id = ?
                    """,
                    (round_row["id"], song["id"]),
                ).fetchone()
                return self._send_json(
                    {
                        "error": "duplicate-track-in-round",
                        "submission_id": int(existing["submission_id"]) if existing else None,
                    },
                    status=HTTPStatus.CONFLICT,
                )

            submission_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self._send_json({"ok": True, "submission_id": int(submission_id), "round_id": int(round_row["id"])})

    def _handle_votes_replace(self) -> None:
        body = self._read_json_body()
        submission_ids = body.get("submission_ids", [])
        if not isinstance(submission_ids, list):
            return self._send_json({"error": "submission_ids-must-be-array"}, status=HTTPStatus.BAD_REQUEST)
        normalized = sorted({int(v) for v in submission_ids})
        if len(normalized) > 3:
            return self._send_json({"error": "vote-limit-reached", "limit": 3}, status=HTTPStatus.CONFLICT)

        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True)
            if not user:
                return
            if not enforce_rate_limit(conn, user.id, "vote", max_count=120, window_seconds=60):
                return self._send_json({"error": "rate-limit"}, status=HTTPStatus.TOO_MANY_REQUESTS)
            round_row = ensure_open_round(conn, self.ctx.cfg.timezone)
            if round_row["status"] != "open":
                return self._send_json({"error": "round-not-open"}, status=HTTPStatus.CONFLICT)

            if normalized:
                placeholders = ",".join("?" for _ in normalized)
                rows = conn.execute(
                    f"""
                    SELECT id
                    FROM submissions
                    WHERE round_id = ? AND is_hidden = 0 AND id IN ({placeholders})
                    """,
                    (round_row["id"], *normalized),
                ).fetchall()
                if len(rows) != len(normalized):
                    return self._send_json({"error": "invalid-submission-selection"}, status=HTTPStatus.BAD_REQUEST)

            conn.execute("DELETE FROM votes WHERE round_id = ? AND user_id = ?", (round_row["id"], user.id))
            for sid in normalized:
                conn.execute(
                    "INSERT INTO votes(round_id, user_id, submission_id, voted_at) VALUES (?, ?, ?, ?)",
                    (round_row["id"], user.id, sid, utc_now_iso()),
                )
            self._send_json({"ok": True, "active_votes": normalized})

    def _handle_public_current_round(self) -> None:
        with self.ctx.db.session() as conn:
            round_row = ensure_open_round(conn, self.ctx.cfg.timezone)
            self._send_json({"round": self._round_payload(conn, round_row)})

    def _handle_public_songs(self) -> None:
        params = self._query_params()
        sort_by = params.get("sort", "popular")
        if sort_by not in {"popular", "recent"}:
            return self._send_json({"error": "invalid-song-sort"}, status=HTTPStatus.BAD_REQUEST)
        with self.ctx.db.session() as conn:
            round_row = ensure_open_round(conn, self.ctx.cfg.timezone)
            items = ranked_submissions(conn, int(round_row["id"]), sort_by=sort_by)
            self._send_json(
                {
                    "round_id": int(round_row["id"]),
                    "round": self._round_payload(conn, round_row),
                    "sort": sort_by,
                    "items": [dict(i) for i in items],
                }
            )

    def _handle_public_results(self) -> None:
        params = self._query_params()
        round_id: int | None = None
        if "round_id" in params:
            try:
                round_id = int(params["round_id"])
            except ValueError:
                return self._send_json({"error": "invalid-round-id"}, status=HTTPStatus.BAD_REQUEST)
        with self.ctx.db.session() as conn:
            if round_id is None:
                current = ensure_open_round(conn, self.ctx.cfg.timezone)
                round_id = int(current["id"])
            result = get_round_result(conn, round_id)
            result["round"] = self._round_payload(conn, result["round"])
            self._send_json(result)

    def _handle_admin_current_settings(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            defaults = current_defaults(conn)
            self._send_json(
                {
                    "defaults": {
                        "cadence": defaults.cadence,
                        "playlist_size": defaults.playlist_size,
                        "target_seconds": defaults.target_seconds,
                        "loudnorm_enabled": defaults.loudnorm_enabled,
                    }
                }
            )

    def _handle_admin_users(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            rows = conn.execute(
                """
                SELECT id, riro_user_key, display_name, is_admin_approved, created_at
                FROM users
                ORDER BY created_at DESC, id DESC
                """
            ).fetchall()
            self._send_json(
                {
                    "users": [
                        {
                            "id": int(row["id"]),
                            "riro_user_key": row["riro_user_key"],
                            "display_name": row["display_name"],
                            "is_admin_approved": bool(row["is_admin_approved"]),
                            "is_super_admin": self._is_super_admin_key(str(row["riro_user_key"])),
                            "created_at": row["created_at"],
                        }
                        for row in rows
                    ]
                }
            )

    def _handle_admin_current_submissions(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            round_row = ensure_open_round(conn, self.ctx.cfg.timezone)
            rows = conn.execute(
                """
                SELECT
                    s.id AS submission_id,
                    s.is_hidden,
                    s.submitted_at,
                    u.id AS user_id,
                    u.display_name,
                    so.title,
                    so.artist,
                    COALESCE(v.votes, 0) AS vote_count
                FROM submissions s
                JOIN users u ON u.id = s.user_id
                JOIN songs so ON so.id = s.song_id
                LEFT JOIN (
                    SELECT submission_id, COUNT(*) AS votes
                    FROM votes
                    WHERE round_id = ?
                    GROUP BY submission_id
                ) v ON v.submission_id = s.id
                WHERE s.round_id = ?
                ORDER BY s.is_hidden ASC, vote_count DESC, s.submitted_at ASC, s.id ASC
                """,
                (int(round_row["id"]), int(round_row["id"])),
            ).fetchall()
            self._send_json(
                {
                    "round_id": int(round_row["id"]),
                    "items": [
                        {
                            "submission_id": int(row["submission_id"]),
                            "title": row["title"],
                            "artist": row["artist"],
                            "vote_count": int(row["vote_count"] or 0),
                            "is_hidden": bool(row["is_hidden"]),
                            "submitted_at": row["submitted_at"],
                            "user_id": int(row["user_id"]),
                            "display_name": row["display_name"],
                        }
                        for row in rows
                    ],
                }
            )

    def _handle_admin_audit_logs(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            params = self._query_params()
            raw_limit = params.get("limit", "20")
            try:
                limit = int(raw_limit)
            except ValueError:
                return self._send_json({"error": "invalid-audit-log-limit"}, status=HTTPStatus.BAD_REQUEST)
            if limit < 1 or limit > 100:
                return self._send_json({"error": "invalid-audit-log-limit"}, status=HTTPStatus.BAD_REQUEST)
            rows = conn.execute(
                """
                SELECT id, action, detail, created_at, actor_user_id, round_id
                FROM audit_logs
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            self._send_json(
                {
                    "items": [
                        {
                            "id": int(row["id"]),
                            "action": row["action"],
                            "detail": row["detail"],
                            "created_at": row["created_at"],
                            "actor_user_id": int(row["actor_user_id"]) if row["actor_user_id"] is not None else None,
                            "round_id": int(row["round_id"]) if row["round_id"] is not None else None,
                        }
                        for row in rows
                    ]
                }
            )

    def _handle_admin_yt_dlp_status(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            self._send_json(self._yt_dlp_status_payload())

    def _handle_admin_yt_dlp_update(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return

            before_version = self._yt_dlp_version()
            cmd = [sys.executable, "-m", "pip", "install", "-U", "yt-dlp"]
            try:
                proc = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=180,
                )
            except subprocess.TimeoutExpired:
                detail = {"before_version": before_version, "error": "timeout"}
                self._insert_audit_log(conn, action="yt_dlp_update_failed", detail=detail, actor_user_id=user.id)
                conn.commit()
                return self._send_json(
                    {
                        "error": "yt-dlp-update-failed",
                        "message": "yt-dlp 업데이트가 제한 시간을 초과했습니다.",
                    },
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )

            summary = self._summarize_command_output(proc.stdout, proc.stderr)
            if proc.returncode != 0:
                detail = {
                    "before_version": before_version,
                    "returncode": proc.returncode,
                    "summary": summary,
                }
                self._insert_audit_log(conn, action="yt_dlp_update_failed", detail=detail, actor_user_id=user.id)
                conn.commit()
                return self._send_json(
                    {
                        "error": "yt-dlp-update-failed",
                        "message": "yt-dlp 업데이트에 실패했습니다.",
                        "summary": summary,
                    },
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )

            self._clear_loaded_module("yt_dlp")
            after_version = self._yt_dlp_version()
            payload = {
                "ok": True,
                "before_version": before_version,
                "after_version": after_version,
                "summary": summary,
                "changed": before_version != after_version,
            }
            self._insert_audit_log(conn, action="yt_dlp_updated", detail=payload, actor_user_id=user.id)
            conn.commit()
            self._send_json(payload)

    def _handle_admin_manual_downloads(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            rows = conn.execute(
                """
                SELECT id, source_url, video_id, title, uploader, duration_seconds, created_at
                FROM manual_downloads
                ORDER BY id DESC
                LIMIT 10
                """
            ).fetchall()
            self._send_json(
                {
                    "items": [
                        {
                            "id": int(row["id"]),
                            "source_url": str(row["source_url"]),
                            "video_id": str(row["video_id"]),
                            "title": str(row["title"]),
                            "uploader": str(row["uploader"]),
                            "duration_seconds": int(row["duration_seconds"] or 0),
                            "created_at": str(row["created_at"]),
                            "download_url": f"/api/admin/manual-downloads/download?id={int(row['id'])}",
                        }
                        for row in rows
                    ]
                }
            )

    def _handle_admin_manual_youtube_download(self) -> None:
        body = self._read_json_body()
        try:
            source_url = self._validated_youtube_url(body.get("url", ""))
        except ValueError:
            return self._send_json({"error": "invalid-youtube-url"}, status=HTTPStatus.BAD_REQUEST)

        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            try:
                download = download_youtube_url(
                    source_url,
                    self.ctx.cfg.uploads_dir / "manual",
                    ffmpeg_path=self.ctx.cfg.ffmpeg_path,
                )
                duration_seconds, validation_error = validate_mp3_and_get_duration_seconds(download.path)
                if validation_error or duration_seconds <= 0:
                    raise RuntimeError(validation_error or "unable-to-parse-mp3-duration")
            except Exception as exc:
                detail = {"source_url": source_url, "error": str(exc) or repr(exc)}
                self._insert_audit_log(conn, action="manual_youtube_download_failed", detail=detail, actor_user_id=user.id)
                conn.commit()
                return self._send_json(
                    {"error": "manual-youtube-download-failed", "message": str(exc) or "download-failed"},
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )

            conn.execute(
                """
                INSERT INTO manual_downloads(actor_user_id, source_url, video_id, title, uploader, file_path, duration_seconds, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user.id,
                    source_url,
                    str(download.candidate.video_id or ""),
                    str(download.candidate.title or download.path.stem),
                    str(download.candidate.uploader or ""),
                    str(download.path),
                    duration_seconds,
                    utc_now_iso(),
                ),
            )
            manual_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
            payload = {
                "ok": True,
                "download": {
                    "id": manual_id,
                    "source_url": source_url,
                    "video_id": str(download.candidate.video_id or ""),
                    "title": str(download.candidate.title or download.path.stem),
                    "uploader": str(download.candidate.uploader or ""),
                    "duration_seconds": duration_seconds,
                    "created_at": utc_now_iso(),
                    "download_url": f"/api/admin/manual-downloads/download?id={manual_id}",
                },
            }
            self._insert_audit_log(conn, action="manual_youtube_download", detail=payload["download"], actor_user_id=user.id)
            conn.commit()
            self._send_json(payload)

    def _handle_admin_manual_download_download(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            params = self._query_params()
            raw_id = params.get("id")
            if raw_id is None:
                return self._send_json({"error": "invalid-manual-download-request"}, status=HTTPStatus.BAD_REQUEST)
            try:
                manual_id = int(raw_id)
            except ValueError:
                return self._send_json({"error": "invalid-manual-download-request"}, status=HTTPStatus.BAD_REQUEST)
            row = conn.execute(
                """
                SELECT id, title, uploader, file_path, created_at
                FROM manual_downloads
                WHERE id = ?
                """,
                (manual_id,),
            ).fetchone()
            if row is None:
                return self._send_json({"error": "manual-download-not-found", "id": manual_id}, status=HTTPStatus.NOT_FOUND)
            file_path = self._manual_download_file_path(row)
            if file_path is None or not file_path.exists():
                return self._send_json({"error": "manual-download-file-missing", "id": manual_id}, status=HTTPStatus.NOT_FOUND)
            download_name = f"manual-{row['uploader']}-{row['title']}.mp3"
            self._send_file(file_path, "audio/mpeg", download_name)

    def _handle_admin_latest_artifact(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            row = conn.execute(
                """
                SELECT ra.*, r.cadence, r.start_at, r.end_at
                FROM round_artifacts ra
                JOIN rounds r ON r.id = ra.round_id
                ORDER BY ra.id DESC
                LIMIT 1
                """
            ).fetchone()
            self._send_json({"artifact": self._artifact_summary_payload(conn, row) if row else None})

    def _handle_admin_artifact_download(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            params = self._query_params()
            artifact_id = params.get("artifact_id")
            artifact_type = params.get("type", "").strip().lower()
            if artifact_id is None:
                return self._send_json({"error": "invalid-artifact-download-request"}, status=HTTPStatus.BAD_REQUEST)
            try:
                artifact_id_int = int(artifact_id)
            except ValueError:
                return self._send_json({"error": "invalid-artifact-download-request"}, status=HTTPStatus.BAD_REQUEST)
            if artifact_type not in {"m3u", "mp3"}:
                return self._send_json({"error": "invalid-artifact-download-request"}, status=HTTPStatus.BAD_REQUEST)
            row = conn.execute(
                """
                SELECT ra.*, r.cadence, r.start_at, r.end_at
                FROM round_artifacts ra
                JOIN rounds r ON r.id = ra.round_id
                WHERE ra.id = ?
                """,
                (artifact_id_int,),
            ).fetchone()
            if row is None:
                return self._send_json(
                    {"error": "artifact-not-found", "artifact_id": artifact_id_int},
                    status=HTTPStatus.NOT_FOUND,
                )
            file_path = self._artifact_file_path(row, artifact_type)
            if file_path is None or not file_path.exists():
                return self._send_json(
                    {"error": "artifact-file-missing", "artifact_id": artifact_id_int, "type": artifact_type},
                    status=HTTPStatus.NOT_FOUND,
                )
            round_label = format_round_label(conn, row, self.ctx.cfg.timezone)
            label_prefix = round_label.replace(" ", "-") if round_label and round_label != "-" else ""
            download_name = f"{label_prefix}-playlist.{artifact_type}" if label_prefix else f"playlist.{artifact_type}"
            content_type = "audio/mpeg" if artifact_type == "mp3" else "audio/x-mpegurl; charset=utf-8"
            self._send_file(file_path, content_type, download_name)

    def _handle_admin_artifact_track_download(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            params = self._query_params()
            artifact_id = params.get("artifact_id")
            track_id = params.get("track_id")
            if artifact_id is None or track_id is None:
                return self._send_json({"error": "invalid-artifact-download-request"}, status=HTTPStatus.BAD_REQUEST)
            try:
                artifact_id_int = int(artifact_id)
                track_id_int = int(track_id)
            except ValueError:
                return self._send_json({"error": "invalid-artifact-download-request"}, status=HTTPStatus.BAD_REQUEST)
            row = conn.execute(
                """
                SELECT rat.*, ra.id AS artifact_id, r.cadence, r.start_at, r.end_at
                FROM round_artifact_tracks rat
                JOIN round_artifacts ra ON ra.id = rat.artifact_id
                JOIN rounds r ON r.id = ra.round_id
                WHERE rat.artifact_id = ? AND rat.track_order = ?
                """,
                (artifact_id_int, track_id_int),
            ).fetchone()
            if row is None:
                return self._send_json(
                    {"error": "artifact-track-not-found", "artifact_id": artifact_id_int, "track_id": track_id_int},
                    status=HTTPStatus.NOT_FOUND,
                )
            file_path = self._artifact_track_file_path(row)
            if file_path is None or not file_path.exists():
                return self._send_json(
                    {"error": "artifact-file-missing", "artifact_id": artifact_id_int, "track_id": track_id_int},
                    status=HTTPStatus.NOT_FOUND,
                )
            round_label = format_round_label(conn, row, self.ctx.cfg.timezone)
            label_prefix = round_label.replace(" ", "-") if round_label and round_label != "-" else "playlist"
            download_name = f"{label_prefix}-{int(row['track_order']):02d}-{row['artist']}-{row['title']}.mp3"
            self._send_file(file_path, "audio/mpeg", download_name)

    def _handle_admin_close_status(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            self._send_json(self._close_status_payload(conn, self._select_latest_close_round(conn)))

    def _handle_admin_close_round(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            active_round = self._select_active_close_round(conn)
            if active_round is not None:
                return self._send_json(self._close_status_payload(conn, active_round), status=HTTPStatus.ACCEPTED)
            round_row = select_round_for_admin_close(conn, self.ctx.cfg.timezone)
            round_id = int(round_row["id"])
            job_id = uuid.uuid4().hex
            now = utc_now_iso()
            conn.execute(
                """
                UPDATE rounds
                SET status = 'closing',
                    close_job_key = ?,
                    close_phase = ?,
                    close_message = ?,
                    close_progress = ?,
                    close_started_at = ?,
                    close_finished_at = NULL,
                    close_error = NULL
                WHERE id = ?
                """,
                (job_id, "queued", "마감 작업을 시작하는 중입니다.", 0, now, round_id),
            )
            state_row = conn.execute("SELECT * FROM rounds WHERE id = ?", (round_id,)).fetchone()
        worker = threading.Thread(
            target=self._run_admin_close_round_job,
            args=(job_id, round_id, user.id),
            daemon=True,
        )
        worker.start()
        self._send_json(self._close_status_payload_from_row(state_row), status=HTTPStatus.ACCEPTED)

    def _run_admin_close_round_job(self, job_id: str, round_id: int, actor_user_id: int) -> None:
        with self.ctx.db.session() as conn:
            def report(stage: str, message: str, progress_percent: int) -> None:
                self._update_close_state(
                    conn,
                    round_id,
                    job_id,
                    phase=stage,
                    message=message,
                    progress_percent=progress_percent,
                )

            try:
                result = close_round(
                    conn,
                    round_id,
                    artifacts_dir=self.ctx.cfg.artifacts_dir,
                    uploads_dir=self.ctx.cfg.uploads_dir,
                    ffmpeg_path=self.ctx.cfg.ffmpeg_path,
                    yt_dlp_enabled=self.ctx.cfg.yt_dlp_enabled,
                    progress_callback=report,
                    already_marked_closing=True,
                )
                summary_row = conn.execute(
                    """
                    SELECT ra.*, r.cadence, r.start_at, r.end_at
                    FROM round_artifacts ra
                    JOIN rounds r ON r.id = ra.round_id
                    WHERE ra.round_id = ?
                    """,
                    (round_id,),
                ).fetchone()
                response_payload = {
                    key: value
                    for key, value in result.items()
                    if key not in {"m3u_path", "mp3_path", "generation_log", "artifact"}
                }
                response_payload["artifact"] = self._artifact_summary_payload(conn, summary_row) if summary_row else None
                self._insert_audit_log(
                    conn,
                    action="manual_close",
                    detail=response_payload,
                    actor_user_id=actor_user_id,
                    round_id=round_id,
                )
                self._mark_close_completed(conn, round_id, job_id)
            except RuntimeError as exc:
                if str(exc) == "no-valid-audio-assets":
                    self._mark_close_failed(
                        conn,
                        round_id,
                        job_id,
                        error_code="no-valid-audio-assets",
                        message="마감할 수 있는 유효한 음원이 없습니다.",
                    )
                    return
                LOGGER.exception("Manual close job failed", extra={"round_id": round_id, "job_id": job_id})
                self._mark_close_failed(
                    conn,
                    round_id,
                    job_id,
                    error_code="close-job-failed",
                    message="회차 마감 중 오류가 발생했습니다.",
                )
            except Exception:
                LOGGER.exception("Manual close job failed", extra={"round_id": round_id, "job_id": job_id})
                self._mark_close_failed(
                    conn,
                    round_id,
                    job_id,
                    error_code="close-job-failed",
                    message="회차 마감 중 오류가 발생했습니다.",
                )

    def _select_active_close_round(self, conn: sqlite3.Connection) -> sqlite3.Row | None:
        return conn.execute(
            """
            SELECT *
            FROM rounds
            WHERE status = 'closing'
            ORDER BY COALESCE(close_started_at, created_at) DESC, id DESC
            LIMIT 1
            """
        ).fetchone()

    def _select_latest_close_round(self, conn: sqlite3.Connection) -> sqlite3.Row | None:
        active_row = self._select_active_close_round(conn)
        if active_row is not None:
            return active_row
        return conn.execute(
            """
            SELECT *
            FROM rounds
            WHERE close_started_at IS NOT NULL OR close_phase IN ('completed', 'failed')
            ORDER BY COALESCE(close_finished_at, close_started_at, created_at) DESC, id DESC
            LIMIT 1
            """
        ).fetchone()

    def _update_close_state(
        self,
        conn: sqlite3.Connection,
        round_id: int,
        job_id: str,
        *,
        phase: str,
        message: str,
        progress_percent: int,
    ) -> None:
        conn.execute(
            """
            UPDATE rounds
            SET close_phase = ?,
                close_message = ?,
                close_progress = ?,
                close_error = NULL
            WHERE id = ? AND close_job_key = ?
            """,
            (phase, message, max(0, min(100, progress_percent)), round_id, job_id),
        )
        conn.commit()

    def _mark_close_completed(self, conn: sqlite3.Connection, round_id: int, job_id: str) -> None:
        conn.execute(
            """
            UPDATE rounds
            SET close_phase = 'completed',
                close_message = ?,
                close_progress = 100,
                close_finished_at = ?,
                close_error = NULL
            WHERE id = ? AND close_job_key = ?
            """,
            ("회차 마감이 완료되었습니다.", utc_now_iso(), round_id, job_id),
        )

    def _mark_close_failed(
        self,
        conn: sqlite3.Connection,
        round_id: int,
        job_id: str,
        *,
        error_code: str,
        message: str,
    ) -> None:
        conn.execute(
            """
            UPDATE rounds
            SET close_phase = 'failed',
                close_message = ?,
                close_finished_at = ?,
                close_error = ?
            WHERE id = ? AND close_job_key = ?
            """,
            (
                message,
                utc_now_iso(),
                json.dumps({"code": error_code, "message": message}, ensure_ascii=True),
                round_id,
                job_id,
            ),
        )

    def _close_status_payload(self, conn: sqlite3.Connection, row: sqlite3.Row | None) -> dict[str, Any]:
        payload = self._close_status_payload_from_row(row)
        if row is None:
            return payload
        round_id = int(row["id"])
        if payload["status"] == "succeeded":
            payload["result"] = self._close_result_payload(conn, round_id)
        elif payload["status"] == "failed":
            payload["error"] = self._parse_close_error(str(row["close_error"] or ""), payload["message"])
        return payload

    def _close_status_payload_from_row(self, row: sqlite3.Row | None) -> dict[str, Any]:
        if row is None:
            return {
                "job_id": None,
                "status": "idle",
                "round_id": None,
                "stage": None,
                "message": None,
                "progress_percent": 0,
                "started_at": None,
                "updated_at": None,
                "finished_at": None,
                "result": None,
                "error": None,
            }
        phase = str(row["close_phase"] or "").strip()
        status = "running"
        if phase == "completed":
            status = "succeeded"
        elif phase == "failed":
            status = "failed"
        return {
            "job_id": str(row["close_job_key"] or "").strip() or None,
            "status": status,
            "round_id": int(row["id"]),
            "stage": phase or None,
            "message": str(row["close_message"] or "").strip() or None,
            "progress_percent": int(row["close_progress"] or 0),
            "started_at": row["close_started_at"],
            "updated_at": row["close_finished_at"] or row["close_started_at"],
            "finished_at": row["close_finished_at"],
            "result": None,
            "error": None,
        }

    def _close_result_payload(self, conn: sqlite3.Connection, round_id: int) -> dict[str, Any] | None:
        summary_row = conn.execute(
            """
            SELECT ra.*, r.cadence, r.start_at, r.end_at
            FROM round_artifacts ra
            JOIN rounds r ON r.id = ra.round_id
            WHERE ra.round_id = ?
            """,
            (round_id,),
        ).fetchone()
        audit_row = conn.execute(
            """
            SELECT detail
            FROM audit_logs
            WHERE round_id = ? AND action = 'manual_close'
            ORDER BY id DESC
            LIMIT 1
            """,
            (round_id,),
        ).fetchone()
        payload: dict[str, Any] = {}
        if audit_row is not None:
            try:
                parsed = json.loads(str(audit_row["detail"] or "{}"))
                if isinstance(parsed, dict):
                    payload.update(parsed)
            except json.JSONDecodeError:
                pass
        payload.setdefault("round_id", round_id)
        if summary_row is not None:
            artifact = self._artifact_summary_payload(conn, summary_row)
            payload.setdefault("total_seconds", int(summary_row["total_seconds"]))
            payload["artifact"] = artifact
        return payload or None

    @staticmethod
    def _parse_close_error(raw: str, fallback_message: str | None) -> dict[str, Any]:
        try:
            parsed = json.loads(raw or "{}")
            if isinstance(parsed, dict) and parsed.get("message"):
                return parsed
        except json.JSONDecodeError:
            pass
        return {"code": "close-job-failed", "message": fallback_message or "회차 마감 중 오류가 발생했습니다."}

    def _handle_admin_settings(self) -> None:
        body = self._read_json_body()
        cadence = str(body.get("cadence", "")).strip()
        target_seconds = int(body.get("target_seconds", 2400))
        loudnorm_enabled = bool(body.get("loudnorm_enabled", True))
        if cadence not in {"weekly", "monthly"}:
            return self._send_json({"error": "cadence-must-be-weekly-or-monthly"}, status=HTTPStatus.BAD_REQUEST)
        if target_seconds < 60 or target_seconds > 10800:
            return self._send_json({"error": "invalid-target-seconds"}, status=HTTPStatus.BAD_REQUEST)
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            playlist_size = int(body.get("playlist_size", get_setting(conn, "default_playlist_size", "12")))
            if playlist_size < 1 or playlist_size > 100:
                return self._send_json({"error": "invalid-playlist-size"}, status=HTTPStatus.BAD_REQUEST)
            set_setting(conn, "round_default_cadence", cadence)
            set_setting(conn, "default_playlist_size", str(playlist_size))
            set_setting(conn, "default_target_seconds", str(target_seconds))
            set_setting(conn, "default_loudnorm_enabled", "1" if loudnorm_enabled else "0")
            payload = {
                "ok": True,
                "defaults": {
                    "cadence": cadence,
                    "playlist_size": playlist_size,
                    "target_seconds": target_seconds,
                    "loudnorm_enabled": loudnorm_enabled,
                },
            }
            self._insert_audit_log(
                conn,
                action="settings_updated",
                detail=payload["defaults"],
                actor_user_id=user.id,
            )
            self._send_json(payload)

    def _handle_admin_approve_user(self) -> None:
        body = self._read_json_body()
        try:
            target = int(body.get("user_id", 0))
        except (TypeError, ValueError):
            return self._send_json({"error": "invalid-admin-user-request"}, status=HTTPStatus.BAD_REQUEST)
        approved = self._coerce_bool(body.get("approved"))
        if target < 1 or approved is None:
            return self._send_json({"error": "invalid-admin-user-request"}, status=HTTPStatus.BAD_REQUEST)
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            row = conn.execute(
                "SELECT id, is_admin_approved, riro_user_key FROM users WHERE id = ?",
                (target,),
            ).fetchone()
            if row is None:
                return self._send_json({"error": "user-not-found", "user_id": target}, status=HTTPStatus.NOT_FOUND)
            if not approved and not self._is_super_admin_user(user):
                return self._send_json({"error": "super-admin-required"}, status=HTTPStatus.FORBIDDEN)
            if not approved and self._is_super_admin_key(str(row["riro_user_key"])):
                return self._send_json({"error": "super-admin-protected"}, status=HTTPStatus.FORBIDDEN)
            if bool(row["is_admin_approved"]) == approved:
                return self._send_json(
                    {"error": "admin-state-unchanged", "user_id": target, "approved": approved},
                    status=HTTPStatus.CONFLICT,
                )
            conn.execute("UPDATE users SET is_admin_approved = ? WHERE id = ?", (1 if approved else 0, target))
            self._insert_audit_log(
                conn,
                action="admin_approval_changed",
                detail={"user_id": target, "approved": approved},
                actor_user_id=user.id,
            )
            conn.commit()
            self._send_json({"ok": True, "user_id": target, "approved": approved})

    def _handle_admin_hide_submission(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            return self._send_json(
                {"error": "submission-hide-disabled", "message": "곡 숨김 기능은 현재 비활성화되어 있습니다."},
                status=HTTPStatus.FORBIDDEN,
            )

    def _serve_static(self, name: str, content_type: str) -> None:
        root = Path(__file__).resolve().parent / "static"
        path = (root / name).resolve()
        if not str(path).startswith(str(root.resolve())) or not path.exists():
            return self._send_json({"error": "not-found"}, status=HTTPStatus.NOT_FOUND)
        try:
            nonce: str | None = None
            data: bytes
            if content_type.startswith("text/html"):
                nonce = secrets.token_urlsafe(16)
                html = path.read_text(encoding="utf-8").replace("__CSP_NONCE__", nonce)
                data = html.encode("utf-8")
            else:
                data = path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            for k, v in self._security_headers(script_nonce=nonce).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)
        except OSError as exc:
            if self._is_client_disconnect(exc):
                return
            raise

    @staticmethod
    def _guess_mime(path: str) -> str:
        if path.endswith(".css"):
            return "text/css; charset=utf-8"
        if path.endswith(".js"):
            return "application/javascript; charset=utf-8"
        return "text/plain; charset=utf-8"

    def _read_json_body(self) -> dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(content_length) if content_length else b"{}"
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK, headers: dict[str, str] | None = None) -> None:
        data = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            for k, v in self._security_headers().items():
                self.send_header(k, v)
            if headers:
                for k, v in headers.items():
                    self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)
        except OSError as exc:
            if self._is_client_disconnect(exc):
                return
            raise

    def _send_file(self, path: Path, content_type: str, download_name: str) -> None:
        data = path.read_bytes()
        ascii_name = "".join(ch if ord(ch) < 128 else "_" for ch in download_name)
        encoded_name = quote(download_name, safe="")
        try:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            for k, v in self._security_headers().items():
                self.send_header(k, v)
            self.send_header(
                "Content-Disposition",
                f'attachment; filename="{ascii_name}"; filename*=UTF-8\'\'{encoded_name}',
            )
            self.end_headers()
            self.wfile.write(data)
        except OSError as exc:
            if self._is_client_disconnect(exc):
                return
            raise

    def _handle_unexpected_error(self, exc: Exception) -> None:
        LOGGER.exception(
            "Unhandled request error",
            extra={
                "path": self.path,
                "method": self.command,
                "client": self.client_address[0] if self.client_address else None,
            },
        )
        self._send_json(
            {"error": "internal-error", "message": "서버 오류가 발생했습니다."},
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )

    def _query_params(self) -> dict[str, str]:
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query, keep_blank_values=True)
        return {key: values[-1] for key, values in params.items()}

    def _enforce_same_origin(self) -> bool:
        if self._has_valid_same_origin_headers():
            return True
        self._send_json({"error": "invalid-origin"}, status=HTTPStatus.FORBIDDEN)
        return False

    def _has_valid_same_origin_headers(self) -> bool:
        host = str(self.headers.get("Host", "")).strip().lower()
        if not host:
            return False
        found_header = False
        for header_name in ("Origin", "Referer"):
            raw_value = str(self.headers.get(header_name, "")).strip()
            if not raw_value:
                continue
            found_header = True
            parsed = urlparse(raw_value)
            if parsed.scheme not in {"http", "https"}:
                return False
            return str(parsed.netloc or "").strip().lower() == host
        return found_header

    def _login_rate_limit_key(self, login_id: str) -> str:
        normalized_login = str(login_id or "").strip().lower() or "-"
        return f"login-user:{normalized_login}"

    def _is_login_rate_limited(self, conn: sqlite3.Connection, login_id: str) -> bool:
        user_key = self._login_rate_limit_key(login_id)
        cfg = self.ctx.cfg
        return not check_keyed_rate_limit(
            conn,
            user_key,
            "login-failure-user",
            cfg.login_failure_limit_per_user,
            cfg.login_failure_window_seconds,
        )

    def _record_failed_login_attempt(self, conn: sqlite3.Connection, login_id: str) -> None:
        user_key = self._login_rate_limit_key(login_id)
        window_seconds = self.ctx.cfg.login_failure_window_seconds
        record_keyed_rate_event(conn, user_key, "login-failure-user", window_seconds)

    def _clear_failed_login_attempts(self, conn: sqlite3.Connection, login_id: str) -> None:
        user_key = self._login_rate_limit_key(login_id)
        clear_keyed_rate_events(conn, user_key, "login-failure-user")

    @staticmethod
    def _validated_text(value: object, *, max_length: int) -> str:
        text = str(value or "").strip()
        if not text or len(text) > max_length:
            raise ValueError("invalid-text")
        if any(ch in text for ch in ("\x00", "\r", "\n")):
            raise ValueError("invalid-text")
        return text

    @staticmethod
    def _validated_optional_url(value: object, *, max_length: int = 2048) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        if len(raw) > max_length:
            raise ValueError("invalid-url")
        parsed = urlparse(raw)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("invalid-url")
        return raw

    @staticmethod
    def _validated_youtube_url(value: object, *, max_length: int = 2048) -> str:
        raw = str(value or "").strip()
        if not raw or len(raw) > max_length:
            raise ValueError("invalid-youtube-url")
        parsed = urlparse(raw)
        host = str(parsed.netloc or "").strip().lower()
        if parsed.scheme not in {"http", "https"} or not host:
            raise ValueError("invalid-youtube-url")
        if host == "youtu.be":
            if not str(parsed.path or "").strip("/"):
                raise ValueError("invalid-youtube-url")
            return raw
        if host == "youtube.com" or host.endswith(".youtube.com"):
            return raw
        raise ValueError("invalid-youtube-url")

    @staticmethod
    def _csp_header(script_nonce: str | None = None) -> str:
        script_src = "script-src 'self'"
        if script_nonce:
            script_src += f" 'nonce-{script_nonce}'"
        return (
            "default-src 'self'; "
            "img-src 'self' https: data:; "
            "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
            "font-src 'self' https://cdn.jsdelivr.net data:; "
            f"{script_src}; "
            "object-src 'none'; "
            "base-uri 'self'; "
            "frame-ancestors 'none'"
        )

    @classmethod
    def _security_headers(cls, script_nonce: str | None = None) -> dict[str, str]:
        return {
            "Content-Security-Policy": cls._csp_header(script_nonce),
            "Referrer-Policy": "same-origin",
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
        }

    def _build_session_cookie(self, token: str, max_age: int) -> str:
        parts = [f"session={token}", "Path=/", "HttpOnly", "SameSite=Lax", f"Max-Age={max_age}"]
        if self.ctx.cfg.session_cookie_secure:
            parts.append("Secure")
        return "; ".join(parts)

    def _insert_audit_log(
        self,
        conn: sqlite3.Connection,
        action: str,
        detail: Any,
        actor_user_id: int | None = None,
        round_id: int | None = None,
    ) -> None:
        serialized = detail if isinstance(detail, str) else json.dumps(detail, ensure_ascii=True)
        conn.execute(
            "INSERT INTO audit_logs(round_id, actor_user_id, action, detail, created_at) VALUES (?, ?, ?, ?, ?)",
            (round_id, actor_user_id, action, serialized, utc_now_iso()),
        )

    def _is_super_admin_key(self, riro_user_key: str) -> bool:
        return riro_user_key in self.ctx.cfg.super_admin_ids

    def _is_super_admin_user(self, user: Any) -> bool:
        return self._is_super_admin_key(str(user.riro_user_key))

    def _round_payload(self, conn: sqlite3.Connection, row: sqlite3.Row | dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        payload = dict(row)
        payload["round_label"] = format_round_label(conn, payload, self.ctx.cfg.timezone)
        return payload

    def _artifact_summary_payload(self, conn: sqlite3.Connection, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        m3u_path = self._artifact_file_path(row, "m3u")
        mp3_path = self._artifact_file_path(row, "mp3")
        tracks = conn.execute(
            """
            SELECT track_order, submission_id, song_id, title, artist, duration_seconds
            FROM round_artifact_tracks
            WHERE artifact_id = ?
            ORDER BY track_order ASC
            """,
            (int(row["id"]),),
        ).fetchall()
        return {
            "id": int(row["id"]),
            "round_id": int(row["round_id"]),
            "cadence": row["cadence"],
            "start_at": row["start_at"],
            "end_at": row["end_at"],
            "total_seconds": int(row["total_seconds"]),
            "created_at": row["created_at"],
            "round_label": format_round_label(conn, row, self.ctx.cfg.timezone),
            "has_m3u": bool(m3u_path and m3u_path.exists()),
            "has_mp3": bool(mp3_path and mp3_path.exists()),
            "generation_summary": self._summarize_generation_log(str(row["generation_log"] or "")),
            "tracks": [
                {
                    "track_id": int(track["track_order"]),
                    "submission_id": int(track["submission_id"]),
                    "song_id": int(track["song_id"]),
                    "title": str(track["title"]),
                    "artist": str(track["artist"]),
                    "duration_seconds": int(track["duration_seconds"]),
                }
                for track in tracks
            ],
        }

    def _artifact_file_path(self, row: sqlite3.Row, artifact_type: str) -> Path | None:
        field_name = "m3u_path" if artifact_type == "m3u" else "mp3_path"
        raw_path = str(row[field_name] or "").strip()
        if not raw_path:
            return None
        resolved = Path(raw_path).resolve()
        root = self.ctx.cfg.artifacts_dir.resolve()
        if resolved != root and root not in resolved.parents:
            return None
        return resolved

    def _artifact_track_file_path(self, row: sqlite3.Row) -> Path | None:
        raw_path = str(row["file_path"] or "").strip()
        if not raw_path:
            return None
        resolved = Path(raw_path).resolve()
        root = self.ctx.cfg.uploads_dir.resolve()
        if resolved != root and root not in resolved.parents:
            return None
        return resolved

    def _manual_download_file_path(self, row: sqlite3.Row) -> Path | None:
        raw_path = str(row["file_path"] or "").strip()
        if not raw_path:
            return None
        resolved = Path(raw_path).resolve()
        root = self.ctx.cfg.uploads_dir.resolve()
        if resolved != root and root not in resolved.parents:
            return None
        return resolved

    @staticmethod
    def _summarize_generation_log(value: str) -> str:
        lines = [line.strip() for line in value.splitlines() if line.strip()]
        if not lines:
            return "생성 완료"
        summary = lines[-1]
        return summary if len(summary) <= 160 else f"{summary[:157]}..."

    @staticmethod
    def _yt_dlp_version() -> str | None:
        try:
            return importlib.metadata.version("yt-dlp")
        except importlib.metadata.PackageNotFoundError:
            return None

    @staticmethod
    def _yt_dlp_latest_version() -> tuple[str | None, str | None]:
        try:
            with urlopen("https://pypi.org/pypi/yt-dlp/json", timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            return None, str(exc)
        latest_version = str(((payload or {}).get("info") or {}).get("version") or "").strip() or None
        return latest_version, None

    @staticmethod
    def _clear_loaded_module(module_prefix: str) -> None:
        for name in list(sys.modules):
            if name == module_prefix or name.startswith(f"{module_prefix}."):
                sys.modules.pop(name, None)

    @staticmethod
    def _summarize_command_output(stdout: str | None, stderr: str | None) -> str:
        chunks: list[str] = []
        for stream in (stdout, stderr):
            text = str(stream or "").strip()
            if not text:
                continue
            last_line = text.splitlines()[-1].strip()
            if last_line:
                chunks.append(last_line[:200])
        return " | ".join(chunks) if chunks else "-"

    def _yt_dlp_status_payload(self) -> dict[str, Any]:
        version = self._yt_dlp_version()
        latest_version, latest_check_error = self._yt_dlp_latest_version()
        return {
            "installed": bool(version),
            "version": version,
            "latest_version": latest_version,
            "update_available": bool(version and latest_version and version != latest_version),
            "latest_check_error": latest_check_error,
            "update_enabled": True,
            "manual_download_enabled": True,
            "active_job": None,
            "python": sys.executable,
        }

    @staticmethod
    def _month_label(value: str | None) -> str | None:
        if not value:
            return None
        try:
            month = value[5:7]
            return f"{int(month)}월"
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_bool(value: Any) -> bool | None:
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in {0, 1}:
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "y", "on"}:
                return True
            if normalized in {"0", "false", "no", "n", "off"}:
                return False
        return None

    @staticmethod
    def _is_client_disconnect(exc: OSError) -> bool:
        return isinstance(exc, (BrokenPipeError, ConnectionResetError)) or getattr(exc, "errno", None) in {32, 104}

    def _session_token(self) -> str | None:
        cookie_header = self.headers.get("Cookie", "")
        if not cookie_header:
            return None
        cookie = SimpleCookie()
        cookie.load(cookie_header)
        morsel = cookie.get("session")
        return morsel.value if morsel else None

    def _require_auth(self, conn: sqlite3.Connection, required: bool, admin: bool = False):
        user = authenticate_session(conn, self._session_token())
        if not user and required:
            self._send_json({"error": "auth-required"}, status=HTTPStatus.UNAUTHORIZED)
            return None
        if user and admin and not user.is_admin_approved:
            self._send_json({"error": "admin-required"}, status=HTTPStatus.FORBIDDEN)
            return None
        return user

    def log_message(self, format: str, *args: Any) -> None:
        # Reduce noisy default HTTP logs in CLI output.
        return


def make_server(ctx: AppContext) -> ThreadingHTTPServer:
    class _Handler(RadioHTTPRequestHandler):
        pass

    _Handler.ctx = ctx
    return ThreadingHTTPServer((ctx.cfg.host, ctx.cfg.port), _Handler)
