from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

from radio_app.auth import authenticate_session, create_or_update_user, issue_session, revoke_session
from radio_app.config import AppConfig
from radio_app.db import DB, utc_now_iso
from radio_app.services.rounds import (
    close_round,
    current_defaults,
    enforce_rate_limit,
    ensure_open_round,
    get_round_result,
    get_setting,
    ranked_submissions,
    select_round_for_admin_close,
    set_setting,
)
from radio_app.services.riro import check_riro_login
from radio_app.services.music_search import ITunesSearchClient, SongSearchError


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
            if path == "/api/admin/artifacts/latest":
                return self._handle_admin_latest_artifact()
            if path == "/api/admin/artifacts/download":
                return self._handle_admin_artifact_download()
            self._send_json({"error": "not-found"}, status=HTTPStatus.NOT_FOUND)
        except Exception as exc:
            self._handle_unexpected_error(exc)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
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
            self._send_json({"error": "not-found"}, status=HTTPStatus.NOT_FOUND)
        except Exception as exc:
            self._handle_unexpected_error(exc)

    def do_PUT(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
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

            result = check_riro_login(riro_id, riro_pw)
            if result.status != "success":
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
        track_id = str(body.get("track_id", body.get("spotify_track_id", ""))).strip()
        title = str(body.get("title", "")).strip()
        artist = str(body.get("artist", "")).strip()
        album_art_url = str(body.get("album_art_url", "")).strip()
        external_url = str(body.get("external_url", "")).strip()
        if not track_id or not title or not artist:
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
            self._send_json({"round": dict(round_row)})

    def _handle_public_songs(self) -> None:
        with self.ctx.db.session() as conn:
            round_row = ensure_open_round(conn, self.ctx.cfg.timezone)
            items = ranked_submissions(conn, int(round_row["id"]))
            self._send_json(
                {
                    "round_id": int(round_row["id"]),
                    "round": dict(round_row),
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
            self._send_json({"artifact": self._artifact_summary_payload(row) if row else None})

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
                SELECT ra.*, r.start_at
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
            round_month = self._month_label(row["start_at"])
            download_name = f"{round_month}-playlist.{artifact_type}" if round_month else f"playlist.{artifact_type}"
            content_type = "audio/mpeg" if artifact_type == "mp3" else "audio/x-mpegurl; charset=utf-8"
            self._send_file(file_path, content_type, download_name)

    def _handle_admin_close_round(self) -> None:
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
            round_row = select_round_for_admin_close(conn, self.ctx.cfg.timezone)
            try:
                result = close_round(
                    conn,
                    int(round_row["id"]),
                    artifacts_dir=self.ctx.cfg.artifacts_dir,
                    uploads_dir=self.ctx.cfg.uploads_dir,
                    ffmpeg_path=self.ctx.cfg.ffmpeg_path,
                    yt_dlp_enabled=self.ctx.cfg.yt_dlp_enabled,
                )
            except RuntimeError as exc:
                if str(exc) == "no-valid-audio-assets":
                    return self._send_json(
                        {
                            "error": "no-valid-audio-assets",
                            "message": "마감할 수 있는 유효한 음원이 없습니다.",
                            "round_id": int(round_row["id"]),
                        },
                        status=HTTPStatus.CONFLICT,
                    )
                raise
            summary_row = conn.execute(
                """
                SELECT ra.*, r.cadence, r.start_at, r.end_at
                FROM round_artifacts ra
                JOIN rounds r ON r.id = ra.round_id
                WHERE ra.round_id = ?
                """,
                (int(round_row["id"]),),
            ).fetchone()
            response_payload = {
                key: value
                for key, value in result.items()
                if key not in {"m3u_path", "mp3_path", "generation_log", "artifact"}
            }
            response_payload["artifact"] = self._artifact_summary_payload(summary_row) if summary_row else None
            self._insert_audit_log(
                conn,
                action="manual_close",
                detail=response_payload,
                actor_user_id=user.id,
                round_id=int(round_row["id"]),
            )
            self._send_json(response_payload)

    def _handle_admin_settings(self) -> None:
        body = self._read_json_body()
        cadence = str(body.get("cadence", "")).strip()
        playlist_size = int(body.get("playlist_size", 12))
        target_seconds = int(body.get("target_seconds", 2400))
        loudnorm_enabled = bool(body.get("loudnorm_enabled", True))
        if cadence not in {"weekly", "monthly"}:
            return self._send_json({"error": "cadence-must-be-weekly-or-monthly"}, status=HTTPStatus.BAD_REQUEST)
        if playlist_size < 1 or playlist_size > 100:
            return self._send_json({"error": "invalid-playlist-size"}, status=HTTPStatus.BAD_REQUEST)
        if target_seconds < 60 or target_seconds > 10800:
            return self._send_json({"error": "invalid-target-seconds"}, status=HTTPStatus.BAD_REQUEST)
        with self.ctx.db.session() as conn:
            user = self._require_auth(conn, required=True, admin=True)
            if not user:
                return
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
                "SELECT id, is_admin_approved FROM users WHERE id = ?",
                (target,),
            ).fetchone()
            if row is None:
                return self._send_json({"error": "user-not-found", "user_id": target}, status=HTTPStatus.NOT_FOUND)
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
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.end_headers()
            self.wfile.write(path.read_bytes())
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

    def _artifact_summary_payload(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        m3u_path = self._artifact_file_path(row, "m3u")
        mp3_path = self._artifact_file_path(row, "mp3")
        return {
            "id": int(row["id"]),
            "round_id": int(row["round_id"]),
            "cadence": row["cadence"],
            "start_at": row["start_at"],
            "end_at": row["end_at"],
            "total_seconds": int(row["total_seconds"]),
            "created_at": row["created_at"],
            "has_m3u": bool(m3u_path and m3u_path.exists()),
            "has_mp3": bool(mp3_path and mp3_path.exists()),
            "generation_summary": self._summarize_generation_log(str(row["generation_log"] or "")),
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

    @staticmethod
    def _summarize_generation_log(value: str) -> str:
        lines = [line.strip() for line in value.splitlines() if line.strip()]
        if not lines:
            return "생성 완료"
        summary = lines[-1]
        return summary if len(summary) <= 160 else f"{summary[:157]}..."

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
