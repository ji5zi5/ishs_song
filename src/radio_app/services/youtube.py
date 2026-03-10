from __future__ import annotations

import json
import logging
import re
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from radio_app.services.audio import validate_mp3_and_get_duration_seconds

logger = logging.getLogger(__name__)

NEGATIVE_TERMS = {
    "live",
    "cover",
    "remix",
    "sped",
    "speed",
    "slowed",
    "karaoke",
    "instrumental",
    "inst",
    "lyrics",
    "lyric",
    "shorts",
}
TOPIC_HINTS = {"topic", "official audio", "provided to youtube by"}
NOISE_TOKENS = {
    "audio",
    "official",
    "video",
    "music",
    "mv",
    "ver",
    "version",
    "feat",
    "ft",
    "with",
}


@dataclass(frozen=True)
class RankedCandidate:
    video_id: str
    video_url: str
    title: str
    uploader: str
    score: int
    confidence: str
    reason: str


@dataclass(frozen=True)
class DownloadedAudio:
    path: Path
    candidate: RankedCandidate


def _sanitize_filename(name: str) -> str:
    """Remove characters that are problematic in file names."""
    for ch in r'\/:*?"<>|':
        name = name.replace(ch, "_")
    return name.strip()[:120]


def _split_query(query: str) -> tuple[str, str]:
    artist, sep, title = query.partition(" - ")
    if not sep:
        return "", query.strip()
    return artist.strip(), title.strip()


def _normalize_text(value: str) -> str:
    normalized = value.lower().strip()
    normalized = re.sub(r"\([^)]*\)|\[[^\]]*\]|\{[^}]*\}", " ", normalized)
    normalized = normalized.replace("feat.", "feat ").replace("ft.", "ft ")
    normalized = re.sub(r"[^0-9a-zA-Z가-힣]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _tokens(value: str) -> set[str]:
    normalized = _normalize_text(value)
    return {token for token in normalized.split() if token and token not in NOISE_TOKENS}


def _contains_phrase(value: str, phrase: str) -> bool:
    haystack = _normalize_text(value)
    needle = _normalize_text(phrase)
    return bool(needle) and needle in haystack


def _overlap_score(requested: set[str], available: set[str]) -> int:
    if not requested or not available:
        return 0
    overlap = requested & available
    return int((len(overlap) / len(requested)) * 100)


def _confidence_band(score: int, title_overlap: int, artist_overlap: int) -> str:
    if score >= 150 and title_overlap >= 90 and artist_overlap >= 45:
        return "strong"
    if score >= 95 and title_overlap >= 60:
        return "good"
    if score >= 45:
        return "weak"
    return "reject"


def _rank_candidate(entry: dict[str, Any], requested_artist: str, requested_title: str) -> RankedCandidate:
    title = str(entry.get("title") or entry.get("track") or "").strip()
    uploader = str(entry.get("uploader") or entry.get("channel") or entry.get("channel_name") or "").strip()
    video_id = str(entry.get("id") or "").strip()
    video_url = str(entry.get("webpage_url") or "").strip()
    if not video_url and video_id:
        video_url = f"https://www.youtube.com/watch?v={video_id}"

    requested_title_tokens = _tokens(requested_title)
    requested_artist_tokens = _tokens(requested_artist)
    title_tokens = _tokens(title)
    metadata_tokens = title_tokens | _tokens(uploader)
    combined_text = f"{title} {uploader} {video_url}"

    title_overlap = _overlap_score(requested_title_tokens, title_tokens)
    artist_overlap = _overlap_score(requested_artist_tokens, metadata_tokens)

    score = title_overlap + int(artist_overlap * 0.7)
    reasons: list[str] = []

    if requested_title and _contains_phrase(title, requested_title):
        score += 28
        reasons.append("title-phrase")
    if requested_artist and _contains_phrase(combined_text, requested_artist):
        score += 16
        reasons.append("artist-phrase")

    normalized_combined = _normalize_text(combined_text)
    if any(hint in normalized_combined for hint in TOPIC_HINTS):
        score += 18
        reasons.append("topic-hint")

    for term in NEGATIVE_TERMS:
        if term in normalized_combined:
            penalty = 24 if term != "shorts" else 60
            score -= penalty
            reasons.append(f"-{term}")

    if not title and not uploader:
        score -= 100
        reasons.append("missing-metadata")

    if title_overlap == 0 and artist_overlap == 0:
        score -= 50
        reasons.append("no-overlap")

    confidence = _confidence_band(score, title_overlap, artist_overlap)
    reason = ",".join(reasons) if reasons else "metadata-match"
    return RankedCandidate(
        video_id=video_id,
        video_url=video_url,
        title=title,
        uploader=uploader,
        score=score,
        confidence=confidence,
        reason=reason,
    )


def search_and_download(
    query: str,
    output_dir: Path,
    ffmpeg_path: str | None = None,
) -> DownloadedAudio:
    """Search YouTube for *query* and download the best-scored result as an mp3.

    Returns the path and metadata for the downloaded mp3 file.
    Raises ``RuntimeError`` if the download fails.
    """
    try:
        import yt_dlp  # noqa: WPS433 – optional dependency
    except ImportError as exc:
        raise RuntimeError(
            "yt-dlp is not installed. Run `pip install yt-dlp` to enable YouTube downloads."
        ) from exc

    output_dir.mkdir(parents=True, exist_ok=True)
    safe_name = _sanitize_filename(query)
    output_template = str(output_dir / f"{safe_name}.%(ext)s")
    existing_files = {p.resolve() for p in output_dir.glob("*.mp3")}

    ffmpeg_loc = ffmpeg_path or shutil.which("ffmpeg")
    base_ydl_opts: dict[str, Any] = {
        "format": "bestaudio/best",
        "outtmpl": output_template,
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "socket_timeout": 15,
        "retries": 2,
        "extractor_retries": 2,
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }
        ],
    }
    if ffmpeg_loc:
        base_ydl_opts["ffmpeg_location"] = str(Path(ffmpeg_loc).parent)

    def _new_downloaded_mp3() -> Path | None:
        expected = output_dir / f"{safe_name}.mp3"
        if expected.exists():
            return expected
        mp3_files = sorted(
            (p for p in output_dir.glob("*.mp3") if p.resolve() not in existing_files),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if mp3_files:
            return mp3_files[0]
        return None

    requested_artist, requested_title = _split_query(query)
    errors: list[str] = []

    search_opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "default_search": "ytsearch8",
        "socket_timeout": 15,
        "retries": 2,
        "extractor_retries": 2,
    }
    try:
        with yt_dlp.YoutubeDL(search_opts) as ydl:
            search_info = ydl.extract_info(query, download=False) or {}
    except Exception as exc:
        raise RuntimeError(f"candidate-search-failed:{exc}") from exc

    raw_entries = search_info.get("entries") or []
    ranked_candidates = sorted(
        (
            _rank_candidate(entry, requested_artist=requested_artist, requested_title=requested_title)
            for entry in raw_entries
            if isinstance(entry, dict)
        ),
        key=lambda candidate: (candidate.score, candidate.video_id),
        reverse=True,
    )

    if not ranked_candidates:
        raise RuntimeError(f"no-candidates-for-query:{query}")

    viable_candidates = [candidate for candidate in ranked_candidates if candidate.confidence != "reject" and candidate.video_url]
    fallback_candidates = [candidate for candidate in ranked_candidates if candidate.video_url]
    download_queue = viable_candidates or fallback_candidates[:1]
    if not download_queue:
        raise RuntimeError(f"no-viable-candidates-for-query:{query}")

    for candidate in download_queue:
        try:
            with yt_dlp.YoutubeDL(base_ydl_opts) as ydl:
                info = ydl.extract_info(candidate.video_url, download=True)
                if info is None:
                    raise RuntimeError(f"yt-dlp returned no info for candidate: {candidate.video_url}")
            found = _new_downloaded_mp3()
            if found:
                return DownloadedAudio(path=found, candidate=candidate)
            errors.append(f"{candidate.video_url}:mp3-not-found-after-download")
        except Exception as exc:
            errors.append(f"{candidate.video_url}:{exc}")
            continue

    detail = errors[-1] if errors else "unknown-error"
    raise RuntimeError(f"mp3 file not found after download for query: {query}; detail={detail}")


def ensure_audio_for_songs(
    conn: sqlite3.Connection,
    songs: list[sqlite3.Row],
    uploads_dir: Path,
    ffmpeg_path: str | None = None,
) -> dict[int, str]:
    """Download missing audio for the given ranked song rows.

    *songs* should be rows from ``ranked_submissions`` (must have keys:
    ``submission_id``, ``title``, ``artist``, ``file_path``, ``is_valid``).

    Returns a mapping of ``submission_id`` → status string.
    """
    results: dict[int, str] = {}
    yt_dir = uploads_dir / "youtube"
    yt_dir.mkdir(parents=True, exist_ok=True)

    def _restore_existing_audio_asset(submission_id: int, song_id: int, path: Path) -> str | None:
        duration, error = validate_mp3_and_get_duration_seconds(path)
        if error or duration <= 0:
            return None

        from radio_app.db import utc_now_iso

        conn.execute(
            """
            INSERT INTO audio_assets(song_id, file_path, duration_seconds, is_valid, validation_error, uploaded_at)
            VALUES (?, ?, ?, 1, NULL, ?)
            ON CONFLICT(song_id) DO UPDATE SET
                file_path = excluded.file_path,
                duration_seconds = excluded.duration_seconds,
                is_valid = 1,
                validation_error = NULL,
                uploaded_at = excluded.uploaded_at
            """,
            (song_id, str(path), duration, utc_now_iso()),
        )
        logger.info("revalidated existing audio for submission=%s → %s (%ds)", submission_id, path, duration)
        return "revalidated-existing"

    for song in songs:
        sub_id = int(song["submission_id"])
        submission_row = conn.execute(
            "SELECT song_id, round_id FROM submissions WHERE id = ?",
            (sub_id,),
        ).fetchone()
        if submission_row is None:
            results[sub_id] = "submission-not-found"
            continue
        song_id = int(submission_row["song_id"])
        round_id = int(submission_row["round_id"])
        file_path = song["file_path"]
        is_valid = int(song["is_valid"]) if song["is_valid"] is not None else 0

        if file_path and Path(file_path).exists():
            existing_path = Path(file_path)
            existing_duration, existing_error = validate_mp3_and_get_duration_seconds(existing_path)
            if not existing_error and existing_duration > 0:
                try:
                    stored_duration = int(song["duration_seconds"] or 0)
                except (IndexError, KeyError):
                    stored_duration = 0
                if not is_valid or stored_duration != existing_duration:
                    restored = _restore_existing_audio_asset(sub_id, song_id, existing_path)
                    if restored:
                        results[sub_id] = restored
                        continue
                results[sub_id] = "already-exists"
                continue
            logger.warning("existing audio invalid for submission=%s: %s", sub_id, existing_error)

        title = song["title"]
        artist = song["artist"]
        query = f"{artist} - {title}"

        try:
            download = search_and_download(query, yt_dir, ffmpeg_path=ffmpeg_path)
        except Exception as exc:
            logger.warning("youtube download failed for %s: %s", query, exc)
            results[sub_id] = f"download-failed:{exc}"
            continue

        duration, error = validate_mp3_and_get_duration_seconds(download.path)
        if error or duration <= 0:
            logger.warning("downloaded file invalid for %s: %s", query, error)
            download.path.unlink(missing_ok=True)
            results[sub_id] = f"invalid-download:{error}"
            continue

        from radio_app.db import utc_now_iso

        conn.execute(
            """
            INSERT INTO audio_assets(song_id, file_path, duration_seconds, is_valid, validation_error, uploaded_at)
            VALUES (?, ?, ?, 1, NULL, ?)
            ON CONFLICT(song_id) DO UPDATE SET
                file_path      = excluded.file_path,
                duration_seconds = excluded.duration_seconds,
                is_valid       = 1,
                validation_error = NULL,
                uploaded_at    = excluded.uploaded_at
            """,
            (song_id, str(download.path), duration, utc_now_iso()),
        )
        audit_detail = {
            "submission_id": sub_id,
            "query": query,
            "video_id": download.candidate.video_id,
            "video_url": download.candidate.video_url,
            "title": download.candidate.title,
            "uploader": download.candidate.uploader,
            "score": download.candidate.score,
            "confidence": download.candidate.confidence,
            "reason": download.candidate.reason,
            "duration_seconds": duration,
        }
        conn.execute(
            "INSERT INTO audit_logs(round_id, action, detail, created_at) VALUES (?, ?, ?, ?)",
            (round_id, "youtube_audio_selected", json.dumps(audit_detail, ensure_ascii=True), utc_now_iso()),
        )
        status_prefix = "downloaded"
        if download.candidate.confidence != "strong":
            status_prefix = "downloaded:weak-fallback"
        results[sub_id] = f"{status_prefix}:duration={duration}s"
        logger.info(
            "downloaded youtube audio for %s → %s (%ds, %s, score=%s)",
            query,
            download.path,
            duration,
            download.candidate.confidence,
            download.candidate.score,
        )

    return results
