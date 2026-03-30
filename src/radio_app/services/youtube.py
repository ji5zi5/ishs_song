from __future__ import annotations

import json
import logging
import re
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from radio_app.services.audio import merge_mp3_files, validate_mp3_and_get_duration_seconds

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
    "shorts",
}
NEGATIVE_PHRASES = {
    "열린 음악회": 30,
    "open concert": 30,
    "broadcast": 30,
    "방송": 30,
}
BROADCAST_SHOW_HINTS = {
    "열린 음악회",
    "열린음악회",
    "open concert",
    "이소라의 프로포즈",
    "유희열의 스케치북",
    "더 리슨",
    "the listen",
    "뮤캉스",
    "더 시즌즈",
    "the seasons",
    "뮤직뱅크",
    "music bank",
    "인기가요",
    "inkigayo",
    "쇼 음악중심",
    "쇼! 음악중심",
    "음악중심",
    "music core",
    "엠카운트다운",
    "m countdown",
    "쇼챔피언",
    "show champion",
    "불후의 명곡",
    "immortal songs",
    "가요무대",
    "가요대전",
    "전국노래자랑",
    "윤도현의 러브레터",
    "드림콘서트",
    "콘서트 7080",
    "콘서트7080",
}
BROADCAST_NETWORK_HINTS = {"kbs", "sbs", "mbc", "ebs", "mnet", "jtbc", "tvn"}
BROADCAST_META_HINTS = {"방송", "broadcast", "무대"}
TOPIC_HINTS = {"topic", "official audio", "provided to youtube by"}
LYRICS_HINTS = {"lyrics", "lyric", "가사"}
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
MAX_MANUAL_PLAYLIST_ITEMS = 20


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


def _is_playlist_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.path.rstrip("/") == "/playlist":
        return True
    return bool(parse_qs(parsed.query).get("list"))


def _new_downloaded_mp3s(output_dir: Path, existing_files: set[Path]) -> list[Path]:
    return sorted(
        (p for p in output_dir.glob("*.mp3") if p.resolve() not in existing_files),
        key=lambda p: p.name,
    )


def _ordered_playlist_files(downloaded_files: list[Path], info: dict[str, Any]) -> list[Path]:
    entries = [entry for entry in (info.get("entries") or []) if isinstance(entry, dict)]
    if not entries:
        return downloaded_files

    remaining = list(downloaded_files)
    ordered: list[Path] = []
    for entry in entries:
        entry_id = str(entry.get("id") or "").strip()
        match = None
        if entry_id:
            suffix = f"-{entry_id}.mp3"
            for path in remaining:
                if path.name.endswith(suffix):
                    match = path
                    break
        if match is None and remaining:
            match = remaining[0]
        if match is not None:
            ordered.append(match)
            remaining.remove(match)
    ordered.extend(remaining)
    return ordered


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


def _looks_like_broadcast_performance(raw_text: str) -> bool:
    lowered = (raw_text or "").casefold()
    if any(hint in lowered for hint in BROADCAST_SHOW_HINTS):
        return True

    has_network = any(hint in lowered for hint in BROADCAST_NETWORK_HINTS)
    has_meta = any(hint in lowered for hint in BROADCAST_META_HINTS)
    has_date = bool(re.search(r"(19|20)\d{2}\s*년", lowered) or re.search(r"\b\d{6,8}\b", lowered))
    return has_network and (has_meta or has_date)


def _confidence_band(score: int, title_overlap: int, artist_overlap: int) -> str:
    if score >= 150 and title_overlap >= 90 and artist_overlap >= 45:
        return "strong"
    if score >= 95 and title_overlap >= 60:
        return "good"
    if score >= 45:
        return "weak"
    return "reject"


def _candidate_priority(candidate: RankedCandidate) -> int:
    normalized = _normalize_text(f"{candidate.title} {candidate.uploader}")
    if any(hint in normalized for hint in TOPIC_HINTS):
        return 2
    if any(hint in normalized for hint in LYRICS_HINTS):
        return 1
    return 0


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
    combined_metadata = f"{title} {uploader}"

    title_overlap = _overlap_score(requested_title_tokens, title_tokens)
    artist_overlap = _overlap_score(requested_artist_tokens, metadata_tokens)
    title_has_requested_title = bool(requested_title) and _contains_phrase(title, requested_title)
    title_has_requested_artist = bool(requested_artist) and _contains_phrase(title, requested_artist)
    title_has_artist_and_title = title_has_requested_title and title_has_requested_artist

    score = title_overlap + int(artist_overlap * 0.7)
    reasons: list[str] = []

    if title_has_requested_title:
        score += 28
        reasons.append("title-phrase")
    if requested_artist and _contains_phrase(combined_text, requested_artist):
        score += 16
        reasons.append("artist-phrase")
    if title_has_artist_and_title:
        score += 36
        reasons.append("title-artist-title")

    normalized_combined = _normalize_text(combined_metadata)
    if any(hint in normalized_combined for hint in TOPIC_HINTS):
        score += 18
        reasons.append("topic-hint")

    for term in NEGATIVE_TERMS:
        if term in normalized_combined:
            penalty = 24 if term != "shorts" else 60
            score -= penalty
            reasons.append(f"-{term}")

    lowered_metadata = combined_metadata.lower()
    for phrase, penalty in NEGATIVE_PHRASES.items():
        if phrase in lowered_metadata:
            score -= penalty
            reasons.append(f"-{phrase}")

    is_broadcast_performance = _looks_like_broadcast_performance(combined_metadata)
    if is_broadcast_performance:
        score -= 140
        reasons.append("broadcast-performance")

    if not title and not uploader:
        score -= 100
        reasons.append("missing-metadata")

    if title_overlap == 0 and artist_overlap == 0:
        score -= 50
        reasons.append("no-overlap")

    confidence = _confidence_band(score, title_overlap, artist_overlap)
    if is_broadcast_performance:
        confidence = "reject"
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
        "ignoreerrors": True,
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
    ranked_candidates = [
        _rank_candidate(entry, requested_artist=requested_artist, requested_title=requested_title)
        for entry in raw_entries
        if isinstance(entry, dict) and (entry.get("id") or entry.get("webpage_url"))
    ]
    ranked_candidates = [
        candidate
        for _index, candidate in sorted(
            enumerate(ranked_candidates),
            key=lambda item: (item[1].score, _candidate_priority(item[1]), -item[0], item[1].video_id),
            reverse=True,
        )
    ]

    if not ranked_candidates:
        raise RuntimeError(f"no-candidates-for-query:{query}")

    viable_candidates = [candidate for candidate in ranked_candidates if candidate.confidence != "reject" and candidate.video_url]
    download_queue = viable_candidates
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


def download_youtube_url(
    url: str,
    output_dir: Path,
    ffmpeg_path: str | None = None,
) -> DownloadedAudio:
    """Download a specific YouTube URL as an mp3."""
    try:
        import yt_dlp  # noqa: WPS433 – optional dependency
    except ImportError as exc:
        raise RuntimeError(
            "yt-dlp is not installed. Run `pip install yt-dlp` to enable YouTube downloads."
        ) from exc

    output_dir.mkdir(parents=True, exist_ok=True)
    file_stub = _sanitize_filename(f"manual-{uuid4().hex}")
    output_template = str(output_dir / f"{file_stub}-%(playlist_index)s-%(id)s.%(ext)s")
    existing_files = {p.resolve() for p in output_dir.glob("*.mp3")}
    playlist_requested = _is_playlist_url(url)

    ffmpeg_loc = ffmpeg_path or shutil.which("ffmpeg")
    ydl_opts: dict[str, Any] = {
        "format": "bestaudio/best",
        "outtmpl": output_template,
        "quiet": True,
        "no_warnings": True,
        "noplaylist": not playlist_requested,
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
    if playlist_requested:
        ydl_opts["playlistend"] = MAX_MANUAL_PLAYLIST_ITEMS
    if ffmpeg_loc:
        ydl_opts["ffmpeg_location"] = str(Path(ffmpeg_loc).parent)

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        if info is None:
            raise RuntimeError(f"yt-dlp returned no info for url: {url}")

    mp3_files = _new_downloaded_mp3s(output_dir, existing_files)
    if not mp3_files:
        raise RuntimeError(f"mp3 file not found after download for url: {url}")

    entries = [entry for entry in (info.get("entries") or []) if isinstance(entry, dict)]
    is_playlist_download = playlist_requested and len(mp3_files) > 1 and bool(entries)
    if is_playlist_download:
        ordered_files = _ordered_playlist_files(mp3_files, info)
        found = output_dir / f"{file_stub}-playlist.mp3"
        merge_mp3_files(ordered_files, found, loudnorm_enabled=False, ffmpeg_path=ffmpeg_path)
        for path in mp3_files:
            path.unlink(missing_ok=True)
    else:
        found = sorted(mp3_files, key=lambda p: p.stat().st_mtime, reverse=True)[0]

    title = str(info.get("title") or info.get("playlist_title") or info.get("track") or found.stem).strip()
    uploader = str(
        info.get("uploader")
        or info.get("channel")
        or info.get("channel_name")
        or info.get("playlist_uploader")
        or ""
    ).strip()
    video_id = str(info.get("id") or "").strip()
    video_url = str(info.get("webpage_url") or url).strip()
    if not video_url and video_id:
        video_url = f"https://www.youtube.com/watch?v={video_id}"

    return DownloadedAudio(
        path=found,
        candidate=RankedCandidate(
            video_id=video_id,
            video_url=video_url or url,
            title=title or found.stem,
            uploader=uploader,
            score=0,
            confidence="direct",
            reason="manual-playlist-url" if is_playlist_download else "manual-url",
        ),
    )


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
