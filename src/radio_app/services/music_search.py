from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from urllib.error import HTTPError, URLError


class SongSearchError(RuntimeError):
    pass


@dataclass
class ITunesSearchClient:
    country: str = "KR"

    def search_tracks(self, query: str, limit: int = 10) -> list[dict]:
        if not query.strip():
            return []
        normalized_limit = max(1, min(limit, 20))
        params = urllib.parse.urlencode(
            {
                "term": query,
                "media": "music",
                "entity": "song",
                "country": self.country,
                "limit": normalized_limit,
            }
        )
        req = urllib.request.Request(f"https://itunes.apple.com/search?{params}")
        try:
            with urllib.request.urlopen(req, timeout=8) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            raise SongSearchError(f"iTunes 검색 API 오류: HTTP {exc.code}") from exc
        except URLError as exc:
            raise SongSearchError("iTunes 검색 API 연결에 실패했습니다.") from exc

        items = payload.get("results", [])
        result: list[dict] = []
        for item in items:
            track_id = item.get("trackId")
            title = (item.get("trackName") or "").strip()
            artist = (item.get("artistName") or "").strip()
            if not track_id or not title or not artist:
                continue
            external_track_id = f"itunes:{track_id}"
            result.append(
                {
                    "track_id": external_track_id,
                    # Backward compatibility for legacy clients.
                    "spotify_track_id": external_track_id,
                    "title": title,
                    "artist": artist,
                    "album_art_url": item.get("artworkUrl100", ""),
                    "external_url": item.get("trackViewUrl", ""),
                }
            )
        return result

