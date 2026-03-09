# Broadcast Playlist MVP

리로스쿨 로그인 기반 신청/투표 + 회차 마감 시 `M3U`/`MP3` 아티팩트를 생성하는 최소 동작 MVP입니다.

## Features
- 신청/투표 로그인 필수, 목록/결과 조회는 공개
- 회차 기본 월간(설정으로 주간 전환 가능)
- 회차당 신청 3개, 투표 3개(중복 곡 투표 불가)
- 외부 검색 `track_id` 기준 회차 내 중복 신청 방지
- 수동 마감 + 자동 마감(백그라운드 스케줄러)
- 40분 목표 길이 초과 시 하위 순위 자동 제외
- 누락/손상 음원 자동 스킵 후 다음 순위로 대체
- `M3U` + 합본 `MP3` 생성 (ffmpeg 있으면 인코딩, 없으면 fallback)

## Run
```bash
python3 main.py
```

- 사용자 페이지: `http://127.0.0.1:8080/`
- 신청 페이지: `http://127.0.0.1:8080/submit`
- 투표 페이지: `http://127.0.0.1:8080/vote`
- 관리자 페이지: `http://127.0.0.1:8080/admin`

관리자 승인 계정도 일반 페이지(`/`, `/submit`, `/vote`)를 그대로 사용할 수 있으며, 필요할 때 `/admin`으로 직접 이동하면 됩니다.

## Admin bootstrap
로그인 후 기본 유저는 admin 권한이 없습니다. 첫 관리자 부여와 관리자 전원 잠금 복구는 break-glass 절차로 DB에서 `is_admin_approved=1`을 직접 지정합니다.

```bash
python3 - <<'PY'
import sqlite3
conn = sqlite3.connect("data/radio.db")
conn.execute("update users set is_admin_approved = 1 where riro_user_key = ?", ("승인할_riro_user_key",))
conn.commit()
print("ok")
PY
```

운영 환경에서 HTTPS를 쓰는 경우 관리자 세션 쿠키는 `RADIO_SESSION_COOKIE_SECURE=1`로 설정하세요.

## API quick map
- `POST /api/auth/login` `{riro_id, riro_pw}` (`RIRO_AUTH_MODE=mock`일 때는 `{riro_user_key, display_name}`도 허용)
- `POST /api/songs/search` `{query, limit}`
- `POST /api/submissions` `{track_id, title, artist, album_art_url, external_url}` (`spotify_track_id`도 하위 호환으로 허용)
- `PUT /api/votes` `{submission_ids:[...]}` (최대 3개)
- `POST /api/admin/rounds/close`
- `POST /api/admin/settings` `{cadence, playlist_size, target_seconds, loudnorm_enabled}`
- `GET /api/admin/settings/current`
- `GET /api/admin/users`
- `GET /api/admin/submissions/current`
- `GET /api/admin/audit-logs?limit=20`
- `GET /api/admin/artifacts/latest`
- `GET /api/admin/artifacts/download?artifact_id=<id>&type=m3u|mp3`

## Notes
- 로그인은 `ISHS_Wiki/route/riroschoolauth.py` 흐름을 참고한 리로스쿨 인증(`RIRO_AUTH_MODE=riro`)이 기본값입니다.
- 로컬 테스트가 필요하면 `RIRO_AUTH_MODE=mock`으로 실행하세요.
- 곡 검색은 iTunes Search API를 사용하며 별도 API 키가 필요 없습니다.
- 관리자 화면의 신청곡 목록은 현재 읽기 전용이며, 곡 숨김 처리는 비활성화되어 있습니다.
- YouTube 음원 확보는 이제 `Topic`/공식 업로더 힌트와 제목/가수 매칭 점수로 후보를 정렬한 뒤 내려받습니다.
- 첫 검색 결과를 무조건 받지 않으며, 더 그럴듯한 후보가 뒤에 있으면 그 후보를 우선 선택합니다.
- 강한 후보가 실패하면 약한 후보도 fallback으로 시도할 수 있으며, 어떤 소스를 골랐는지는 관리자 운영 로그에서 추적할 수 있습니다.
- ffmpeg/ffprobe가 설치되어 있으면 더 정확한 길이 계산/합본이 수행됩니다.
