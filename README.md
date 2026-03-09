# Broadcast Playlist App

리로스쿨 로그인 기반으로 곡 신청, 투표, 회차 마감, 플레이리스트 아티팩트 생성을 처리하는 Python 웹 앱입니다.

## What It Does
- 로그인한 사용자만 곡 신청과 투표 가능
- 현재 월간 회차를 기본으로 운영하고, 관리자에서 주간으로 전환 가능
- 회차당 신청 3곡, 투표 3곡 제한
- iTunes Search API로 곡 검색
- 마감 시 `M3U`와 합본 `MP3` 생성
- 누락된 음원은 YouTube 후보를 점수화해 `yt-dlp`로 자동 확보 시도
- 관리자 화면에서 설정 변경, 수동 마감, 관리자 승인, 아티팩트 다운로드, 운영 로그 확인 가능

## Requirements
- Python 3.12+
- `ffmpeg`
- `ffprobe`
- Python dependencies from `requirements.txt`

`ffmpeg`와 `ffprobe`가 없으면 서버가 시작되지 않습니다.

## Quick Start
의존성 설치 명령은 이 문서 작성 중 다시 실행하지 않았습니다. 현재 워크스페이스에는 이미 설치된 상태였습니다.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

로컬 개발에서는 mock 인증으로 실행하는 편이 안전합니다.

```bash
RIRO_AUTH_MODE=mock RADIO_PORT=8092 python3 main.py
```

검증된 엔드포인트:
- `http://127.0.0.1:8092/`
- `http://127.0.0.1:8092/admin`
- `http://127.0.0.1:8092/api/health`
- `http://127.0.0.1:8092/api/public/current-round`

## Verified Commands
이 문서 기준으로 실제 실행 확인한 명령입니다.

```bash
RIRO_AUTH_MODE=mock RADIO_PORT=8092 python3 main.py
curl -sS http://127.0.0.1:8092/api/health
curl -sS http://127.0.0.1:8092/api/public/current-round
python3 -m unittest discover -s tests -v
```

## Auth Modes
- `RIRO_AUTH_MODE=riro`
  - 기본값
  - 실제 리로스쿨 로그인 사용
- `RIRO_AUTH_MODE=mock`
  - 로컬 테스트용
  - 로그인 요청에 `riro_user_key`, `display_name` 사용 가능

## Main Pages
- `/` : 메인 화면, 현재 순위와 로그인 버튼
- `/submit` : 신청 화면
- `/vote` : 투표 화면
- `/admin` : 관리자 운영 화면

관리자 승인 계정도 일반 사용자 페이지를 그대로 사용할 수 있고, 필요할 때만 `/admin`으로 이동하면 됩니다.

## Main API
### Public / User
- `GET /api/health`
- `GET /api/me`
- `GET /api/me/votes`
- `GET /api/public/current-round`
- `GET /api/public/songs`
- `GET /api/public/results?round_id=<id>`
- `POST /api/auth/login`
- `POST /api/auth/logout`
- `POST /api/songs/search`
- `POST /api/submissions`
- `PUT /api/votes`

### Admin
- `GET /api/admin/settings/current`
- `POST /api/admin/settings`
- `GET /api/admin/users`
- `POST /api/admin/users/approve`
- `GET /api/admin/submissions/current`
- `POST /api/admin/rounds/close`
- `GET /api/admin/artifacts/latest`
- `GET /api/admin/artifacts/download?artifact_id=<id>&type=m3u|mp3`
- `GET /api/admin/audit-logs?limit=20`

관리자 곡 숨김 API는 현재 비활성화되어 있고, 호출 시 `403 submission-hide-disabled`를 반환합니다.

## Environment Variables
| Variable | Default | Meaning |
|---|---:|---|
| `RADIO_HOST` | `127.0.0.1` | bind host |
| `RADIO_PORT` | `8080` | bind port |
| `RADIO_DB_PATH` | `data/radio.db` | SQLite path |
| `RADIO_UPLOADS_DIR` | `uploads` | downloaded audio root |
| `RADIO_ARTIFACTS_DIR` | `artifacts` | generated playlist root |
| `RADIO_SESSION_TTL_HOURS` | `24` | session lifetime |
| `RADIO_TIMEZONE` | `Asia/Seoul` | round window timezone |
| `RADIO_SEARCH_COUNTRY` | `KR` | iTunes search region |
| `RIRO_AUTH_MODE` | `riro` | `riro` or `mock` |
| `RADIO_FFMPEG_PATH` | unset | explicit `ffmpeg` binary or bin dir |
| `RADIO_SCHEDULER_INTERVAL_SECONDS` | `30` | background scheduler interval |
| `RADIO_FILE_RETENTION_SECONDS` | `1800` | automatic deletion threshold |
| `RADIO_YT_DLP_ENABLED` | `1` | YouTube auto-download on/off |
| `RADIO_SESSION_COOKIE_SECURE` | `0` | add `Secure` to session cookie |

## Audio Pipeline
- 검색은 iTunes Search API 사용
- 저장되는 `track_id`는 `itunes:<id>` 형식
- 회차 마감 시 음원이 없는 곡은 YouTube 검색 후보를 최대 8개까지 비교
- `Topic`, `official audio`, 업로더/제목 일치도에 가산점 부여
- `live`, `cover`, `remix`, `karaoke`, `lyrics`, `shorts` 등에 감점 부여
- 가능한 후보를 내려받아 `audio_assets`에 저장
- 최종 선택 소스는 `audit_logs`에 `youtube_audio_selected`로 기록

## Retention Policy
기본적으로 30분이 지나면 다음 항목을 자동 정리합니다.
- `uploads/` 아래 개별 음원 파일
- `artifacts/` 아래 회차 결과 파일
- 해당 파일과 연결된 `audio_assets`, `round_artifacts` DB row

자동 정리를 끄려면 다음처럼 실행합니다.

```bash
RIRO_AUTH_MODE=mock RADIO_PORT=8093 RADIO_FILE_RETENTION_SECONDS=0 python3 main.py
```

## Admin Bootstrap
첫 관리자 부여와 전원 잠금 복구는 break-glass 절차로 DB에서 직접 처리합니다.

```bash
python3 - <<'PY'
import sqlite3
conn = sqlite3.connect("data/radio.db")
conn.execute(
    "update users set is_admin_approved = 1 where riro_user_key = ?",
    ("승인할_riro_user_key",),
)
conn.commit()
print("ok")
PY
```

이 SQL 패턴은 임시 SQLite DB로 동작 확인했습니다.

## Testing
```bash
python3 -m unittest discover -s tests -v
```

## More
운영 절차와 장애 대응은 [docs/operations-guide.md](docs/operations-guide.md)를 참고하세요.
