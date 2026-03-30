# Operations Guide

## Before You Run
- Python dependencies installed
- `ffmpeg` installed
- `ffprobe` installed
- 쓰기 가능한 `data/`, `uploads/`, `artifacts/` 디렉터리

서버는 시작 시 `ffmpeg`와 `ffprobe`를 검사합니다. 하나라도 없으면 바로 종료합니다.

## Recommended Local Run
```bash
RIRO_AUTH_MODE=mock RADIO_PORT=8092 python3 main.py
```

확인:
```bash
curl -sS http://127.0.0.1:8092/api/health
curl -sS http://127.0.0.1:8092/api/public/current-round
```

## Daily Admin Flow
1. `/admin` 접속
2. 현재 기본 설정 확인
3. 필요한 경우 cadence, target seconds, loudnorm 설정 수정
4. 사용자 목록에서 관리자 승인 처리
5. 제출/투표가 끝났으면 수동 마감 실행
6. 진행 패널에서 단계별 진행률과 실패 사유를 확인
7. 최신 artifact에서 `m3u`, 합본 `mp3`, 개별 선정곡 다운로드
8. 운영 로그는 요약 카드 기준으로 먼저 보고, 필요한 항목만 펼쳐 상세 확인
9. 운영 로그에서 `manual_close`, `youtube_audio_selected`, `retention_cleanup` 확인

## Admin Screen Sections
- 운영 요약: 현재 유저 수, 관리자 수, 제출 수, 최근 상태
- 기본 설정: 월간/주간, 목표 길이, loudnorm 여부
- 수동 마감: 현재 열린 회차를 즉시 마감하고, 진행 바에서 단계 상태를 표시
- 관리자 승인: 특정 사용자에게 관리자 권한 부여
- 관리자 권한 회수: `RADIO_SUPER_ADMIN_IDS`에 등록된 슈퍼관리자만 가능, 단 슈퍼관리자 대상 회수는 불가
- 최근 아티팩트: 최신 `m3u/mp3`와 개별 선정곡 다운로드
- 유지보수: 모든 관리자가 `yt-dlp` 최신 버전 확인, 업데이트, 수동 유튜브 다운로드 실행 가능
- 운영 로그: 최근 관리 작업과 음원 선택 로그를 요약/펼침형으로 확인

## First Admin Bootstrap
기본 유저는 관리자 권한이 없습니다. 첫 관리자는 DB에서 직접 올려야 합니다.
슈퍼관리자 기준은 DB가 아니라 환경변수 `RADIO_SUPER_ADMIN_IDS=riro1,riro2` 로 정합니다.

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

주의:
- 이 절차는 운영용 break-glass 절차입니다.
- 공개 API로 첫 관리자 부여를 열어두지 않았습니다.

## Audio Download Behavior
- 음원이 비어 있는 곡은 마감 시 자동 확보를 시도합니다.
- 검색은 YouTube 후보 기반입니다.
- 첫 결과를 바로 받지 않고, 후보 점수화 후 더 나은 항목을 먼저 시도합니다.
- 완전한 보장은 없으므로 운영 로그에서 `youtube_audio_selected`를 확인하는 편이 안전합니다.
- `yt-dlp`가 비활성화되어 있거나 실패하면 해당 곡은 스킵될 수 있습니다.

## Retention and Disk Use
- `RADIO_FILE_RETENTION_SECONDS=86400`
- `RADIO_AUDIT_LOG_RETENTION_DAYS=30`

이 시간이 지나면 스케줄러가 다음을 정리합니다.
- 오래된 `uploads/` 파일
- 오래된 `artifacts/` 파일
- 오래된 `manual_downloads` 파일
- 연결된 `audio_assets`, `round_artifacts` row
- 오래된 `audit_logs` row

운영에서 더 길게 보관하려면:
```bash
RIRO_AUTH_MODE=mock RADIO_PORT=8094 RADIO_FILE_RETENTION_SECONDS=86400 python3 main.py
```

완전히 끄려면:
```bash
RIRO_AUTH_MODE=mock RADIO_PORT=8093 RADIO_FILE_RETENTION_SECONDS=0 python3 main.py
```

## Recommended Production Settings
- `RADIO_SESSION_COOKIE_SECURE=1` when serving over HTTPS
- `RADIO_FILE_RETENTION_SECONDS` adjusted to your storage policy
- `RADIO_AUDIT_LOG_RETENTION_DAYS` adjusted to your audit policy
- keep SQLite defaults at `WAL` / `NORMAL` / `busy_timeout=5000ms` unless you have a specific reason to change them
- `RADIO_YT_DLP_ENABLED=1` unless you have a separate audio ingestion path
- `RIRO_AUTH_MODE=riro` in real deployment

## Common Failure Cases
### Server exits immediately on startup
원인:
- `ffmpeg` 또는 `ffprobe` 없음

대응:
- binary 설치
- 필요하면 `RADIO_FFMPEG_PATH`를 binary 경로 또는 bin 디렉터리로 지정

### `song-search-unavailable`
원인:
- iTunes Search API 연결 실패

대응:
- 서버 네트워크 상태 확인
- outbound HTTPS 제한 여부 확인

### `riro-auth-failed`
원인:
- 리로스쿨 인증 실패 또는 응답 이상

대응:
- 계정 정보 확인
- 로컬에서는 `RIRO_AUTH_MODE=mock` 사용

### `artifact-file-missing`
원인:
- artifact row는 있지만 파일이 삭제됨
- 자동 정리 정책으로 이미 정리됨

대응:
- 보관 시간을 늘리거나 자동 정리 비활성화
- 마감 다시 실행 여부 검토

### `submission-hide-disabled`
원인:
- 관리자 곡 숨김 기능이 현재 비활성화됨

대응:
- 현재는 제출 목록 조회만 가능

## Useful Logs
`audit_logs`에서 특히 볼 값:
- `manual_close`
- `settings_updated`
- `admin_approval_changed`
- `youtube_audio_selected`
- `retention_cleanup`
- `auto_close_failed`
- `close_failed`
