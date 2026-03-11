from __future__ import annotations

import html
import re
import time
from dataclasses import dataclass

import requests


@dataclass
class RiroAuthResult:
    status: str
    message: str | None = None
    name: str | None = None
    student_number: str | None = None
    generation: int | None = None
    student: str | None = None
    riro_id: str | None = None


_RIRO_BLOCK_MESSAGE = "리로 인증 서버 접근이 차단되었습니다. VPN/프록시를 끄고 다시 시도하세요."
_RIRO_TIMEOUT_MESSAGE = "리로 인증 서버 응답이 지연되고 있습니다. 잠시 후 다시 시도하세요."


def _strip_tags(value: str) -> str:
    text = re.sub(r"<[^>]+>", "", value or "")
    return html.unescape(text).strip()


def _extract_by_class(raw_html: str, class_name: str) -> list[str]:
    pattern = re.compile(
        rf"<([a-zA-Z0-9]+)([^>]*class=[\"'][^\"']*{re.escape(class_name)}[^\"']*[\"'][^>]*)>(.*?)</\1>",
        re.IGNORECASE | re.DOTALL,
    )
    return [_strip_tags(match.group(3)) for match in pattern.finditer(raw_html)]


def _safe_generation(riro_id: str) -> int:
    if len(riro_id) >= 2 and riro_id[:2].isdigit():
        return int("20" + riro_id[:2]) - 1994 + 1
    return 0


def _normalize_student_number(raw: str) -> str:
    raw = re.sub(r"\s+", "", raw or "")
    if len(raw) >= 3:
        return raw[0] + raw[2:]
    return raw


def _extract_profile_from_html(raw_html: str, login_id: str) -> dict | None:
    student_values = _extract_by_class(raw_html, "m_level3") or _extract_by_class(raw_html, "m_level1")
    disabled_values = _extract_by_class(raw_html, "input_disabled")

    if student_values and len(disabled_values) >= 2:
        name = disabled_values[0]
        student_number = _normalize_student_number(disabled_values[1])
        student = student_values[0]
        generation = _safe_generation(login_id)
        if name and student_number and student and generation > 0:
            return {
                "name": name,
                "student_number": student_number,
                "generation": generation,
                "student": student,
                "riro_id": login_id,
            }

    elem_fix_values = _extract_by_class(raw_html, "elem_fix")
    if elem_fix_values and len(disabled_values) >= 2:
        elem = elem_fix_values[0]
        digits = "".join(ch for ch in elem if ch.isdigit())
        riro_id = digits[:8] if len(digits) >= 8 else login_id
        student = elem[15:-1].strip() if len(elem) > 16 else elem.strip()
        name = disabled_values[0]
        student_number = _normalize_student_number(disabled_values[1])
        generation = _safe_generation(riro_id)
        if name and student_number and student and generation > 0:
            return {
                "name": name,
                "student_number": student_number,
                "generation": generation,
                "student": student,
                "riro_id": riro_id,
            }

    return None


def _looks_like_error_page(raw_text: str) -> bool:
    normalized = (raw_text or "").lower()
    return any(
        marker in normalized
        for marker in (
            "<title>에러페이지</title>",
            "403.jpg",
            "페이지를 찾을 수 없습니다.",
            "error_wrap",
        )
    )


def _is_access_blocked(status_code: int, raw_text: str) -> bool:
    return status_code == 403 or _looks_like_error_page(raw_text)


def check_riro_login(user_id: str, user_pw: str, max_retries: int = 5, sleep_seconds: int = 2) -> RiroAuthResult:
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
    }

    last_message = "인증 서버와 통신 중 오류가 발생했습니다."

    for _ in range(max_retries):
        session = requests.Session()
        try:
            try:
                session.post("https://iscience.riroschool.kr/user.php?action=user_logout", timeout=10)
            except requests.RequestException:
                pass

            login_resp = session.post(
                "https://iscience.riroschool.kr/ajax.php",
                headers=headers,
                data={
                    "app": "user",
                    "mode": "login",
                    "userType": "1",
                    "id": user_id,
                    "pw": user_pw,
                    "deeplink": "",
                    "redirect_link": "",
                },
                timeout=15,
            )
            if _is_access_blocked(login_resp.status_code, login_resp.text):
                return RiroAuthResult(status="error", message=_RIRO_BLOCK_MESSAGE)
            try:
                login_json = login_resp.json()
            except ValueError:
                if _looks_like_error_page(login_resp.text):
                    return RiroAuthResult(status="error", message=_RIRO_BLOCK_MESSAGE)
                return RiroAuthResult(status="error", message="인증 서버에서 잘못된 응답을 받았습니다.")

            code = str(login_json.get("code"))
            if code == "902":
                return RiroAuthResult(status="error", message="아이디 또는 비밀번호가 틀렸습니다.")
            if code != "000":
                return RiroAuthResult(status="error", message=f"로그인 실패 code={code}")

            token = login_json.get("token")
            if not token:
                return RiroAuthResult(status="error", message="토큰을 받지 못했습니다.")

            profile_resp = session.post(
                "https://iscience.riroschool.kr/user.php",
                headers=headers,
                data={"pw": user_pw},
                cookies={"cookie_token": token},
                allow_redirects=False,
                timeout=15,
            )
            if _is_access_blocked(profile_resp.status_code, profile_resp.text):
                return RiroAuthResult(status="error", message=_RIRO_BLOCK_MESSAGE)
            profile = _extract_profile_from_html(profile_resp.text, user_id)
            if profile:
                return RiroAuthResult(status="success", **profile)
            last_message = "로그인에는 성공했지만 사용자 정보를 확인하지 못했습니다. 잠시 후 다시 시도하세요."
        except requests.Timeout:
            last_message = _RIRO_TIMEOUT_MESSAGE
        except requests.RequestException:
            last_message = "인증 서버와 통신 중 오류가 발생했습니다."

        time.sleep(sleep_seconds)

    return RiroAuthResult(status="error", message=last_message)
