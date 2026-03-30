from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class AdminUiTest(unittest.TestCase):
    def test_admin_audit_panel_uses_internal_scroll_container(self) -> None:
        html = (ROOT / "src" / "radio_app" / "static" / "admin.html").read_text(encoding="utf-8")

        self.assertIn('class="panel audit-panel"', html)
        self.assertIn('class="list audit-list"', html)
        self.assertIn(".audit-list {", html)

    def test_admin_page_contains_close_progress_controls(self) -> None:
        html = (ROOT / "src" / "radio_app" / "static" / "admin.html").read_text(encoding="utf-8")

        self.assertIn('id="closeButton"', html)
        self.assertIn('id="closeProgress"', html)
        self.assertIn('id="closeProgressBar"', html)
        self.assertIn('id="closePhase"', html)
        self.assertIn('id="closeHint"', html)

    def test_admin_page_uses_round_wording_instead_of_month_close(self) -> None:
        html = (ROOT / "src" / "radio_app" / "static" / "admin.html").read_text(encoding="utf-8")

        self.assertIn("회차 마감", html)
        self.assertIn("대상 회차", html)
        self.assertNotIn("월 마감", html)
        self.assertNotIn("운영 메모", html)

    def test_admin_page_uses_time_based_settings_without_song_count_field(self) -> None:
        html = (ROOT / "src" / "radio_app" / "static" / "admin.html").read_text(encoding="utf-8")

        self.assertNotIn('id="playlistSize"', html)
        self.assertNotIn("기본 곡 수", html)
        self.assertIn("목표 초", html)

    def test_index_page_uses_round_wording_for_live_rank_title(self) -> None:
        html = (ROOT / "src" / "radio_app" / "static" / "index.html").read_text(encoding="utf-8")

        self.assertIn("이번 회차 실시간 순위", html)
        self.assertNotIn("이번 달 실시간 순위", html)

    def test_admin_page_gates_revoke_controls_by_super_admin_flag(self) -> None:
        html = (ROOT / "src" / "radio_app" / "static" / "admin.html").read_text(encoding="utf-8")

        self.assertIn("let currentUserIsSuperAdmin = false;", html)
        self.assertIn("currentUserIsSuperAdmin = Boolean(data.user.is_super_admin);", html)
        self.assertIn("const canToggleAdmin = !user.is_admin_approved || (currentUserIsSuperAdmin && !user.is_super_admin);", html)

    def test_submit_and_vote_pages_expose_sort_controls(self) -> None:
        submit_html = (ROOT / "src" / "radio_app" / "static" / "submit.html").read_text(encoding="utf-8")
        vote_html = (ROOT / "src" / "radio_app" / "static" / "vote.html").read_text(encoding="utf-8")

        self.assertIn('id="songsSort"', submit_html)
        self.assertIn('value="popular">인기순', submit_html)
        self.assertIn('value="recent">등록순', submit_html)
        self.assertIn('/api/public/songs?sort=${encodeURIComponent(sort)}', submit_html)

        self.assertIn('id="songsSort"', vote_html)
        self.assertIn('value="popular">인기순', vote_html)
        self.assertIn('value="recent">등록순', vote_html)
        self.assertIn('/api/public/songs?sort=${encodeURIComponent(currentSongsSort())}', vote_html)

    def test_admin_page_exposes_yt_dlp_maintenance_controls(self) -> None:
        html = (ROOT / "src" / "radio_app" / "static" / "admin.html").read_text(encoding="utf-8")

        self.assertIn('id="maintenancePanel"', html)
        self.assertIn("yt-dlp 업데이트", html)
        self.assertIn("최신 버전", html)
        self.assertIn("업데이트 필요", html)
        self.assertIn('/api/admin/maintenance/yt-dlp', html)
        self.assertIn('/api/admin/maintenance/yt-dlp/update', html)
        self.assertIn("관리자면 서버 가상환경의 `yt-dlp` 버전을 확인하고 업데이트할 수 있습니다.", html)
        self.assertIn('document.getElementById("maintenancePanel").hidden = false;', html)
        self.assertIn("let maintenanceCapabilities = {", html)
        self.assertIn("updateEnabled: true", html)

    def test_admin_page_exposes_manual_youtube_download_controls(self) -> None:
        html = (ROOT / "src" / "radio_app" / "static" / "admin.html").read_text(encoding="utf-8")

        self.assertIn("유튜브 링크 직접 다운로드", html)
        self.assertIn('id="manualYoutubeUrl"', html)
        self.assertIn('id="manualYoutubeDownloadBtn"', html)
        self.assertIn('/api/admin/manual-downloads', html)
        self.assertIn('/api/admin/manual-downloads/youtube', html)
        self.assertIn('/api/admin/manual-downloads/download', html)
        self.assertIn("manualDownloadEnabled", html)
        self.assertIn("유튜브 직접 다운로드가 비활성화되어 있습니다.", html)

    def test_admin_page_exposes_individual_track_download_controls(self) -> None:
        html = (ROOT / "src" / "radio_app" / "static" / "admin.html").read_text(encoding="utf-8")

        self.assertIn("선정곡 개별 다운로드", html)
        self.assertIn('data-action="download-track"', html)
        self.assertIn('/api/admin/artifacts/download-track', html)

    def test_all_pages_include_shared_footer_credit(self) -> None:
        for page in ("index.html", "submit.html", "vote.html", "admin.html"):
            html = (ROOT / "src" / "radio_app" / "static" / page).read_text(encoding="utf-8")
            self.assertIn('class="site-footer"', html)
            self.assertIn("ISHS 32nd 엄지오", html)


if __name__ == "__main__":
    unittest.main()
