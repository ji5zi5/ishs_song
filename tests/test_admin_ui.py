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


if __name__ == "__main__":
    unittest.main()
