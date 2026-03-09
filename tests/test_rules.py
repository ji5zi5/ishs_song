from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from radio_app.db import DB, utc_now_iso
from radio_app.services.rounds import enforce_rate_limit


class RulesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = DB(path=self.root / "test.db")
        self.db.init_schema()
        with self.db.session() as conn:
            conn.execute(
                "INSERT INTO users(riro_user_key, display_name, is_admin_approved, created_at) VALUES ('u1', 'u1', 0, ?)",
                (utc_now_iso(),),
            )
            self.user_id = int(conn.execute("SELECT id FROM users WHERE riro_user_key = 'u1'").fetchone()["id"])

    def test_rate_limit_blocks_after_threshold(self) -> None:
        with self.db.session() as conn:
            self.assertTrue(enforce_rate_limit(conn, self.user_id, "submit", max_count=2, window_seconds=60))
            self.assertTrue(enforce_rate_limit(conn, self.user_id, "submit", max_count=2, window_seconds=60))
            self.assertFalse(enforce_rate_limit(conn, self.user_id, "submit", max_count=2, window_seconds=60))


if __name__ == "__main__":
    unittest.main()
