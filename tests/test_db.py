from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from radio_app.db import DB


class DBMigrationTest(unittest.TestCase):
    def test_init_schema_adds_round_close_tracking_columns_to_legacy_db(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "legacy.db"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE rounds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cadence TEXT NOT NULL,
                    status TEXT NOT NULL,
                    start_at TEXT NOT NULL,
                    end_at TEXT NOT NULL,
                    playlist_size INTEGER NOT NULL DEFAULT 12,
                    target_seconds INTEGER NOT NULL DEFAULT 2400,
                    loudnorm_enabled INTEGER NOT NULL DEFAULT 1,
                    close_job_key TEXT,
                    created_at TEXT NOT NULL,
                    closed_at TEXT
                );
                """
            )
            conn.commit()
            conn.close()

            db = DB(path=db_path)
            db.init_schema()

            with db.session() as check_conn:
                columns = {str(row["name"]) for row in check_conn.execute("PRAGMA table_info(rounds)").fetchall()}

            self.assertTrue(
                {
                    "close_phase",
                    "close_message",
                    "close_progress",
                    "close_started_at",
                    "close_finished_at",
                    "close_error",
                }.issubset(columns)
            )


if __name__ == "__main__":
    unittest.main()
