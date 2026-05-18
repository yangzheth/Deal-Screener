from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3


class DedupStore:
    """SQLite-backed seen-event store with a rolling TTL.

    The dedup key is ``(source, content_hash)`` as produced by
    ``RawEvent.dedup_key``. Rows older than ``ttl_days`` are pruned on open so
    a long-dormant story can resurface instead of being suppressed forever.
    """

    def __init__(self, db_path: str | Path, ttl_days: int = 14) -> None:
        self.db_path = str(db_path)
        self.ttl_days = ttl_days
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_events (
                dedup_key TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL
            )
            """
        )
        self._conn.commit()
        self.prune()

    def prune(self) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=self.ttl_days)).isoformat()
        cursor = self._conn.execute("DELETE FROM seen_events WHERE last_seen < ?", (cutoff,))
        self._conn.commit()
        return cursor.rowcount

    def is_new(self, dedup_key: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM seen_events WHERE dedup_key = ?", (dedup_key,)
        ).fetchone()
        return row is None

    def mark_seen(self, dedup_key: str, source: str, content_hash: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO seen_events (dedup_key, source, content_hash, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(dedup_key) DO UPDATE SET last_seen = excluded.last_seen
            """,
            (dedup_key, source, content_hash, now, now),
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "DedupStore":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
