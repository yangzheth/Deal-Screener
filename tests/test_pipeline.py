from __future__ import annotations

from datetime import date, datetime, timezone
import unittest

from market_intel_watch.models import SourceDocument
from market_intel_watch.pipeline import dedupe_documents, filter_recent_documents


class PipelineTests(unittest.TestCase):
    def test_filter_recent_documents_excludes_future_items(self) -> None:
        documents = [
            SourceDocument(
                source_id="test",
                channel="news",
                title="Recent item",
                url="https://example.com/recent",
                published_at=datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc),
            ),
            SourceDocument(
                source_id="test",
                channel="news",
                title="Future item",
                url="https://example.com/future",
                published_at=datetime(2026, 4, 1, 10, 0, tzinfo=timezone.utc),
            ),
            SourceDocument(
                source_id="test",
                channel="news",
                title="Undated item",
                url="https://example.com/undated",
                published_at=None,
            ),
        ]

        filtered = filter_recent_documents(documents, run_date=date(2026, 3, 31), max_age_days=7)

        self.assertEqual(["Recent item", "Undated item"], [document.title for document in filtered])

    def test_dedupe_documents_keeps_richer_duplicate(self) -> None:
        documents = [
            SourceDocument(
                source_id="test",
                channel="news",
                title="Signal title",
                url="https://example.com/duplicate",
                published_at=datetime(2026, 3, 30, 8, 0, tzinfo=timezone.utc),
                summary="Short summary",
            ),
            SourceDocument(
                source_id="test",
                channel="news",
                title="Signal title",
                url="https://example.com/duplicate",
                published_at=datetime(2026, 3, 30, 9, 0, tzinfo=timezone.utc),
                summary="Longer summary with more useful context",
                content="Expanded details from a richer source.",
            ),
        ]

        deduped = dedupe_documents(documents)

        self.assertEqual(1, len(deduped))
        self.assertEqual("Expanded details from a richer source.", deduped[0].content)


if __name__ == "__main__":
    unittest.main()
