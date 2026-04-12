from __future__ import annotations

from datetime import date, datetime, timezone
import unittest

from market_intel_watch.models import Signal, SourceDocument
from market_intel_watch.pipeline import cluster_signals, dedupe_documents, filter_recent_documents


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

    def test_cluster_signals_merges_multi_source_event(self) -> None:
        signals = [
            Signal(
                event_type="funding",
                title="Isara raises $94M for AI agent swarm software",
                summary="Primary source summary.",
                url="https://example.com/isara-1",
                source_id="source-a",
                channel="news",
                published_at=datetime(2026, 3, 30, 9, 0, tzinfo=timezone.utc),
                company_name="Isara",
                amount="$94M",
                categories=["Agent"],
                cluster_key="funding|isara|94m",
                score=90.0,
                supporting_urls=["https://example.com/isara-1"],
                confidence=0.72,
            ),
            Signal(
                event_type="funding",
                title="Isara secures $94M for agent infrastructure",
                summary="Second source summary.",
                url="https://example.com/isara-2",
                source_id="source-b",
                channel="rss",
                published_at=datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc),
                company_name="Isara",
                amount="$94M",
                categories=["Agent", "Infra"],
                cluster_key="funding|isara|94m",
                score=88.0,
                supporting_urls=["https://example.com/isara-2"],
                confidence=0.75,
            ),
        ]

        clustered = cluster_signals(signals)

        self.assertEqual(1, len(clustered))
        self.assertEqual(2, clustered[0].source_count)
        self.assertIn("Infra", clustered[0].categories)
        self.assertEqual(2, len(clustered[0].supporting_urls))
        self.assertGreater(clustered[0].score, 90.0)


if __name__ == "__main__":
    unittest.main()
