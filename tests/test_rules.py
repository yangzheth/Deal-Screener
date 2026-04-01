from __future__ import annotations

from datetime import date, datetime, timezone
import unittest

from market_intel_watch.extractors.rules import RuleBasedSignalExtractor
from market_intel_watch.models import SourceDocument, WatchEntity


class RuleBasedSignalExtractorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.extractor = RuleBasedSignalExtractor(
            entities=[
                WatchEntity(name="Anthropic", aliases=["Anthropic"], entity_type="company", geography="US", priority=3, tags=["ai"]),
                WatchEntity(name="OpenAI", aliases=["OpenAI"], entity_type="company", geography="US", priority=3, tags=["ai"]),
                WatchEntity(name="xAI", aliases=["xAI", "x.ai"], entity_type="company", geography="US", priority=3, tags=["ai"]),
            ],
            ai_keywords=["ai agent", "artificial intelligence", "ai startup"],
            source_weights={"news": 8, "manual_drop": 14},
            run_date=date(2026, 3, 31),
        )

    def test_left_details_is_not_treated_as_departure(self) -> None:
        document = SourceDocument(
            source_id="test-news",
            channel="news",
            title="Anthropic left details of an unreleased model in a public database",
            url="https://example.com/anthropic-left-details",
            published_at=datetime(2026, 3, 30, 9, 0, tzinfo=timezone.utc),
            summary="A security story about exposed model details.",
        )

        signals = self.extractor.extract(document)

        self.assertEqual([], [signal.event_type for signal in signals])

    def test_joins_funding_is_only_treated_as_funding(self) -> None:
        document = SourceDocument(
            source_id="test-news",
            channel="news",
            title="OpenAI joins funding for Isara's $94M raise to develop AI agent swarms",
            url="https://example.com/openai-funding",
            published_at=datetime(2026, 3, 30, 12, 0, tzinfo=timezone.utc),
            summary="Funding announcement for an AI agent company.",
        )

        signals = self.extractor.extract(document)

        self.assertEqual(["funding"], [signal.event_type for signal in signals])

    def test_leave_with_company_context_is_kept_as_departure(self) -> None:
        document = SourceDocument(
            source_id="test-news",
            channel="news",
            title="Ross Nordeen leaves xAI after helping launch the company",
            url="https://example.com/xai-departure",
            published_at=datetime(2026, 3, 30, 15, 0, tzinfo=timezone.utc),
            summary="The last xAI co-founder leaves the company.",
        )

        signals = self.extractor.extract(document)

        self.assertIn("talent_departure", [signal.event_type for signal in signals])

    def test_join_with_role_context_is_kept_as_hire(self) -> None:
        document = SourceDocument(
            source_id="manual-drop",
            channel="wechat",
            title="Former OpenAI researcher joins a startup as CTO",
            url="https://example.com/openai-hire",
            published_at=datetime(2026, 3, 30, 8, 0, tzinfo=timezone.utc),
            summary="A senior researcher moved into a startup leadership role.",
            content="A former OpenAI researcher joined a startup as CTO after leaving the company.",
        )

        signals = self.extractor.extract(document)

        self.assertIn("talent_hire", [signal.event_type for signal in signals])


if __name__ == "__main__":
    unittest.main()
