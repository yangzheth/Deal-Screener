from __future__ import annotations

from datetime import date, datetime, timezone
import unittest

from market_intel_watch.models import Signal
from market_intel_watch.reporting.markdown import _dedupe_urgent, render_markdown_report


def build_signal(**overrides: object) -> Signal:
    defaults: dict[str, object] = {
        "event_type": "funding",
        "title": "Moonshot AI completes strategic round",
        "summary": "Summary.",
        "url": "https://example.com/moonshot",
        "source_id": "manual-drop",
        "channel": "manual",
        "published_at": datetime(2026, 3, 30, tzinfo=timezone.utc),
        "matched_entities": ["Moonshot AI"],
        "geography": "CN",
        "score": 90.0,
        "company_name": "Moonshot AI",
        "follow_verdict": "Must Chase",
        "suggested_action": "Follow up today.",
    }
    defaults.update(overrides)
    return Signal(**defaults)  # type: ignore[arg-type]


class DedupeUrgentTests(unittest.TestCase):
    def test_same_url_merged_with_combined_event_types(self) -> None:
        a = build_signal(event_type="funding", score=92.0)
        b = build_signal(event_type="talent_departure", score=88.0)
        grouped = _dedupe_urgent([a, b])
        self.assertEqual(1, len(grouped))
        signal, events = grouped[0]
        self.assertEqual(["funding", "talent_departure"], events)
        self.assertEqual(92.0, signal.score)

    def test_different_urls_not_merged(self) -> None:
        a = build_signal(url="https://example.com/a")
        b = build_signal(url="https://example.com/b")
        self.assertEqual(2, len(_dedupe_urgent([a, b])))

    def test_empty_url_falls_back_to_title(self) -> None:
        a = build_signal(url="", title="Shared title", event_type="funding")
        b = build_signal(url="", title="Shared title", event_type="talent_hire")
        grouped = _dedupe_urgent([a, b])
        self.assertEqual(1, len(grouped))
        self.assertEqual(["funding", "talent_hire"], grouped[0][1])


class RenderMarkdownReportTests(unittest.TestCase):
    def test_urgent_section_dedupes_by_url(self) -> None:
        a = build_signal(event_type="funding", score=95.0)
        b = build_signal(event_type="talent_departure", score=90.0)
        report = render_markdown_report(
            run_date=date(2026, 4, 1),
            documents_fetched=2,
            documents_deduped=2,
            signals=[a, b],
            errors=[],
        )
        urgent_block = report.split("## Immediate Follow-Up", 1)[1].split("##", 1)[0]
        self.assertEqual(1, urgent_block.count("https://example.com/moonshot"))
        self.assertIn("Funding/Departure", urgent_block)


if __name__ == "__main__":
    unittest.main()
