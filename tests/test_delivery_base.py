from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import unittest

from market_intel_watch.delivery.base import DeliveryChannel
from market_intel_watch.models import DailyRunResult, Signal


class FakeDelivery(DeliveryChannel):
    def deliver(self, result: DailyRunResult, output_path: Path) -> None:
        del result, output_path


def build_signal(
    event_type: str,
    title: str,
    *,
    follow_verdict: str,
    company_name: str = "",
    key_people: list[str] | None = None,
    amount: str = "",
    round_stage: str = "",
    investors: list[str] | None = None,
    score: float = 90.0,
) -> Signal:
    return Signal(
        event_type=event_type,
        title=title,
        summary="summary",
        url=f"https://example.com/{title.replace(' ', '-').lower()}",
        source_id="test",
        channel="news",
        published_at=datetime(2026, 3, 30, tzinfo=timezone.utc),
        score=score,
        follow_verdict=follow_verdict,
        company_name=company_name,
        key_people=key_people or [],
        amount=amount,
        round_stage=round_stage,
        investors=investors or [],
    )


class DeliveryChannelTests(unittest.TestCase):
    def test_select_signals_can_keep_only_actionable_high_verdict_leads(self) -> None:
        result = DailyRunResult(
            run_date=datetime(2026, 3, 31, tzinfo=timezone.utc),
            documents_fetched=4,
            documents_deduped=4,
            signals=[
                build_signal(
                    "funding",
                    "Specific Deal",
                    follow_verdict="Must Chase",
                    company_name="Isara",
                    amount="$94M",
                ),
                build_signal(
                    "funding",
                    "Macro Funding Report",
                    follow_verdict="Worth Tracking",
                    company_name="",
                ),
                build_signal(
                    "talent_hire",
                    "Named Person Move",
                    follow_verdict="Worth Tracking",
                    company_name="Anthropic",
                    key_people=["Jane Doe"],
                ),
                build_signal(
                    "talent_hire",
                    "Role Only Move",
                    follow_verdict="Monitor",
                    company_name="Anthropic",
                ),
            ],
            errors=[],
            report_text="report",
        )
        delivery = FakeDelivery(
            {
                "id": "test",
                "follow_verdicts": ["Must Chase", "Worth Tracking"],
                "require_actionable": True,
            }
        )

        selected = delivery.select_signals(result)

        self.assertEqual(["Specific Deal", "Named Person Move"], [signal.title for signal in selected])


if __name__ == "__main__":
    unittest.main()
