from __future__ import annotations

from datetime import date, datetime, timezone
import os
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

from market_intel_watch.delivery.email_smtp import SMTPEmailDelivery
from market_intel_watch.models import DailyRunResult, Signal
from market_intel_watch.reporting.email_digest import (
    rank_for_digest,
    render_email_html,
    render_email_subject,
    render_email_text,
)


def build_signal(
    *,
    llm_score: float | None = None,
    llm_tldr: str = "",
    llm_reason: str = "",
    **overrides: object,
) -> Signal:
    defaults: dict[str, object] = {
        "event_type": "funding",
        "title": "Moonshot raises Series B",
        "summary": "Primary summary.",
        "url": "https://example.com/moonshot",
        "source_id": "google-news-cn-funding",
        "channel": "news",
        "published_at": datetime(2026, 3, 30, tzinfo=timezone.utc),
        "matched_entities": ["Moonshot AI"],
        "geography": "CN",
        "score": 90.0,
        "company_name": "Moonshot AI",
        "amount": "$250M",
        "follow_verdict": "Must Chase",
    }
    defaults.update(overrides)
    signal = Signal(**defaults)  # type: ignore[arg-type]
    metadata: dict[str, str] = {}
    if llm_score is not None:
        metadata["llm_score"] = f"{llm_score:.1f}"
    if llm_tldr:
        metadata["llm_tldr"] = llm_tldr
    if llm_reason:
        metadata["llm_reason"] = llm_reason
    if metadata:
        signal.metadata = {**signal.metadata, **metadata}
    return signal


class RankForDigestTests(unittest.TestCase):
    def test_filters_below_min_llm_score(self) -> None:
        high = build_signal(llm_score=9.0, llm_tldr="T1", cluster_key="a", url="https://e/a")
        low = build_signal(llm_score=5.0, llm_tldr="T2", cluster_key="b", url="https://e/b")
        items = rank_for_digest([high, low], top_n=5, min_llm_score=7.0)
        self.assertEqual(1, len(items))
        self.assertEqual("T1", items[0].tldr)

    def test_ranks_by_llm_score_desc(self) -> None:
        a = build_signal(llm_score=7.5, cluster_key="a", url="https://e/a")
        b = build_signal(llm_score=9.5, cluster_key="b", url="https://e/b")
        items = rank_for_digest([a, b], min_llm_score=7.0)
        self.assertEqual("https://e/b", items[0].signal.url)

    def test_falls_back_to_rule_score_when_no_llm(self) -> None:
        signal = build_signal(score=95.0)
        items = rank_for_digest([signal], min_llm_score=7.0)
        self.assertEqual(1, len(items))
        self.assertAlmostEqual(9.5, items[0].llm_score, places=1)

    def test_top_n_caps_output(self) -> None:
        signals = [
            build_signal(llm_score=9.0, cluster_key=f"k{i}", url=f"https://e/{i}")
            for i in range(10)
        ]
        items = rank_for_digest(signals, top_n=3, min_llm_score=7.0)
        self.assertEqual(3, len(items))
        self.assertEqual([1, 2, 3], [item.rank for item in items])


class RendererTests(unittest.TestCase):
    def _items(self):
        return rank_for_digest(
            [
                build_signal(
                    llm_score=9.0,
                    llm_tldr="Moonshot 拿下 $250M Series B",
                    llm_reason="watchlist 命中，CN 市场头部。",
                )
            ],
            min_llm_score=7.0,
        )

    def test_subject_includes_entity_and_count(self) -> None:
        subject = render_email_subject(date(2026, 4, 1), self._items())
        self.assertIn("2026-04-01", subject)
        self.assertIn("Moonshot AI", subject)
        self.assertIn("1", subject)

    def test_subject_handles_empty(self) -> None:
        subject = render_email_subject(date(2026, 4, 1), [])
        self.assertIn("今日无高分信号", subject)

    def test_text_contains_tldr_reason_url(self) -> None:
        text = render_email_text(date(2026, 4, 1), self._items())
        self.assertIn("Moonshot 拿下 $250M Series B", text)
        self.assertIn("推荐理由：watchlist 命中", text)
        self.assertIn("https://example.com/moonshot", text)

    def test_html_escapes_and_includes_link(self) -> None:
        items = rank_for_digest(
            [
                build_signal(
                    llm_score=9.0,
                    llm_tldr="Moonshot & DeepSeek <update>",
                    llm_reason="值得 follow",
                    title="Title with <script>",
                )
            ],
            min_llm_score=7.0,
        )
        html = render_email_html(date(2026, 4, 1), items)
        self.assertIn("Moonshot &amp; DeepSeek &lt;update&gt;", html)
        self.assertIn("&lt;script&gt;", html)
        self.assertIn("https://example.com/moonshot", html)


def build_result(signals: list[Signal]) -> DailyRunResult:
    return DailyRunResult(
        run_date=datetime(2026, 4, 1, tzinfo=timezone.utc),
        documents_fetched=10,
        documents_deduped=8,
        signals=signals,
        errors=[],
        report_text="# body",
    )


class SMTPEmailDeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = {
            "id": "email-daily-digest",
            "type": "email_smtp",
            "smtp_host": "smtp.example.com",
            "smtp_port": 2525,
            "use_starttls": True,
            "username_env": "TEST_SMTP_USERNAME",
            "password_env": "TEST_SMTP_PASSWORD",
            "from_addr": "alerts@example.com",
            "from_name": "Watch",
            "to": ["reader@example.com"],
            "top_n": 5,
            "min_llm_score": 7.0,
        }

    def test_missing_credentials_raise(self) -> None:
        delivery = SMTPEmailDelivery(self.config)
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                delivery._load_credentials()
        self.assertIn("username", str(ctx.exception))
        self.assertIn("password", str(ctx.exception))

    def test_deliver_sends_message_via_smtp(self) -> None:
        delivery = SMTPEmailDelivery(self.config)
        signal = build_signal(llm_score=9.0, llm_tldr="TLDR", llm_reason="REASON")
        result = build_result([signal])

        fake_client = MagicMock()
        fake_client.__enter__.return_value = fake_client
        fake_client.__exit__.return_value = False

        env = {"TEST_SMTP_USERNAME": "user@example.com", "TEST_SMTP_PASSWORD": "app-password"}
        with patch.dict(os.environ, env, clear=False):
            with patch("market_intel_watch.delivery.email_smtp.smtplib.SMTP", return_value=fake_client) as smtp_ctor:
                delivery.deliver(result, Path("output/report.md"))

        smtp_ctor.assert_called_once_with("smtp.example.com", 2525, timeout=30)
        fake_client.starttls.assert_called_once()
        fake_client.login.assert_called_once_with("user@example.com", "app-password")
        fake_client.send_message.assert_called_once()

        sent_msg = fake_client.send_message.call_args[0][0]
        self.assertIn("AI Market Watch", sent_msg["Subject"])
        self.assertIn("reader@example.com", sent_msg["To"])

        body_parts = [part.get_content() for part in sent_msg.iter_parts()] if sent_msg.is_multipart() else [sent_msg.get_content()]
        combined = "\n".join(body_parts)
        self.assertIn("TLDR", combined)
        self.assertIn("REASON", combined)


if __name__ == "__main__":
    unittest.main()
