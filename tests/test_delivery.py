from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from market_intel_watch.delivery import build_deliveries
from market_intel_watch.delivery.renderers import build_wecom_markdown
from market_intel_watch.delivery.webhook import WebhookDelivery
from market_intel_watch.delivery.wecom_bot import WeComBotDelivery
from market_intel_watch.models import DailyRunResult, Signal


def build_signal(**overrides: object) -> Signal:
    defaults: dict[str, object] = {
        "event_type": "funding",
        "title": "Isara raises $94M",
        "summary": "Funding summary.",
        "url": "https://example.com/isara",
        "source_id": "google-news-us-funding",
        "channel": "news",
        "published_at": datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc),
        "matched_entities": ["Isara"],
        "geography": "US",
        "score": 92.0,
        "company_name": "Isara",
        "amount": "$94M",
        "round_stage": "Series A",
        "investors": ["Sequoia"],
        "categories": ["Agent"],
        "cluster_key": "funding|isara|series-a",
        "follow_verdict": "Must Chase",
        "follow_reason": "High-conviction AI agent round.",
        "suggested_action": "Reach out to founder.",
        "confidence": 0.82,
        "source_count": 2,
    }
    defaults.update(overrides)
    return Signal(**defaults)  # type: ignore[arg-type]


def build_result(signals: list[Signal] | None = None, errors: list[str] | None = None) -> DailyRunResult:
    return DailyRunResult(
        run_date=datetime(2026, 3, 31, tzinfo=timezone.utc),
        documents_fetched=10,
        documents_deduped=8,
        signals=signals if signals is not None else [build_signal()],
        errors=errors or [],
        report_text="# Daily report body",
    )


class FakeResponse:
    def __init__(self, body: bytes = b"ok") -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class BuildDeliveriesTests(unittest.TestCase):
    def test_build_webhook_and_wecom(self) -> None:
        deliveries = build_deliveries(
            [
                {"id": "w1", "type": "webhook", "url": "https://example.com/hook"},
                {"id": "b1", "type": "wecom_bot", "url": "https://example.com/bot"},
            ]
        )
        self.assertEqual(2, len(deliveries))
        self.assertIsInstance(deliveries[0], WebhookDelivery)
        self.assertIsInstance(deliveries[1], WeComBotDelivery)

    def test_unsupported_type_raises(self) -> None:
        with self.assertRaises(ValueError):
            build_deliveries([{"id": "x", "type": "pigeon"}])


class SelectSignalsTests(unittest.TestCase):
    def test_min_score_filters_low_signals(self) -> None:
        delivery = WebhookDelivery({"id": "w", "type": "webhook", "url": "x", "min_score": 90})
        low = build_signal(score=80.0, cluster_key="low", url="https://e/low")
        high = build_signal(score=95.0, cluster_key="hi", url="https://e/hi")
        result = build_result(signals=[low, high])
        self.assertEqual([high], delivery.select_signals(result))

    def test_event_type_filter(self) -> None:
        delivery = WebhookDelivery(
            {"id": "w", "type": "webhook", "url": "x", "event_types": ["talent_departure"]}
        )
        funding = build_signal(event_type="funding", cluster_key="a", url="https://e/a")
        departure = build_signal(event_type="talent_departure", cluster_key="b", url="https://e/b")
        result = build_result(signals=[funding, departure])
        self.assertEqual([departure], delivery.select_signals(result))

    def test_max_items_truncates(self) -> None:
        delivery = WebhookDelivery({"id": "w", "type": "webhook", "url": "x", "max_items": 1})
        a = build_signal(cluster_key="a", url="https://e/a")
        b = build_signal(cluster_key="b", url="https://e/b")
        result = build_result(signals=[a, b])
        self.assertEqual([a], delivery.select_signals(result))


class WebhookDeliveryTests(unittest.TestCase):
    def test_deliver_posts_json_payload(self) -> None:
        delivery = WebhookDelivery({"id": "w", "type": "webhook", "url": "https://example.com/hook"})
        result = build_result()
        captured: dict[str, object] = {}

        def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
            captured["url"] = request.full_url
            captured["method"] = request.get_method()
            captured["headers"] = dict(request.header_items())
            captured["body"] = request.data
            captured["timeout"] = timeout
            return FakeResponse()

        with patch("market_intel_watch.delivery.webhook.urlopen", side_effect=fake_urlopen):
            delivery.deliver(result, Path("output/report.md"))

        self.assertEqual("https://example.com/hook", captured["url"])
        self.assertEqual("POST", captured["method"])
        self.assertEqual(20, captured["timeout"])
        payload = json.loads(captured["body"])
        self.assertEqual("AI Primary Market Watch - 2026-03-31", payload["title"])
        self.assertEqual("# Daily report body", payload["text"])
        self.assertEqual(1, payload["signals_detected"])
        self.assertEqual("Isara", payload["signals"][0]["company_name"])
        self.assertEqual("$94M", payload["signals"][0]["amount"])

    def test_deliver_respects_custom_headers(self) -> None:
        delivery = WebhookDelivery(
            {
                "id": "w",
                "type": "webhook",
                "url": "https://example.com/hook",
                "headers": {"Content-Type": "application/json", "X-Token": "abc"},
            }
        )
        captured: dict[str, object] = {}

        def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
            captured["headers"] = dict(request.header_items())
            return FakeResponse()

        with patch("market_intel_watch.delivery.webhook.urlopen", side_effect=fake_urlopen):
            delivery.deliver(build_result(), Path("output/report.md"))

        self.assertEqual("abc", captured["headers"]["X-token"])


class WeComBotDeliveryTests(unittest.TestCase):
    def test_build_payload_contains_markdown(self) -> None:
        delivery = WeComBotDelivery({"id": "b", "type": "wecom_bot", "url": "https://example.com/bot"})
        payload = delivery.build_payload(build_result())
        self.assertEqual("markdown", payload["msgtype"])
        content = payload["markdown"]["content"]
        self.assertIn("AI Primary Market Watch 2026-03-31", content)
        self.assertIn("Must Chase", content)
        self.assertIn("Isara", content)

    def test_deliver_posts_payload(self) -> None:
        delivery = WeComBotDelivery({"id": "b", "type": "wecom_bot", "url": "https://example.com/bot"})
        captured: dict[str, object] = {}

        def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
            captured["url"] = request.full_url
            captured["body"] = request.data
            captured["timeout"] = timeout
            return FakeResponse()

        with patch("market_intel_watch.delivery.wecom_bot.urlopen", side_effect=fake_urlopen):
            delivery.deliver(build_result(), Path("output/report.md"))

        self.assertEqual("https://example.com/bot", captured["url"])
        self.assertEqual(20, captured["timeout"])
        body = json.loads(captured["body"])
        self.assertEqual("markdown", body["msgtype"])
        self.assertIn("AI Primary Market Watch", body["markdown"]["content"])


class BuildWecomMarkdownTests(unittest.TestCase):
    def test_no_signals_shows_fallback_line(self) -> None:
        content = build_wecom_markdown(
            signals=[],
            run_date="2026-03-31",
            documents_fetched=5,
            documents_deduped=4,
            errors=[],
        )
        self.assertIn("今日没有符合阈值", content)

    def test_errors_appear_in_alert_section(self) -> None:
        content = build_wecom_markdown(
            signals=[build_signal()],
            run_date="2026-03-31",
            documents_fetched=5,
            documents_deduped=4,
            errors=["src-a: timeout", "src-b: 403", "src-c: parse", "src-d: reset"],
        )
        self.assertIn("## 数据源告警", content)
        self.assertIn("src-a: timeout", content)
        self.assertIn("另外还有 1 条告警已省略", content)

    def test_content_is_truncated_to_max_bytes(self) -> None:
        many_signals = [
            build_signal(cluster_key=f"k-{i}", url=f"https://e/{i}", title=f"Signal number {i}")
            for i in range(30)
        ]
        content = build_wecom_markdown(
            signals=many_signals,
            run_date="2026-03-31",
            documents_fetched=100,
            documents_deduped=80,
            errors=[],
            max_items=30,
            max_bytes=400,
        )
        self.assertLessEqual(len(content.encode("utf-8")), 400)
        self.assertIn("消息过长", content)


if __name__ == "__main__":
    unittest.main()
