from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

from market_intel_watch.delivery import build_deliveries
from market_intel_watch.delivery.notion import NotionDatabaseDelivery, REQUIRED_PROPERTY_TYPES
from market_intel_watch.models import DailyRunResult, Signal


def build_result() -> DailyRunResult:
    return DailyRunResult(
        run_date=datetime(2026, 3, 31, 0, 0, tzinfo=timezone.utc),
        documents_fetched=10,
        documents_deduped=8,
        signals=[
            Signal(
                event_type="funding",
                title="OpenAI joins a $94M round",
                summary="Funding signal summary.",
                url="https://example.com/openai-round",
                source_id="google-news-us-funding",
                channel="news",
                published_at=datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc),
                matched_entities=["OpenAI"],
                geography="US",
                score=98.0,
                rationale=["event=funding", "market=US"],
            )
        ],
        errors=[],
        report_text="report",
    )


def build_schema() -> dict:
    delivery = NotionDatabaseDelivery({"id": "x", "data_source_id": "collection://demo", "type": "notion_database"})
    properties = {}
    for key, value_type in REQUIRED_PROPERTY_TYPES.items():
        properties[delivery.properties[key]] = {"type": value_type}
    return {"properties": properties}


def build_existing_page(page_id: str, signal_key: str) -> dict:
    return {
        "id": page_id,
        "properties": {
            "Signal Key": {
                "rich_text": [
                    {
                        "plain_text": signal_key,
                    }
                ]
            }
        },
    }


class NotionDatabaseDeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = {
            "id": "notion-market-watch",
            "type": "notion_database",
            "data_source_id": "collection://a1f37ba1-0e98-457e-82f1-db47ec20ab17",
            "auth_token_env": "NOTION_API_TOKEN",
            "upsert": True,
        }
        self.result = build_result()
        self.output_path = Path("output/report.md")

    def test_build_deliveries_supports_notion_database(self) -> None:
        deliveries = build_deliveries([self.config])
        self.assertEqual(1, len(deliveries))
        self.assertIsInstance(deliveries[0], NotionDatabaseDelivery)

    def test_create_page_when_signal_is_new(self) -> None:
        delivery = NotionDatabaseDelivery(self.config)
        calls: list[tuple[str, str, dict | None]] = []

        def fake_request(instance: NotionDatabaseDelivery, method: str, path: str, body: dict | None = None) -> dict:
            del instance
            calls.append((method, path, body))
            if method == "GET":
                return build_schema()
            if path.endswith("/query"):
                return {"results": [], "has_more": False}
            if path == "/v1/pages":
                return {"id": "page-new"}
            raise AssertionError(f"Unexpected request: {method} {path}")

        with patch.object(NotionDatabaseDelivery, "_request_json", autospec=True, side_effect=fake_request):
            delivery.deliver(self.result, self.output_path)

        create_call = calls[-1]
        self.assertEqual("POST", create_call[0])
        self.assertEqual("/v1/pages", create_call[1])
        self.assertEqual(
            "a1f37ba1-0e98-457e-82f1-db47ec20ab17",
            create_call[2]["parent"]["data_source_id"],
        )
        self.assertEqual(
            "Funding",
            create_call[2]["properties"]["Event Type"]["select"]["name"],
        )

    def test_update_page_when_signal_already_exists(self) -> None:
        delivery = NotionDatabaseDelivery(self.config)
        calls: list[tuple[str, str, dict | None]] = []
        signal_key = self.result.signals[0].stable_key()

        def fake_request(instance: NotionDatabaseDelivery, method: str, path: str, body: dict | None = None) -> dict:
            del instance
            calls.append((method, path, body))
            if method == "GET":
                return build_schema()
            if path.endswith("/query"):
                return {"results": [build_existing_page("page-existing", signal_key)], "has_more": False}
            if path == "/v1/pages/page-existing":
                return {"id": "page-existing"}
            raise AssertionError(f"Unexpected request: {method} {path}")

        with patch.object(NotionDatabaseDelivery, "_request_json", autospec=True, side_effect=fake_request):
            delivery.deliver(self.result, self.output_path)

        update_call = calls[-1]
        self.assertEqual("PATCH", update_call[0])
        self.assertEqual("/v1/pages/page-existing", update_call[1])
        self.assertIn("properties", update_call[2])

    def test_archives_stale_pages_that_are_no_longer_in_latest_run(self) -> None:
        delivery = NotionDatabaseDelivery(self.config)
        calls: list[tuple[str, str, dict | None]] = []
        current_key = self.result.signals[0].stable_key()

        def fake_request(instance: NotionDatabaseDelivery, method: str, path: str, body: dict | None = None) -> dict:
            del instance
            calls.append((method, path, body))
            if method == "GET":
                return build_schema()
            if path.endswith("/query"):
                return {
                    "results": [
                        build_existing_page("page-current", current_key),
                        build_existing_page("page-stale", "stale-key"),
                    ],
                    "has_more": False,
                }
            if path == "/v1/pages/page-current":
                return {"id": "page-current"}
            if path == "/v1/pages/page-stale":
                return {"id": "page-stale"}
            raise AssertionError(f"Unexpected request: {method} {path}")

        with patch.object(NotionDatabaseDelivery, "_request_json", autospec=True, side_effect=fake_request):
            delivery.deliver(self.result, self.output_path)

        archive_call = calls[-1]
        self.assertEqual("PATCH", archive_call[0])
        self.assertEqual("/v1/pages/page-stale", archive_call[1])
        self.assertEqual({"archived": True}, archive_call[2])

    def test_curl_fallback_parses_success_response(self) -> None:
        delivery = NotionDatabaseDelivery(self.config)
        with patch.dict(os.environ, {"NOTION_API_TOKEN": "token"}, clear=False):
            with patch("market_intel_watch.delivery.notion.shutil.which", return_value="curl.exe"):
                with patch(
                    "market_intel_watch.delivery.notion.subprocess.run",
                    return_value=subprocess.CompletedProcess(
                        args=["curl.exe"],
                        returncode=0,
                        stdout='{"results": []}\n200',
                        stderr="",
                    ),
                ):
                    response = delivery._request_json_with_curl(
                        "POST",
                        "/v1/data_sources/demo/query",
                        {"page_size": 1},
                        "token",
                        RuntimeError("ssl failed"),
                    )

        self.assertEqual([], response["results"])


if __name__ == "__main__":
    unittest.main()
