from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

from market_intel_watch.delivery import build_deliveries
from market_intel_watch.delivery.notion import (
    DEFAULT_RELATION_TARGETS,
    NotionDatabaseDelivery,
    OPTIONAL_PROPERTY_TYPES,
    REQUIRED_PROPERTY_TYPES,
)
from market_intel_watch.models import DailyRunResult, Signal


MAIN_DATA_SOURCE_ID = "a1f37ba1-0e98-457e-82f1-db47ec20ab17"
COMPANY_DATA_SOURCE_ID = "8a47f584-7a5b-4cba-9eb6-d6d6ca8789cc"
TRACKER_DATA_SOURCE_ID = "b7f972e1-ad2d-46e6-8b13-f298e99a3602"
PIPELINE_DATA_SOURCE_ID = "0b6c5021-4235-4b5d-b8bb-3f50da0c277b"


def build_result(follow_verdict: str = "Must Chase") -> DailyRunResult:
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
                company_name="Isara",
                amount="$94M",
                round_stage="Series A",
                investors=["OpenAI Ventures"],
                categories=["Agent"],
                cluster_key="funding|isara|series-a",
                follow_verdict=follow_verdict,
                follow_reason="Immediate follow-up.",
                suggested_action="Open the company record.",
                confidence=0.84,
                source_count=2,
            )
        ],
        errors=[],
        report_text="report",
    )


def build_main_schema(include_optional: bool = False, include_relations: bool = False) -> dict:
    delivery = NotionDatabaseDelivery({"id": "x", "data_source_id": "collection://demo", "type": "notion_database"})
    properties = {}
    for key, value_type in REQUIRED_PROPERTY_TYPES.items():
        properties[delivery.properties[key]] = {"type": value_type}
    if include_optional:
        for key, value_type in OPTIONAL_PROPERTY_TYPES.items():
            if value_type == "relation" and not include_relations:
                continue
            properties[delivery.properties[key]] = {"type": value_type}
    return {"properties": properties}


def build_company_schema() -> dict:
    return {
        "properties": {
            "Company": {"type": "title"},
            "Description": {"type": "rich_text"},
            "Status": {"type": "select", "select": {"options": [{"name": "Radar List"}]}},
            "Deal Score": {"type": "number"},
            "Tags": {"type": "rich_text"},
        }
    }


def build_tracker_schema() -> dict:
    return {
        "properties": {
            "Deal": {"type": "title"},
            "Company": {"type": "rich_text"},
            "Amount": {"type": "rich_text"},
            "Announced": {"type": "date"},
            "Stage / Round": {
                "type": "select",
                "select": {"options": [{"name": "Seed"}, {"name": "A"}, {"name": "B"}, {"name": "Unknown"}]},
            },
            "Status": {"type": "status", "status": {"options": [{"name": "To review"}]}},
            "Summary": {"type": "rich_text"},
            "Source URL": {"type": "url"},
            "Category": {"type": "multi_select", "multi_select": {"options": [{"name": "Agent"}, {"name": "Infra"}]}},
            "Investor": {"type": "select", "select": {"options": [{"name": "Sequoia"}, {"name": "Other"}]}},
        }
    }


def build_pipeline_schema() -> dict:
    return {
        "properties": {
            "项目名称": {"type": "title"},
            "Priority": {"type": "select", "select": {"options": [{"name": "1"}, {"name": "2"}, {"name": "Hold"}]}},
            "Reason to Invest": {"type": "rich_text"},
            "简介": {"type": "rich_text"},
            "公司介绍": {"type": "rich_text"},
            "融资轮次": {"type": "rich_text"},
            "Investors": {"type": "rich_text"},
            "Deal Dynamic": {"type": "rich_text"},
            "负责人": {"type": "rich_text"},
            "Category (新)": {"type": "rich_text"},
            "相关文档链接": {"type": "rich_text"},
            "内部阶段": {"type": "rich_text"},
        }
    }


def build_existing_page(page_id: str, signal_key: str) -> dict:
    return {
        "id": page_id,
        "properties": {
            "Signal Key": {
                "type": "rich_text",
                "rich_text": [{"plain_text": signal_key}],
            }
        },
    }


def build_relation_target_config() -> list[dict]:
    return [
        {
            "property_key": target["property_key"],
            "data_source_id": target["data_source_id"],
            "lookup_properties": target["lookup_properties"],
            "signal_fields": target["signal_fields"],
        }
        for target in DEFAULT_RELATION_TARGETS
    ]


class NotionDatabaseDeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = {
            "id": "notion-market-watch",
            "type": "notion_database",
            "data_source_id": f"collection://{MAIN_DATA_SOURCE_ID}",
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
                return build_main_schema(include_optional=True, include_relations=False)
            if path.endswith("/query"):
                return {"results": [], "has_more": False}
            if path == "/v1/pages":
                return {"id": "page-new"}
            raise AssertionError(f"Unexpected request: {method} {path}")

        with patch.object(NotionDatabaseDelivery, "_request_json", autospec=True, side_effect=fake_request):
            delivery.deliver(self.result, self.output_path)

        main_create_call = next(call for call in calls if call[0] == "POST" and call[1] == "/v1/pages")
        self.assertEqual(MAIN_DATA_SOURCE_ID, main_create_call[2]["parent"]["data_source_id"])
        self.assertEqual("Funding", main_create_call[2]["properties"]["Event Type"]["select"]["name"])
        self.assertEqual("Isara", main_create_call[2]["properties"]["Company"]["rich_text"][0]["text"]["content"])
        self.assertEqual("Must Chase", main_create_call[2]["properties"]["Follow Verdict"]["select"]["name"])

    def test_update_page_when_signal_already_exists(self) -> None:
        delivery = NotionDatabaseDelivery(self.config)
        calls: list[tuple[str, str, dict | None]] = []
        signal_key = self.result.signals[0].stable_key()

        def fake_request(instance: NotionDatabaseDelivery, method: str, path: str, body: dict | None = None) -> dict:
            del instance
            calls.append((method, path, body))
            if method == "GET":
                return build_main_schema(include_optional=True, include_relations=False)
            if path.endswith("/query"):
                return {"results": [build_existing_page("page-existing", signal_key)], "has_more": False}
            if path == "/v1/pages/page-existing":
                return {"id": "page-existing"}
            raise AssertionError(f"Unexpected request: {method} {path}")

        with patch.object(NotionDatabaseDelivery, "_request_json", autospec=True, side_effect=fake_request):
            delivery.deliver(self.result, self.output_path)

        update_call = next(call for call in calls if call[0] == "PATCH" and call[1] == "/v1/pages/page-existing")
        self.assertIn("properties", update_call[2])
        self.assertNotIn("Review Status", update_call[2]["properties"])

    def test_archives_stale_pages_that_are_no_longer_in_latest_run(self) -> None:
        delivery = NotionDatabaseDelivery(self.config)
        calls: list[tuple[str, str, dict | None]] = []
        current_key = self.result.signals[0].stable_key()

        def fake_request(instance: NotionDatabaseDelivery, method: str, path: str, body: dict | None = None) -> dict:
            del instance
            calls.append((method, path, body))
            if method == "GET":
                return build_main_schema()
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

    def test_auto_creates_related_pages_for_high_signal(self) -> None:
        config = {**self.config, "relation_targets": build_relation_target_config()}
        delivery = NotionDatabaseDelivery(config)
        calls: list[tuple[str, str, dict | None]] = []

        def fake_request(instance: NotionDatabaseDelivery, method: str, path: str, body: dict | None = None) -> dict:
            del instance
            calls.append((method, path, body))
            if method == "GET" and path == f"/v1/data_sources/{MAIN_DATA_SOURCE_ID}":
                return build_main_schema(include_optional=True, include_relations=True)
            if method == "GET" and path == f"/v1/data_sources/{COMPANY_DATA_SOURCE_ID}":
                return build_company_schema()
            if method == "GET" and path == f"/v1/data_sources/{TRACKER_DATA_SOURCE_ID}":
                return build_tracker_schema()
            if method == "GET" and path == f"/v1/data_sources/{PIPELINE_DATA_SOURCE_ID}":
                return build_pipeline_schema()
            if path.endswith("/query"):
                return {"results": [], "has_more": False}
            if path == "/v1/pages":
                parent_id = body["parent"]["data_source_id"]
                if parent_id == COMPANY_DATA_SOURCE_ID:
                    return {"id": "company-page"}
                if parent_id == TRACKER_DATA_SOURCE_ID:
                    return {"id": "tracker-page"}
                if parent_id == PIPELINE_DATA_SOURCE_ID:
                    return {"id": "pipeline-page"}
                if parent_id == MAIN_DATA_SOURCE_ID:
                    return {"id": "signal-page"}
            raise AssertionError(f"Unexpected request: {method} {path}")

        with patch.object(NotionDatabaseDelivery, "_request_json", autospec=True, side_effect=fake_request):
            delivery.deliver(self.result, self.output_path)

        page_creates = [call for call in calls if call[0] == "POST" and call[1] == "/v1/pages"]
        self.assertEqual(4, len(page_creates))

        company_create = next(call for call in page_creates if call[2]["parent"]["data_source_id"] == COMPANY_DATA_SOURCE_ID)
        tracker_create = next(call for call in page_creates if call[2]["parent"]["data_source_id"] == TRACKER_DATA_SOURCE_ID)
        pipeline_create = next(call for call in page_creates if call[2]["parent"]["data_source_id"] == PIPELINE_DATA_SOURCE_ID)
        main_create = next(call for call in page_creates if call[2]["parent"]["data_source_id"] == MAIN_DATA_SOURCE_ID)

        self.assertEqual("Isara", company_create[2]["properties"]["Company"]["title"][0]["text"]["content"])
        self.assertEqual("Radar List", company_create[2]["properties"]["Status"]["select"]["name"])
        self.assertEqual("A", tracker_create[2]["properties"]["Stage / Round"]["select"]["name"])
        self.assertEqual("To review", tracker_create[2]["properties"]["Status"]["status"]["name"])
        self.assertEqual("1", pipeline_create[2]["properties"]["Priority"]["select"]["name"])
        self.assertEqual("Isara", pipeline_create[2]["properties"]["项目名称"]["title"][0]["text"]["content"])

        main_properties = main_create[2]["properties"]
        self.assertEqual([{"id": "company-page"}], main_properties["Company Record"]["relation"])
        self.assertEqual([{"id": "tracker-page"}], main_properties["AI Tracker Deal"]["relation"])
        self.assertEqual([{"id": "pipeline-page"}], main_properties["Pipeline Deal"]["relation"])
    def test_monitor_signal_does_not_auto_create_related_pages(self) -> None:
        config = {**self.config, "relation_targets": build_relation_target_config()}
        delivery = NotionDatabaseDelivery(config)
        result = build_result(follow_verdict="Monitor")
        calls: list[tuple[str, str, dict | None]] = []

        def fake_request(instance: NotionDatabaseDelivery, method: str, path: str, body: dict | None = None) -> dict:
            del instance
            calls.append((method, path, body))
            if method == "GET" and path == f"/v1/data_sources/{MAIN_DATA_SOURCE_ID}":
                return build_main_schema(include_optional=True, include_relations=True)
            if method == "GET" and path == f"/v1/data_sources/{COMPANY_DATA_SOURCE_ID}":
                return build_company_schema()
            if method == "GET" and path == f"/v1/data_sources/{TRACKER_DATA_SOURCE_ID}":
                return build_tracker_schema()
            if method == "GET" and path == f"/v1/data_sources/{PIPELINE_DATA_SOURCE_ID}":
                return build_pipeline_schema()
            if path.endswith("/query"):
                return {"results": [], "has_more": False}
            if path == "/v1/pages":
                return {"id": "signal-page"}
            raise AssertionError(f"Unexpected request: {method} {path}")

        with patch.object(NotionDatabaseDelivery, "_request_json", autospec=True, side_effect=fake_request):
            delivery.deliver(result, self.output_path)

        page_creates = [call for call in calls if call[0] == "POST" and call[1] == "/v1/pages"]
        self.assertEqual(1, len(page_creates))
        self.assertEqual(MAIN_DATA_SOURCE_ID, page_creates[0][2]["parent"]["data_source_id"])
        self.assertNotIn("Company Record", page_creates[0][2]["properties"])

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

