from __future__ import annotations

import copy
from datetime import datetime
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from market_intel_watch.delivery.base import DeliveryChannel
from market_intel_watch.models import DailyRunResult, Signal


DEFAULT_NOTION_VERSION = "2025-09-03"
DEFAULT_API_BASE = "https://api.notion.com"
TEXT_CHUNK_SIZE = 1800
MAX_RICH_TEXT_PARTS = 20
MAX_CURL_RETRIES = 3
LOOKUP_NORMALIZE_RE = re.compile(r"[^a-z0-9\u4e00-\u9fff]+")
CORPORATE_SUFFIX_RE = re.compile(r"\b(inc|corp|corporation|ltd|limited|holdings|holding|co)\b", re.IGNORECASE)

EVENT_LABELS = {
    "funding": "Funding",
    "talent_departure": "Talent Departure",
    "talent_hire": "Talent Hire",
}

DEFAULT_PROPERTIES = {
    "title": "Signal",
    "signal_key": "Signal Key",
    "run_date": "Run Date",
    "published_at": "Published At",
    "event_type": "Event Type",
    "geography": "Geography",
    "score": "Score",
    "source_id": "Source ID",
    "channel": "Channel",
    "entities": "Entities",
    "summary": "Summary",
    "rationale": "Rationale",
    "source_url": "Source URL",
    "local_report_path": "Local Report Path",
    "company_name": "Company",
    "key_people": "Key People",
    "amount": "Amount",
    "round_stage": "Round / Stage",
    "investors": "Investors",
    "categories": "Category",
    "follow_verdict": "Follow Verdict",
    "follow_reason": "Follow Reason",
    "suggested_action": "Suggested Action",
    "confidence": "Confidence",
    "source_count": "Source Count",
    "cluster_key": "Cluster Key",
    "review_status": "Review Status",
    "company_record": "Company Record",
    "ai_tracker_deal": "AI Tracker Deal",
    "pipeline_deal": "Pipeline Deal",
}

REQUIRED_PROPERTY_TYPES = {
    "title": "title",
    "signal_key": "rich_text",
    "run_date": "date",
    "published_at": "date",
    "event_type": "select",
    "geography": "select",
    "score": "number",
    "source_id": "rich_text",
    "channel": "rich_text",
    "entities": "rich_text",
    "summary": "rich_text",
    "rationale": "rich_text",
    "source_url": "url",
    "local_report_path": "rich_text",
}

OPTIONAL_PROPERTY_TYPES = {
    "company_name": "rich_text",
    "key_people": "rich_text",
    "amount": "rich_text",
    "round_stage": "rich_text",
    "investors": "rich_text",
    "categories": "multi_select",
    "follow_verdict": "select",
    "follow_reason": "rich_text",
    "suggested_action": "rich_text",
    "confidence": "number",
    "source_count": "number",
    "cluster_key": "rich_text",
    "review_status": "select",
    "company_record": "relation",
    "ai_tracker_deal": "relation",
    "pipeline_deal": "relation",
}

DEFAULT_RELATION_TARGETS = [
    {
        "property_key": "company_record",
        "data_source_id": "collection://8a47f584-7a5b-4cba-9eb6-d6d6ca8789cc",
        "lookup_properties": ["Company"],
        "signal_fields": ["company_name"],
        "create_if_missing": True,
        "create_on_verdicts": ["Must Chase", "Worth Tracking"],
        "create_required_fields": ["company_name"],
    },
    {
        "property_key": "ai_tracker_deal",
        "data_source_id": "collection://b7f972e1-ad2d-46e6-8b13-f298e99a3602",
        "lookup_properties": ["Company", "Deal"],
        "signal_fields": ["company_name", "title"],
        "create_if_missing": True,
        "create_on_event_types": ["funding"],
        "create_on_verdicts": ["Must Chase", "Worth Tracking"],
        "create_required_fields": ["company_name"],
    },
    {
        "property_key": "pipeline_deal",
        "data_source_id": "collection://0b6c5021-4235-4b5d-b8bb-3f50da0c277b",
        "lookup_properties": ["项目名称"],
        "signal_fields": ["company_name", "title"],
        "create_if_missing": True,
        "create_on_verdicts": ["Must Chase"],
        "create_required_fields": ["company_name"],
    },
]


class NotionDatabaseDelivery(DeliveryChannel):
    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self.api_base = config.get("api_base", DEFAULT_API_BASE).rstrip("/")
        self.notion_version = config.get("notion_version", DEFAULT_NOTION_VERSION)
        self.auth_token_env = config.get("auth_token_env", "NOTION_API_TOKEN")
        self.data_source_id = self._normalize_data_source_id(config["data_source_id"])
        self.upsert = bool(config.get("upsert", True))
        self.properties = {**DEFAULT_PROPERTIES, **config.get("properties", {})}
        self.default_review_status = config.get("default_review_status", "To Review")
        self.relation_targets = self._merge_relation_targets(config.get("relation_targets"))
        self.remote_properties: dict[str, dict] = {}

    def deliver(self, result: DailyRunResult, output_path: Path) -> None:
        self._validate_schema()
        relation_contexts = self._build_relation_contexts()
        signals = self.select_signals(result)
        existing_pages = self._list_existing_pages_for_run_date(result) if self.upsert else {}
        active_keys: set[str] = set()

        for signal in signals:
            signal_key = signal.stable_key()
            active_keys.add(signal_key)
            page_id = existing_pages.get(signal_key)
            properties = self._build_properties(
                result,
                signal,
                output_path,
                relation_contexts=relation_contexts,
                for_create=page_id is None,
            )
            if page_id:
                self._request_json("PATCH", f"/v1/pages/{page_id}", {"properties": properties})
            else:
                self._request_json(
                    "POST",
                    "/v1/pages",
                    {
                        "parent": {"data_source_id": self.data_source_id},
                        "properties": properties,
                    },
                )

        if self.upsert:
            stale_page_ids = [page_id for key, page_id in existing_pages.items() if key not in active_keys]
            for page_id in stale_page_ids:
                self._request_json("PATCH", f"/v1/pages/{page_id}", {"archived": True})
    def _merge_relation_targets(self, configured_targets: list[dict] | None) -> list[dict]:
        if not configured_targets:
            return [copy.deepcopy(target) for target in DEFAULT_RELATION_TARGETS]

        defaults_by_key = {target["property_key"]: target for target in DEFAULT_RELATION_TARGETS}
        merged: list[dict] = []
        for target in configured_targets:
            property_key = target.get("property_key", "")
            base = copy.deepcopy(defaults_by_key.get(property_key, {}))
            merged.append(self._deep_merge(base, target))
        return merged

    def _deep_merge(self, base: dict, override: dict) -> dict:
        result = copy.deepcopy(base)
        for key, value in override.items():
            if isinstance(value, dict) and isinstance(result.get(key), dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def _list_existing_pages_for_run_date(self, result: DailyRunResult) -> dict[str, str]:
        pages: dict[str, str] = {}
        cursor: str | None = None
        while True:
            body: dict[str, object] = {
                "filter": {
                    "property": self.properties["run_date"],
                    "date": {"equals": result.run_date.date().isoformat()},
                },
                "page_size": 100,
            }
            if cursor:
                body["start_cursor"] = cursor
            response = self._request_json("POST", f"/v1/data_sources/{self.data_source_id}/query", body)
            for item in response.get("results", []):
                signal_key = self._extract_property_plain_text(item, self.properties["signal_key"])
                if signal_key:
                    pages[signal_key] = item["id"]
            if not response.get("has_more"):
                return pages
            cursor = response.get("next_cursor")

    def _build_properties(
        self,
        result: DailyRunResult,
        signal: Signal,
        output_path: Path,
        *,
        relation_contexts: dict[str, dict],
        for_create: bool,
    ) -> dict:
        properties: dict[str, dict] = {
            self.properties["title"]: {"title": self._rich_text_parts(signal.title, max_parts=1)},
            self.properties["signal_key"]: {"rich_text": self._rich_text_parts(signal.stable_key(), max_parts=1)},
            self.properties["run_date"]: {"date": {"start": result.run_date.date().isoformat()}},
            self.properties["event_type"]: {"select": {"name": EVENT_LABELS.get(signal.event_type, signal.event_type)}},
            self.properties["geography"]: {"select": {"name": signal.geography or "unknown"}},
            self.properties["score"]: {"number": round(signal.score, 2)},
            self.properties["source_id"]: {"rich_text": self._rich_text_parts(signal.source_id, max_parts=1)},
            self.properties["channel"]: {"rich_text": self._rich_text_parts(signal.channel, max_parts=1)},
            self.properties["entities"]: {"rich_text": self._rich_text_parts(", ".join(signal.matched_entities) or "unmatched")},
            self.properties["summary"]: {"rich_text": self._rich_text_parts(signal.summary)},
            self.properties["rationale"]: {"rich_text": self._rich_text_parts(", ".join(signal.rationale))},
            self.properties["local_report_path"]: {"rich_text": self._rich_text_parts(str(output_path), max_parts=1)},
        }
        if signal.published_at is not None:
            properties[self.properties["published_at"]] = {"date": {"start": self._format_datetime(signal.published_at)}}
        if signal.url:
            properties[self.properties["source_url"]] = {"url": signal.url}

        self._set_optional_rich_text(properties, "company_name", signal.company_name, max_parts=1)
        self._set_optional_rich_text(properties, "key_people", ", ".join(signal.key_people), max_parts=1)
        self._set_optional_rich_text(properties, "amount", signal.amount, max_parts=1)
        self._set_optional_rich_text(properties, "round_stage", signal.round_stage, max_parts=1)
        self._set_optional_rich_text(properties, "investors", ", ".join(signal.investors))
        self._set_optional_multi_select(properties, "categories", signal.categories)
        self._set_optional_select(properties, "follow_verdict", signal.follow_verdict)
        self._set_optional_rich_text(properties, "follow_reason", signal.follow_reason)
        self._set_optional_rich_text(properties, "suggested_action", signal.suggested_action)
        self._set_optional_number(properties, "confidence", round(signal.confidence, 2))
        self._set_optional_number(properties, "source_count", signal.source_count)
        self._set_optional_rich_text(properties, "cluster_key", signal.cluster_key, max_parts=1)
        if for_create:
            self._set_optional_select(properties, "review_status", self.default_review_status)

        for property_key, relation_ids in self._resolve_relation_ids(signal, relation_contexts).items():
            if relation_ids and self._has_property(property_key):
                properties[self.properties[property_key]] = {"relation": [{"id": item_id} for item_id in relation_ids[:10]]}
        return properties

    def _set_optional_rich_text(self, properties: dict, property_key: str, value: str, max_parts: int = MAX_RICH_TEXT_PARTS) -> None:
        if self._has_property(property_key) and (value or "").strip():
            properties[self.properties[property_key]] = {"rich_text": self._rich_text_parts(value, max_parts=max_parts)}

    def _set_optional_multi_select(self, properties: dict, property_key: str, values: list[str]) -> None:
        if self._has_property(property_key) and values:
            properties[self.properties[property_key]] = {"multi_select": [{"name": value} for value in values]}

    def _set_optional_select(self, properties: dict, property_key: str, value: str) -> None:
        if self._has_property(property_key) and (value or "").strip():
            properties[self.properties[property_key]] = {"select": {"name": value}}

    def _set_optional_number(self, properties: dict, property_key: str, value: float | int) -> None:
        if self._has_property(property_key):
            properties[self.properties[property_key]] = {"number": value}

    def _has_property(self, property_key: str) -> bool:
        property_name = self.properties.get(property_key)
        return bool(property_name and property_name in self.remote_properties)

    def _validate_schema(self) -> None:
        schema = self._request_json("GET", f"/v1/data_sources/{self.data_source_id}")
        self.remote_properties = schema.get("properties", {})
        missing: list[str] = []
        mismatched: list[str] = []
        for key, expected_type in REQUIRED_PROPERTY_TYPES.items():
            property_name = self.properties[key]
            remote_property = self.remote_properties.get(property_name)
            if remote_property is None:
                missing.append(f"{property_name} ({expected_type})")
                continue
            actual_type = remote_property.get("type")
            if actual_type != expected_type:
                mismatched.append(f"{property_name}: expected {expected_type}, got {actual_type}")
        for key, expected_type in OPTIONAL_PROPERTY_TYPES.items():
            property_name = self.properties[key]
            remote_property = self.remote_properties.get(property_name)
            if remote_property is None:
                continue
            actual_type = remote_property.get("type")
            if actual_type != expected_type:
                mismatched.append(f"{property_name}: expected {expected_type}, got {actual_type}")
        if missing or mismatched:
            details = []
            if missing:
                details.append("missing: " + ", ".join(missing))
            if mismatched:
                details.append("type mismatch: " + ", ".join(mismatched))
            raise RuntimeError(
                "Notion data source schema does not match the expected delivery schema; " + "; ".join(details)
            )
    def _build_relation_contexts(self) -> dict[str, dict]:
        contexts: dict[str, dict] = {}
        for target in self.relation_targets:
            property_key = target.get("property_key", "")
            if not self._has_property(property_key):
                continue
            data_source_id = self._normalize_data_source_id(target["data_source_id"])
            schema = self._request_json("GET", f"/v1/data_sources/{data_source_id}").get("properties", {})
            pages = self._query_all_pages(data_source_id)
            contexts[property_key] = {
                "config": target,
                "data_source_id": data_source_id,
                "schema": schema,
                "index": self._build_relation_index(pages, target.get("lookup_properties", [])),
            }
        return contexts

    def _build_relation_index(self, pages: list[dict], lookup_properties: list[str]) -> dict[str, list[str]]:
        index: dict[str, list[str]] = {}
        for page in pages:
            for property_name in lookup_properties:
                value = self._extract_property_plain_text(page, property_name)
                normalized = self._normalize_lookup(value)
                if not normalized:
                    continue
                page_ids = index.setdefault(normalized, [])
                if page["id"] not in page_ids:
                    page_ids.append(page["id"])
        return index

    def _resolve_relation_ids(self, signal: Signal, relation_contexts: dict[str, dict]) -> dict[str, list[str]]:
        resolved: dict[str, list[str]] = {}
        for property_key, context in relation_contexts.items():
            candidates = self._relation_candidates(signal, context["config"])
            relation_ids = self._find_relation_ids(context["index"], candidates)
            if not relation_ids:
                created_page_id = self._maybe_create_relation_page(signal, context, candidates)
                if created_page_id:
                    relation_ids = [created_page_id]
            if relation_ids:
                resolved[property_key] = relation_ids
        return resolved

    def _relation_candidates(self, signal: Signal, target: dict) -> list[str]:
        candidates: list[str] = []
        for field_name in target.get("signal_fields", []):
            value = getattr(signal, field_name, "")
            if isinstance(value, list):
                candidates.extend(item for item in value if item)
            elif value:
                candidates.append(str(value))
        return candidates

    def _maybe_create_relation_page(self, signal: Signal, context: dict, candidates: list[str]) -> str:
        target = context["config"]
        if not self._should_create_relation_page(signal, target):
            return ""

        properties = self._build_related_page_properties(signal, context)
        if not properties:
            return ""

        response = self._request_json(
            "POST",
            "/v1/pages",
            {
                "parent": {"data_source_id": context["data_source_id"]},
                "properties": properties,
            },
        )
        page_id = response.get("id", "")
        if not page_id:
            return ""
        self._index_created_relation_page(context, page_id, candidates)
        return page_id

    def _should_create_relation_page(self, signal: Signal, target: dict) -> bool:
        if not target.get("create_if_missing"):
            return False

        allowed_event_types = target.get("create_on_event_types", [])
        if allowed_event_types and signal.event_type not in allowed_event_types:
            return False

        allowed_verdicts = target.get("create_on_verdicts", [])
        if allowed_verdicts and signal.follow_verdict not in allowed_verdicts:
            return False

        required_fields = target.get("create_required_fields", [])
        for field_name in required_fields:
            value = getattr(signal, field_name, "")
            if isinstance(value, list):
                if not any(item for item in value if item):
                    return False
            elif not str(value or "").strip():
                return False
        return True

    def _build_related_page_properties(self, signal: Signal, context: dict) -> dict:
        property_key = context["config"].get("property_key", "")
        if property_key == "company_record":
            return self._build_company_record_properties(signal, context["schema"])
        if property_key == "ai_tracker_deal":
            return self._build_ai_tracker_deal_properties(signal, context["schema"])
        if property_key == "pipeline_deal":
            return self._build_pipeline_deal_properties(signal, context["schema"])
        return {}

    def _build_company_record_properties(self, signal: Signal, schema: dict[str, dict]) -> dict:
        properties: dict[str, dict] = {}
        self._set_target_title(properties, schema, "Company", signal.company_name)
        self._set_target_rich_text(properties, schema, "Description", signal.summary)
        self._set_target_select(properties, schema, "Status", "Radar List")
        self._set_target_number(properties, schema, "Deal Score", round(signal.score, 2))
        tags = ", ".join(item for item in [signal.follow_verdict, signal.event_type, *signal.categories] if item)
        self._set_target_rich_text(properties, schema, "Tags", tags)
        return properties

    def _build_ai_tracker_deal_properties(self, signal: Signal, schema: dict[str, dict]) -> dict:
        properties: dict[str, dict] = {}
        self._set_target_title(properties, schema, "Deal", signal.title)
        self._set_target_rich_text(properties, schema, "Company", signal.company_name)
        self._set_target_rich_text(properties, schema, "Amount", signal.amount)
        if signal.published_at is not None:
            self._set_target_date(properties, schema, "Announced", signal.published_at.date().isoformat())
        stage_label = self._tracker_round_label(signal.round_stage)
        if stage_label:
            stage_option = self._match_option_name(schema, "Stage / Round", [stage_label], fallback="Unknown")
            self._set_target_select(properties, schema, "Stage / Round", stage_option)
        self._set_target_status(properties, schema, "Status", "To review")
        self._set_target_rich_text(properties, schema, "Summary", signal.summary)
        self._set_target_url(properties, schema, "Source URL", signal.url)
        self._set_target_multi_select(properties, schema, "Category", signal.categories)
        investor_option = self._investor_option_name(schema, signal.investors)
        if investor_option:
            self._set_target_select(properties, schema, "Investor", investor_option)
        return properties

    def _build_pipeline_deal_properties(self, signal: Signal, schema: dict[str, dict]) -> dict:
        properties: dict[str, dict] = {}
        title = signal.company_name or signal.title
        self._set_target_title(properties, schema, "项目名称", title)
        self._set_target_select(properties, schema, "Priority", self._pipeline_priority(signal.follow_verdict))
        self._set_target_rich_text(properties, schema, "Reason to Invest", signal.follow_reason or signal.suggested_action)
        self._set_target_rich_text(properties, schema, "简介", signal.summary)
        self._set_target_rich_text(properties, schema, "公司介绍", signal.summary)
        self._set_target_rich_text(properties, schema, "融资轮次", signal.round_stage)
        self._set_target_rich_text(properties, schema, "Investors", ", ".join(signal.investors))
        self._set_target_rich_text(properties, schema, "Deal Dynamic", signal.title)
        self._set_target_rich_text(properties, schema, "负责人", ", ".join(signal.key_people))
        self._set_target_rich_text(properties, schema, "Category (新)", ", ".join(signal.categories))
        self._set_target_rich_text(properties, schema, "相关文档链接", signal.url)
        self._set_target_rich_text(properties, schema, "内部阶段", "Primary Market Watch")
        return properties
    def _set_target_title(self, properties: dict, schema: dict[str, dict], property_name: str, value: str) -> None:
        if schema.get(property_name, {}).get("type") == "title" and (value or "").strip():
            properties[property_name] = {"title": self._rich_text_parts(value, max_parts=1)}

    def _set_target_rich_text(self, properties: dict, schema: dict[str, dict], property_name: str, value: str) -> None:
        if schema.get(property_name, {}).get("type") == "rich_text" and (value or "").strip():
            properties[property_name] = {"rich_text": self._rich_text_parts(value)}

    def _set_target_number(self, properties: dict, schema: dict[str, dict], property_name: str, value: float | int) -> None:
        if schema.get(property_name, {}).get("type") == "number":
            properties[property_name] = {"number": value}

    def _set_target_url(self, properties: dict, schema: dict[str, dict], property_name: str, value: str) -> None:
        if schema.get(property_name, {}).get("type") == "url" and (value or "").strip():
            properties[property_name] = {"url": value}

    def _set_target_select(self, properties: dict, schema: dict[str, dict], property_name: str, value: str) -> None:
        if schema.get(property_name, {}).get("type") == "select" and (value or "").strip():
            properties[property_name] = {"select": {"name": value}}

    def _set_target_status(self, properties: dict, schema: dict[str, dict], property_name: str, value: str) -> None:
        if schema.get(property_name, {}).get("type") == "status" and (value or "").strip():
            properties[property_name] = {"status": {"name": value}}

    def _set_target_multi_select(self, properties: dict, schema: dict[str, dict], property_name: str, values: list[str]) -> None:
        if schema.get(property_name, {}).get("type") != "multi_select":
            return
        matched_values = self._match_multi_select_values(schema, property_name, values)
        if matched_values:
            properties[property_name] = {"multi_select": [{"name": value} for value in matched_values]}

    def _set_target_date(self, properties: dict, schema: dict[str, dict], property_name: str, value: str) -> None:
        if schema.get(property_name, {}).get("type") == "date" and (value or "").strip():
            properties[property_name] = {"date": {"start": value}}

    def _match_multi_select_values(self, schema: dict[str, dict], property_name: str, values: list[str]) -> list[str]:
        options = self._option_names(schema, property_name)
        normalized_options = {self._normalize_lookup(item): item for item in options}
        matched: list[str] = []
        for value in values:
            normalized = self._normalize_lookup(value)
            option_name = normalized_options.get(normalized, "")
            if not option_name:
                for option_key, option_value in normalized_options.items():
                    if normalized and (normalized in option_key or option_key in normalized):
                        option_name = option_value
                        break
            if option_name and option_name not in matched:
                matched.append(option_name)
        return matched

    def _match_option_name(self, schema: dict[str, dict], property_name: str, candidates: list[str], fallback: str = "") -> str:
        options = self._option_names(schema, property_name)
        if not options:
            return ""
        normalized_options = {self._normalize_lookup(item): item for item in options}
        for candidate in candidates:
            normalized = self._normalize_lookup(candidate)
            option_name = normalized_options.get(normalized)
            if option_name:
                return option_name
        for candidate in candidates:
            normalized = self._normalize_lookup(candidate)
            for option_key, option_value in normalized_options.items():
                if normalized and (normalized in option_key or option_key in normalized):
                    return option_value
        if fallback:
            fallback_option = normalized_options.get(self._normalize_lookup(fallback))
            if fallback_option:
                return fallback_option
        return ""

    def _option_names(self, schema: dict[str, dict], property_name: str) -> list[str]:
        property_data = schema.get(property_name, {})
        property_type = property_data.get("type")
        type_payload = property_data.get(property_type, {})
        return [item.get("name", "") for item in type_payload.get("options", []) if item.get("name")]

    def _tracker_round_label(self, round_stage: str) -> str:
        mapping = {
            "Pre-Seed": "Seed",
            "Seed": "Seed",
            "Angel": "Angel",
            "Pre-A": "Pre-A",
            "Series A": "A",
            "Series B": "B",
            "Series C": "C+",
            "Series D+": "C+",
            "Strategic": "Strategic",
        }
        return mapping.get((round_stage or "").strip(), (round_stage or "").strip())

    def _investor_option_name(self, schema: dict[str, dict], investors: list[str]) -> str:
        if not investors:
            return ""
        option_name = self._match_option_name(schema, "Investor", investors)
        if option_name:
            return option_name
        return self._match_option_name(schema, "Investor", ["Other"])

    def _pipeline_priority(self, follow_verdict: str) -> str:
        if follow_verdict == "Must Chase":
            return "1"
        if follow_verdict == "Worth Tracking":
            return "2"
        return "Hold"

    def _index_created_relation_page(self, context: dict, page_id: str, candidates: list[str]) -> None:
        index = context["index"]
        for candidate in candidates:
            normalized = self._normalize_lookup(candidate)
            if not normalized:
                continue
            page_ids = index.setdefault(normalized, [])
            if page_id not in page_ids:
                page_ids.append(page_id)

    def _find_relation_ids(self, index: dict[str, list[str]], candidates: list[str]) -> list[str]:
        normalized_candidates = [self._normalize_lookup(candidate) for candidate in candidates if self._normalize_lookup(candidate)]
        for candidate in normalized_candidates:
            if candidate in index:
                return index[candidate]
        fuzzy_matches: list[str] = []
        for candidate in normalized_candidates:
            for key, value in index.items():
                if candidate and (candidate in key or key in candidate):
                    fuzzy_matches.extend(value)
            if fuzzy_matches:
                break
        seen: set[str] = set()
        ordered: list[str] = []
        for item_id in fuzzy_matches:
            if item_id in seen:
                continue
            seen.add(item_id)
            ordered.append(item_id)
        return ordered

    def _normalize_lookup(self, value: str) -> str:
        normalized = LOOKUP_NORMALIZE_RE.sub(" ", (value or "").lower())
        normalized = CORPORATE_SUFFIX_RE.sub(" ", normalized)
        return " ".join(normalized.split())

    def _query_all_pages(self, data_source_id: str) -> list[dict]:
        pages: list[dict] = []
        cursor: str | None = None
        while True:
            body: dict[str, object] = {"page_size": 100}
            if cursor:
                body["start_cursor"] = cursor
            response = self._request_json("POST", f"/v1/data_sources/{data_source_id}/query", body)
            pages.extend(response.get("results", []))
            if not response.get("has_more"):
                return pages
            cursor = response.get("next_cursor")

    def _extract_property_plain_text(self, page: dict, property_name: str) -> str:
        property_data = page.get("properties", {}).get(property_name, {})
        property_type = property_data.get("type")
        if property_type == "title":
            return "".join(part.get("plain_text", "") for part in property_data.get("title", [])).strip()
        if property_type == "rich_text":
            return "".join(part.get("plain_text", "") for part in property_data.get("rich_text", [])).strip()
        if property_type == "select":
            return (property_data.get("select") or {}).get("name", "")
        if property_type == "status":
            return (property_data.get("status") or {}).get("name", "")
        return ""
    def _request_json(self, method: str, path: str, body: dict | None = None) -> dict:
        token = os.environ.get(self.auth_token_env, "").strip()
        if not token:
            raise RuntimeError(f"Missing Notion token. Set the {self.auth_token_env} environment variable first.")
        data = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": self.notion_version,
            "Content-Type": "application/json",
        }
        request = Request(f"{self.api_base}{path}", data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=30) as response:
                payload = response.read()
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            message = raw
            try:
                parsed = json.loads(raw)
                message = parsed.get("message") or parsed.get("code") or raw
            except json.JSONDecodeError:
                pass
            raise RuntimeError(f"Notion API {method} {path} failed: {message}") from exc
        except Exception as exc:
            if os.name == "nt":
                return self._request_json_with_curl(method, path, body, token, exc)
            raise RuntimeError(f"Notion API {method} {path} failed: {exc}") from exc
        if not payload:
            return {}
        return json.loads(payload.decode("utf-8"))

    def _request_json_with_curl(
        self,
        method: str,
        path: str,
        body: dict | None,
        token: str,
        original_error: Exception,
    ) -> dict:
        curl_path = shutil.which("curl.exe") or shutil.which("curl")
        if not curl_path:
            raise RuntimeError(f"Notion API {method} {path} failed: {original_error}") from original_error

        command = [
            curl_path,
            "-sS",
            "-X",
            method,
            "-H",
            f"Authorization: Bearer {token}",
            "-H",
            f"Notion-Version: {self.notion_version}",
            "-H",
            "Content-Type: application/json",
            "-w",
            "\n%{http_code}",
            f"{self.api_base}{path}",
        ]
        if body is not None:
            command.extend(["--data", json.dumps(body, ensure_ascii=False)])

        last_message = str(original_error)
        for attempt in range(MAX_CURL_RETRIES):
            completed = subprocess.run(command, capture_output=True, text=True, encoding="utf-8")
            if completed.returncode != 0:
                last_message = completed.stderr.strip() or str(original_error)
                if attempt < MAX_CURL_RETRIES - 1:
                    time.sleep(attempt + 1)
                    continue
                raise RuntimeError(f"Notion API {method} {path} failed: {last_message}") from original_error

            raw_output = completed.stdout.rstrip()
            if not raw_output:
                return {}
            body_text, separator, status_text = raw_output.rpartition("\n")
            if not separator:
                body_text = raw_output
                status_text = "200"

            try:
                status_code = int(status_text.strip())
            except ValueError:
                body_text = raw_output
                status_code = 200

            if status_code in {429} or status_code >= 500:
                last_message = body_text or f"HTTP {status_code}"
                if attempt < MAX_CURL_RETRIES - 1:
                    time.sleep(attempt + 1)
                    continue
                raise RuntimeError(f"Notion API {method} {path} failed: {last_message}") from original_error

            if status_code >= 400:
                message = body_text
                try:
                    parsed = json.loads(body_text)
                    message = parsed.get("message") or parsed.get("code") or body_text
                except json.JSONDecodeError:
                    pass
                raise RuntimeError(f"Notion API {method} {path} failed: {message}") from original_error

            if not body_text:
                return {}
            return json.loads(body_text)

        raise RuntimeError(f"Notion API {method} {path} failed: {last_message}") from original_error

    def _rich_text_parts(self, value: str, max_parts: int = MAX_RICH_TEXT_PARTS) -> list[dict]:
        text = (value or "").strip()
        if not text:
            return []
        parts: list[dict] = []
        for index in range(0, len(text), TEXT_CHUNK_SIZE):
            if len(parts) >= max_parts:
                break
            chunk = text[index : index + TEXT_CHUNK_SIZE]
            parts.append({"type": "text", "text": {"content": chunk}})
        return parts

    def _format_datetime(self, value: datetime) -> str:
        return value.isoformat()

    def _normalize_data_source_id(self, value: str) -> str:
        normalized = (value or "").strip()
        if normalized.startswith("collection://"):
            normalized = normalized[len("collection://") :]
        return normalized.strip("{}")

