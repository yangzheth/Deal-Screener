from __future__ import annotations

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
    },
    {
        "property_key": "ai_tracker_deal",
        "data_source_id": "collection://b7f972e1-ad2d-46e6-8b13-f298e99a3602",
        "lookup_properties": ["Company", "Deal"],
        "signal_fields": ["company_name", "title"],
    },
    {
        "property_key": "pipeline_deal",
        "data_source_id": "collection://0b6c5021-4235-4b5d-b8bb-3f50da0c277b",
        "lookup_properties": ["项目名称"],
        "signal_fields": ["company_name", "title"],
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
        self.relation_targets = config.get("relation_targets", DEFAULT_RELATION_TARGETS)
        self.remote_properties: dict[str, dict] = {}

    def deliver(self, result: DailyRunResult, output_path: Path) -> None:
        self._validate_schema()
        relation_indexes = self._build_relation_indexes()
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
                relation_indexes=relation_indexes,
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
        relation_indexes: dict[str, dict[str, list[str]]],
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

        for property_key, relation_ids in self._resolve_relation_ids(signal, relation_indexes).items():
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

    def _build_relation_indexes(self) -> dict[str, dict[str, list[str]]]:
        indexes: dict[str, dict[str, list[str]]] = {}
        for target in self.relation_targets:
            property_key = target.get("property_key", "")
            if not self._has_property(property_key):
                continue
            pages = self._query_all_pages(self._normalize_data_source_id(target["data_source_id"]))
            index: dict[str, list[str]] = {}
            for page in pages:
                for property_name in target.get("lookup_properties", []):
                    value = self._extract_property_plain_text(page, property_name)
                    normalized = self._normalize_lookup(value)
                    if not normalized:
                        continue
                    index.setdefault(normalized, []).append(page["id"])
            indexes[property_key] = index
        return indexes

    def _resolve_relation_ids(self, signal: Signal, relation_indexes: dict[str, dict[str, list[str]]]) -> dict[str, list[str]]:
        resolved: dict[str, list[str]] = {}
        for target in self.relation_targets:
            property_key = target.get("property_key", "")
            index = relation_indexes.get(property_key)
            if not index:
                continue
            candidates: list[str] = []
            for field_name in target.get("signal_fields", []):
                value = getattr(signal, field_name, "")
                if isinstance(value, list):
                    candidates.extend(item for item in value if item)
                elif value:
                    candidates.append(str(value))
            relation_ids = self._find_relation_ids(index, candidates)
            if relation_ids:
                resolved[property_key] = relation_ids
        return resolved

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
