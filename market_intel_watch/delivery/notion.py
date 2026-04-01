from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
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


class NotionDatabaseDelivery(DeliveryChannel):
    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self.api_base = config.get("api_base", DEFAULT_API_BASE).rstrip("/")
        self.notion_version = config.get("notion_version", DEFAULT_NOTION_VERSION)
        self.auth_token_env = config.get("auth_token_env", "NOTION_API_TOKEN")
        self.data_source_id = self._normalize_data_source_id(config["data_source_id"])
        self.upsert = bool(config.get("upsert", True))
        max_items = int(config.get("max_items", 0))
        self.max_items = max_items if max_items > 0 else None
        self.properties = {**DEFAULT_PROPERTIES, **config.get("properties", {})}

    def deliver(self, result: DailyRunResult, output_path: Path) -> None:
        self._validate_schema()
        signals = result.signals[: self.max_items] if self.max_items else result.signals
        existing_pages = self._list_existing_pages_for_run_date(result) if self.upsert else {}
        active_keys: set[str] = set()

        for signal in signals:
            signal_key = signal.stable_key()
            active_keys.add(signal_key)
            properties = self._build_properties(result, signal, output_path)
            page_id = existing_pages.get(signal_key)
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
            response = self._request_json(
                "POST",
                f"/v1/data_sources/{self.data_source_id}/query",
                body,
            )
            for item in response.get("results", []):
                signal_key = self._extract_rich_text_value(item, self.properties["signal_key"])
                if signal_key:
                    pages[signal_key] = item["id"]
            if not response.get("has_more"):
                return pages
            cursor = response.get("next_cursor")

    def _extract_rich_text_value(self, page: dict, property_name: str) -> str:
        property_data = page.get("properties", {}).get(property_name, {})
        parts = property_data.get("rich_text", [])
        return "".join(part.get("plain_text", "") for part in parts).strip()

    def _build_properties(
        self,
        result: DailyRunResult,
        signal: Signal,
        output_path: Path,
    ) -> dict:
        properties = {
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
            properties[self.properties["published_at"]] = {
                "date": {"start": self._format_datetime(signal.published_at)}
            }
        if signal.url:
            properties[self.properties["source_url"]] = {"url": signal.url}
        return properties

    def _validate_schema(self) -> None:
        schema = self._request_json("GET", f"/v1/data_sources/{self.data_source_id}")
        remote_properties = schema.get("properties", {})
        missing: list[str] = []
        mismatched: list[str] = []
        for key, expected_type in REQUIRED_PROPERTY_TYPES.items():
            property_name = self.properties[key]
            remote_property = remote_properties.get(property_name)
            if remote_property is None:
                missing.append(f"{property_name} ({expected_type})")
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
                "Notion data source schema does not match the expected delivery schema; "
                + "; ".join(details)
            )

    def _request_json(self, method: str, path: str, body: dict | None = None) -> dict:
        token = os.environ.get(self.auth_token_env, "").strip()
        if not token:
            raise RuntimeError(
                f"Missing Notion token. Set the {self.auth_token_env} environment variable first."
            )
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
