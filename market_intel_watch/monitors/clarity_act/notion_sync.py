from __future__ import annotations

import os

from market_intel_watch.monitors.clarity_act.http_util import HttpJsonError, request_json
from market_intel_watch.monitors.clarity_act.models import ClassifiedEvent, MarketSnapshot


_TEXT_CHUNK = 1800
_MAX_CHUNKS = 25


def _normalize_data_source_id(value: str) -> str:
    value = (value or "").strip()
    if value.startswith("collection://"):
        value = value[len("collection://") :]
    return value.strip("{}")


def _rich_text(value: str) -> list[dict]:
    text = (value or "").strip()
    if not text:
        return []
    parts: list[dict] = []
    for index in range(0, len(text), _TEXT_CHUNK):
        if len(parts) >= _MAX_CHUNKS:
            break
        parts.append({"type": "text", "text": {"content": text[index : index + _TEXT_CHUNK]}})
    return parts


class NotionSync:
    """Writes monitor output into the CLARITY Act Tracker Notion databases.

    Degrades gracefully: if the API token is missing the sync is disabled and
    the rest of the pipeline still runs.
    """

    def __init__(self, config: dict) -> None:
        cfg = config.get("notion", {})
        self.config_enabled = bool(cfg.get("enabled", True))
        self.api_base = cfg.get("api_base", "https://api.notion.com").rstrip("/")
        self.notion_version = cfg.get("notion_version", "2025-09-03")
        self.token = os.environ.get(cfg.get("auth_token_env", "NOTION_API_TOKEN"), "").strip()
        self.events_ds = _normalize_data_source_id(cfg.get("events_log_data_source_id", ""))
        self.milestones_ds = _normalize_data_source_id(cfg.get("milestones_data_source_id", ""))
        self.senators_ds = _normalize_data_source_id(cfg.get("senators_data_source_id", ""))
        self.market_ds = _normalize_data_source_id(cfg.get("market_signals_data_source_id", ""))
        self.errors: list[str] = []
        self._milestone_index: dict[str, str] | None = None
        self._senator_index: dict[str, str] | None = None

    @property
    def enabled(self) -> bool:
        return self.config_enabled and bool(self.token) and bool(self.events_ds)

    def _request(self, method: str, path: str, payload: dict | None = None) -> dict:
        return request_json(
            method,
            f"{self.api_base}{path}",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Notion-Version": self.notion_version,
            },
            payload=payload,
        )

    def _query_all(self, data_source_id: str, body: dict | None = None) -> list[dict]:
        pages: list[dict] = []
        cursor: str | None = None
        while True:
            request_body: dict = {"page_size": 100, **(body or {})}
            if cursor:
                request_body["start_cursor"] = cursor
            response = self._request(
                "POST", f"/v1/data_sources/{data_source_id}/query", request_body
            )
            pages.extend(response.get("results", []))
            if not response.get("has_more"):
                return pages
            cursor = response.get("next_cursor")

    @staticmethod
    def _plain_title(page: dict) -> str:
        for prop in page.get("properties", {}).values():
            if prop.get("type") == "title":
                return "".join(part.get("plain_text", "") for part in prop.get("title", []))
        return ""

    @staticmethod
    def _plain_select(page: dict, name: str) -> str:
        prop = page.get("properties", {}).get(name, {})
        return (prop.get("select") or {}).get("name", "")

    def _milestone_ids(self, stages: list[str]) -> list[str]:
        if self._milestone_index is None:
            self._milestone_index = {}
            if self.milestones_ds:
                for page in self._query_all(self.milestones_ds):
                    stage = self._plain_select(page, "Stage")
                    if stage:
                        self._milestone_index[stage] = page["id"]
        return [self._milestone_index[s] for s in stages if s in self._milestone_index]

    def _senator_ids(self, names: list[str]) -> list[str]:
        if self._senator_index is None:
            self._senator_index = {}
            if self.senators_ds:
                for page in self._query_all(self.senators_ds):
                    title = self._plain_title(page).strip()
                    if title:
                        self._senator_index[title.lower()] = page["id"]
        return [self._senator_index[n.lower()] for n in names if n.lower() in self._senator_index]

    def _existing_events(self, keys: list[str]) -> dict[str, str]:
        """Map of Event Key -> page id for already-synced events."""
        existing: dict[str, str] = {}
        wanted = set(keys)
        for page in self._query_all(self.events_ds):
            prop = page.get("properties", {}).get("Event Key", {})
            key = "".join(part.get("plain_text", "") for part in prop.get("rich_text", []))
            if key in wanted:
                existing[key] = page["id"]
        return existing

    def sync_events(self, events: list[ClassifiedEvent]) -> int:
        """Upsert material events into the Events Log. Returns rows written."""
        if not self.enabled:
            self.errors.append("notion sync skipped: token or events data source unset")
            return 0
        material = [item for item in events if item.material]
        if not material:
            return 0
        try:
            existing = self._existing_events([item.event.dedup_key() for item in material])
        except HttpJsonError as exc:
            self.errors.append(f"events log query failed: {exc}")
            return 0

        written = 0
        for item in material:
            try:
                properties = self._event_properties(item)
            except HttpJsonError as exc:
                self.errors.append(f"relation lookup failed: {exc}")
                continue
            key = item.event.dedup_key()
            try:
                if key in existing:
                    self._request("PATCH", f"/v1/pages/{existing[key]}", {"properties": properties})
                else:
                    self._request(
                        "POST",
                        "/v1/pages",
                        {"parent": {"data_source_id": self.events_ds}, "properties": properties},
                    )
                written += 1
            except HttpJsonError as exc:
                self.errors.append(f"event '{item.event.title}' sync failed: {exc}")
        return written

    def _event_properties(self, item: ClassifiedEvent) -> dict:
        event = item.event
        properties: dict[str, dict] = {
            "Title": {"title": _rich_text(event.title) or [{"type": "text", "text": {"content": "(untitled)"}}]},
            "Event Type": {"select": {"name": event.event_type}},
            "Source Authority": {"select": {"name": event.source_authority}},
            "Description": {"rich_text": _rich_text(item.summary_cn or event.description)},
            "Material?": {"checkbox": item.material},
            "Impact Score": {"number": round(item.score, 2)},
            "Raw Snippet": {"rich_text": _rich_text(event.raw_snippet or event.description)},
            "Event Key": {"rich_text": _rich_text(event.dedup_key())},
        }
        if event.occurred_at is not None:
            properties["Date"] = {"date": {"start": event.occurred_at.date().isoformat()}}
        if event.url:
            properties["Source URL"] = {"url": event.url}
        milestone_ids = self._milestone_ids(item.affects_milestones)
        if milestone_ids:
            properties["Affects Which Milestone"] = {
                "relation": [{"id": page_id} for page_id in milestone_ids]
            }
        senator_ids = self._senator_ids(item.affects_senators)
        if senator_ids:
            properties["Affects Which Senator"] = {
                "relation": [{"id": page_id} for page_id in senator_ids]
            }
        return properties

    def append_market_snapshot(self, snapshot: MarketSnapshot) -> bool:
        """Append one row to the Market & Analyst Signals database."""
        if not self.enabled or not self.market_ds:
            return False
        captured = snapshot.captured_at.date().isoformat()
        properties: dict[str, dict] = {
            "Snapshot": {"title": [{"type": "text", "text": {"content": f"Snapshot {captured}"}}]},
            "Date": {"date": {"start": captured}},
        }
        number_fields = {
            "Polymarket Signed 2026 Odds": snapshot.polymarket_signed_2026,
            "Polymarket Pass Senate by July Odds": snapshot.polymarket_pass_senate_july,
            "Kalshi Equivalent": snapshot.kalshi_equivalent,
            "BTC Price": snapshot.btc_price,
            "COIN Price": snapshot.coin_price,
        }
        for name, value in number_fields.items():
            if value is not None:
                properties[name] = {"number": value}
        if snapshot.analyst_note:
            properties["Analyst Note"] = {"rich_text": _rich_text(snapshot.analyst_note)}
        try:
            self._request(
                "POST",
                "/v1/pages",
                {"parent": {"data_source_id": self.market_ds}, "properties": properties},
            )
            return True
        except HttpJsonError as exc:
            self.errors.append(f"market snapshot sync failed: {exc}")
            return False
