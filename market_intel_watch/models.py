from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import re
from urllib.parse import urlparse, urlunparse


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    cleaned = parsed._replace(fragment="", params="", query="")
    return urlunparse(cleaned).rstrip("/")


@dataclass(slots=True)
class SourceDocument:
    source_id: str
    channel: str
    title: str
    url: str
    published_at: datetime | None
    summary: str = ""
    content: str = ""
    authors: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, str] = field(default_factory=dict)

    def text_blob(self) -> str:
        parts = [
            self.title,
            self.summary,
            self.content,
            " ".join(self.tags),
            " ".join(self.authors),
        ]
        return normalize_whitespace(" ".join(part for part in parts if part))

    def stable_key(self) -> str:
        token = normalize_url(self.url) or normalize_whitespace(self.title).lower()
        return hashlib.sha256(token.encode("utf-8")).hexdigest()


@dataclass(slots=True)
class WatchEntity:
    name: str
    aliases: list[str]
    entity_type: str
    geography: str
    priority: int = 1
    tags: list[str] = field(default_factory=list)


@dataclass(slots=True)
class Signal:
    event_type: str
    title: str
    summary: str
    url: str
    source_id: str
    channel: str
    published_at: datetime | None
    matched_entities: list[str] = field(default_factory=list)
    geography: str = "unknown"
    score: float = 0.0
    rationale: list[str] = field(default_factory=list)
    metadata: dict[str, str] = field(default_factory=dict)
    company_name: str = ""
    key_people: list[str] = field(default_factory=list)
    amount: str = ""
    round_stage: str = ""
    investors: list[str] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)
    cluster_key: str = ""
    source_count: int = 1
    supporting_urls: list[str] = field(default_factory=list)
    follow_verdict: str = "Monitor"
    follow_reason: str = ""
    suggested_action: str = ""
    confidence: float = 0.0

    def stable_key(self) -> str:
        token = self.cluster_key or f"{self.event_type}|{normalize_url(self.url)}|{normalize_whitespace(self.title).lower()}"
        return hashlib.sha256(token.encode("utf-8")).hexdigest()


@dataclass(slots=True)
class DailyRunResult:
    run_date: datetime
    documents_fetched: int
    documents_deduped: int
    signals: list[Signal]
    errors: list[str]
    report_text: str
