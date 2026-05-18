from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import re


_WHITESPACE_RE = re.compile(r"\s+")

# Events Log "Event Type" select options in the Notion tracker.
EVENT_TYPES = (
    "Official Action",
    "Markup",
    "Vote",
    "Hearing",
    "Statement",
    "Amendment",
    "Media Report",
    "Market Signal",
)

# Events Log "Source Authority" select options.
SOURCE_AUTHORITIES = (
    "Congress.gov",
    "Committee",
    "Senator",
    "White House",
    "Media",
    "Market",
)

# Senator Position Tracker "Current Stance" select options.
STANCES = ("Yes", "Lean Yes", "Swing", "Lean No", "No", "Unknown")

# Recommended push tiers returned by the classifier.
RECOMMENDED_ACTIONS = ("notify_now", "weekly_digest", "skip")


def normalize(value: str) -> str:
    return _WHITESPACE_RE.sub(" ", (value or "").strip().lower())


@dataclass(slots=True)
class RawEvent:
    """An unclassified record pulled from a single source."""

    source: str
    source_authority: str
    title: str
    description: str
    url: str
    occurred_at: datetime | None
    event_type: str = "Media Report"
    raw_snippet: str = ""
    metadata: dict[str, str] = field(default_factory=dict)

    def content_hash(self) -> str:
        token = "|".join(normalize(part) for part in (self.title, self.description, self.url))
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def dedup_key(self) -> str:
        """Stable key used for both the dedup store and Notion upserts."""
        return f"{self.source}:{self.content_hash()}"

    @property
    def auto_material(self) -> bool:
        """True when the event bypasses the LLM and is always pushed."""
        return self.metadata.get("auto_material") == "true"


@dataclass(slots=True)
class ClassifiedEvent:
    """A RawEvent enriched with a materiality verdict."""

    event: RawEvent
    score: float
    material: bool
    category: str
    affects_milestones: list[str] = field(default_factory=list)
    affects_senators: list[str] = field(default_factory=list)
    summary_cn: str = ""
    recommended_action: str = "weekly_digest"
    confidence: float = 0.0
    classifier: str = "rules"

    @property
    def tier(self) -> str:
        if self.score >= 4:
            return "notify_now"
        if self.score >= 2:
            return "weekly_digest"
        return "skip"


@dataclass(slots=True)
class MarketSnapshot:
    """One point in the prediction-market / price time series."""

    captured_at: datetime
    polymarket_signed_2026: float | None = None
    polymarket_pass_senate_july: float | None = None
    kalshi_equivalent: float | None = None
    btc_price: float | None = None
    coin_price: float | None = None
    analyst_note: str = ""

    def odds(self) -> dict[str, float]:
        """Probability fields that are present, keyed by name."""
        fields = {
            "polymarket_signed_2026": self.polymarket_signed_2026,
            "polymarket_pass_senate_july": self.polymarket_pass_senate_july,
            "kalshi_equivalent": self.kalshi_equivalent,
        }
        return {key: value for key, value in fields.items() if value is not None}


@dataclass(slots=True)
class SenatorPosition:
    name: str
    party_state: str
    committee_member: bool
    stance: str
    committee_vote: str
    note: str
    confidence: float


@dataclass(slots=True)
class Milestone:
    name: str
    stage: str
    status: str
    required_threshold: str
    notes: str
    target_date: str = ""
    actual_date: str = ""
    vote_tally: str = ""
    source_url: str = ""


@dataclass(slots=True)
class MonitorRunResult:
    run_at: datetime
    raw_events: int
    new_events: int
    classified: list[ClassifiedEvent]
    market: MarketSnapshot | None
    errors: list[str] = field(default_factory=list)
    digest_path: str = ""

    def by_tier(self, tier: str) -> list[ClassifiedEvent]:
        return [item for item in self.classified if item.tier == tier]
