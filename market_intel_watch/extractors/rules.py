from __future__ import annotations

from datetime import date, datetime
import re

from market_intel_watch.models import Signal, SourceDocument, WatchEntity


EVENT_PATTERNS = {
    "funding": [
        re.compile(
            r"\b(raised|raising|funding|financing|seed|series [a-f]|valuation|backed by|led by)\b",
            re.IGNORECASE,
        ),
        re.compile(r"\b(angel round|pre-seed|pre-a|series a|series b|series c)\b", re.IGNORECASE),
        re.compile("(\u878d\u8d44|\u83b7\u6295|\u5b8c\u6210.*\u8f6e|\u79cd\u5b50\u8f6e|\u5929\u4f7f\u8f6e|\u6218\u7565\u6295\u8d44)"),
    ],
    "talent_departure": [
        re.compile(
            r"\b(resigns?|resigned|steps? down|leaves?|left|departed|quit|quits|exit|exits)\b",
            re.IGNORECASE,
        ),
        re.compile("(\u79bb\u804c|\u8f9e\u4efb|\u5378\u4efb|\u79bb\u5f00|\u51fa\u8d70|\u4e0d\u518d\u62c5\u4efb)"),
    ],
    "talent_hire": [
        re.compile(
            r"\b(joins?|joined|hired|appoint(?:ed|s)?|poached|recruited|named as)\b",
            re.IGNORECASE,
        ),
        re.compile("(\u52a0\u5165|\u52a0\u76df|\u4efb\u547d|\u51fa\u4efb|\u6316\u89d2|\u6316\u6765|\u5165\u804c)"),
    ],
}

GEOGRAPHY_HINTS = {
    "CN": [
        "china",
        "\u4e2d\u56fd",
        "beijing",
        "\u5317\u4eac",
        "shanghai",
        "\u4e0a\u6d77",
        "shenzhen",
        "\u6df1\u5733",
        "hangzhou",
        "\u676d\u5dde",
        "guangzhou",
        "\u5e7f\u5dde",
        "hong kong",
        "\u9999\u6e2f",
    ],
    "US": [
        "united states",
        "\u7f8e\u56fd",
        "us startup",
        "silicon valley",
        "san francisco",
        "palo alto",
        "new york",
        "seattle",
        "boston",
    ],
}

EVENT_BASE_SCORES = {
    "funding": 70.0,
    "talent_departure": 75.0,
    "talent_hire": 60.0,
}


class RuleBasedSignalExtractor:
    def __init__(
        self,
        entities: list[WatchEntity],
        ai_keywords: list[str],
        source_weights: dict[str, int],
        run_date: date,
    ) -> None:
        self.entities = entities
        self.ai_keywords = [keyword.lower() for keyword in ai_keywords]
        self.source_weights = source_weights
        self.run_date = run_date

    def extract(self, document: SourceDocument) -> list[Signal]:
        text = document.text_blob()
        normalized = text.lower()
        event_types = self._detect_event_types(normalized)
        if not event_types:
            return []

        matched_entities = self._match_entities(normalized)
        if not self._is_ai_relevant(normalized, matched_entities):
            return []

        geography = self._infer_geography(normalized, matched_entities)
        signals: list[Signal] = []
        for event_type in event_types:
            score, rationale = self._score(document, event_type, matched_entities, geography)
            signals.append(
                Signal(
                    event_type=event_type,
                    title=document.title,
                    summary=document.summary or document.content[:400],
                    url=document.url,
                    source_id=document.source_id,
                    channel=document.channel,
                    published_at=document.published_at,
                    matched_entities=[entity.name for entity in matched_entities],
                    geography=geography,
                    score=score,
                    rationale=rationale,
                    metadata=document.metadata,
                )
            )
        return signals

    def _detect_event_types(self, text: str) -> list[str]:
        matches: list[str] = []
        for event_type in ("talent_departure", "funding", "talent_hire"):
            for pattern in EVENT_PATTERNS[event_type]:
                if pattern.search(text):
                    matches.append(event_type)
                    break
        return matches

    def _match_entities(self, text: str) -> list[WatchEntity]:
        matches: list[WatchEntity] = []
        for entity in self.entities:
            aliases = [entity.name, *entity.aliases]
            for alias in aliases:
                alias_lower = alias.lower().strip()
                if not alias_lower or len(alias_lower) < 3:
                    continue
                if alias_lower in text:
                    matches.append(entity)
                    break
        matches.sort(key=lambda item: item.priority, reverse=True)
        deduped: list[WatchEntity] = []
        seen: set[str] = set()
        for entity in matches:
            if entity.name in seen:
                continue
            seen.add(entity.name)
            deduped.append(entity)
        return deduped

    def _is_ai_relevant(self, text: str, entities: list[WatchEntity]) -> bool:
        if any("ai" in entity.tags for entity in entities):
            return True
        return any(keyword in text for keyword in self.ai_keywords)

    def _infer_geography(self, text: str, entities: list[WatchEntity]) -> str:
        if entities:
            return entities[0].geography
        for geography, hints in GEOGRAPHY_HINTS.items():
            for hint in hints:
                if hint in text:
                    return geography
        return "unknown"

    def _score(
        self,
        document: SourceDocument,
        event_type: str,
        entities: list[WatchEntity],
        geography: str,
    ) -> tuple[float, list[str]]:
        score = EVENT_BASE_SCORES[event_type]
        rationale = [f"event={event_type}"]

        source_weight = self._source_weight(document)
        if source_weight:
            score += float(source_weight)
            rationale.append(f"source_weight={source_weight}")

        if entities:
            entity_score = sum(4 + (entity.priority * 2) for entity in entities[:3])
            score += float(entity_score)
            rationale.append(f"watchlist_hits={len(entities[:3])}")

        if geography in {"CN", "US"}:
            score += 5.0
            rationale.append(f"market={geography}")

        recency_bonus = self._recency_bonus(document.published_at)
        if recency_bonus:
            score += recency_bonus
            rationale.append(f"recency_bonus={recency_bonus}")

        return min(score, 100.0), rationale

    def _recency_bonus(self, published_at: datetime | None) -> float:
        if not published_at:
            return 0.0
        age_days = (self.run_date - published_at.date()).days
        if age_days <= 1:
            return 5.0
        if age_days <= 3:
            return 2.0
        return 0.0

    def _source_weight(self, document: SourceDocument) -> int:
        candidates = [
            document.channel,
            document.source_id,
            document.metadata.get("source_type", ""),
            document.channel.replace("-", "_"),
            document.source_id.replace("-", "_"),
        ]
        for candidate in candidates:
            if candidate in self.source_weights:
                return int(self.source_weights[candidate])
        return 0
