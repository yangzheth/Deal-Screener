from __future__ import annotations

from datetime import date, datetime
import re

from market_intel_watch.models import Signal, SourceDocument, WatchEntity, normalize_whitespace


FUNDING_PATTERNS = [
    re.compile(
        r"\b(raised|raising|funding|financing|seed|series [a-f]|valuation|backed by|led by|strategic investment)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\b(angel round|pre-seed|pre-a|series a|series b|series c|series d|series e)\b", re.IGNORECASE),
    re.compile("(\u878d\u8d44|\u83b7\u6295|\u5b8c\u6210.*\u8f6e|\u79cd\u5b50\u8f6e|\u5929\u4f7f\u8f6e|\u6218\u7565\u6295\u8d44)"),
]

DEPARTURE_STRONG_PATTERNS = [
    re.compile(r"\b(resigns?|resigned|steps? down|stepped down|departed|quit|quits)\b", re.IGNORECASE),
    re.compile("(\u79bb\u804c|\u8f9e\u4efb|\u5378\u4efb|\u4e0d\u518d\u62c5\u4efb)"),
]

DEPARTURE_WEAK_PATTERNS = [
    re.compile(r"\b(leaves?|left|exit|exits)\b", re.IGNORECASE),
    re.compile("(\u79bb\u5f00|\u51fa\u8d70)"),
]

DEPARTURE_EXCLUSION_PATTERNS = [
    re.compile(r"\bleft (details|out|behind|open|unanswered|untouched)\b", re.IGNORECASE),
]

HIRE_STRONG_PATTERNS = [
    re.compile(r"\b(hired|appoint(?:ed|s)?|poached|recruited|named as|named)\b", re.IGNORECASE),
    re.compile("(\u4efb\u547d|\u51fa\u4efb|\u6316\u89d2|\u6316\u6765|\u5165\u804c)"),
]

HIRE_WEAK_PATTERNS = [
    re.compile(r"\b(joins?|joined)\b", re.IGNORECASE),
    re.compile("(\u52a0\u5165|\u52a0\u76df)"),
]

HIRE_EXCLUSION_PATTERNS = [
    re.compile(
        r"\b(joins?|joined)\s+(the\s+)?(funding|financing|round|raise|investment|bid)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\b(joins?|joined)\s+(in|for)\s+(the\s+)?(funding|financing|round|raise)\b", re.IGNORECASE),
]

TALENT_CONTEXT_PATTERNS = [
    re.compile(
        r"\b(founder|cofounder|co-founder|executive|researcher|employee|operator|partner|chair|chief|"
        r"ceo|cto|cfo|coo|chief scientist|president|vp|vice president|director|manager|lead)\b",
        re.IGNORECASE,
    ),
    re.compile("(\u521b\u59cb\u4eba|\u8054\u5408\u521b\u59cb\u4eba|\u9ad8\u7ba1|\u7814\u7a76\u5458|\u5458\u5de5|\u5408\u4f19\u4eba|\u8463\u4e8b|\u603b\u88c1|\u603b\u76d1|\u526f\u603b\u88c1|\u8d1f\u8d23\u4eba)"),
]

FUNDING_COMPANY_PATTERNS = [
    re.compile(r"(?P<company>[A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*){0,4})\s+(?:raises?|raised|raising|secures?|secured|lands?|bagged?)\b"),
    re.compile(r"for\s+(?P<company>[A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Za-z0-9&.\-]+){0,4})'?s\s+(?:\$|US\$|USD|RMB|\u00a5|\d)", re.IGNORECASE),
    re.compile(r"(?P<company>[\u4e00-\u9fffA-Za-z0-9&.\-]{2,40})\s*(?:\u5b8c\u6210|\u83b7|\u83b7\u5f97|\u5ba3\u5e03\u5b8c\u6210).{0,18}(?:\u878d\u8d44|\u6295\u8d44)"),
]

TALENT_COMPANY_PATTERNS = [
    re.compile(r"joins?\s+(?P<company>[A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Za-z0-9&.\-]+){0,4})\s+(?:as|to|from)\b", re.IGNORECASE),
    re.compile(r"(?:leaves?|left|departed|resigned from|stepped down from)\s+(?P<company>[A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Za-z0-9&.\-]+){0,4}?)(?=\s+(?:as|after|to|for|with|at)\b|[.,;:]|$)", re.IGNORECASE),
    re.compile(r"(?P<company>[A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Za-z0-9&.\-]+){0,4})\s+(?:appoint(?:ed|s)?|hires?|hired|names?|named)\b", re.IGNORECASE),
    re.compile(r"\u52a0\u5165(?P<company>[\u4e00-\u9fffA-Za-z0-9&.\-]{2,30})"),
    re.compile(r"(?P<company>[\u4e00-\u9fffA-Za-z0-9&.\-]{2,30})(?:\u4efb\u547d|\u8058\u4efb|\u8f9e\u4efb|\u79bb\u804c)"),
]

PERSON_PATTERNS = [
    re.compile(r"(?P<person>[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+(?:joins?|joined|leaves?|left|departed|resigned|stepped down|appointed|named|hired)\b"),
    re.compile(r"(?:appointed|named|hired)\s+(?P<person>[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\b", re.IGNORECASE),
]

AMOUNT_PATTERNS = [
    re.compile(r"(?:US\$|USD\s?|\$)\s?\d+(?:\.\d+)?\s?(?:[MBK]|million|billion)", re.IGNORECASE),
    re.compile(r"(?:RMB|CNY|\u00a5)\s?\d+(?:\.\d+)?\s?(?:[MBK]|million|billion)?", re.IGNORECASE),
    re.compile(r"\d+(?:\.\d+)?\s?(?:million|billion)\s?(?:USD|dollars?)", re.IGNORECASE),
    re.compile(r"[\d\.]+\s*(?:\u4ebf|\u4e07)\s*(?:\u7f8e\u5143|\u5143|\u4eba\u6c11\u5e01)"),
]

ROUND_STAGE_PATTERNS = [
    (re.compile(r"\bpre-seed\b", re.IGNORECASE), "Pre-Seed"),
    (re.compile(r"\bseed\b", re.IGNORECASE), "Seed"),
    (re.compile(r"\bangel\b", re.IGNORECASE), "Angel"),
    (re.compile(r"\bpre-a\b", re.IGNORECASE), "Pre-A"),
    (re.compile(r"\bseries a\b|\ba round\b", re.IGNORECASE), "Series A"),
    (re.compile(r"\bseries b\b|\bb round\b", re.IGNORECASE), "Series B"),
    (re.compile(r"\bseries c\b|\bc round\b", re.IGNORECASE), "Series C"),
    (re.compile(r"\bseries d\b|\bd round\b", re.IGNORECASE), "Series D+"),
    (re.compile(r"\bstrategic\b", re.IGNORECASE), "Strategic"),
    (re.compile("\u79cd\u5b50\u8f6e"), "Seed"),
    (re.compile("\u5929\u4f7f\u8f6e"), "Angel"),
    (re.compile(r"pre[- ]?A", re.IGNORECASE), "Pre-A"),
    (re.compile("A\u8f6e"), "Series A"),
    (re.compile("B\u8f6e"), "Series B"),
    (re.compile("C\u8f6e"), "Series C"),
    (re.compile("\u6218\u7565\u6295\u8d44"), "Strategic"),
]

INVESTOR_PATTERNS = [
    re.compile(r"(?:led by|co-led by|backed by|with participation from|including)\s+(?P<investors>[^.;:]+)", re.IGNORECASE),
    re.compile(r"\u7531(?P<investors>[^\uff0c\u3002\uff1b]{2,60})\u9886\u6295"),
    re.compile(r"\u6295\u8d44\u65b9\u5305\u62ec(?P<investors>[^\uff0c\u3002\uff1b]{2,80})"),
]

CATEGORY_HINTS = {
    "Agent": ["agent", "copilot", "assistant", "workflow automation", "agentic", "\u667a\u80fd\u4f53", "AI agent"],
    "Foundation Model": ["foundation model", "large language model", "llm", "multimodal model", "model lab", "\u5927\u6a21\u578b", "\u57fa\u7840\u6a21\u578b"],
    "Infra": ["inference", "gpu", "developer tool", "vector database", "mlops", "infra", "\u63a8\u7406", "\u7b97\u529b", "\u5de5\u5177\u94fe"],
    "Robotics": ["robot", "robotics", "autonomous", "embodied", "\u5177\u8eab", "\u673a\u5668\u4eba"],
    "Healthcare": ["healthcare", "medical", "biotech", "drug discovery", "\u533b\u7597", "\u5236\u836f"],
    "Enterprise": ["enterprise", "b2b", "sales", "workflow", "customer support", "\u4f01\u4e1a\u670d\u52a1", "\u529e\u516c"],
    "Consumer": ["consumer", "creator", "social", "video", "gaming", "education", "companion", "\u6d88\u8d39", "C\u7aef", "\u5185\u5bb9"],
}

EVENT_BASE_SCORES = {
    "funding": 70.0,
    "talent_departure": 75.0,
    "talent_hire": 60.0,
}

SENTENCE_SPLIT_RE = re.compile(r"[.!?;\n\r\u3002\uff01\uff1f\uff1b]+")
KEY_FRAGMENT_RE = re.compile(r"[^a-z0-9\u4e00-\u9fff]+")

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
        matched_entities = self._match_entities(normalized)
        event_types = self._detect_event_types(normalized, matched_entities)
        if not event_types:
            return []
        if not self._is_ai_relevant(normalized, matched_entities):
            return []

        geography = self._infer_geography(normalized, matched_entities)
        company_name = self._extract_company_name(text, normalized, matched_entities, event_types)
        key_people = self._extract_key_people(text, matched_entities)
        amount = self._extract_amount(text)
        round_stage = self._extract_round_stage(text)
        investors = self._extract_investors(text)
        categories = self._classify_categories(normalized)

        signals: list[Signal] = []
        for event_type in event_types:
            score, rationale = self._score(
                document,
                event_type,
                matched_entities,
                geography,
                amount=amount,
                round_stage=round_stage,
                investors=investors,
            )
            cluster_key = self._build_cluster_key(event_type, company_name, key_people, round_stage, amount, document.title)
            follow_verdict, follow_reason, suggested_action, confidence = self._assess_follow_up(
                text=normalized,
                event_type=event_type,
                score=score,
                matched_entities=matched_entities,
                company_name=company_name,
                key_people=key_people,
                amount=amount,
                round_stage=round_stage,
                investors=investors,
                categories=categories,
            )
            rationale = rationale + [f"follow_verdict={follow_verdict}"]
            if amount:
                rationale.append(f"amount={amount}")
            if round_stage:
                rationale.append(f"round={round_stage}")
            if investors:
                rationale.append(f"investors={', '.join(investors[:3])}")
            if categories:
                rationale.append(f"category={', '.join(categories)}")

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
                    company_name=company_name,
                    key_people=key_people,
                    amount=amount,
                    round_stage=round_stage,
                    investors=investors,
                    categories=categories,
                    cluster_key=cluster_key,
                    supporting_urls=[document.url] if document.url else [],
                    follow_verdict=follow_verdict,
                    follow_reason=follow_reason,
                    suggested_action=suggested_action,
                    confidence=confidence,
                )
            )
        return signals

    def _detect_event_types(self, text: str, matched_entities: list[WatchEntity]) -> list[str]:
        matches: list[str] = []
        if self._detect_talent_departure(text, matched_entities):
            matches.append("talent_departure")
        if self._detect_funding(text):
            matches.append("funding")
        if self._detect_talent_hire(text, matched_entities):
            matches.append("talent_hire")
        return matches

    def _detect_funding(self, text: str) -> bool:
        return any(pattern.search(text) for pattern in FUNDING_PATTERNS)

    def _detect_talent_departure(self, text: str, matched_entities: list[WatchEntity]) -> bool:
        return self._detect_talent_event(
            text,
            matched_entities,
            strong_patterns=DEPARTURE_STRONG_PATTERNS,
            weak_patterns=DEPARTURE_WEAK_PATTERNS,
            exclusion_patterns=DEPARTURE_EXCLUSION_PATTERNS,
        )

    def _detect_talent_hire(self, text: str, matched_entities: list[WatchEntity]) -> bool:
        return self._detect_talent_event(
            text,
            matched_entities,
            strong_patterns=HIRE_STRONG_PATTERNS,
            weak_patterns=HIRE_WEAK_PATTERNS,
            exclusion_patterns=HIRE_EXCLUSION_PATTERNS,
        )

    def _detect_talent_event(
        self,
        text: str,
        matched_entities: list[WatchEntity],
        *,
        strong_patterns: list[re.Pattern[str]],
        weak_patterns: list[re.Pattern[str]],
        exclusion_patterns: list[re.Pattern[str]],
    ) -> bool:
        for sentence in self._split_sentences(text):
            if any(pattern.search(sentence) for pattern in exclusion_patterns):
                continue
            if any(pattern.search(sentence) for pattern in strong_patterns):
                return True
            if not any(pattern.search(sentence) for pattern in weak_patterns):
                continue
            if self._has_talent_context(sentence, matched_entities):
                return True
        return False

    def _split_sentences(self, text: str) -> list[str]:
        sentences = [part.strip() for part in SENTENCE_SPLIT_RE.split(text) if part.strip()]
        return sentences or [text]

    def _has_talent_context(self, text: str, matched_entities: list[WatchEntity]) -> bool:
        if any(pattern.search(text) for pattern in TALENT_CONTEXT_PATTERNS):
            return True
        if any(entity.entity_type == "person" and self._contains_entity_alias(text, entity) for entity in matched_entities):
            return True
        return any(self._mentions_entity_transition(text, entity) for entity in matched_entities)

    def _contains_entity_alias(self, text: str, entity: WatchEntity) -> bool:
        return any(alias in text for alias in self._normalized_aliases(entity))

    def _mentions_entity_transition(self, text: str, entity: WatchEntity) -> bool:
        for alias in self._normalized_aliases(entity):
            escaped = re.escape(alias)
            if re.search(rf"\b(left|leaves?|exit|exits|joins?|joined)\s+{escaped}\b", text):
                return True
            if re.search(
                rf"\b{escaped}\s+(hired|hires|appoint(?:ed|s)?|named|recruited|poached)\b",
                text,
            ):
                return True
        return False

    def _normalized_aliases(self, entity: WatchEntity) -> list[str]:
        aliases = [entity.name, *entity.aliases]
        return [alias.lower().strip() for alias in aliases if alias and len(alias.strip()) >= 3]

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

    def _extract_company_name(
        self,
        text: str,
        normalized: str,
        matched_entities: list[WatchEntity],
        event_types: list[str],
    ) -> str:
        patterns = FUNDING_COMPANY_PATTERNS if "funding" in event_types else TALENT_COMPANY_PATTERNS
        for pattern in patterns:
            match = pattern.search(text)
            if not match:
                continue
            company = self._clean_capture(match.group("company"))
            if self._looks_like_company(company):
                return company

        company_entities = [entity.name for entity in matched_entities if entity.entity_type == "company"]
        if company_entities:
            return company_entities[0]

        if "company" in normalized:
            return ""
        return ""

    def _extract_key_people(self, text: str, matched_entities: list[WatchEntity]) -> list[str]:
        people = [entity.name for entity in matched_entities if entity.entity_type == "person"]
        for pattern in PERSON_PATTERNS:
            for match in pattern.finditer(text):
                people.append(self._clean_capture(match.group("person")))
        return self._unique_preserve(people)

    def _extract_amount(self, text: str) -> str:
        for pattern in AMOUNT_PATTERNS:
            match = pattern.search(text)
            if match:
                return normalize_whitespace(match.group(0))
        return ""

    def _extract_round_stage(self, text: str) -> str:
        for pattern, label in ROUND_STAGE_PATTERNS:
            if pattern.search(text):
                return label
        return ""

    def _extract_investors(self, text: str) -> list[str]:
        matches: list[str] = []
        for pattern in INVESTOR_PATTERNS:
            match = pattern.search(text)
            if not match:
                continue
            raw = match.group("investors")
            for chunk in re.split(r",| and |;|\u3001|\uff0c", raw):
                cleaned = self._clean_capture(chunk)
                if not cleaned:
                    continue
                cleaned = re.sub(r"\b(with participation from|including|others?)\b", "", cleaned, flags=re.IGNORECASE).strip()
                if len(cleaned) >= 2:
                    matches.append(cleaned)
        return self._unique_preserve(matches)[:5]

    def _classify_categories(self, text: str) -> list[str]:
        categories: list[str] = []
        for category, hints in CATEGORY_HINTS.items():
            if any(hint.lower() in text for hint in hints):
                categories.append(category)
        return categories

    def _score(
        self,
        document: SourceDocument,
        event_type: str,
        entities: list[WatchEntity],
        geography: str,
        *,
        amount: str,
        round_stage: str,
        investors: list[str],
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

        if amount:
            score += 3.0
        if round_stage:
            score += 3.0
        if investors:
            score += min(3.0, float(len(investors)))

        recency_bonus = self._recency_bonus(document.published_at)
        if recency_bonus:
            score += recency_bonus
            rationale.append(f"recency_bonus={recency_bonus}")

        return min(score, 100.0), rationale

    def _assess_follow_up(
        self,
        *,
        text: str,
        event_type: str,
        score: float,
        matched_entities: list[WatchEntity],
        company_name: str,
        key_people: list[str],
        amount: str,
        round_stage: str,
        investors: list[str],
        categories: list[str],
    ) -> tuple[str, str, str, float]:
        evidence_points = 0
        evidence_points += 1 if company_name else 0
        evidence_points += 1 if key_people else 0
        evidence_points += 1 if amount else 0
        evidence_points += 1 if round_stage else 0
        evidence_points += 1 if investors else 0
        evidence_points += 1 if matched_entities else 0
        evidence_points += 1 if categories else 0

        has_priority_watch = any(entity.priority >= 3 for entity in matched_entities)
        founder_context = bool(re.search(r"\b(founder|cofounder|co-founder)\b|\u521b\u59cb\u4eba|\u8054\u5408\u521b\u59cb\u4eba", text))

        if event_type == "funding":
            if score >= 88 and (amount or round_stage) and (has_priority_watch or company_name):
                verdict = "Must Chase"
            elif score >= 76 and company_name:
                verdict = "Worth Tracking"
            elif score >= 66:
                verdict = "Monitor"
            else:
                verdict = "Ignore"
        elif event_type == "talent_departure":
            if has_priority_watch or founder_context:
                verdict = "Must Chase"
            elif score >= 74:
                verdict = "Worth Tracking"
            elif score >= 64:
                verdict = "Monitor"
            else:
                verdict = "Ignore"
        else:
            if has_priority_watch and (key_people or re.search(r"\b(ceo|cto|cfo|coo|chief|vp|researcher|founder)\b", text)):
                verdict = "Worth Tracking"
            elif score >= 70:
                verdict = "Monitor"
            else:
                verdict = "Ignore"

        reason_map = {
            "Must Chase": "High-signal item with enough context to justify immediate follow-up.",
            "Worth Tracking": "Relevant signal with enough detail to compare against your investable list.",
            "Monitor": "Useful market context, but it still needs another confirming signal or deeper diligence.",
            "Ignore": "Low-conviction signal unless it links back to an active thesis, company, or founder on your radar.",
        }
        action_map = {
            "Must Chase": "Open the company record, verify the round or personnel move, and decide whether it should enter Deal Pipeline today.",
            "Worth Tracking": "Check the company, investor syndicate, and category fit, then link it to AI Investment Tracker or Companies if it is new.",
            "Monitor": "Keep it in the watch inbox and wait for another source, a round size, or more company context.",
            "Ignore": "Archive after a quick scan unless it touches an existing portfolio or investable company.",
        }
        confidence = min(0.95, 0.3 + (evidence_points * 0.08) + (0.08 if has_priority_watch else 0.0))
        return verdict, reason_map[verdict], action_map[verdict], round(confidence, 2)

    def _build_cluster_key(
        self,
        event_type: str,
        company_name: str,
        key_people: list[str],
        round_stage: str,
        amount: str,
        title: str,
    ) -> str:
        parts = [event_type]
        if company_name:
            parts.append(self._normalize_key_fragment(company_name))
        if key_people:
            parts.append(self._normalize_key_fragment(key_people[0]))
        if round_stage:
            parts.append(self._normalize_key_fragment(round_stage))
        elif amount and event_type == "funding":
            parts.append(self._normalize_key_fragment(amount))
        if len(parts) == 1:
            parts.append(self._normalize_key_fragment(title)[:40])
        return "|".join(part for part in parts if part)

    def _normalize_key_fragment(self, value: str) -> str:
        cleaned = KEY_FRAGMENT_RE.sub(" ", value.lower()).strip()
        return cleaned.replace(" ", "-")

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

    def _clean_capture(self, value: str) -> str:
        cleaned = normalize_whitespace(value.strip(" .,:;|[](){}'\""))
        cleaned = re.sub(r"'s$", "", cleaned, flags=re.IGNORECASE)
        return cleaned

    def _looks_like_company(self, value: str) -> bool:
        if not value or len(value) < 2 or len(value) > 60:
            return False
        lowered = value.lower()
        blocked = {"series a", "series b", "funding round", "startup", "company"}
        return lowered not in blocked

    def _unique_preserve(self, values: list[str]) -> list[str]:
        seen: set[str] = set()
        deduped: list[str] = []
        for value in values:
            cleaned = self._clean_capture(value)
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(cleaned)
        return deduped

