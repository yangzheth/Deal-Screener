from __future__ import annotations

from collections import Counter
from datetime import date

from market_intel_watch.models import Signal


EVENT_LABELS = {
    "funding": "Funding Signals",
    "talent_departure": "Talent Departure Signals",
    "talent_hire": "Talent Hire Signals",
}


URGENT_EVENT_SHORT_LABELS = {
    "funding": "Funding",
    "talent_departure": "Departure",
    "talent_hire": "Hire",
}


def _format_list(values: list[str], fallback: str = "unknown") -> str:
    return ", ".join(values) if values else fallback


def _dedupe_urgent(signals: list[Signal]) -> list[tuple[Signal, list[str]]]:
    grouped: dict[str, tuple[Signal, list[str]]] = {}
    order: list[str] = []
    for signal in signals:
        key = signal.url or signal.title
        existing = grouped.get(key)
        if existing is None:
            grouped[key] = (signal, [signal.event_type])
            order.append(key)
            continue
        primary, event_types = existing
        if signal.event_type not in event_types:
            event_types.append(signal.event_type)
        if signal.score > primary.score:
            grouped[key] = (signal, event_types)
    return [grouped[key] for key in order]


def render_markdown_report(
    run_date: date,
    documents_fetched: int,
    documents_deduped: int,
    signals: list[Signal],
    errors: list[str],
) -> str:
    counts = Counter(signal.event_type for signal in signals)
    urgent = [signal for signal in signals if signal.follow_verdict == "Must Chase"]
    lines: list[str] = [
        "# AI Primary Market Watch",
        "",
        f"Run date: {run_date.isoformat()}",
        "",
        "## Snapshot",
        f"- Documents fetched: {documents_fetched}",
        f"- Documents after dedupe: {documents_deduped}",
        f"- Signals detected: {len(signals)}",
        f"- Funding signals: {counts.get('funding', 0)}",
        f"- Talent departure signals: {counts.get('talent_departure', 0)}",
        f"- Talent hire signals: {counts.get('talent_hire', 0)}",
        f"- Must-chase items: {len(urgent)}",
        "",
    ]

    if urgent:
        lines.extend(["## Immediate Follow-Up", ""])
        for signal, event_types in _dedupe_urgent(urgent)[:8]:
            labels = "/".join(URGENT_EVENT_SHORT_LABELS.get(event, event) for event in event_types)
            lines.append(f"- [{signal.title}]({signal.url})")
            lines.append(
                f"  Events: {labels} | Verdict: {signal.follow_verdict} | Company: {signal.company_name or 'unknown'} | Score: {signal.score:.1f} | Sources: {signal.source_count}"
            )
            lines.append(f"  Action: {signal.suggested_action}")
        lines.append("")

    if errors:
        lines.extend(["## Warnings", ""])
        for error in errors:
            lines.append(f"- {error}")
        lines.append("")

    for event_type in ("talent_departure", "funding", "talent_hire"):
        section_signals = [signal for signal in signals if signal.event_type == event_type]
        if not section_signals:
            continue
        lines.extend([f"## {EVENT_LABELS[event_type]}", ""])
        for index, signal in enumerate(section_signals, start=1):
            entities = ", ".join(signal.matched_entities) if signal.matched_entities else "unmatched"
            published = signal.published_at.isoformat() if signal.published_at else "unknown"
            lines.append(f"{index}. [{signal.title}]({signal.url})")
            lines.append(
                f"   Score: {signal.score:.1f} | Verdict: {signal.follow_verdict} | Confidence: {signal.confidence:.2f} | Geography: {signal.geography}"
            )
            lines.append(
                f"   Company: {signal.company_name or 'unknown'} | People: {_format_list(signal.key_people)} | Entities: {entities}"
            )
            lines.append(
                f"   Round: {signal.round_stage or 'unknown'} | Amount: {signal.amount or 'unknown'} | Investors: {_format_list(signal.investors)}"
            )
            lines.append(
                f"   Category: {_format_list(signal.categories)} | Sources: {signal.source_count} | Source IDs: {signal.source_id or 'unknown'}"
            )
            lines.append(f"   Published: {published}")
            if signal.summary:
                lines.append(f"   Summary: {signal.summary}")
            if signal.follow_reason:
                lines.append(f"   Why Track: {signal.follow_reason}")
            if signal.suggested_action:
                lines.append(f"   Suggested Action: {signal.suggested_action}")
            if signal.rationale:
                lines.append(f"   Why: {', '.join(signal.rationale)}")
            lines.append("")

    if not signals:
        lines.extend(
            [
                "## No Signals",
                "",
                "No AI fundraising or talent-move items passed the current rule set.",
                "",
            ]
        )

    lines.extend(
        [
            "## Next Actions",
            "",
            "- Review all Must Chase items first and link them to Companies or Deal Pipeline.",
            "- Confirm funding amounts, investor syndicates, and category tags before moving a deal into AI Investment Tracker.",
            "- Keep lower-confidence items in the watch inbox until a second source or stronger context appears.",
            "",
        ]
    )

    return "\n".join(lines)
