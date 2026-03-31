from __future__ import annotations

from collections import Counter
from datetime import date

from market_intel_watch.models import Signal


EVENT_LABELS = {
    "funding": "Funding Signals",
    "talent_departure": "Talent Departure Signals",
    "talent_hire": "Talent Hire Signals",
}


def render_markdown_report(
    run_date: date,
    documents_fetched: int,
    documents_deduped: int,
    signals: list[Signal],
    errors: list[str],
) -> str:
    counts = Counter(signal.event_type for signal in signals)
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
        "",
    ]

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
                f"   Score: {signal.score:.1f} | Geography: {signal.geography} | Entities: {entities}"
            )
            lines.append(
                f"   Source: {signal.source_id} ({signal.channel}) | Published: {published}"
            )
            if signal.summary:
                lines.append(f"   Summary: {signal.summary}")
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
            "- Tighten the watchlist around companies, founders, and operators you care about most.",
            "- Add manual drops from WeChat / Xiaohongshu / Maimai once you have an export workflow.",
            "- Route only high-score items into real-time push; keep everything else in the daily digest.",
            "",
        ]
    )

    return "\n".join(lines)
