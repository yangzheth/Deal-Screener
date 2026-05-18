from __future__ import annotations

from pathlib import Path

from market_intel_watch.monitors.clarity_act.http_util import HttpJsonError, request_json
from market_intel_watch.monitors.clarity_act.models import (
    ClassifiedEvent,
    MarketSnapshot,
    MonitorRunResult,
)


def _format_event(item: ClassifiedEvent) -> str:
    event = item.event
    lines = [f"- **{event.title}** — score {item.score:.1f} ({item.classifier})"]
    if item.summary_cn and item.summary_cn != event.title:
        lines.append(f"  - {item.summary_cn}")
    tags: list[str] = []
    if item.affects_milestones:
        tags.append("Milestones: " + ", ".join(item.affects_milestones))
    if item.affects_senators:
        tags.append("Senators: " + ", ".join(item.affects_senators))
    if tags:
        lines.append("  - " + " | ".join(tags))
    if event.url:
        lines.append(f"  - {event.url}")
    return "\n".join(lines)


def _format_market(market: MarketSnapshot) -> str:
    rows: list[str] = []

    def pct(value: float | None) -> str:
        return f"{value * 100:.0f}%" if value is not None else "n/a"

    def usd(value: float | None) -> str:
        return f"${value:,.0f}" if value is not None else "n/a"

    rows.append(f"- Polymarket \"Signed 2026\": {pct(market.polymarket_signed_2026)}")
    rows.append(f"- Polymarket \"Pass Senate by July\": {pct(market.polymarket_pass_senate_july)}")
    rows.append(f"- Kalshi equivalent: {pct(market.kalshi_equivalent)}")
    rows.append(f"- BTC: {usd(market.btc_price)} | COIN: {usd(market.coin_price)}")
    return "\n".join(rows)


def render_digest(result: MonitorRunResult) -> str:
    run_date = result.run_at.date().isoformat()
    lines = [f"# [CLARITY] Act Monitor — {run_date}", ""]
    lines.append(
        f"Raw events: {result.raw_events} | new: {result.new_events} | "
        f"classified: {len(result.classified)}"
    )
    lines.append("")

    immediate = result.by_tier("notify_now")
    digest = result.by_tier("weekly_digest")

    lines.append(f"## Immediate alerts — score >= 4 ({len(immediate)})")
    if immediate:
        lines.extend(_format_event(item) for item in immediate)
    else:
        lines.append("- None.")
    lines.append("")

    lines.append(f"## Daily digest — score 2-3 ({len(digest)})")
    if digest:
        lines.extend(_format_event(item) for item in digest)
    else:
        lines.append("- None.")
    lines.append("")

    if result.market is not None:
        lines.append("## Market snapshot")
        lines.append(_format_market(result.market))
        lines.append("")

    triggered = sorted(
        {stage for item in immediate for stage in item.affects_milestones}
    )
    if triggered:
        lines.append("## Deep-analysis triggers touched")
        lines.append(
            "These milestones moved today and warrant a structured deep-dive: "
            + ", ".join(triggered)
        )
        lines.append("")

    if result.errors:
        lines.append("## Pipeline warnings")
        lines.extend(f"- {error}" for error in result.errors)
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def deliver_digest(text: str, config: dict, output_dir: Path) -> str:
    """Write the digest to disk and optionally POST it to a webhook."""
    delivery = config.get("delivery", {})
    output_dir.mkdir(parents=True, exist_ok=True)
    digest_path = output_dir / delivery.get("digest_filename", "clarity-act-digest.md")
    digest_path.write_text(text, encoding="utf-8")

    webhook_url = (delivery.get("webhook_url") or "").strip()
    if webhook_url:
        try:
            request_json("POST", webhook_url, payload={"text": text})
        except HttpJsonError:
            pass
    return str(digest_path)
