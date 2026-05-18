from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from market_intel_watch.monitors.clarity_act.classifier import MaterialClassifier
from market_intel_watch.monitors.clarity_act.config import load_clarity_config
from market_intel_watch.monitors.clarity_act.dedup import DedupStore
from market_intel_watch.monitors.clarity_act.digest import deliver_digest, render_digest
from market_intel_watch.monitors.clarity_act.models import (
    MarketSnapshot,
    MonitorRunResult,
    RawEvent,
)
from market_intel_watch.monitors.clarity_act.notion_sync import NotionSync
from market_intel_watch.monitors.clarity_act.sources import MarketWatcher, build_event_sources


# Probability fields tracked for day-over-day move detection.
_ODDS_FIELDS = {
    "polymarket_signed_2026": 'Polymarket "Signed 2026"',
    "polymarket_pass_senate_july": 'Polymarket "Pass Senate by July"',
    "kalshi_equivalent": "Kalshi equivalent",
}


def _load_previous_odds(state_path: Path) -> dict:
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _save_odds(state_path: Path, snapshot: MarketSnapshot) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(snapshot.odds()), encoding="utf-8")


def detect_odds_moves(
    snapshot: MarketSnapshot,
    previous: dict,
    threshold_pct: float,
) -> list[RawEvent]:
    """Build auto-material events for prediction-market odds moving past the threshold."""
    events: list[RawEvent] = []
    for field, label in _ODDS_FIELDS.items():
        new_value = getattr(snapshot, field)
        old_value = previous.get(field)
        if new_value is None or old_value is None:
            continue
        delta_pp = (new_value - old_value) * 100
        if abs(delta_pp) < threshold_pct:
            continue
        sign = "+" if delta_pp >= 0 else ""
        title = (
            f"{label} odds moved {sign}{delta_pp:.0f}pp to {new_value * 100:.0f}%"
        )
        description = (
            f"{label} probability moved from {old_value * 100:.0f}% to "
            f"{new_value * 100:.0f}% since the previous snapshot."
        )
        events.append(
            RawEvent(
                source="market_move",
                source_authority="Market",
                title=title,
                description=description,
                url="",
                occurred_at=snapshot.captured_at,
                event_type="Market Signal",
                raw_snippet=description,
                metadata={
                    "auto_material": "true",
                    "auto_score": "4",
                    "category": "Market Signal",
                    "summary_cn": title,
                },
            )
        )
    return events


def run_monitor(config_dir: Path, output_dir: Path, *, dry_run: bool = False) -> MonitorRunResult:
    """Run one full CLARITY Act monitoring cycle."""
    config = load_clarity_config(config_dir)
    run_at = datetime.now(timezone.utc)
    errors: list[str] = []

    raw_events: list[RawEvent] = []
    for source in build_event_sources(config):
        try:
            raw_events.extend(source.fetch())
        except Exception as exc:  # pragma: no cover - defensive for remote feeds
            errors.append(f"{source.source_id}: {exc}")
        errors.extend(f"{source.source_id}: {issue}" for issue in source.errors)

    market_watcher = MarketWatcher(config)
    snapshot = market_watcher.snapshot()
    errors.extend(market_watcher.errors)

    state_path = output_dir / "clarity_act_last_market.json"
    previous_odds = _load_previous_odds(state_path)
    raw_events.extend(
        detect_odds_moves(snapshot, previous_odds, float(config["thresholds"]["odds_move_pct"]))
    )
    if not dry_run:
        _save_odds(state_path, snapshot)

    total_raw = len(raw_events)

    dedup_cfg = config["dedup"]
    db_path = ":memory:" if dry_run else dedup_cfg["db_path"]
    new_events: list[RawEvent] = []
    with DedupStore(db_path, ttl_days=int(dedup_cfg["ttl_days"])) as store:
        for event in raw_events:
            key = event.dedup_key()
            if store.is_new(key):
                store.mark_seen(key, event.source, event.content_hash())
                new_events.append(event)

    classifier = MaterialClassifier(config)
    classified = [classifier.classify(event) for event in new_events]
    classified.sort(key=lambda item: item.score, reverse=True)

    if not dry_run:
        notion = NotionSync(config)
        notion.sync_events(classified)
        notion.append_market_snapshot(snapshot)
        errors.extend(notion.errors)

    result = MonitorRunResult(
        run_at=run_at,
        raw_events=total_raw,
        new_events=len(new_events),
        classified=classified,
        market=snapshot,
        errors=errors,
    )
    digest_text = render_digest(result)
    result.digest_path = deliver_digest(digest_text, config, output_dir)
    return result
