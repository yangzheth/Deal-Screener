from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path

from market_intel_watch.config import load_source_config, load_watch_config
from market_intel_watch.extractors.rules import RuleBasedSignalExtractor
from market_intel_watch.logging_config import get_logger
from market_intel_watch.models import DailyRunResult, Signal, SourceDocument
from market_intel_watch.reporting.markdown import render_markdown_report
from market_intel_watch.sources import build_sources


logger = get_logger(__name__)


def _document_quality(document: SourceDocument) -> tuple[int, datetime]:
    richness = len(document.title) + len(document.summary) + len(document.content)
    return richness, document.published_at or datetime.min


def _signal_quality(signal: Signal) -> tuple[float, int, datetime]:
    richness = len(signal.summary) + len(signal.amount) + len(signal.round_stage) + len(signal.follow_reason)
    return signal.score, richness, signal.published_at or datetime.min


def _unique_preserve(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        cleaned = (value or "").strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(cleaned)
    return deduped


def dedupe_documents(documents: list[SourceDocument]) -> list[SourceDocument]:
    seen: dict[str, SourceDocument] = {}
    for document in documents:
        key = document.stable_key()
        existing = seen.get(key)
        if existing is None or _document_quality(document) > _document_quality(existing):
            seen[key] = document
    return list(seen.values())


def filter_recent_documents(
    documents: list[SourceDocument],
    run_date: date,
    max_age_days: int = 7,
) -> list[SourceDocument]:
    filtered: list[SourceDocument] = []
    for document in documents:
        if document.published_at is None:
            filtered.append(document)
            continue
        age_days = (run_date - document.published_at.date()).days
        if age_days < 0:
            continue
        if age_days <= max_age_days:
            filtered.append(document)
    return filtered


def cluster_signals(signals: list[Signal]) -> list[Signal]:
    grouped: dict[str, list[Signal]] = {}
    for signal in signals:
        key = signal.cluster_key or signal.stable_key()
        grouped.setdefault(key, []).append(signal)

    clustered: list[Signal] = []
    for key, items in grouped.items():
        if len(items) == 1:
            item = items[0]
            item.cluster_key = key
            item.source_count = max(item.source_count, len(_unique_preserve(item.supporting_urls or [item.url])))
            clustered.append(item)
            continue

        primary = max(items, key=_signal_quality)
        supporting_urls = _unique_preserve([signal.url for signal in items if signal.url])
        matched_entities = _unique_preserve([name for signal in items for name in signal.matched_entities])
        key_people = _unique_preserve([name for signal in items for name in signal.key_people])
        investors = _unique_preserve([name for signal in items for name in signal.investors])
        categories = _unique_preserve([name for signal in items for name in signal.categories])
        summaries = _unique_preserve([signal.summary for signal in items if signal.summary])
        source_ids = _unique_preserve([signal.source_id for signal in items if signal.source_id])
        channels = _unique_preserve([signal.channel for signal in items if signal.channel])

        summary = primary.summary
        if len(summaries) > 1:
            summary = f"{summaries[0]} Also reported by {len(summaries) - 1} additional source(s)."

        rationale = _unique_preserve(primary.rationale + [f"cluster_sources={len(supporting_urls) or len(items)}"])
        primary.metadata = {**primary.metadata, "clustered": "true"}
        primary.cluster_key = key
        primary.summary = summary
        primary.matched_entities = matched_entities
        primary.key_people = key_people
        primary.investors = investors
        primary.categories = categories
        primary.source_id = ", ".join(source_ids[:4])
        primary.channel = ", ".join(channels[:4])
        primary.supporting_urls = supporting_urls
        primary.source_count = len(supporting_urls) or len(items)
        primary.rationale = rationale
        primary.score = min(100.0, max(signal.score for signal in items) + min(6.0, (len(items) - 1) * 1.5))
        primary.confidence = min(0.99, max(signal.confidence for signal in items) + min(0.12, 0.03 * (len(items) - 1)))
        clustered.append(primary)

    return sorted(
        clustered,
        key=lambda item: (item.score, item.source_count, item.published_at or datetime.min),
        reverse=True,
    )


def dedupe_signals(signals: list[Signal]) -> list[Signal]:
    clustered = cluster_signals(signals)
    seen: dict[str, Signal] = {}
    for signal in clustered:
        key = signal.stable_key()
        existing = seen.get(key)
        if existing is None or _signal_quality(signal) > _signal_quality(existing):
            seen[key] = signal
    return sorted(
        seen.values(),
        key=lambda item: (item.score, item.source_count, item.published_at or datetime.min),
        reverse=True,
    )


def run_daily(config_dir: Path, output_dir: Path, run_date: date) -> DailyRunResult:
    del output_dir
    watch_config = load_watch_config(config_dir)
    source_configs = load_source_config(config_dir)

    sources = build_sources(source_configs, root_dir=config_dir.parent)
    documents: list[SourceDocument] = []
    errors: list[str] = []

    for source in sources:
        try:
            fetched = source.fetch(run_date)
        except Exception as exc:  # pragma: no cover - defensive for remote feeds
            logger.warning("source fetch failed: %s: %s", source.source_id, exc)
            errors.append(f"{source.source_id}: {exc}")
            continue
        logger.info("source %s returned %d documents", source.source_id, len(fetched))
        documents.extend(fetched)

    recent_documents = filter_recent_documents(documents, run_date=run_date)
    deduped_documents = dedupe_documents(recent_documents)
    extractor = RuleBasedSignalExtractor(
        entities=watch_config["entities"],
        ai_keywords=watch_config["ai_keywords"],
        source_weights=watch_config["source_weights"],
        run_date=run_date,
    )

    signals: list[Signal] = []
    for document in deduped_documents:
        signals.extend(extractor.extract(document))

    deduped_signals = dedupe_signals(signals)
    report_text = render_markdown_report(
        run_date=run_date,
        documents_fetched=len(documents),
        documents_deduped=len(deduped_documents),
        signals=deduped_signals,
        errors=errors,
    )

    return DailyRunResult(
        run_date=datetime.combine(run_date, time.min),
        documents_fetched=len(documents),
        documents_deduped=len(deduped_documents),
        signals=deduped_signals,
        errors=errors,
        report_text=report_text,
    )
