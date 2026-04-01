from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path

from market_intel_watch.config import load_source_config, load_watch_config
from market_intel_watch.extractors.rules import RuleBasedSignalExtractor
from market_intel_watch.models import DailyRunResult, Signal, SourceDocument
from market_intel_watch.reporting.markdown import render_markdown_report
from market_intel_watch.sources import build_sources


def _document_quality(document: SourceDocument) -> tuple[int, datetime]:
    richness = len(document.title) + len(document.summary) + len(document.content)
    return richness, document.published_at or datetime.min


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


def dedupe_signals(signals: list[Signal]) -> list[Signal]:
    seen: dict[str, Signal] = {}
    for signal in signals:
        key = signal.stable_key()
        existing = seen.get(key)
        if existing is None or signal.score > existing.score:
            seen[key] = signal
    return sorted(
        seen.values(),
        key=lambda item: (item.score, item.published_at or datetime.min),
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
            documents.extend(source.fetch(run_date))
        except Exception as exc:  # pragma: no cover - defensive for remote feeds
            errors.append(f"{source.source_id}: {exc}")

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
