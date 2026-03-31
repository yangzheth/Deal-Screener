from __future__ import annotations

from pathlib import Path

from market_intel_watch.sources.base import SourceAdapter
from market_intel_watch.sources.google_news import GoogleNewsSource
from market_intel_watch.sources.manual_drop import ManualDropSource
from market_intel_watch.sources.rss import RSSSource


def build_sources(configs: list[dict], root_dir: Path) -> list[SourceAdapter]:
    sources: list[SourceAdapter] = []
    for config in configs:
        source_type = config["type"]
        if source_type == "google_news":
            sources.append(GoogleNewsSource(config))
        elif source_type == "rss":
            sources.append(RSSSource(config))
        elif source_type == "manual_drop":
            sources.append(ManualDropSource(config, root_dir=root_dir))
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    return sources
