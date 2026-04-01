from __future__ import annotations

from pathlib import Path
import json

from market_intel_watch.models import WatchEntity


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _resolve_config_file(config_dir: Path, name: str) -> Path:
    primary = config_dir / f"{name}.json"
    fallback = config_dir / f"{name}.sample.json"
    if primary.exists():
        return primary
    if fallback.exists():
        return fallback
    raise FileNotFoundError(f"Missing config file: {primary}")


def load_watch_config(config_dir: Path) -> dict:
    path = _resolve_config_file(config_dir, "watchlist")
    raw = _load_json(path)
    entities = [
        WatchEntity(
            name=item["name"],
            aliases=item.get("aliases", []),
            entity_type=item.get("entity_type", "company"),
            geography=item.get("geography", "unknown"),
            priority=int(item.get("priority", 1)),
            tags=item.get("tags", []),
        )
        for item in raw.get("entities", [])
    ]
    return {
        "markets": raw.get("markets", ["CN", "US"]),
        "ai_keywords": raw.get("ai_keywords", []),
        "source_weights": raw.get("source_weights", {}),
        "entities": entities,
    }


def load_source_config(config_dir: Path) -> list[dict]:
    path = _resolve_config_file(config_dir, "sources")
    raw = _load_json(path)
    return [item for item in raw.get("sources", []) if item.get("enabled", True)]


def load_delivery_config(config_dir: Path) -> list[dict]:
    primary = config_dir / "delivery.json"
    fallback = config_dir / "delivery.sample.json"
    path = primary if primary.exists() else fallback
    if not path.exists():
        return []
    raw = _load_json(path)
    return [item for item in raw.get("deliveries", []) if item.get("enabled", True)]
