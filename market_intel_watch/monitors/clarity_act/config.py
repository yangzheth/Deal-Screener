from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any


# Notion data source IDs for the "CLARITY Act Tracker" workspace. These are
# identifiers, not secrets; only the API token is sensitive.
DEFAULT_CONFIG: dict[str, Any] = {
    "bill": {"congress": 119, "bill_type": "hr", "bill_number": 3633},
    "congress_api": {
        "enabled": True,
        "api_key_env": "CONGRESS_API_KEY",
        "limit": 50,
    },
    "senate_banking": {
        "enabled": True,
        "rss_url": "",
    },
    "news_rss": {
        "enabled": True,
        "feeds": [
            "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml",
            "https://www.theblock.co/rss/feed",
        ],
        "keywords": [
            "CLARITY Act",
            "H.R. 3633",
            "HR 3633",
            "market structure bill",
            "digital asset market structure",
        ],
        "max_items_per_feed": 40,
    },
    "twitter": {
        "enabled": False,
        "bearer_token_env": "TWITTER_BEARER_TOKEN",
        "accounts": [
            "SenScott",
            "ewarren",
            "SenWarner",
            "a16zcrypto",
            "blockchain_assn",
            "WhiteHouse",
        ],
    },
    "polymarket": {
        "enabled": True,
        "api_base": "https://gamma-api.polymarket.com",
        "signed_2026_slug": "",
        "pass_senate_july_slug": "",
    },
    "kalshi": {
        "enabled": True,
        "api_base": "https://api.elections.kalshi.com/trade-api/v2",
        "market_ticker": "",
    },
    "prices": {"btc": True, "coin": True},
    "classifier": {
        "provider": "claude",
        "model": "claude-haiku-4-5-20251001",
        "api_key_env": "ANTHROPIC_API_KEY",
        "api_base": "https://api.anthropic.com",
        "max_tokens": 700,
    },
    "thresholds": {
        "notify_score": 4,
        "digest_score": 2,
        "odds_move_pct": 8,
    },
    "dedup": {
        "db_path": "output/clarity_act_dedup.sqlite3",
        "ttl_days": 14,
    },
    "notion": {
        "enabled": True,
        "auth_token_env": "NOTION_API_TOKEN",
        "api_base": "https://api.notion.com",
        "notion_version": "2025-09-03",
        "events_log_data_source_id": "collection://9d6794ec-80c7-4580-841e-05a4536a6eca",
        "milestones_data_source_id": "collection://8191dea3-a5cc-468a-b899-4ea63082a5a9",
        "senators_data_source_id": "collection://e13d831b-564d-4c90-90a4-0fb8efd7143f",
        "market_signals_data_source_id": "collection://a89e1abb-3ff9-439a-9e3e-2513cf2f4e30",
    },
    "delivery": {
        "webhook_url": "",
        "digest_filename": "clarity-act-digest.md",
    },
}


def _deep_merge(base: dict, override: dict) -> dict:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_clarity_config(config_dir: Path) -> dict:
    """Load ``clarity_act.json`` (falling back to the sample) merged onto defaults."""
    primary = config_dir / "clarity_act.json"
    fallback = config_dir / "clarity_act.sample.json"
    path = primary if primary.exists() else fallback
    if not path.exists():
        return copy.deepcopy(DEFAULT_CONFIG)
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    return _deep_merge(DEFAULT_CONFIG, raw)
