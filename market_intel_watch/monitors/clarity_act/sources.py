from __future__ import annotations

import csv
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import html
import json
import os
import re
import xml.etree.ElementTree as ET

from market_intel_watch.monitors.base import EventSource
from market_intel_watch.monitors.clarity_act.http_util import HttpJsonError, get_json
from market_intel_watch.monitors.clarity_act.models import MarketSnapshot, RawEvent
from market_intel_watch.sources.http_fetch import fetch_url_bytes


_TAG_RE = re.compile(r"<[^>]+>")
_ATOM_NS = "{http://www.w3.org/2005/Atom}"


def _strip_html(value: str) -> str:
    return html.unescape(_TAG_RE.sub(" ", value or "")).strip()


def _truncate(text: str, limit: int = 220) -> str:
    text = (text or "").strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError):
        pass
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value[: len(fmt) + 2], fmt)
        except ValueError:
            continue
    return None


def _parse_feed(payload: bytes) -> list[dict]:
    """Parse an RSS or Atom feed into a list of normalized item dicts."""
    root = ET.fromstring(payload)
    items: list[dict] = []
    for item in root.findall(".//item"):
        items.append(
            {
                "title": (item.findtext("title") or "").strip(),
                "link": (item.findtext("link") or "").strip(),
                "description": _strip_html(item.findtext("description") or ""),
                "published": item.findtext("pubDate"),
            }
        )
    if items:
        return items
    for entry in root.findall(f".//{_ATOM_NS}entry"):
        link_el = entry.find(f"{_ATOM_NS}link")
        items.append(
            {
                "title": (entry.findtext(f"{_ATOM_NS}title") or "").strip(),
                "link": link_el.get("href", "") if link_el is not None else "",
                "description": _strip_html(entry.findtext(f"{_ATOM_NS}summary") or ""),
                "published": entry.findtext(f"{_ATOM_NS}updated")
                or entry.findtext(f"{_ATOM_NS}published"),
            }
        )
    return items


class CongressActionsSource(EventSource):
    """Official action log for the bill from the Congress.gov API.

    Every new action is treated as auto-material: the classifier pushes it
    immediately without an LLM call.
    """

    def fetch(self) -> list[RawEvent]:
        bill = self.config.get("bill", {})
        api = self.config.get("congress_api", {})
        api_key = os.environ.get(api.get("api_key_env", "CONGRESS_API_KEY"), "").strip()
        if not api_key:
            raise RuntimeError(
                "Congress.gov source enabled but the API key env var is unset "
                "(get a free key at https://api.congress.gov/sign-up/)."
            )
        url = (
            f"https://api.congress.gov/v3/bill/{bill.get('congress', 119)}/"
            f"{bill.get('bill_type', 'hr')}/{bill.get('bill_number', 3633)}/actions"
            f"?format=json&limit={int(api.get('limit', 50))}&api_key={api_key}"
        )
        payload = get_json(url)
        bill_url = (
            f"https://www.congress.gov/bill/{bill.get('congress', 119)}th-congress/"
            f"house-bill/{bill.get('bill_number', 3633)}"
        )
        events: list[RawEvent] = []
        for action in payload.get("actions", []):
            text = (action.get("text") or "").strip()
            if not text:
                continue
            events.append(
                RawEvent(
                    source=self.source_id,
                    source_authority="Congress.gov",
                    title=_truncate(text, 120),
                    description=text,
                    url=bill_url,
                    occurred_at=_parse_date(action.get("actionDate")),
                    event_type=_classify_action_type(text),
                    raw_snippet=text,
                    metadata={"auto_material": "true", "action_type": action.get("type", "")},
                )
            )
        return events


def _classify_action_type(text: str) -> str:
    lowered = text.lower()
    if "cloture" in lowered:
        return "Vote"
    if "passed" in lowered or "agreed to" in lowered or "roll call" in lowered:
        return "Vote"
    if "amendment" in lowered:
        return "Amendment"
    if "committee" in lowered:
        return "Markup"
    return "Official Action"


class SenateBankingSource(EventSource):
    """Senate Banking Committee newsroom feed (RSS).

    Best-effort: the committee site does not always expose a stable RSS
    endpoint, so a feed URL must be supplied in config. With no URL the
    source yields nothing instead of failing the run.
    """

    def fetch(self) -> list[RawEvent]:
        rss_url = (self.config.get("senate_banking", {}).get("rss_url") or "").strip()
        if not rss_url:
            return []
        payload = fetch_url_bytes(rss_url, timeout=20)
        events: list[RawEvent] = []
        for item in _parse_feed(payload):
            if not item["title"]:
                continue
            events.append(
                RawEvent(
                    source=self.source_id,
                    source_authority="Committee",
                    title=item["title"],
                    description=item["description"],
                    url=item["link"],
                    occurred_at=_parse_date(item["published"]),
                    event_type="Statement",
                    raw_snippet=_truncate(item["description"], 400),
                )
            )
        return events


class NewsRSSSource(EventSource):
    """Crypto-policy news RSS feeds, filtered to CLARITY Act coverage."""

    def fetch(self) -> list[RawEvent]:
        news = self.config.get("news_rss", {})
        feeds = news.get("feeds", [])
        keywords = [kw.lower() for kw in news.get("keywords", []) if kw]
        max_items = int(news.get("max_items_per_feed", 40))
        events: list[RawEvent] = []
        for feed_url in feeds:
            try:
                payload = fetch_url_bytes(feed_url, timeout=20)
                items = _parse_feed(payload)
            except Exception as exc:  # pragma: no cover - defensive for remote feeds
                self.errors.append(f"{feed_url}: {exc}")
                continue
            for item in items[:max_items]:
                blob = f"{item['title']} {item['description']}".lower()
                if keywords and not any(kw in blob for kw in keywords):
                    continue
                events.append(
                    RawEvent(
                        source=self.source_id,
                        source_authority="Media",
                        title=item["title"],
                        description=item["description"],
                        url=item["link"],
                        occurred_at=_parse_date(item["published"]),
                        event_type="Media Report",
                        raw_snippet=_truncate(item["description"], 400),
                    )
                )
        return events


class TwitterSource(EventSource):
    """Key X/Twitter accounts via API v2 recent search.

    Requires elevated API access and a bearer token, so it is disabled by
    default. Without a token the source yields nothing.
    """

    def fetch(self) -> list[RawEvent]:
        twitter = self.config.get("twitter", {})
        token = os.environ.get(twitter.get("bearer_token_env", "TWITTER_BEARER_TOKEN"), "").strip()
        accounts = twitter.get("accounts", [])
        if not token or not accounts:
            return []
        from_clause = " OR ".join(f"from:{handle}" for handle in accounts)
        query = f"({from_clause}) (CLARITY OR \"H.R. 3633\" OR \"market structure\")"
        url = (
            "https://api.twitter.com/2/tweets/search/recent"
            f"?query={query}&max_results=50&tweet.fields=created_at,author_id"
        )
        try:
            payload = get_json(url, headers={"Authorization": f"Bearer {token}"})
        except HttpJsonError:
            return []
        events: list[RawEvent] = []
        for tweet in payload.get("data", []):
            text = (tweet.get("text") or "").strip()
            if not text:
                continue
            tweet_id = tweet.get("id", "")
            events.append(
                RawEvent(
                    source=self.source_id,
                    source_authority="Senator",
                    title=_truncate(text, 120),
                    description=text,
                    url=f"https://twitter.com/i/web/status/{tweet_id}",
                    occurred_at=_parse_date(tweet.get("created_at")),
                    event_type="Statement",
                    raw_snippet=text,
                )
            )
        return events


def build_event_sources(config: dict) -> list[EventSource]:
    """Instantiate every enabled news/official EventSource for the monitor."""
    sources: list[EventSource] = []
    if config.get("congress_api", {}).get("enabled"):
        sources.append(CongressActionsSource({**config, "id": "congress_actions"}))
    if config.get("senate_banking", {}).get("enabled"):
        sources.append(SenateBankingSource({**config, "id": "senate_banking"}))
    if config.get("news_rss", {}).get("enabled"):
        sources.append(NewsRSSSource({**config, "id": "news_rss"}))
    if config.get("twitter", {}).get("enabled"):
        sources.append(TwitterSource({**config, "id": "twitter"}))
    return sources


class MarketWatcher:
    """Fetches the prediction-market and price snapshot for the bill."""

    def __init__(self, config: dict) -> None:
        self.config = config
        self.errors: list[str] = []

    def snapshot(self) -> MarketSnapshot:
        return MarketSnapshot(
            captured_at=datetime.now(timezone.utc),
            polymarket_signed_2026=self._polymarket("signed_2026_slug"),
            polymarket_pass_senate_july=self._polymarket("pass_senate_july_slug"),
            kalshi_equivalent=self._kalshi(),
            btc_price=self._btc_price(),
            coin_price=self._coin_price(),
        )

    def _polymarket(self, slug_key: str) -> float | None:
        poly = self.config.get("polymarket", {})
        if not poly.get("enabled"):
            return None
        slug = (poly.get(slug_key) or "").strip()
        if not slug:
            return None
        url = f"{poly.get('api_base', 'https://gamma-api.polymarket.com')}/markets?slug={slug}"
        try:
            markets = get_json(url)
        except HttpJsonError as exc:
            self.errors.append(f"polymarket {slug}: {exc}")
            return None
        if isinstance(markets, dict):
            markets = markets.get("data", [])
        for market in markets or []:
            price = _polymarket_yes_price(market)
            if price is not None:
                return price
        return None

    def _kalshi(self) -> float | None:
        kalshi = self.config.get("kalshi", {})
        if not kalshi.get("enabled"):
            return None
        ticker = (kalshi.get("market_ticker") or "").strip()
        if not ticker:
            return None
        url = f"{kalshi.get('api_base', 'https://api.elections.kalshi.com/trade-api/v2')}/markets/{ticker}"
        try:
            payload = get_json(url)
        except HttpJsonError as exc:
            self.errors.append(f"kalshi {ticker}: {exc}")
            return None
        market = payload.get("market", payload)
        last_price = market.get("last_price")
        if last_price is None:
            return None
        return round(float(last_price) / 100.0, 4)

    def _btc_price(self) -> float | None:
        if not self.config.get("prices", {}).get("btc"):
            return None
        try:
            payload = get_json("https://api.coinbase.com/v2/prices/BTC-USD/spot")
            return round(float(payload["data"]["amount"]), 2)
        except (HttpJsonError, KeyError, ValueError, TypeError) as exc:
            self.errors.append(f"btc price: {exc}")
            return None

    def _coin_price(self) -> float | None:
        if not self.config.get("prices", {}).get("coin"):
            return None
        try:
            raw = fetch_url_bytes(
                "https://stooq.com/q/l/?s=coin.us&f=sd2t2ohlcv&h&e=csv", timeout=15
            )
            rows = list(csv.DictReader(raw.decode("utf-8").splitlines()))
            if rows and rows[0].get("Close") not in (None, "", "N/D"):
                return round(float(rows[0]["Close"]), 2)
        except Exception as exc:  # pragma: no cover - best-effort price feed
            self.errors.append(f"coin price: {exc}")
        return None


def _polymarket_yes_price(market: dict) -> float | None:
    outcomes = market.get("outcomes")
    prices = market.get("outcomePrices")
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)
    if isinstance(prices, str):
        prices = json.loads(prices)
    if not outcomes or not prices:
        return None
    for outcome, price in zip(outcomes, prices):
        if str(outcome).strip().lower() == "yes":
            return round(float(price), 4)
    return None
