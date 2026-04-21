from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from market_intel_watch.sources.google_news import GoogleNewsSource, strip_html
from market_intel_watch.sources.manual_drop import ManualDropSource
from market_intel_watch.sources.rss import RSSSource


SAMPLE_FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <title>Example Feed</title>
  <item>
    <title>Isara raises $94M Series A for AI agents</title>
    <link>https://example.com/isara-a</link>
    <description>&lt;p&gt;Isara, an agent startup, closed $94M&lt;/p&gt;</description>
    <pubDate>Mon, 30 Mar 2026 10:00:00 GMT</pubDate>
  </item>
  <item>
    <title>Second Story</title>
    <link>https://example.com/second</link>
    <description>Short teaser</description>
    <pubDate>Mon, 30 Mar 2026 11:00:00 GMT</pubDate>
  </item>
  <item>
    <title>Third Story</title>
    <link>https://example.com/third</link>
    <description>Another teaser</description>
    <pubDate>Mon, 30 Mar 2026 12:00:00 GMT</pubDate>
  </item>
</channel></rss>
"""


class StripHTMLTests(unittest.TestCase):
    def test_removes_tags_and_unescapes_entities(self) -> None:
        self.assertEqual("Hello  &  world", strip_html("<b>Hello</b> &amp; <i>world</i>"))

    def test_handles_empty_input(self) -> None:
        self.assertEqual("", strip_html(""))
        self.assertEqual("", strip_html(None))  # type: ignore[arg-type]


class GoogleNewsSourceTests(unittest.TestCase):
    def _build_config(self, **overrides: object) -> dict:
        config = {
            "id": "google-news-test",
            "type": "google_news",
            "channel": "news",
            "query": "AI funding",
            "hl": "en-US",
            "gl": "US",
            "ceid": "US:en",
        }
        config.update(overrides)
        return config

    def test_feed_url_encodes_query(self) -> None:
        source = GoogleNewsSource(self._build_config(query="AI funding round"))
        url = source._feed_url()
        self.assertIn("q=AI+funding+round", url)
        self.assertIn("hl=en-US", url)
        self.assertIn("gl=US", url)
        self.assertIn("ceid=US:en", url)

    def test_fetch_parses_feed_items(self) -> None:
        source = GoogleNewsSource(self._build_config())
        with patch(
            "market_intel_watch.sources.google_news.fetch_url_bytes",
            return_value=SAMPLE_FEED,
        ):
            docs = source.fetch(date(2026, 3, 31))

        self.assertEqual(3, len(docs))
        first = docs[0]
        self.assertEqual("google-news-test", first.source_id)
        self.assertEqual("news", first.channel)
        self.assertEqual("Isara raises $94M Series A for AI agents", first.title)
        self.assertEqual("https://example.com/isara-a", first.url)
        self.assertIn("Isara, an agent startup, closed $94M", first.summary)
        self.assertIsNotNone(first.published_at)
        self.assertEqual("google_news", first.metadata["source_type"])

    def test_fetch_respects_max_items(self) -> None:
        source = GoogleNewsSource(self._build_config(max_items=2))
        with patch(
            "market_intel_watch.sources.google_news.fetch_url_bytes",
            return_value=SAMPLE_FEED,
        ):
            docs = source.fetch(date(2026, 3, 31))
        self.assertEqual(2, len(docs))

    def test_enrichment_failure_returns_original_document(self) -> None:
        source = GoogleNewsSource(self._build_config(fetch_article_body=True))
        with patch(
            "market_intel_watch.sources.google_news.fetch_url_bytes",
            return_value=SAMPLE_FEED,
        ):
            with patch(
                "market_intel_watch.sources.google_news.fetch_article_snapshot",
                side_effect=RuntimeError("network down"),
            ):
                docs = source.fetch(date(2026, 3, 31))
        self.assertEqual(3, len(docs))
        self.assertNotIn("article_enriched", docs[0].metadata)

    def test_enrichment_merges_snapshot_fields(self) -> None:
        source = GoogleNewsSource(self._build_config(fetch_article_body=True))
        snapshot = {
            "title": "Enriched Title",
            "canonical_url": "https://example.com/canonical",
            "summary": "Enriched summary",
            "content": "Full article body",
        }
        with patch(
            "market_intel_watch.sources.google_news.fetch_url_bytes",
            return_value=SAMPLE_FEED,
        ):
            with patch(
                "market_intel_watch.sources.google_news.fetch_article_snapshot",
                return_value=snapshot,
            ):
                docs = source.fetch(date(2026, 3, 31))
        first = docs[0]
        self.assertEqual("Enriched Title", first.title)
        self.assertEqual("https://example.com/canonical", first.url)
        self.assertEqual("Full article body", first.content)
        self.assertEqual("true", first.metadata["article_enriched"])


class RSSSourceTests(unittest.TestCase):
    def _build_config(self, **overrides: object) -> dict:
        config = {
            "id": "rss-test",
            "type": "rss",
            "channel": "news",
            "url": "https://example.com/feed.xml",
        }
        config.update(overrides)
        return config

    def test_fetch_parses_items(self) -> None:
        source = RSSSource(self._build_config())
        with patch(
            "market_intel_watch.sources.rss.fetch_url_bytes",
            return_value=SAMPLE_FEED,
        ):
            docs = source.fetch(date(2026, 3, 31))
        self.assertEqual(3, len(docs))
        self.assertEqual("rss-test", docs[0].source_id)
        self.assertEqual("rss", docs[0].metadata["source_type"])

    def test_fetch_respects_max_items(self) -> None:
        source = RSSSource(self._build_config(max_items=1))
        with patch(
            "market_intel_watch.sources.rss.fetch_url_bytes",
            return_value=SAMPLE_FEED,
        ):
            docs = source.fetch(date(2026, 3, 31))
        self.assertEqual(1, len(docs))

    def test_enrichment_failure_falls_back_gracefully(self) -> None:
        source = RSSSource(self._build_config(fetch_article_body=True))
        with patch(
            "market_intel_watch.sources.rss.fetch_url_bytes",
            return_value=SAMPLE_FEED,
        ):
            with patch(
                "market_intel_watch.sources.rss.fetch_article_snapshot",
                side_effect=TimeoutError("slow"),
            ):
                docs = source.fetch(date(2026, 3, 31))
        self.assertEqual(3, len(docs))
        self.assertNotIn("article_enriched", docs[0].metadata)


class ManualDropSourceTests(unittest.TestCase):
    def test_fetch_returns_empty_when_directory_missing(self) -> None:
        with tempfile.TemporaryDirectory() as root_str:
            root = Path(root_str)
            source = ManualDropSource(
                {"id": "manual", "type": "manual_drop", "channel": "manual", "path": "inbox/missing"},
                root_dir=root,
            )
            self.assertEqual([], source.fetch(date(2026, 3, 31)))

    def test_fetch_parses_jsonl_lines(self) -> None:
        with tempfile.TemporaryDirectory() as root_str:
            root = Path(root_str)
            drop_dir = root / "inbox" / "manual"
            drop_dir.mkdir(parents=True)
            payload = [
                {
                    "title": "Founder departure",
                    "url": "https://example.com/a",
                    "published_at": "2026-03-30T12:00:00",
                    "summary": "summary a",
                    "authors": ["A"],
                    "tags": ["t1"],
                    "metadata": {"lang": "en"},
                },
                {
                    "title": "Second drop",
                    "url": "https://example.com/b",
                },
            ]
            with (drop_dir / "drops.jsonl").open("w", encoding="utf-8") as handle:
                for row in payload:
                    handle.write(json.dumps(row) + "\n")
                handle.write("\n")

            source = ManualDropSource(
                {"id": "manual", "type": "manual_drop", "channel": "wechat", "path": "inbox/manual"},
                root_dir=root,
            )
            docs = source.fetch(date(2026, 3, 31))

        self.assertEqual(2, len(docs))
        self.assertEqual("Founder departure", docs[0].title)
        self.assertEqual("wechat", docs[0].channel)
        self.assertEqual(["A"], docs[0].authors)
        self.assertEqual("en", docs[0].metadata["lang"])
        self.assertIsNotNone(docs[0].published_at)
        self.assertIsNone(docs[1].published_at)

    def test_fetch_honors_custom_channel_override(self) -> None:
        with tempfile.TemporaryDirectory() as root_str:
            root = Path(root_str)
            drop_dir = root / "inbox" / "manual"
            drop_dir.mkdir(parents=True)
            (drop_dir / "drops.jsonl").write_text(
                json.dumps({"title": "x", "url": "https://e/x", "channel": "xhs"}) + "\n",
                encoding="utf-8",
            )

            source = ManualDropSource(
                {"id": "m", "type": "manual_drop", "channel": "wechat", "path": "inbox/manual"},
                root_dir=root,
            )
            docs = source.fetch(date(2026, 3, 31))

        self.assertEqual("xhs", docs[0].channel)

    def test_absolute_path_is_respected(self) -> None:
        with tempfile.TemporaryDirectory() as drop_str:
            drop_dir = Path(drop_str)
            (drop_dir / "a.jsonl").write_text(
                json.dumps({"title": "abs", "url": "https://e/abs"}) + "\n",
                encoding="utf-8",
            )
            source = ManualDropSource(
                {
                    "id": "m",
                    "type": "manual_drop",
                    "channel": "manual",
                    "path": str(drop_dir),
                },
                root_dir=Path("/does/not/matter"),
            )
            docs = source.fetch(date(2026, 3, 31))
        self.assertEqual(1, len(docs))


if __name__ == "__main__":
    unittest.main()
