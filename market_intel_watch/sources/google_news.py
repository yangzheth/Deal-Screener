from __future__ import annotations

from datetime import date
from email.utils import parsedate_to_datetime
import html
import re
from urllib.parse import quote_plus
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

from market_intel_watch.models import SourceDocument
from market_intel_watch.sources.base import SourceAdapter


TAG_RE = re.compile(r"<[^>]+>")


def strip_html(value: str) -> str:
    return html.unescape(TAG_RE.sub(" ", value or "")).strip()


class GoogleNewsSource(SourceAdapter):
    def _feed_url(self) -> str:
        query = quote_plus(self.config["query"])
        hl = self.config.get("hl", "en-US")
        gl = self.config.get("gl", "US")
        ceid = self.config.get("ceid", "US:en")
        return f"https://news.google.com/rss/search?q={query}&hl={hl}&gl={gl}&ceid={ceid}"

    def fetch(self, run_date: date) -> list[SourceDocument]:
        del run_date
        request = Request(
            self._feed_url(),
            headers={"User-Agent": "Mozilla/5.0 (compatible; AIPrimaryMarketWatch/0.1)"},
        )
        with urlopen(request, timeout=20) as response:
            payload = response.read()

        root = ET.fromstring(payload)
        documents: list[SourceDocument] = []
        max_items = int(self.config.get("max_items", 50))
        for item in root.findall("./channel/item")[:max_items]:
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            description = strip_html(item.findtext("description") or "")
            pub_date = item.findtext("pubDate")
            published_at = parsedate_to_datetime(pub_date) if pub_date else None

            documents.append(
                SourceDocument(
                    source_id=self.source_id,
                    channel=self.channel,
                    title=title,
                    url=link,
                    published_at=published_at,
                    summary=description,
                    metadata={"source_type": "google_news"},
                )
            )
        return documents
