from __future__ import annotations

from datetime import date
from email.utils import parsedate_to_datetime
import html
import re
import xml.etree.ElementTree as ET

from market_intel_watch.logging_config import get_logger
from market_intel_watch.models import SourceDocument
from market_intel_watch.sources.base import SourceAdapter
from market_intel_watch.sources.html_fetch import fetch_article_snapshot
from market_intel_watch.sources.http_fetch import fetch_url_bytes


TAG_RE = re.compile(r"<[^>]+>")
USER_AGENT = "Mozilla/5.0 (compatible; AIPrimaryMarketWatch/0.2)"
logger = get_logger(__name__)


def strip_html(value: str) -> str:
    return html.unescape(TAG_RE.sub(" ", value or "")).strip()


class RSSSource(SourceAdapter):
    def _enrich_document(self, document: SourceDocument) -> SourceDocument:
        if not self.config.get("fetch_article_body"):
            return document
        try:
            snapshot = fetch_article_snapshot(
                document.url,
                user_agent=USER_AGENT,
                timeout=int(self.config.get("article_timeout", 15)),
            )
        except Exception as exc:
            logger.debug("article enrichment failed for %s: %s", document.url, exc)
            return document

        return SourceDocument(
            source_id=document.source_id,
            channel=document.channel,
            title=snapshot["title"] or document.title,
            url=snapshot["canonical_url"] or document.url,
            published_at=document.published_at,
            summary=snapshot["summary"] or document.summary,
            content=snapshot["content"] or document.content,
            authors=document.authors,
            tags=document.tags,
            metadata={**document.metadata, "article_enriched": "true"},
        )

    def fetch(self, run_date: date) -> list[SourceDocument]:
        del run_date
        payload = fetch_url_bytes(self.config["url"], user_agent=USER_AGENT, timeout=20)

        root = ET.fromstring(payload)
        documents: list[SourceDocument] = []
        max_items = int(self.config.get("max_items", 50))

        for item in root.findall("./channel/item")[:max_items]:
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            description = strip_html(item.findtext("description") or "")
            pub_date = item.findtext("pubDate")
            published_at = parsedate_to_datetime(pub_date) if pub_date else None
            document = SourceDocument(
                source_id=self.source_id,
                channel=self.channel,
                title=title,
                url=link,
                published_at=published_at,
                summary=description,
                metadata={"source_type": "rss"},
            )
            documents.append(self._enrich_document(document))
        return documents
