from __future__ import annotations

import html
import re

from market_intel_watch.sources.http_fetch import fetch_url_bytes


BODY_TAG_RE = re.compile(r"<body[^>]*>(?P<body>.*?)</body>", re.IGNORECASE | re.DOTALL)
TITLE_TAG_RE = re.compile(r"<title[^>]*>(?P<value>.*?)</title>", re.IGNORECASE | re.DOTALL)
ARTICLE_TAG_RE = re.compile(r"<article[^>]*>(?P<value>.*?)</article>", re.IGNORECASE | re.DOTALL)
META_TAG_RE = re.compile(
    r'<meta[^>]+(?:name|property)=["\'](?P<name>[^"\']+)["\'][^>]+content=["\'](?P<content>[^"\']+)["\']',
    re.IGNORECASE,
)
LINK_CANONICAL_RE = re.compile(
    r'<link[^>]+rel=["\'][^"\']*canonical[^"\']*["\'][^>]+href=["\'](?P<href>[^"\']+)["\']',
    re.IGNORECASE,
)
CONTENT_BLOCK_RE = re.compile(r"<(p|li|h1|h2|h3|blockquote)[^>]*>(?P<value>.*?)</\\1>", re.IGNORECASE | re.DOTALL)
SCRIPT_STYLE_RE = re.compile(r"<(script|style|noscript|svg)[^>]*>.*?</\\1>", re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")
MAX_HTML_BYTES = 1_500_000
MAX_BLOCKS = 12


def _clean_text(value: str) -> str:
    text = html.unescape(TAG_RE.sub(" ", value or ""))
    return WHITESPACE_RE.sub(" ", text).strip()


def _extract_meta(html_text: str, *names: str) -> str:
    wanted = {name.lower() for name in names}
    for match in META_TAG_RE.finditer(html_text):
        name = (match.group("name") or "").lower().strip()
        if name in wanted:
            return _clean_text(match.group("content"))
    return ""


def _extract_blocks(html_text: str) -> list[str]:
    stripped = SCRIPT_STYLE_RE.sub(" ", html_text)
    article_match = ARTICLE_TAG_RE.search(stripped)
    candidate = article_match.group("value") if article_match else stripped
    blocks: list[str] = []
    seen: set[str] = set()
    for match in CONTENT_BLOCK_RE.finditer(candidate):
        value = _clean_text(match.group("value"))
        if len(value) < 40:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        blocks.append(value)
        if len(blocks) >= MAX_BLOCKS:
            break
    return blocks


def fetch_article_snapshot(url: str, *, user_agent: str, timeout: int = 15) -> dict[str, str]:
    payload = fetch_url_bytes(url, user_agent=user_agent, timeout=timeout)[:MAX_HTML_BYTES]
    html_text = payload.decode("utf-8", errors="replace")
    body_match = BODY_TAG_RE.search(html_text)
    body = body_match.group("body") if body_match else html_text

    canonical_url = _extract_meta(html_text, "og:url")
    if not canonical_url:
        canonical_match = LINK_CANONICAL_RE.search(html_text)
        canonical_url = canonical_match.group("href").strip() if canonical_match else ""

    title = _extract_meta(html_text, "og:title", "twitter:title")
    if not title:
        title_match = TITLE_TAG_RE.search(html_text)
        title = _clean_text(title_match.group("value")) if title_match else ""

    summary = _extract_meta(html_text, "description", "og:description", "twitter:description")
    blocks = _extract_blocks(body)
    content = "\n\n".join(blocks)
    if not summary and blocks:
        summary = blocks[0]

    return {
        "canonical_url": canonical_url,
        "title": title,
        "summary": summary,
        "content": content,
    }
