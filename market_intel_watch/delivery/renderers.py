from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from market_intel_watch.models import DailyRunResult, Signal


EVENT_LABELS = {
    "funding": "\u878d\u8d44",
    "talent_departure": "\u79bb\u804c",
    "talent_hire": "\u52a0\u5165/\u4efb\u547d",
}


@dataclass(slots=True)
class SignalGroup:
    title: str
    url: str
    entities: list[str]
    event_types: list[str]
    score: float


def _truncate_utf8(value: str, max_bytes: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value
    truncated = encoded[:max_bytes]
    while True:
        try:
            return truncated.decode("utf-8").rstrip()
        except UnicodeDecodeError:
            truncated = truncated[:-1]


def _group_signals(signals: list[Signal], max_items: int) -> list[SignalGroup]:
    grouped: dict[tuple[str, str], SignalGroup] = {}
    for signal in sorted(signals, key=lambda item: item.score, reverse=True):
        key = (signal.title, signal.url)
        existing = grouped.get(key)
        if existing is None:
            grouped[key] = SignalGroup(
                title=signal.title,
                url=signal.url,
                entities=signal.matched_entities[:2],
                event_types=[signal.event_type],
                score=signal.score,
            )
            continue
        if signal.event_type not in existing.event_types:
            existing.event_types.append(signal.event_type)
        existing.score = max(existing.score, signal.score)
    return list(grouped.values())[:max_items]


def _signal_line(group: SignalGroup) -> str:
    entities = ", ".join(group.entities) if group.entities else "watchlist-unmatched"
    labels = "/".join(EVENT_LABELS.get(event_type, event_type) for event_type in group.event_types)
    return f"- **{labels}** | {entities} | [{group.title}]({group.url})"


def build_wecom_markdown(
    result: DailyRunResult,
    *,
    max_items: int = 8,
    max_bytes: int = 3800,
) -> str:
    counts = Counter(signal.event_type for signal in result.signals)
    lines: list[str] = [
        f"# AI Primary Market Watch {result.run_date.date().isoformat()}",
        f"> 抓取 {result.documents_fetched} 条，去重后 {result.documents_deduped} 条，命中 {len(result.signals)} 条",
        "",
        "## 事件概览",
        f"- 融资: <font color=\"warning\">{counts.get('funding', 0)}</font>",
        f"- 离职: <font color=\"comment\">{counts.get('talent_departure', 0)}</font>",
        f"- 任命/加入: <font color=\"info\">{counts.get('talent_hire', 0)}</font>",
        "",
        "## 高优先级线索",
    ]

    top_groups = _group_signals(result.signals, max_items=max_items)
    if top_groups:
        lines.extend(_signal_line(group) for group in top_groups)
    else:
        lines.append("- 今日没有命中规则的 AI 融资或人才异动线索")

    if result.errors:
        lines.extend(["", "## 数据源告警"])
        for error in result.errors[:3]:
            lines.append(f"> {error}")
        if len(result.errors) > 3:
            lines.append(f"> 另有 {len(result.errors) - 3} 条告警已省略")

    rendered = "\n".join(lines)
    trimmed = _truncate_utf8(rendered, max_bytes=max_bytes)
    if trimmed != rendered:
        suffix = "\n\n> 消息过长，已截断，请查看完整日报。"
        budget = max_bytes - len(suffix.encode("utf-8"))
        trimmed = _truncate_utf8(rendered, max_bytes=budget).rstrip() + suffix
    return trimmed
