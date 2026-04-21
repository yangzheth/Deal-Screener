from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from html import escape

from market_intel_watch.models import Signal


EVENT_LABELS = {
    "funding": "融资",
    "talent_departure": "离职",
    "talent_hire": "加入/任命",
}


@dataclass(slots=True)
class DigestItem:
    rank: int
    signal: Signal
    llm_score: float
    tldr: str
    reason: str


def _llm_meta(signal: Signal) -> tuple[float | None, str, str]:
    raw = signal.metadata.get("llm_score")
    score: float | None
    try:
        score = float(raw) if raw is not None else None
    except (TypeError, ValueError):
        score = None
    tldr = signal.metadata.get("llm_tldr", "").strip()
    reason = signal.metadata.get("llm_reason", "").strip()
    return score, tldr, reason


def rank_for_digest(signals: list[Signal], *, top_n: int = 8, min_llm_score: float = 7.0) -> list[DigestItem]:
    """Pick the top N signals for the daily email using LLM score when available.

    Fallback: if no LLM score is present on a signal, use its rule score / 10 as the ordering key.
    """
    scored: list[tuple[float, Signal, float, str, str]] = []
    for signal in signals:
        llm_score, tldr, reason = _llm_meta(signal)
        if llm_score is None:
            sort_key = signal.score / 10.0
            effective_score = sort_key
        else:
            sort_key = llm_score
            effective_score = llm_score
            if llm_score < min_llm_score:
                continue
        scored.append((sort_key, signal, effective_score, tldr, reason))

    scored.sort(key=lambda item: (item[0], item[1].source_count), reverse=True)
    items: list[DigestItem] = []
    for rank, (_, signal, effective_score, tldr, reason) in enumerate(scored[:top_n], start=1):
        items.append(DigestItem(rank=rank, signal=signal, llm_score=effective_score, tldr=tldr, reason=reason))
    return items


def render_email_subject(run_date: date, items: list[DigestItem]) -> str:
    if not items:
        return f"[AI Market Watch] {run_date.isoformat()} · 今日无高分信号"
    top = items[0].signal
    entity = top.company_name or (top.matched_entities[0] if top.matched_entities else top.title[:30])
    return f"[AI Market Watch] {run_date.isoformat()} · {entity} 等 {len(items)} 条"


def render_email_text(run_date: date, items: list[DigestItem], *, fallback_note: str | None = None) -> str:
    lines: list[str] = [f"AI Primary Market Watch — {run_date.isoformat()}", ""]
    if not items:
        lines.append("今日没有达到阈值的信号。")
        if fallback_note:
            lines.extend(["", fallback_note])
        return "\n".join(lines)

    for item in items:
        signal = item.signal
        event_label = EVENT_LABELS.get(signal.event_type, signal.event_type)
        tldr = item.tldr or signal.summary or signal.title
        reason = item.reason or "符合个性化偏好。"
        header = f"[{item.rank}] {tldr}"
        meta = (
            f"    {event_label} · 评分 {item.llm_score:.1f}/10 · {signal.company_name or '未识别公司'} · "
            f"来源 {signal.source_id or signal.channel or 'unknown'}"
        )
        lines.extend([header, meta, f"    推荐理由：{reason}", f"    原文：{signal.url}", ""])
    if fallback_note:
        lines.extend(["--", fallback_note])
    return "\n".join(lines)


def render_email_html(run_date: date, items: list[DigestItem], *, fallback_note: str | None = None) -> str:
    if not items:
        body_html = "<p>今日没有达到阈值的信号。</p>"
        if fallback_note:
            body_html += f"<p style='color:#666;font-size:12px;'>{escape(fallback_note)}</p>"
    else:
        cards: list[str] = []
        for item in items:
            signal = item.signal
            event_label = EVENT_LABELS.get(signal.event_type, signal.event_type)
            tldr = escape(item.tldr or signal.summary or signal.title)
            reason = escape(item.reason or "符合个性化偏好。")
            company = escape(signal.company_name or "未识别公司")
            source = escape(signal.source_id or signal.channel or "unknown")
            url = escape(signal.url)
            title = escape(signal.title)
            cards.append(
                f"""
                <div style="padding:12px 0;border-bottom:1px solid #eee;">
                  <div style="font-size:16px;font-weight:600;">
                    <span style="color:#888;margin-right:6px;">{item.rank}.</span>{tldr}
                  </div>
                  <div style="font-size:12px;color:#666;margin-top:4px;">
                    {escape(event_label)} · 评分 {item.llm_score:.1f}/10 · {company} · 来源 {source}
                  </div>
                  <div style="font-size:13px;color:#333;margin-top:6px;">
                    <strong>推荐理由：</strong>{reason}
                  </div>
                  <div style="font-size:13px;margin-top:6px;">
                    <a href="{url}" style="color:#0366d6;text-decoration:none;">{title} →</a>
                  </div>
                </div>
                """
            )
        body_html = "\n".join(cards)
        if fallback_note:
            body_html += f"<p style='color:#888;font-size:12px;margin-top:12px;'>{escape(fallback_note)}</p>"

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family:-apple-system,Segoe UI,sans-serif;max-width:640px;margin:0 auto;padding:16px;color:#1a1a1a;">
  <h1 style="font-size:20px;margin:0 0 4px;">AI Primary Market Watch</h1>
  <div style="color:#666;font-size:13px;margin-bottom:16px;">{run_date.isoformat()}</div>
  {body_html}
</body></html>"""
