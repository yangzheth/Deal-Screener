from __future__ import annotations

import json
import os
import re

from market_intel_watch.monitors.clarity_act.http_util import HttpJsonError, request_json
from market_intel_watch.monitors.clarity_act.models import RawEvent, ClassifiedEvent
from market_intel_watch.monitors.clarity_act import seed_data


SYSTEM_PROMPT = """你是 CLARITY Act（H.R. 3633，数字资产市场结构法案）的立法追踪员。
判断输入事件的 materiality（重要性），给出 0-5 分：

HIGH (4-5 分): 正式投票、cloture 投票、官方法案文本变更、关键议员立场翻转、
              总统或财长公开承诺、conference 程序开始或 conference report 发布。
MEDIUM (2-3 分): 摇摆议员公开发言、prediction market 单日大幅变动、
                committee chair 的时间表声明、实质性修正案。
LOW (0-1 分): 一般媒体评论、行业声明、重复或边缘信息。

只输出一个 JSON 对象，不要加任何解释或 markdown 代码块：
{
  "score": 0-5 的数字,
  "category": 简短英文分类,
  "affects_milestones": [里程碑 Stage 名称数组],
  "affects_senators": [参议员全名数组],
  "summary_cn": "一句话中文总结",
  "recommended_action": "notify_now" | "weekly_digest" | "skip",
  "confidence": 0-1 的数字
}

里程碑 Stage 取值只能是: %(stages)s
参议员全名只能取自: %(senators)s
recommended_action 规则: score>=4 用 notify_now, score 2-3 用 weekly_digest, score<=1 用 skip。
""" % {
    "stages": ", ".join(seed_data.stage_keywords().keys()),
    "senators": ", ".join(seed_data.senator_names()),
}

_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def _extract_json(text: str) -> dict:
    match = _JSON_RE.search(text or "")
    if not match:
        raise ValueError("no JSON object found in model response")
    return json.loads(match.group(0))


def _match_senators(text: str) -> list[str]:
    lowered = text.lower()
    hits: list[str] = []
    for senator in seed_data.SENATORS:
        last_name = senator.name.split()[-1].lower()
        if senator.name.lower() in lowered or re.search(rf"\b{re.escape(last_name)}\b", lowered):
            hits.append(senator.name)
    return hits


def _match_milestones(text: str) -> list[str]:
    lowered = text.lower()
    hits: list[str] = []
    for stage, keywords in seed_data.stage_keywords().items():
        if any(keyword in lowered for keyword in keywords):
            hits.append(stage)
    return hits


_HIGH_SIGNALS = (
    "cloture",
    "passed the senate",
    "signed into law",
    "final passage",
    "conference report",
    "roll call vote",
    "vote scheduled",
    "veto",
)
_MEDIUM_SIGNALS = (
    "hearing",
    "amendment",
    "markup",
    "floor vote",
    "schedule",
    "statement",
    "endorse",
    "oppose",
)


class MaterialClassifier:
    """Scores raw events for materiality.

    Uses the Claude API when an API key is available, and falls back to a
    deterministic keyword scorer otherwise so the pipeline (and CI) can run
    without secrets. Auto-material events skip classification entirely.
    """

    def __init__(self, config: dict) -> None:
        cfg = config.get("classifier", {})
        self.provider = cfg.get("provider", "claude")
        self.model = cfg.get("model", "claude-haiku-4-5-20251001")
        self.api_base = cfg.get("api_base", "https://api.anthropic.com").rstrip("/")
        self.max_tokens = int(cfg.get("max_tokens", 700))
        self.api_key = os.environ.get(cfg.get("api_key_env", "ANTHROPIC_API_KEY"), "").strip()
        thresholds = config.get("thresholds", {})
        self.notify_score = float(thresholds.get("notify_score", 4))
        self.digest_score = float(thresholds.get("digest_score", 2))

    @property
    def llm_enabled(self) -> bool:
        return self.provider == "claude" and bool(self.api_key)

    def classify(self, event: RawEvent) -> ClassifiedEvent:
        if event.auto_material:
            return self._auto(event)
        if self.llm_enabled:
            try:
                return self._classify_with_llm(event)
            except (HttpJsonError, ValueError, KeyError):
                pass
        return self._classify_with_rules(event)

    def _auto(self, event: RawEvent) -> ClassifiedEvent:
        score = float(event.metadata.get("auto_score", 5))
        text = f"{event.title} {event.description}"
        return ClassifiedEvent(
            event=event,
            score=score,
            material=True,
            category=event.metadata.get("category", event.event_type),
            affects_milestones=_match_milestones(text),
            affects_senators=_match_senators(text),
            summary_cn=event.metadata.get("summary_cn", event.title),
            recommended_action="notify_now",
            confidence=1.0,
            classifier="rule:auto",
        )

    def _classify_with_rules(self, event: RawEvent) -> ClassifiedEvent:
        text = f"{event.title} {event.description}".lower()
        if any(signal in text for signal in _HIGH_SIGNALS):
            score = 4.0
        elif any(signal in text for signal in _MEDIUM_SIGNALS) or _match_senators(text):
            score = 2.5
        else:
            score = 1.0
        full_text = f"{event.title} {event.description}"
        if score >= self.notify_score:
            action = "notify_now"
        elif score >= self.digest_score:
            action = "weekly_digest"
        else:
            action = "skip"
        return ClassifiedEvent(
            event=event,
            score=score,
            material=score >= self.digest_score,
            category=event.event_type,
            affects_milestones=_match_milestones(full_text),
            affects_senators=_match_senators(full_text),
            summary_cn=event.title,
            recommended_action=action,
            confidence=0.55,
            classifier="rules",
        )

    def _classify_with_llm(self, event: RawEvent) -> ClassifiedEvent:
        user_content = (
            f"来源: {event.source} / {event.source_authority}\n"
            f"事件类型: {event.event_type}\n"
            f"标题: {event.title}\n"
            f"内容: {event.description}\n"
            f"链接: {event.url}"
        )
        payload = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "system": SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_content}],
        }
        response = request_json(
            "POST",
            f"{self.api_base}/v1/messages",
            headers={
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
            },
            payload=payload,
        )
        text_parts = [
            block.get("text", "")
            for block in response.get("content", [])
            if block.get("type") == "text"
        ]
        data = _extract_json("".join(text_parts))

        score = float(data.get("score", 0))
        valid_stages = set(seed_data.stage_keywords().keys())
        valid_senators = set(seed_data.senator_names())
        action = data.get("recommended_action", "weekly_digest")
        if action not in ("notify_now", "weekly_digest", "skip"):
            action = "weekly_digest"
        return ClassifiedEvent(
            event=event,
            score=score,
            material=score >= self.digest_score,
            category=str(data.get("category", event.event_type)),
            affects_milestones=[s for s in data.get("affects_milestones", []) if s in valid_stages],
            affects_senators=[s for s in data.get("affects_senators", []) if s in valid_senators],
            summary_cn=str(data.get("summary_cn", event.title)),
            recommended_action=action,
            confidence=float(data.get("confidence", 0.7)),
            classifier=f"claude:{self.model}",
        )
