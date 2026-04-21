from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import subprocess

from market_intel_watch.logging_config import get_logger
from market_intel_watch.models import Signal


logger = get_logger(__name__)

DEFAULT_MODEL = "claude-sonnet-4-6"
DEFAULT_TIMEOUT_SECONDS = 90
DEFAULT_MAX_BATCH = 20


@dataclass(slots=True)
class LLMScore:
    score: float
    tldr: str
    reason: str


SYSTEM_INSTRUCTIONS = """You are an information-filter agent for a specific AI-focused venture investor.

You will be given:
1. A reader profile (in Markdown) that describes what this reader values and rejects.
2. A batch of candidate market-intelligence signals (each with title, source, URL, summary, event metadata).

For each signal, produce a score from 0 to 10 for THIS READER, following the reader profile's judgment scenarios:
- 9-10: Must read today. Fits "High Signal" and specific "值得看" criteria.
- 6-8: Worth a quick scan. Relevant but not urgent.
- 0-5: Should be skipped or deprioritised. Matches "Low Signal / Noise" rules.

Respond in strict JSON. No prose outside JSON. Shape:
{
  "scores": [
    {"signal_id": "<id>", "score": <int 0-10>, "tldr": "<one-sentence, <=120 chars, no hype words>", "reason": "<why THIS reader should care; reference profile sections; <=200 chars>"},
    ...
  ]
}

Rules:
- tldr must be specific and factual. No filler like "an important move" or "big news".
- reason must explain why the reader should care, ideally referring to specific watchlist entities, topics, or rejection rules from the profile.
- Prefer Chinese for CN market signals, English for US market signals, matching the source material.
- Never fabricate facts not present in the signal. If the signal lacks data (e.g. no amount), say so in reason.
"""


def load_reader_profile(config_dir: Path) -> str:
    primary = config_dir / "reader_profile.md"
    fallback = config_dir / "reader_profile.sample.md"
    path = primary if primary.exists() else fallback
    if not path.exists():
        raise FileNotFoundError(
            f"reader profile missing; expected {primary} or {fallback}"
        )
    return path.read_text(encoding="utf-8")


def _signal_payload(signal: Signal, signal_id: str) -> dict:
    return {
        "signal_id": signal_id,
        "event_type": signal.event_type,
        "title": signal.title,
        "summary": signal.summary,
        "url": signal.url,
        "source_id": signal.source_id,
        "channel": signal.channel,
        "published_at": signal.published_at.isoformat() if signal.published_at else None,
        "geography": signal.geography,
        "company_name": signal.company_name,
        "matched_entities": signal.matched_entities,
        "amount": signal.amount,
        "round_stage": signal.round_stage,
        "investors": signal.investors,
        "key_people": signal.key_people,
        "categories": signal.categories,
        "rule_score": signal.score,
        "source_count": signal.source_count,
    }


def build_prompt(reader_profile: str, signals: list[tuple[str, Signal]]) -> str:
    payload = {"signals": [_signal_payload(signal, signal_id) for signal_id, signal in signals]}
    return (
        "## Reader profile\n\n"
        f"{reader_profile.strip()}\n\n"
        "## Candidate signals\n\n"
        f"```json\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n```\n\n"
        "Return only the JSON object described in the system instructions."
    )


class ClaudeCLIRunner:
    """Invoke claude CLI headlessly. Uses CLAUDE_CODE_OAUTH_TOKEN from env."""

    def __init__(
        self,
        *,
        model: str = DEFAULT_MODEL,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        cli_path: str | None = None,
    ) -> None:
        self.model = model
        self.timeout = timeout
        self.cli_path = cli_path or shutil.which("claude") or "claude"

    def run(self, prompt: str) -> str:
        command = [
            self.cli_path,
            "-p",
            prompt,
            "--model",
            self.model,
            "--output-format",
            "json",
        ]
        logger.info("invoking claude CLI (model=%s, prompt_bytes=%d)", self.model, len(prompt.encode("utf-8")))
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=self.timeout,
            env=os.environ.copy(),
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"claude CLI failed (code={completed.returncode}): {completed.stderr.strip()[:400]}"
            )
        return completed.stdout


def _extract_result_text(raw_stdout: str) -> str:
    """`claude -p --output-format json` emits a JSON envelope with a `result` string.

    Fall back to raw text if the envelope shape is missing (e.g. plain mode).
    """
    raw_stdout = raw_stdout.strip()
    if not raw_stdout:
        raise ValueError("empty claude CLI output")
    try:
        envelope = json.loads(raw_stdout)
    except json.JSONDecodeError:
        return raw_stdout
    if isinstance(envelope, dict) and isinstance(envelope.get("result"), str):
        return envelope["result"]
    return raw_stdout


def _coerce_scores(raw_text: str, signal_ids: list[str]) -> dict[str, LLMScore]:
    text = raw_text.strip()
    if text.startswith("```"):
        text = text.strip("`").split("\n", 1)[1] if "\n" in text else text.strip("`")
        if text.endswith("```"):
            text = text[:-3]
    # Models sometimes wrap with ```json fences even inside `result`.
    fence_idx = text.find("{")
    if fence_idx > 0:
        text = text[fence_idx:]
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"failed to parse LLM JSON: {exc}; head={text[:200]!r}") from exc

    items = parsed.get("scores") if isinstance(parsed, dict) else None
    if not isinstance(items, list):
        raise ValueError(f"LLM response missing 'scores' array: {parsed!r}")

    scores: dict[str, LLMScore] = {}
    allowed = set(signal_ids)
    for item in items:
        if not isinstance(item, dict):
            continue
        signal_id = str(item.get("signal_id", ""))
        if signal_id not in allowed:
            continue
        raw_score = item.get("score", 0)
        try:
            score = float(raw_score)
        except (TypeError, ValueError):
            continue
        scores[signal_id] = LLMScore(
            score=max(0.0, min(10.0, score)),
            tldr=str(item.get("tldr", "")).strip(),
            reason=str(item.get("reason", "")).strip(),
        )
    return scores


class LLMSignalScorer:
    def __init__(
        self,
        reader_profile: str,
        *,
        runner: ClaudeCLIRunner | None = None,
        max_batch: int = DEFAULT_MAX_BATCH,
    ) -> None:
        self.reader_profile = reader_profile
        self.runner = runner or ClaudeCLIRunner()
        self.max_batch = max_batch

    def score(self, signals: list[Signal]) -> list[Signal]:
        if not signals:
            return signals

        indexed = [(f"s{index}", signal) for index, signal in enumerate(signals)]
        aggregated: dict[str, LLMScore] = {}

        for start in range(0, len(indexed), self.max_batch):
            batch = indexed[start : start + self.max_batch]
            prompt = build_prompt(self.reader_profile, batch)
            full_prompt = f"{SYSTEM_INSTRUCTIONS}\n\n{prompt}"
            try:
                raw = self.runner.run(full_prompt)
                result_text = _extract_result_text(raw)
                aggregated.update(_coerce_scores(result_text, [pair[0] for pair in batch]))
            except Exception as exc:
                logger.warning("LLM scoring batch failed (start=%d): %s", start, exc)
                continue

        enriched: list[Signal] = []
        for signal_id, signal in indexed:
            score_obj = aggregated.get(signal_id)
            if score_obj is None:
                signal.metadata = {**signal.metadata, "llm_scored": "false"}
                enriched.append(signal)
                continue
            signal.metadata = {
                **signal.metadata,
                "llm_scored": "true",
                "llm_score": f"{score_obj.score:.1f}",
                "llm_tldr": score_obj.tldr,
                "llm_reason": score_obj.reason,
            }
            enriched.append(signal)
        return enriched


def is_llm_enabled() -> bool:
    """Enabled when an auth token is present; disabled otherwise so tests and offline runs stay clean."""
    return bool(
        os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
        or os.environ.get("ANTHROPIC_API_KEY")
    )
