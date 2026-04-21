from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from market_intel_watch.extractors.llm_scorer import (
    ClaudeCLIRunner,
    LLMSignalScorer,
    _coerce_scores,
    _extract_result_text,
    build_prompt,
    is_llm_enabled,
    load_reader_profile,
)
from market_intel_watch.models import Signal


def build_signal(**overrides: object) -> Signal:
    defaults: dict[str, object] = {
        "event_type": "funding",
        "title": "Moonshot raises Series B",
        "summary": "Summary.",
        "url": "https://example.com/moonshot",
        "source_id": "google-news-cn-funding",
        "channel": "news",
        "published_at": datetime(2026, 3, 30, tzinfo=timezone.utc),
        "matched_entities": ["Moonshot AI"],
        "geography": "CN",
        "score": 90.0,
        "company_name": "Moonshot AI",
        "amount": "$250M",
        "follow_verdict": "Must Chase",
    }
    defaults.update(overrides)
    return Signal(**defaults)  # type: ignore[arg-type]


class HelpersTests(unittest.TestCase):
    def test_extract_result_text_unwraps_envelope(self) -> None:
        envelope = json.dumps({"result": "inner text", "duration_ms": 1000})
        self.assertEqual("inner text", _extract_result_text(envelope))

    def test_extract_result_text_passes_through_non_envelope(self) -> None:
        self.assertEqual("plain text", _extract_result_text("plain text"))

    def test_coerce_scores_parses_valid_payload(self) -> None:
        raw = json.dumps(
            {
                "scores": [
                    {"signal_id": "s0", "score": 9, "tldr": "Moonshot $250M", "reason": "Matches CN watchlist."},
                    {"signal_id": "s1", "score": 3, "tldr": "Roundup", "reason": "Low-signal secondary recap."},
                ]
            }
        )
        scores = _coerce_scores(raw, ["s0", "s1"])
        self.assertEqual(9.0, scores["s0"].score)
        self.assertEqual("Moonshot $250M", scores["s0"].tldr)
        self.assertIn("watchlist", scores["s0"].reason)
        self.assertEqual(3.0, scores["s1"].score)

    def test_coerce_scores_strips_markdown_fence(self) -> None:
        raw = "```json\n" + json.dumps({"scores": [{"signal_id": "s0", "score": 8}]}) + "\n```"
        scores = _coerce_scores(raw, ["s0"])
        self.assertEqual(8.0, scores["s0"].score)

    def test_coerce_scores_ignores_unknown_ids_and_bad_scores(self) -> None:
        raw = json.dumps(
            {
                "scores": [
                    {"signal_id": "s0", "score": "not-a-number"},
                    {"signal_id": "unknown", "score": 5},
                    {"signal_id": "s1", "score": 11},
                ]
            }
        )
        scores = _coerce_scores(raw, ["s0", "s1"])
        self.assertNotIn("s0", scores)
        self.assertNotIn("unknown", scores)
        self.assertEqual(10.0, scores["s1"].score)

    def test_coerce_scores_raises_on_malformed(self) -> None:
        with self.assertRaises(ValueError):
            _coerce_scores("not json", ["s0"])


class BuildPromptTests(unittest.TestCase):
    def test_includes_profile_and_signals(self) -> None:
        prompt = build_prompt("Reader profile body", [("s0", build_signal())])
        self.assertIn("Reader profile body", prompt)
        self.assertIn("Moonshot AI", prompt)
        self.assertIn('"signal_id": "s0"', prompt)


class LoadReaderProfileTests(unittest.TestCase):
    def test_prefers_primary_file(self) -> None:
        with tempfile.TemporaryDirectory() as root_str:
            root = Path(root_str)
            (root / "reader_profile.md").write_text("primary", encoding="utf-8")
            (root / "reader_profile.sample.md").write_text("sample", encoding="utf-8")
            self.assertEqual("primary", load_reader_profile(root))

    def test_falls_back_to_sample(self) -> None:
        with tempfile.TemporaryDirectory() as root_str:
            root = Path(root_str)
            (root / "reader_profile.sample.md").write_text("sample", encoding="utf-8")
            self.assertEqual("sample", load_reader_profile(root))

    def test_missing_raises(self) -> None:
        with tempfile.TemporaryDirectory() as root_str:
            with self.assertRaises(FileNotFoundError):
                load_reader_profile(Path(root_str))


class LLMSignalScorerTests(unittest.TestCase):
    def test_scoring_enriches_metadata(self) -> None:
        signals = [build_signal(title=f"Signal {i}") for i in range(3)]
        runner = ClaudeCLIRunner()

        def fake_run(self, prompt: str) -> str:  # type: ignore[no-untyped-def]
            del self, prompt
            return json.dumps(
                {
                    "result": json.dumps(
                        {
                            "scores": [
                                {"signal_id": "s0", "score": 9, "tldr": "T0", "reason": "R0"},
                                {"signal_id": "s1", "score": 6, "tldr": "T1", "reason": "R1"},
                                {"signal_id": "s2", "score": 2, "tldr": "T2", "reason": "R2"},
                            ]
                        }
                    )
                }
            )

        with patch.object(ClaudeCLIRunner, "run", autospec=True, side_effect=fake_run):
            scorer = LLMSignalScorer("profile", runner=runner)
            scored = scorer.score(signals)

        self.assertEqual(3, len(scored))
        self.assertEqual("9.0", scored[0].metadata["llm_score"])
        self.assertEqual("T0", scored[0].metadata["llm_tldr"])
        self.assertEqual("true", scored[0].metadata["llm_scored"])
        self.assertEqual("2.0", scored[2].metadata["llm_score"])

    def test_scoring_batches_respects_max_batch(self) -> None:
        signals = [build_signal(title=f"S{i}", url=f"https://e/{i}") for i in range(5)]
        call_sizes: list[int] = []

        def fake_run(self, prompt: str) -> str:  # type: ignore[no-untyped-def]
            del self
            batch_size = prompt.count('"event_type":')
            call_sizes.append(batch_size)
            ids = [f"s{idx}" for idx in range(batch_size)]
            return json.dumps(
                {"result": json.dumps({"scores": [{"signal_id": sid, "score": 8} for sid in ids]})}
            )

        with patch.object(ClaudeCLIRunner, "run", autospec=True, side_effect=fake_run):
            scorer = LLMSignalScorer("profile", max_batch=2)
            scorer.score(signals)

        self.assertEqual([2, 2, 1], call_sizes)

    def test_scoring_failure_preserves_original_signals(self) -> None:
        signals = [build_signal()]

        with patch.object(ClaudeCLIRunner, "run", autospec=True, side_effect=RuntimeError("boom")):
            scorer = LLMSignalScorer("profile")
            scored = scorer.score(signals)

        self.assertEqual(1, len(scored))
        self.assertEqual("false", scored[0].metadata["llm_scored"])
        self.assertNotIn("llm_score", scored[0].metadata)

    def test_empty_signals_returns_empty(self) -> None:
        self.assertEqual([], LLMSignalScorer("profile").score([]))


class ClaudeCLIRunnerTests(unittest.TestCase):
    def test_run_builds_expected_command(self) -> None:
        captured: dict[str, object] = {}

        def fake_run(command, capture_output, text, timeout, env):  # type: ignore[no-untyped-def]
            captured["command"] = command
            captured["timeout"] = timeout
            return subprocess.CompletedProcess(args=command, returncode=0, stdout='{"result": "ok"}', stderr="")

        with patch("market_intel_watch.extractors.llm_scorer.subprocess.run", side_effect=fake_run):
            runner = ClaudeCLIRunner(model="claude-sonnet-4-6", cli_path="/usr/local/bin/claude")
            output = runner.run("hello")

        self.assertIn("-p", captured["command"])
        self.assertIn("--output-format", captured["command"])
        self.assertIn("claude-sonnet-4-6", captured["command"])
        self.assertIn("ok", output)

    def test_run_raises_on_nonzero_exit(self) -> None:
        def fake_run(command, capture_output, text, timeout, env):  # type: ignore[no-untyped-def]
            return subprocess.CompletedProcess(args=command, returncode=1, stdout="", stderr="auth failed")

        with patch("market_intel_watch.extractors.llm_scorer.subprocess.run", side_effect=fake_run):
            runner = ClaudeCLIRunner(cli_path="/bin/claude")
            with self.assertRaises(RuntimeError) as ctx:
                runner.run("prompt")
        self.assertIn("auth failed", str(ctx.exception))


class IsLLMEnabledTests(unittest.TestCase):
    def test_true_when_oauth_token_present(self) -> None:
        with patch.dict("os.environ", {"CLAUDE_CODE_OAUTH_TOKEN": "x"}, clear=True):
            self.assertTrue(is_llm_enabled())

    def test_true_when_api_key_present(self) -> None:
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "x"}, clear=True):
            self.assertTrue(is_llm_enabled())

    def test_false_when_neither_present(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            self.assertFalse(is_llm_enabled())


if __name__ == "__main__":
    unittest.main()
