from __future__ import annotations

import copy
from datetime import datetime, timezone
from pathlib import Path
import unittest

from market_intel_watch.monitors.clarity_act import seed_data
from market_intel_watch.monitors.clarity_act.classifier import (
    MaterialClassifier,
    _match_milestones,
    _match_senators,
)
from market_intel_watch.monitors.clarity_act.config import DEFAULT_CONFIG, load_clarity_config
from market_intel_watch.monitors.clarity_act.dedup import DedupStore
from market_intel_watch.monitors.clarity_act.digest import render_digest
from market_intel_watch.monitors.clarity_act.models import (
    STANCES,
    MarketSnapshot,
    MonitorRunResult,
    RawEvent,
)
from market_intel_watch.monitors.clarity_act.pipeline import detect_odds_moves


CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"


def _test_config() -> dict:
    config = copy.deepcopy(DEFAULT_CONFIG)
    # Force the rule-based path so the suite never reaches the network.
    config["classifier"]["api_key_env"] = "CLARITY_ACT_TEST_UNSET_KEY"
    return config


def _raw_event(title: str, description: str = "", **kwargs) -> RawEvent:
    return RawEvent(
        source=kwargs.get("source", "test"),
        source_authority=kwargs.get("source_authority", "Media"),
        title=title,
        description=description,
        url=kwargs.get("url", "https://example.com/article"),
        occurred_at=kwargs.get("occurred_at", datetime(2026, 5, 18, tzinfo=timezone.utc)),
        event_type=kwargs.get("event_type", "Media Report"),
        metadata=kwargs.get("metadata", {}),
    )


class SeedDataTests(unittest.TestCase):
    def test_roster_counts(self) -> None:
        self.assertEqual(14, len(seed_data.SENATORS))
        self.assertEqual(7, len(seed_data.MILESTONES))

    def test_stances_are_valid(self) -> None:
        for senator in seed_data.SENATORS:
            self.assertIn(senator.stance, STANCES)

    def test_milestone_stages_unique_and_keyed(self) -> None:
        stages = [milestone.stage for milestone in seed_data.MILESTONES]
        self.assertEqual(len(stages), len(set(stages)))
        self.assertEqual(set(stages), set(seed_data.stage_keywords().keys()))

    def test_house_passage_recorded(self) -> None:
        house = next(m for m in seed_data.MILESTONES if m.stage == "House Passage")
        self.assertEqual("Completed", house.status)
        self.assertEqual("294-134", house.vote_tally)


class RawEventTests(unittest.TestCase):
    def test_dedup_key_is_stable(self) -> None:
        first = _raw_event("Cloture vote scheduled")
        second = _raw_event("Cloture vote scheduled")
        self.assertEqual(first.dedup_key(), second.dedup_key())

    def test_dedup_key_changes_with_content(self) -> None:
        first = _raw_event("Cloture vote scheduled")
        second = _raw_event("Cloture vote delayed")
        self.assertNotEqual(first.dedup_key(), second.dedup_key())

    def test_auto_material_flag(self) -> None:
        self.assertTrue(_raw_event("x", metadata={"auto_material": "true"}).auto_material)
        self.assertFalse(_raw_event("x").auto_material)


class ClassifierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.classifier = MaterialClassifier(_test_config())

    def test_rule_path_is_used_without_api_key(self) -> None:
        self.assertFalse(self.classifier.llm_enabled)

    def test_auto_material_event_scores_max(self) -> None:
        result = self.classifier.classify(
            _raw_event("Bill received in the Senate", metadata={"auto_material": "true"})
        )
        self.assertEqual(5.0, result.score)
        self.assertTrue(result.material)
        self.assertEqual("rule:auto", result.classifier)

    def test_high_signal_event_is_material(self) -> None:
        result = self.classifier.classify(
            _raw_event(
                "Senate invokes cloture on the CLARITY Act",
                "The Senate voted to invoke cloture on H.R. 3633.",
            )
        )
        self.assertGreaterEqual(result.score, 4.0)
        self.assertEqual("notify_now", result.recommended_action)
        self.assertIn("Senate Floor Cloture", result.affects_milestones)

    def test_senator_mention_is_medium(self) -> None:
        result = self.classifier.classify(
            _raw_event("Bill chatter", "Andy Kim weighed in on the digital asset bill today.")
        )
        self.assertEqual(2.5, result.score)
        self.assertIn("Andy Kim", result.affects_senators)

    def test_low_signal_event(self) -> None:
        result = self.classifier.classify(
            _raw_event("Opinion: crypto regulation explained", "A general industry explainer.")
        )
        self.assertEqual(1.0, result.score)
        self.assertFalse(result.material)

    def test_matchers(self) -> None:
        self.assertEqual(["Elizabeth Warren"], _match_senators("Elizabeth Warren spoke"))
        self.assertIn("Senate Floor Cloture", _match_milestones("a cloture vote looms"))


class DedupStoreTests(unittest.TestCase):
    def test_in_memory_roundtrip(self) -> None:
        with DedupStore(":memory:") as store:
            self.assertTrue(store.is_new("k1"))
            store.mark_seen("k1", "test", "hash1")
            self.assertFalse(store.is_new("k1"))
            self.assertTrue(store.is_new("k2"))


class OddsMoveTests(unittest.TestCase):
    def _snapshot(self, signed: float | None) -> MarketSnapshot:
        return MarketSnapshot(
            captured_at=datetime(2026, 5, 18, tzinfo=timezone.utc),
            polymarket_signed_2026=signed,
        )

    def test_large_move_triggers_event(self) -> None:
        events = detect_odds_moves(
            self._snapshot(0.62), {"polymarket_signed_2026": 0.50}, threshold_pct=8
        )
        self.assertEqual(1, len(events))
        self.assertTrue(events[0].auto_material)

    def test_small_move_is_ignored(self) -> None:
        events = detect_odds_moves(
            self._snapshot(0.62), {"polymarket_signed_2026": 0.60}, threshold_pct=8
        )
        self.assertEqual([], events)

    def test_missing_baseline_is_ignored(self) -> None:
        self.assertEqual([], detect_odds_moves(self._snapshot(0.62), {}, threshold_pct=8))


class ConfigTests(unittest.TestCase):
    def test_sample_config_merges_onto_defaults(self) -> None:
        config = load_clarity_config(CONFIG_DIR)
        self.assertEqual(8, config["thresholds"]["odds_move_pct"])
        self.assertTrue(
            config["notion"]["events_log_data_source_id"].startswith("collection://")
        )


class DigestTests(unittest.TestCase):
    def test_render_digest_has_clarity_header(self) -> None:
        classifier = MaterialClassifier(_test_config())
        classified = classifier.classify(
            _raw_event("Senate invokes cloture on the CLARITY Act", "Cloture vote held.")
        )
        result = MonitorRunResult(
            run_at=datetime(2026, 5, 18, tzinfo=timezone.utc),
            raw_events=1,
            new_events=1,
            classified=[classified],
            market=MarketSnapshot(captured_at=datetime(2026, 5, 18, tzinfo=timezone.utc)),
        )
        text = render_digest(result)
        self.assertIn("[CLARITY] Act Monitor", text)
        self.assertIn("Immediate alerts", text)


if __name__ == "__main__":
    unittest.main()
