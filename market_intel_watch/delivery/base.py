from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from market_intel_watch.models import DailyRunResult, Signal


class DeliveryChannel(ABC):
    def __init__(self, config: dict) -> None:
        self.config = config
        self.delivery_id = config["id"]

    def select_signals(self, result: DailyRunResult) -> list[Signal]:
        signals = result.signals
        min_score = float(self.config.get("min_score", 0))
        if min_score:
            signals = [signal for signal in signals if signal.score >= min_score]

        allowed_event_types = self.config.get("event_types", [])
        if allowed_event_types:
            allowed = {item for item in allowed_event_types}
            signals = [signal for signal in signals if signal.event_type in allowed]

        max_items = int(self.config.get("max_items", 0))
        if max_items > 0:
            signals = signals[:max_items]
        return signals

    @abstractmethod
    def deliver(self, result: DailyRunResult, output_path: Path) -> None:
        raise NotImplementedError
