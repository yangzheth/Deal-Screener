from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from market_intel_watch.models import DailyRunResult


class DeliveryChannel(ABC):
    def __init__(self, config: dict) -> None:
        self.config = config
        self.delivery_id = config["id"]

    @abstractmethod
    def deliver(self, result: DailyRunResult, output_path: Path) -> None:
        raise NotImplementedError
