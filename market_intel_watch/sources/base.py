from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

from market_intel_watch.models import SourceDocument


class SourceAdapter(ABC):
    def __init__(self, config: dict) -> None:
        self.config = config
        self.source_id = config["id"]
        self.channel = config.get("channel", config["type"])

    @abstractmethod
    def fetch(self, run_date: date) -> list[SourceDocument]:
        raise NotImplementedError
