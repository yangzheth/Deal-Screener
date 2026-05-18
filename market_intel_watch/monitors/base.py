from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class EventSource(ABC):
    """A single data source feeding a monitor pipeline.

    Concrete sources return a list of raw, unclassified records. The owning
    pipeline is responsible for dedup, classification, and delivery, so a
    source only has to know how to fetch and shape its own feed.
    """

    def __init__(self, config: dict) -> None:
        self.config = config
        self.source_id = config.get("id", self.__class__.__name__)
        self.enabled = bool(config.get("enabled", True))
        # Non-fatal issues collected during fetch (e.g. one feed of many failing).
        self.errors: list[str] = []

    @abstractmethod
    def fetch(self) -> list[Any]:
        raise NotImplementedError
