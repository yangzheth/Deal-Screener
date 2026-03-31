from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
import json

from market_intel_watch.models import SourceDocument
from market_intel_watch.sources.base import SourceAdapter


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


class ManualDropSource(SourceAdapter):
    def __init__(self, config: dict, root_dir: Path) -> None:
        super().__init__(config)
        relative_path = Path(config.get("path", "inbox/manual"))
        self.path = relative_path if relative_path.is_absolute() else root_dir / relative_path
        self.file_glob = config.get("file_glob", "*.jsonl")

    def fetch(self, run_date: date) -> list[SourceDocument]:
        del run_date
        documents: list[SourceDocument] = []
        if not self.path.exists():
            return documents

        for file_path in sorted(self.path.glob(self.file_glob)):
            with file_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    raw = json.loads(line)
                    documents.append(
                        SourceDocument(
                            source_id=self.source_id,
                            channel=raw.get("channel", self.channel),
                            title=raw["title"],
                            url=raw.get("url", ""),
                            published_at=_parse_datetime(raw.get("published_at")),
                            summary=raw.get("summary", ""),
                            content=raw.get("content", ""),
                            authors=raw.get("authors", []),
                            tags=raw.get("tags", []),
                            metadata=raw.get("metadata", {}),
                        )
                    )
        return documents
