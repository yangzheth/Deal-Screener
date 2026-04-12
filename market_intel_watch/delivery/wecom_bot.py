from __future__ import annotations

import json
from pathlib import Path
from urllib.request import Request, urlopen

from market_intel_watch.delivery.base import DeliveryChannel
from market_intel_watch.delivery.renderers import build_wecom_markdown
from market_intel_watch.models import DailyRunResult


class WeComBotDelivery(DeliveryChannel):
    def build_payload(self, result: DailyRunResult) -> dict:
        signals = self.select_signals(result)
        content = build_wecom_markdown(
            signals,
            run_date=result.run_date.date().isoformat(),
            documents_fetched=result.documents_fetched,
            documents_deduped=result.documents_deduped,
            errors=result.errors,
            max_items=int(self.config.get("max_items", 8)),
            max_bytes=int(self.config.get("max_bytes", 3800)),
        )
        return {
            "msgtype": "markdown",
            "markdown": {
                "content": content,
            },
        }

    def deliver(self, result: DailyRunResult, output_path: Path) -> None:
        del output_path
        payload = json.dumps(self.build_payload(result)).encode("utf-8")
        request = Request(
            self.config["url"],
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=20) as response:
            response.read()
