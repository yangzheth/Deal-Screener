from __future__ import annotations

import json
from pathlib import Path
from urllib.request import Request, urlopen

from market_intel_watch.delivery.base import DeliveryChannel
from market_intel_watch.models import DailyRunResult


class WebhookDelivery(DeliveryChannel):
    def deliver(self, result: DailyRunResult, output_path: Path) -> None:
        payload = json.dumps(
            {
                "title": f"AI Primary Market Watch - {result.run_date.date().isoformat()}",
                "text": result.report_text,
                "report_path": str(output_path),
                "documents_fetched": result.documents_fetched,
                "documents_deduped": result.documents_deduped,
                "signals_detected": len(result.signals),
            }
        ).encode("utf-8")
        headers = self.config.get("headers", {"Content-Type": "application/json"})
        request = Request(
            self.config["url"],
            data=payload,
            headers=headers,
            method="POST",
        )
        with urlopen(request, timeout=20) as response:
            response.read()
