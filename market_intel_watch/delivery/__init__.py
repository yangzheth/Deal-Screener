from __future__ import annotations

from market_intel_watch.delivery.base import DeliveryChannel
from market_intel_watch.delivery.notion import NotionDatabaseDelivery
from market_intel_watch.delivery.webhook import WebhookDelivery
from market_intel_watch.delivery.wecom_bot import WeComBotDelivery


def build_deliveries(configs: list[dict]) -> list[DeliveryChannel]:
    deliveries: list[DeliveryChannel] = []
    for config in configs:
        delivery_type = config["type"]
        if delivery_type == "webhook":
            deliveries.append(WebhookDelivery(config))
        elif delivery_type == "wecom_bot":
            deliveries.append(WeComBotDelivery(config))
        elif delivery_type == "notion_database":
            deliveries.append(NotionDatabaseDelivery(config))
        else:
            raise ValueError(f"Unsupported delivery type: {delivery_type}")
    return deliveries
