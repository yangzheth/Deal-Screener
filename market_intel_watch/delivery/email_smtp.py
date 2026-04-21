from __future__ import annotations

from email.message import EmailMessage
from email.utils import formataddr
import os
from pathlib import Path
import smtplib

from market_intel_watch.delivery.base import DeliveryChannel
from market_intel_watch.logging_config import get_logger
from market_intel_watch.models import DailyRunResult
from market_intel_watch.reporting.email_digest import (
    DigestItem,
    rank_for_digest,
    render_email_html,
    render_email_subject,
    render_email_text,
)


logger = get_logger(__name__)


class SMTPEmailDelivery(DeliveryChannel):
    def _env(self, key: str) -> str | None:
        env_key = self.config.get(f"{key}_env")
        if env_key:
            return os.environ.get(env_key)
        return None

    def _load_credentials(self) -> tuple[str, str, str, int, bool]:
        host = self.config.get("smtp_host")
        port = int(self.config.get("smtp_port", 587))
        use_starttls = bool(self.config.get("use_starttls", True))

        username = self.config.get("username") or self._env("username")
        password = self._env("password")

        missing = [
            name for name, value in (("smtp_host", host), ("username", username), ("password", password)) if not value
        ]
        if missing:
            raise RuntimeError(f"SMTP config missing: {', '.join(missing)}")
        return host, username, password, port, use_starttls  # type: ignore[return-value]

    def _build_items(self, result: DailyRunResult) -> list[DigestItem]:
        top_n = int(self.config.get("top_n", 8))
        min_llm_score = float(self.config.get("min_llm_score", 7.0))
        return rank_for_digest(
            self.select_signals(result),
            top_n=top_n,
            min_llm_score=min_llm_score,
        )

    def _build_message(self, result: DailyRunResult, items: list[DigestItem]) -> EmailMessage:
        run_date = result.run_date.date()
        subject = render_email_subject(run_date, items)
        fallback_note = None
        if not any(item.tldr for item in items):
            fallback_note = "提示：部分条目无 LLM 摘要，使用规则摘要兜底。"

        text = render_email_text(run_date, items, fallback_note=fallback_note)
        html = render_email_html(run_date, items, fallback_note=fallback_note)

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = formataddr((self.config.get("from_name", "AI Market Watch"), self.config["from_addr"]))
        recipients = self.config.get("to") or []
        if isinstance(recipients, str):
            recipients = [recipients]
        if not recipients:
            raise RuntimeError("SMTP delivery: 'to' must be a non-empty list or string")
        msg["To"] = ", ".join(recipients)
        msg.set_content(text)
        msg.add_alternative(html, subtype="html")
        return msg

    def deliver(self, result: DailyRunResult, output_path: Path) -> None:
        del output_path
        items = self._build_items(result)
        host, username, password, port, use_starttls = self._load_credentials()
        msg = self._build_message(result, items)

        with smtplib.SMTP(host, port, timeout=30) as client:
            client.ehlo()
            if use_starttls:
                client.starttls()
                client.ehlo()
            client.login(username, password)
            client.send_message(msg)
        logger.info("email delivered to %s (items=%d)", msg["To"], len(items))
