from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path

from market_intel_watch.config import load_delivery_config
from market_intel_watch.delivery import build_deliveries
from market_intel_watch.pipeline import run_daily


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AI primary market watch")
    subparsers = parser.add_subparsers(dest="command", required=True)

    daily = subparsers.add_parser("daily", help="Run the daily intelligence job")
    daily.add_argument(
        "--config-dir",
        type=Path,
        default=Path("config"),
        help="Directory containing watchlist.json and sources.json",
    )
    daily.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory where reports will be written",
    )
    daily.add_argument(
        "--date",
        dest="run_date",
        default=None,
        help="Override run date in YYYY-MM-DD format",
    )
    return parser.parse_args()


def _parse_date(value: str | None) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> int:
    args = parse_args()
    if args.command != "daily":
        raise ValueError(f"Unsupported command: {args.command}")

    run_date = _parse_date(args.run_date)
    result = run_daily(args.config_dir, args.output_dir, run_date)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / f"{run_date.isoformat()}-daily-report.md"
    output_path.write_text(result.report_text, encoding="utf-8")

    delivery_errors: list[str] = []
    delivery_configs = load_delivery_config(args.config_dir)
    deliveries = build_deliveries(delivery_configs)
    for delivery in deliveries:
        try:
            delivery.deliver(result, output_path)
        except Exception as exc:
            delivery_errors.append(f"{delivery.delivery_id}: {exc}")

    print(f"Wrote report: {output_path}")
    print(f"Documents fetched: {result.documents_fetched}")
    print(f"Documents after dedupe: {result.documents_deduped}")
    print(f"Signals detected: {len(result.signals)}")
    if result.errors:
        print("Warnings:")
        for item in result.errors:
            print(f"- {item}")
    if delivery_errors:
        print("Delivery warnings:")
        for item in delivery_errors:
            print(f"- {item}")
    return 0
