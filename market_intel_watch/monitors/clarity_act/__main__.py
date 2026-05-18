from __future__ import annotations

import argparse
from pathlib import Path

from market_intel_watch.monitors.clarity_act.pipeline import run_monitor


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m market_intel_watch.monitors.clarity_act",
        description="CLARITY Act (H.R. 3633) legislative monitor",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run", help="Run one monitoring cycle")
    run.add_argument("--config-dir", type=Path, default=Path("config"))
    run.add_argument("--output-dir", type=Path, default=Path("output"))
    run.add_argument(
        "--dry-run",
        action="store_true",
        help="Classify and render the digest without writing to Notion or the dedup store",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = run_monitor(args.config_dir, args.output_dir, dry_run=args.dry_run)

    print(f"Run at: {result.run_at.isoformat()}")
    print(f"Raw events: {result.raw_events}")
    print(f"New events after dedup: {result.new_events}")
    print(f"Immediate alerts: {len(result.by_tier('notify_now'))}")
    print(f"Digest items: {len(result.by_tier('weekly_digest'))}")
    print(f"Digest written: {result.digest_path}")
    if result.errors:
        print("Warnings:")
        for error in result.errors:
            print(f"- {error}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
