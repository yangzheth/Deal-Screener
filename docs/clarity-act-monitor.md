# CLARITY Act Monitor

A legislative monitor for **H.R. 3633 — the Digital Asset Market Clarity
(CLARITY) Act** — that tracks the bill through the Senate, scores incoming
events for materiality, and syncs them into a dedicated Notion workspace.

It is a self-contained sub-package of `market_intel_watch` and reuses the
repository's stdlib-only, GitHub-Actions-driven design.

## Notion tracker

A standalone **CLARITY Act Tracker** page holds four databases:

- Parent page: https://www.notion.so/36458fe9458f812a860bf9f6faadc4e3

| Database | Purpose | Data source ID |
| --- | --- | --- |
| Milestones | Legislative stage gates, target vs actual dates | `collection://8191dea3-a5cc-468a-b899-4ea63082a5a9` |
| Senator Position Tracker | Whip count for Banking Committee Democrats + floor watch | `collection://e13d831b-564d-4c90-90a4-0fb8efd7143f` |
| Events Log | Pipeline-written feed of material events | `collection://9d6794ec-80c7-4580-841e-05a4536a6eca` |
| Market & Analyst Signals | Polymarket / Kalshi odds time series | `collection://a89e1abb-3ff9-439a-9e3e-2513cf2f4e30` |

Milestones (7 stage gates) and the Senator Position Tracker (14 senators) are
seeded from `seed_data.py`. The Events Log and Market & Analyst Signals
databases are written by the pipeline; the first two are updated by the
pipeline's relation links or by hand.

## Repository layout

```text
market_intel_watch/monitors/
  base.py                     # EventSource abstract base
  clarity_act/
    models.py                 # RawEvent, ClassifiedEvent, MarketSnapshot, ...
    seed_data.py              # senator + milestone baseline (Notion seed)
    config.py                 # config loader + defaults
    sources.py                # Congress.gov, Senate Banking, news RSS, X, markets
    classifier.py             # materiality scoring (Claude API + rule fallback)
    dedup.py                  # SQLite seen-event store, 14-day TTL
    notion_sync.py            # Events Log upsert + market snapshot append
    digest.py                 # digest rendering + delivery
    pipeline.py               # run_monitor() orchestration
    __main__.py               # CLI entry point
```

## How it runs

`run_monitor()` performs one cycle:

1. Collect raw events from every enabled source.
2. Take a market snapshot (Polymarket, Kalshi, BTC, COIN) and emit a synthetic
   event when an odds field moves past `thresholds.odds_move_pct` (default 8pp).
3. Drop events already in the SQLite dedup store (key = `source:content_hash`).
4. Classify each new event for materiality.
5. Upsert material events into the Notion Events Log and append the market
   snapshot to Market & Analyst Signals.
6. Render and deliver a `[CLARITY]`-prefixed digest.

### Materiality scoring

`classifier.py` scores events 0–5:

- **Auto-material (no LLM):** Congress.gov actions and large prediction-market
  moves are pushed immediately.
- **Claude API:** when `ANTHROPIC_API_KEY` is set, events are scored by the
  model configured under `classifier.model`.
- **Rule fallback:** without an API key a deterministic keyword scorer runs, so
  the pipeline and CI work offline.

Push tiers: score ≥ 4 → immediate alert, score 2–3 → digest, score ≤ 1 → noise.

## Configuration

Copy `config/clarity_act.sample.json` to `config/clarity_act.json` and edit. The
sample already carries the Notion data source IDs (identifiers, not secrets).
Items worth setting before relying on full coverage:

- `polymarket.signed_2026_slug` / `polymarket.pass_senate_july_slug`
- `kalshi.market_ticker`
- `senate_banking.rss_url` (the committee site has no stable default feed)

### Secrets / environment variables

| Variable | Needed for | Notes |
| --- | --- | --- |
| `NOTION_API_TOKEN` | Notion sync | Share the four databases with the integration. |
| `CONGRESS_API_KEY` | Congress.gov actions | Free at https://api.congress.gov/sign-up/. |
| `ANTHROPIC_API_KEY` | LLM classification | Optional; falls back to rules if unset. |
| `TWITTER_BEARER_TOKEN` | X/Twitter source | Optional; the source is disabled by default. |

## Running

```bash
# one cycle, writes to Notion
python -m market_intel_watch.monitors.clarity_act run --config-dir config --output-dir output

# classify and render the digest only — no Notion / dedup writes
python -m market_intel_watch.monitors.clarity_act run --dry-run
```

In the cloud, `.github/workflows/clarity-act-monitor.yml` runs the monitor every
four hours. It caches the dedup store and last market snapshot between runs and
uploads the digest as an artifact.

## Coverage status

- **Production-ready:** Congress.gov actions, Polymarket, Kalshi, news RSS,
  BTC price, dedup store, classifier, Notion sync, digest.
- **Best-effort / needs input:** Senate Banking RSS (needs a feed URL), the COIN
  price feed (free CSV endpoint, fragile), and the X/Twitter source (requires
  elevated API access; disabled by default).

## Deep-analysis triggers

Some events warrant a structured human deep-dive rather than just a log entry.
The digest flags milestones touched by immediate alerts; the trigger list lives
in `seed_data.DEEP_ANALYSIS_TRIGGERS` (cloture scheduling, the 48-hour
pre-cloture window, vote results, the House concur-vs-conference fork, and any
hard-no senator softening — the real leading indicator).
