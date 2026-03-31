# AI Primary Market Watch

`AI Primary Market Watch` is a lightweight MVP for investors who want a daily
signal feed focused on:

- primary-market fundraising
- founder / executive departures
- strategic hires and leadership changes

The default scope is China and the United States, with a bias toward AI-native
startups, founders, and senior operators.

## What This MVP Does

This repository gives you a working backbone for a daily intelligence pipeline:

1. ingest public machine-readable sources first
2. accept semi-automatic drops for restricted social platforms
3. detect fundraising and talent-move signals
4. rank signals by watchlist relevance and source weight
5. write a daily Markdown report

By default, the pipeline keeps documents published within the past 7 days so
old news does not keep resurfacing in a daily digest.

The code is intentionally standard-library only, so you can run it after
installing Python without adding external packages.

## Why The Design Is Hybrid

Not every platform exposes a public API for pulling third-party content at
scale. As checked on `2026-03-30`, the official documentation I could find for
WeChat Official Accounts and Xiaohongshu centers on building on top of your own
account or app context, not on bulk-searching arbitrary third-party content.
For Maimai, I could not find an official public content-ingestion developer API
on the official domain. See [docs/source-strategy.md](docs/source-strategy.md)
for the source matrix and links.

That means the safest production architecture is:

- public feeds and search feeds for broad coverage
- semi-automatic imports for restricted social channels
- optional paid data vendors for deeper coverage later

## Project Layout

```text
config/
  delivery.sample.json
  sources.sample.json
  watchlist.sample.json
docs/
  source-strategy.md
inbox/
  manual/
market_intel_watch/
  delivery/
  extractors/
  reporting/
  sources/
  config.py
  main.py
  models.py
  pipeline.py
output/
```

## Source Types In This MVP

- `google_news`: public search-feed ingestion for broad market coverage
- `rss`: any direct RSS/Atom feed you decide to trust
- `manual_drop`: JSONL records exported from WeChat / Xiaohongshu / Maimai /
  browser-operator workflows

## Quick Start

1. Install Python `3.11+`.
2. Copy:

```text
config/watchlist.sample.json -> config/watchlist.json
config/sources.sample.json -> config/sources.json
```

3. Edit the watchlist and source queries.
4. Optional: copy `config/delivery.sample.json -> config/delivery.json` and fill
   a webhook if you want push delivery.
5. Run:

```bash
python -m market_intel_watch daily --config-dir config --output-dir output
```

6. Read the generated report in `output/YYYY-MM-DD-daily-report.md`.

If `config/delivery.json` exists, the app will also attempt the configured
deliveries after writing the file.

## Enterprise WeChat Delivery

This repository now supports Enterprise WeChat group-robot delivery through
`type: "wecom_bot"`.

Setup:

1. In Enterprise WeChat, create a group robot and copy the webhook URL.
2. Put that URL into `config/delivery.json`.
3. Keep `enabled` as `true`.
4. Run the daily job.

Example:

```json
{
  "deliveries": [
    {
      "id": "wecom-daily-digest",
      "type": "wecom_bot",
      "enabled": true,
      "url": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY",
      "max_items": 8,
      "max_bytes": 3800
    }
  ]
}
```

The Enterprise WeChat push is intentionally shorter than the full Markdown
report. It sends:

- a one-screen summary
- funding / departure / hire counts
- the top scored signals
- the first few source warnings

## Manual Drop Format

For restricted channels, create `.jsonl` files in `inbox/manual/` with one
record per line:

```json
{"channel":"wechat","title":"Founder leaves stealth AI startup","url":"https://example.com/post/1","published_at":"2026-03-30T08:00:00+08:00","summary":"Short clue from a trusted operator chat.","content":"Original text or cleaned export from your browser workflow."}
```

Accepted fields:

- `channel`
- `title`
- `url`
- `published_at`
- `summary`
- `content`
- `authors`
- `tags`
- `metadata`

## What To Build Next

- Add a browser-assisted exporter for WeChat article links you can legally view.
- Add a Xiaohongshu / Maimai capture tool that exports cleaned JSONL into
  `inbox/manual/`.
- Add push delivery to email, Feishu, WeCom, Telegram, or Slack.
- Add paid APIs such as Crunchbase / PitchBook / Tracxn / ITjuzi if you have
  licenses.
- Replace the rule-based extractor with an LLM classifier once you have a
  stable event schema and feedback loop.

## Notes

- This repository was scaffolded in an environment without Python or Git
  installed, so I could not execute or test the code locally here.
- The code is written to be easy to extend rather than fully productionized on
  day one.
