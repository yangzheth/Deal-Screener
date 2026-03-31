# Source Strategy

Checked on `2026-03-30`.

## Official References

- WeChat Official Account developer overview:
  <https://developers.weixin.qq.com/doc/offiaccount/Getting_Started/Overview.html>
- Xiaohongshu Mini App Open Platform:
  <https://miniapp.xiaohongshu.com/>
- Xiaohongshu Mini App management rules:
  <https://miniapp.xiaohongshu.com/doc/DC246380>
- Maimai official homepage:
  <https://maimai.cn/>

## Interpretation

This is the design assumption behind the MVP:

- WeChat Official Accounts: official developer docs are for your own official
  account integration and message flows.
- Xiaohongshu: official docs are for mini apps and platform operations.
- Maimai: I could not find an official public developer content API on the
  official domain on `2026-03-30`.

Inference:

There does not appear to be a broadly available, official, third-party API that
lets you bulk pull arbitrary public content across WeChat Official Accounts,
Xiaohongshu notes, and Maimai posts the way you would use a standard news API.

That is why the application should treat sources in three tiers.

## Tier 1: Pull Automatically Now

These are the channels this MVP supports directly today.

| Channel | Access Path | Why It Works | Use In MVP |
| --- | --- | --- | --- |
| Google News search RSS | Public RSS-style search feed | Fast, broad, easy to query by AI + funding / talent terms | `google_news` |
| Direct RSS / Atom feeds | Standard feed endpoints | Stable machine-readable input | `rss` |
| Company newsrooms / blogs | Feed or static pages you control in config | High precision for named companies | `rss` or future HTML adapter |
| Job boards / press rooms / SEC style disclosures | Public pages or feeds | Good for executive changes and official announcements | future adapter |

## Tier 2: Semi-Automatic, Human In The Loop

These channels are strategically important but should be handled through
exporters or browser-assisted capture, not through a generic bulk crawler.

| Channel | Recommended Path | Why |
| --- | --- | --- |
| WeChat Official Accounts | Save article URLs you can access, export title / summary / text into JSONL | Official docs do not expose a public bulk-search API for arbitrary third-party content |
| Xiaohongshu | Browser-assisted export from accounts / searches you are allowed to access | Official platform focus is mini apps, not open third-party note ingestion |
| Maimai | Browser-assisted export from company pages, topics, and posts you can access | No public content API found on official domain |

## Tier 3: Paid / Licensed Data

These are usually worth adding once the daily workflow proves useful.

| Source | Typical Value |
| --- | --- |
| Crunchbase | Funding rounds, investors, entity graph |
| PitchBook | Deeper private-market and people data |
| Tracxn | Startup tracking and sector views |
| ITjuzi | China startup and financing coverage |
| Qichacha / Tianyancha | China entity and legal / hiring context |

## Recommended Production Architecture

1. Public feeds for coverage.
2. Named watchlists for precision.
3. Manual or browser-assisted exports for restricted social channels.
4. Rule-based extraction first.
5. LLM enrichment after you have a clean evidence store.
6. Push only high-scoring items in real time; send the full digest once daily.

## What "All Possible Channels" Means In Practice

For an investor workflow, "all possible channels" should mean:

- every source you can ingest reliably
- every source you have rights to access
- every source you can normalize into a common event schema

It should not mean brittle scraping around authentication, rate limits, or
anti-bot controls. The more durable approach is to standardize the downstream
schema and keep source adapters pluggable.
