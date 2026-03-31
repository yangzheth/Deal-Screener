# Enterprise WeChat Delivery

Checked on `2026-03-30`.

## Design Choice

This project uses the Enterprise WeChat group robot webhook and sends messages
as `markdown`.

Why this path:

- easiest setup for a single investor or small team
- no separate app registration needed
- works well for one-way daily market digests

## Config

Use a delivery block like this:

```json
{
  "id": "wecom-daily-digest",
  "type": "wecom_bot",
  "enabled": true,
  "url": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY",
  "max_items": 8,
  "max_bytes": 3800
}
```

## What Gets Sent

The app does not send the full local report by default. Instead it composes a
compact message with:

- run-date snapshot
- funding / departure / hire counts
- top ranked signals
- up to three source warnings

This makes the message much more readable on mobile and avoids overrunning the
group robot message size budget.

## References

- Enterprise WeChat group robot docs:
  <https://developer.work.weixin.qq.com/document/path/91770>
- Older official API doc path still commonly referenced:
  <https://work.weixin.qq.com/api/doc/90000/90136/91770>

Note:

I was able to confirm the official documentation paths and the `markdown`
message approach during implementation, but the official pages were not
consistently retrievable in this environment. The code targets the documented
group-robot webhook pattern and markdown message schema.
