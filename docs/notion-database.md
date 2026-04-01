# Notion Database Delivery

This project can now sync daily signals into a dedicated Notion database.

## Current Database

- Database: [AI Primary Market Watch](https://www.notion.so/d49f43cf43f44b30b9ea7faa774d6d1c)
- Parent page: [Deal Highlights](https://www.notion.so/e68a7aea4d5449c3b7a2873e1eac05a1)
- Data source ID: `collection://a1f37ba1-0e98-457e-82f1-db47ec20ab17`

## Local Setup

1. Create a Notion internal integration in the Notion developer console.
2. Copy the integration token.
3. Share the `AI Primary Market Watch` database with that integration inside Notion.
4. Set the token in PowerShell:

```powershell
$env:NOTION_API_TOKEN = "your_notion_token"
```

To persist it for future shells:

```powershell
setx NOTION_API_TOKEN "your_notion_token"
```

After using `setx`, open a new shell before running the project again.

## Local Config

The live local config is wired in [config/delivery.json](D:\Codex Project\Deal-Screener\config\delivery.json).

The Notion delivery expects these properties in the target database:

- `Signal`
- `Signal Key`
- `Run Date`
- `Published At`
- `Event Type`
- `Geography`
- `Score`
- `Source ID`
- `Channel`
- `Entities`
- `Summary`
- `Rationale`
- `Source URL`
- `Local Report Path`

## Cloud Schedule

This repo now includes a GitHub Actions workflow at [daily-market-watch.yml](D:\Codex Project\Deal-Screener\.github\workflows\daily-market-watch.yml).

Default behavior:

- Runs every day at `09:00` in the `Asia/Shanghai` time zone.
- Can also be started manually from the GitHub `Actions` tab.
- Creates a runtime-only `config/delivery.json` on the runner.
- Runs the test suite before syncing to Notion.

## GitHub Setup

1. Push the latest repo changes to the default branch on GitHub.
2. In GitHub, open `Settings -> Secrets and variables -> Actions`.
3. Add a repository secret named `NOTION_API_TOKEN`.
4. Use a fresh token there.

Recommended: rotate the token you pasted into chat, then store the new token in GitHub Actions.

Once the secret is set, GitHub will run the workflow in the cloud each day without needing your laptop to stay on.

## Behavior

- Each detected signal becomes one row in Notion.
- Re-running the same day updates matching rows instead of inserting duplicates.
- If a later run removes a same-day signal, the old Notion row is archived automatically.
- The sync key is based on the signal identity plus run date.

## Run Locally

```powershell
python -m market_intel_watch daily --config-dir config --output-dir output
```

## Official References

- https://developers.notion.com/docs/working-with-databases
- https://developers.notion.com/reference/post-page
- https://developers.notion.com/reference/query-a-data-source
- https://docs.github.com/actions/using-workflows/events-that-trigger-workflows#schedule
- https://docs.github.com/actions/security-guides/using-secrets-in-github-actions
