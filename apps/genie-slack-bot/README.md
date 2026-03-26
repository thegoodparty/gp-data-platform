# GP Data Bot — Databricks Genie ↔ Slack Integration

A Slack bot that connects [Databricks AI/BI Genie](https://docs.databricks.com/aws/en/genie/) to Slack, allowing users to ask natural language questions about GoodParty civic data and receive SQL-backed answers directly in Slack.

Built on the [Genie Conversation API](https://docs.databricks.com/aws/en/genie/conversation-api) and deployed as a [Databricks App](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/).

## Current Scope

- **Genie space:** Civics Genie (DATA-1570) — 7-table space covering elections, election stages, candidates, candidacies, candidacy stages, campaigns, and users
- **Auth model:** Single service principal (M2M) — all users share the same data access
- **Slack surface:** `#genie-feedback` channel + direct messages to the bot
- **Runtime:** Socket mode bot plus lightweight HTTP health endpoint for Databricks App readiness

## Architecture

```
Slack (socket mode)
  │
  ├── @mention in #genie-feedback
  ├── DM to GP Data Bot
  │
  ▼
Databricks App (gp-genie-slack-bot)
  │  Python bot using slack-bolt + databricks-sdk
  │  Runs on Medium compute (0.5 DBU/hr)
  │
  ▼
Genie Conversation API
  │  Translates natural language → SQL
  │  Runs queries via SQL warehouse
  │
  ▼
Unity Catalog (civics mart tables)
```

**Key behavior:**
- Each new Slack thread starts a new Genie conversation
- In channels, users should `@mention` the bot for each follow-up question
- In DMs, each top-level message starts a new Genie conversation and replies in that DM thread continue it
- Bot posts a "Thinking..." indicator that updates in-place with the answer
- Tabular results rendered as formatted code blocks (max 10 rows)
- Every successful response includes a link to **Open Genie Console** so users can ask follow-up questions with Inspection mode, charts, and full results in the Databricks UI (note: Inspection/Analysis is UI-only and not available via the Conversation API)
- When results exceed 10 rows, the main response text includes a **truncation warning**
- Users can type **"show sql"** (or similar phrases) in a thread to see the SQL that Genie generated for the last query — this is served from cache without calling Genie again
- Thumbs up/down feedback flows back to the Genie space Monitoring tab
- Suggested follow-up questions displayed after each response

## Files

| File | Purpose |
|------|---------|
| `app.py` | Entry point — validates config, initializes clients, starts socket mode |
| `config.py` | Reads and validates environment variables |
| `databricks_genie_client.py` | Genie Conversation API wrapper (start/continue conversations, poll results, feedback, SQL extraction, console deeplinks) |
| `slack_bot.py` | Slack Bolt event handlers, message formatting, feedback buttons, SQL-on-request, deeplinks |
| `app.yaml` | Databricks App runtime config — env vars via `valueFrom` references |
| `requirements.txt` | Python dependencies |

## Infrastructure

### Databricks App

- **App name:** `gp-genie-slack-bot`
- **App URL:** `https://gp-genie-slack-bot-3578414625112071.aws.databricksapps.com`
- **Service principal:** `app-2mjyej gp-genie-slack-bot`

#### App Resources

| Key | Type | Details | Permissions |
|-----|------|---------|-------------|
| `genie-civics-space` | Genie space | Civics Genie (DATA-1570) | Can view |
| `slack-bot-token` | Secret | genie-slack-bot/SLACK_BOT_TOKEN | Can read |
| `slack-app-token` | Secret | genie-slack-bot/SLACK_APP_TOKEN | Can read |
| `slack-signing-secret` | Secret | genie-slack-bot/SLACK_SIGNING_SECRET | Can read |

#### Service Principal Permissions Required

```sql
-- Unity Catalog grants for the SP
GRANT USE CATALOG ON CATALOG <catalog> TO `app-2mjyej gp-genie-slack-bot`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `app-2mjyej gp-genie-slack-bot`;
GRANT SELECT ON TABLE <catalog>.<schema>.<table> TO `app-2mjyej gp-genie-slack-bot`;
-- Repeat for each table in the Genie space
```

Plus CAN USE on the SQL warehouse (granted via warehouse permissions UI).

### Secrets (Databricks Secret Scope)

Slack tokens are stored in a Databricks secret scope

- **Scope name:** `genie-slack-bot`
- **Keys:** `SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`, `SLACK_SIGNING_SECRET`

```bash
# View existing secrets (values never shown)
databricks secrets list-secrets genie-slack-bot

# Update a secret
databricks secrets put-secret genie-slack-bot <KEY_NAME> --string-value "new-value"
```

The `app.yaml` references these via `valueFrom` — the Databricks App runtime resolves
them at deploy time from the app resources configured in the UI.

### Slack App

- **App name:** GP Data Bot
- **Socket mode:** Enabled (no public URL required)
- **Manage at:** https://api.slack.com/apps (select GP Data Bot)

#### Bot Token Scopes

| Scope | Purpose |
|-------|---------|
| `app_mentions:read` | Respond when @mentioned |
| `channels:history` | Read messages in channels the bot is in |
| `chat:write` | Post messages |
| `im:history` | Read direct messages |
| `im:write` | Respond in DMs |

#### Event Subscriptions

| Event | Trigger |
|-------|---------|
| `app_mention` | Someone @mentions the bot |
| `message.channels` | Message posted in a channel the bot is in |
| `message.im` | Direct message to the bot |

Notes:
- In this workspace's current beta setup, plain thread follow-ups in channels are not reliably delivered to the app. Use explicit `@mentions` in channels.
- After adding new bot events or scopes in Slack, reinstall the app to the workspace so the changes take effect.

#### Tokens

| Token | Prefix | Where stored |
|-------|--------|-------------|
| Bot User OAuth Token | `xoxb-...` | Databricks secret: `SLACK_BOT_TOKEN` |
| App-Level Token | `xapp-...` | Databricks secret: `SLACK_APP_TOKEN` |
| Signing Secret | (hex string) | Databricks secret: `SLACK_SIGNING_SECRET` |

## Development

### Local Development

For local testing without deploying to Databricks Apps:

```bash
# Set environment variables
export DATABRICKS_HOST="https://dbc-3d8ca484-79f3.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
export DATABRICKS_GENIE_SPACE_ID="your-genie-space-id"
export SLACK_BOT_TOKEN="xoxb-..."
export SLACK_APP_TOKEN="xapp-..."
export SLACK_SIGNING_SECRET="..."
export PORT="3000"

# Install dependencies
pip install -r requirements.txt

# Run
python app.py
```

The Databricks SDK auto-detects `DATABRICKS_HOST` + `DATABRICKS_TOKEN` for local dev,
and auto-detects the service principal credentials when running inside a Databricks App.

### Deploy

```bash
# Sync files to workspace
databricks sync --watch . /Workspace/Users/sanjay@goodparty.org/gp-genie-slack-bot

# Deploy
databricks apps deploy gp-genie-slack-bot \
  --source-code-path /Workspace/Users/sanjay@goodparty.org/gp-genie-slack-bot
```

Check the **Logs** tab in the Databricks App UI for startup errors.

### After Deploy

Verify in the Logs tab:
- "Configuration validated successfully"
- "Healthcheck server listening on port ..."
- "Connected to Databricks workspace: ..."
- "Starting Slack bot in socket mode..."

Then test by @mentioning the bot in `#genie-feedback`.

## Known Limitations

| Limitation | Impact | Future fix |
|------------|--------|------------|
| In-memory conversation map | App restart resets thread context (existing threads start new Genie conversations) | Persist to Delta table |
| In-memory feedback map | Feedback routing state resets on restart and is bounded with eviction | Persist feedback state if long-lived threads matter |
| No chart rendering | Genie auto-generated charts don't render in Slack; users see tables + reasoning text | Render charts as images, upload to Slack |
| Table results capped at 10 rows | Longer results truncated; truncation warning + deeplink now shown in main text | Add pagination if needed |
| Single service principal | All users see the same data | Move to U2M OAuth for per-user permissions |
| No channel restriction for new mentions | Bot responds to new `@mentions` in any channel it's added to, plus DMs | Add channel allowlist in event handler |

## Deferred Improvements

The following are tracked but deferred from the current phase:

| Item | Rationale |
|------|-----------|
| **Automatic 0-row retry** | Space config hardening should reduce 0-count results at the source. Revisit if benchmarks still show frequent failures. |
| **SDK migration** | The Databricks SDK `GenieAPI` is a thin wrapper with no inspection/refinement capabilities. The bot's custom retry logic is more valuable. Revisit if SDK adds inspection mode. |
| **`office_level` dbt normalization** | 29 casing variants in the data. Using `UPPER()` in Genie instructions as stopgap. Revisit in next dbt model update. |
| **Chart/visualization rendering** | Genie Conversation API does not return chart data — UI only. Deeplink is the workaround. |

## Expanding to General Availability

When moving to broader rollout, consider:

### Genie Space

- Add tables to the Genie space and update SP grants as the data model expands
- Add more SQL examples and disambiguation instructions based on production feedback
- Review Genie Monitoring tab for common failure patterns and refine instructions

### Auth Model

- Evaluate whether M2M (shared SP) is still appropriate or switch to U2M (per-user OAuth)
- U2M requires each user to have Databricks credentials and appropriate UC grants
- If role-based access is needed (e.g., some users can see candidate data, others can't), U2M is required
- See: https://www.databricks.com/blog/access-genie-everywhere for OAuth pattern guidance

### Slack

- Add the bot to additional channels beyond `#genie-feedback`
- Consider channel allowlisting in the event handler to prevent unintended use
- Add slash commands (e.g., `/genie ask ...`) for a cleaner UX
- Consider rate limiting per user to prevent abuse

### Infrastructure

- Persist conversation map to a Delta table for restart resilience
- Add structured logging / metrics (query count, latency, error rate, feedback ratio)
- Set up alerting on app health (Databricks App restarts, error spikes)
- Consider moving from Databricks Apps to a more robust deployment if scale demands it

### Secrets Rotation

- Slack tokens can be regenerated in the Slack app settings
- Update Databricks secret scope after regeneration:
  ```bash
  databricks secrets put-secret genie-slack-bot <KEY> --string-value "new-value"
  ```
- Redeploy the app to pick up new values

## References

- [Databricks Genie Conversation API docs](https://docs.databricks.com/aws/en/genie/conversation-api)
- [Databricks Apps: Add Genie space resource](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/genie)
- [Databricks Apps: app.yaml configuration](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/app-runtime)
- [Databricksters: Integrate Slack with Genie via Databricks Apps](https://www.databricksters.com/p/integrate-slack-with-genie-natively)
- [Databricks Blog: Access Genie Everywhere (OAuth patterns)](https://www.databricks.com/blog/access-genie-everywhere)
- [Databricks Blog: Genie Conversation APIs Public Preview](https://www.databricks.com/blog/genie-conversation-apis-public-preview)
- [Reference repo (original source)](https://github.com/adgitdemo/ad_databricks/tree/main/genie-slack-app)
