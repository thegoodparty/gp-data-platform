---
keywords: databricks, data matching, ICP, joins, campaigns, ballotready, position ID, race ID
summary: Patterns and join paths for matching ad-hoc tables to ICP flags and other reference data in Databricks
last_updated: 2026-02-13
---

# Data Matching Runbook

> **For Claude**: When asked to help with a data matching exercise, read this runbook first for established join paths, reference tables, and known gotchas — don't build queries from scratch. After completing a matching task, update this runbook with any new patterns, join paths, or gotchas discovered, but keep entries concise so this remains a quick reference.

## Key Reference Tables

| Table | Schema | Join Key(s) | What It Provides |
|-------|--------|-------------|------------------|
| `int__icp_offices` | `dbt` | `br_database_position_id` | `icp_office_win`, `icp_office_serve`, `voter_count`, `normalized_position_type`, `judicial`, `appointed` |
| `campaigns` | `mart_civics` | `user_email`, `ballotready_position_id`, `user_id`, `campaign_id` | Bridge from email/user to BallotReady position ID |
| `stg_airbyte_source__ballotready_api_race` | `dbt` | `database_id` (race ID), `position.databaseId` (struct) | Bridge from race ID to position ID |
| `candidacy` | `mart_civics` | `br_position_database_id`, `gp_candidacy_id` | Candidacy-level data with direct position ID |

All tables live under `goodparty_data_catalog.<schema>`.

---

## Join Paths to ICP Offices

### Path 1: Via Email → Campaigns (no position/race ID available)

**When to use**: Your table has user emails but no BallotReady IDs.

```
email → campaigns.user_email → campaigns.ballotready_position_id → int__icp_offices.br_database_position_id
```

```sql
SELECT
    h.*,
    c.ballotready_position_id,
    c.campaign_id,
    icp.icp_office_win,
    icp.icp_office_serve,
    icp.voter_count,
    icp.normalized_position_type,
    icp.judicial,
    icp.appointed
FROM your_table h
LEFT JOIN goodparty_data_catalog.mart_civics.campaigns c
    ON LOWER(h.email) = LOWER(c.user_email)
LEFT JOIN goodparty_data_catalog.dbt.int__icp_offices icp
    ON c.ballotready_position_id = icp.br_database_position_id
```

**Used in**: `win_icp_signups_p2v_w_icp` (2026-02-13) — 90% match rate (55/61).

### Path 2: Via BallotReady Race ID (no position ID available)

**When to use**: Your table has `br_race_id` but not `br_position_database_id`.

```
br_race_id → stg_airbyte_source__ballotready_api_race.database_id → position.databaseId → int__icp_offices.br_database_position_id
```

```sql
SELECT
    h.*,
    icp.icp_office_win,
    icp.icp_office_serve,
    icp.voter_count,
    icp.normalized_position_type,
    icp.judicial,
    icp.appointed
FROM your_table h
LEFT JOIN goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r
    ON TRY_CAST(h.br_race_id AS INT) = r.database_id
LEFT JOIN goodparty_data_catalog.dbt.int__icp_offices icp
    ON r.position.databaseId = icp.br_database_position_id
```

**Used in**: `hubspot_leads_jack_20260211_w_icp` (2026-02-11) — 92.5% match rate (1164/1259).

### Path 3: Direct Position ID (simplest)

**When to use**: Your table already has `br_position_database_id` or `ballotready_position_id`.

```sql
SELECT h.*, icp.*
FROM your_table h
LEFT JOIN goodparty_data_catalog.dbt.int__icp_offices icp
    ON h.br_position_database_id = icp.br_database_position_id
```

**Used in**: dbt models like `leads_win_candidacy`, `users_win_candidacy`.

---

## Gotchas

- **Delta column names**: `CREATE OR REPLACE TABLE` fails if source columns have spaces. Alias everything to snake_case.
- **`TRY_CAST` for race IDs**: Some `br_race_id` values are non-numeric strings (e.g. `"ts_found_race_net_new"`). Use `TRY_CAST(br_race_id AS INT)` instead of `CAST`.
- **Struct access**: The race staging table stores position as a struct — use `r.position.databaseId` (camelCase).
- **Email case**: Always use `LOWER()` on both sides when joining on email.
- **One-to-many risk**: A user email can match multiple campaigns. Check for duplicates with `GROUP BY email HAVING COUNT(*) > 1` before saving.

---

## Output Tables Created

| Table | Date | Source | Join Path | Rows | Match Rate |
|-------|------|--------|-----------|------|------------|
| `private_tristan.hubspot_leads_jack_20260211_w_icp` | 2026-02-11 | HubSpot leads export | Path 2 (race ID) | 1,259 | 92.5% |
| `private_tristan.win_icp_signups_p2v_w_icp` | 2026-02-13 | Win ICP signups P2V sample | Path 1 (email) | 61 | 90.2% |
