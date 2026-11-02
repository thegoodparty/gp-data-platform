# Outcome variables reference

Part of the **win-analytics-knowledge** skill. Measuring "did they win?" and "did the product work?"

Owns the detail for two canonical concepts defined in [canonical_metrics.md](canonical_metrics.md):
**Win/loss outcome (preferred)** and **PMF (KR2)**.

## Quick reference

- **Business context:** two parallel success measures — electoral outcome (did they win the race) and self-reported PMF/satisfaction (did the product work for them).
- **Entity grain:** outcomes at `gp_candidacy_id` (or per-stage at `candidacy_stage`); survey responses at `submission_id`.
- **Standard hygiene filter:** for binary outcomes, `candidacy_result IN ('Won', 'Lost')`; for surveys, dedupe at `submission_id` and read rates as directional (severe self-selection).

## Routing triggers

- IF the question is binary win/loss → use `candidacy_result` (the settled outcome). Use `latest_stage_result` + `latest_stage_reached` only for funnel position; see Gotchas.
- IF the question is about the general-election stage specifically → `general_election_result`.
- IF the question compares primary vs general dynamics → per-stage `candidacy_stage.election_result`.
- IF the question is about vote share → `candidacy_stage.votes_received` / `vote_percentage` (STRING; clean before casting).
- IF the question is about product satisfaction / PMF / KR2 → the HubSpot survey section below.
- IF you need the join recipe to attach surveys → see [joins.md](joins.md).

## Electoral outcome columns, in order of preference

| Column | Grain | When to use | Caveat |
|---|---|---|---|
| **`candidacy.candidacy_result`** | candidacy | **Preferred** for won/lost (see [canonical_metrics.md](canonical_metrics.md)). The settled outcome: Won = won the seat, Lost = eliminated at any stage (so primary losers are labeled), null while still open. Same meaning in `users_win_candidacy`, `leads_win_candidacy` and `user_journey`. | Final-outcome field since 2026-07-15 (DATA-2042). Known gap: where a primary majority wins the seat outright, it reads null until the general resolves. |
| `candidacy.latest_stage_result` + `latest_stage_reached` | candidacy | Funnel position: the deepest stage reached and the result there. | A primary winner waiting on the general reads Won here, so it is not an outcome. In `user_journey` it is `candidacy_latest_stage_result`. |
| `candidacy.general_election_result` | candidacy | Strict general-stage outcome only. Use when the analytical question is "did they win the general?" specifically. | Excludes candidates who never reached the general, including primary losers. |
| `candidacy_stage.election_result` | stage | Per-stage analyses. Use when comparing primary vs general dynamics. | Funnel-aware. `accepted_values` test enforces a constrained set. |
| `candidacy_stage.is_winner` | stage | Boolean form of the above. DDHQ-authoritative when present. | Boolean. Same domain as `election_result`. |
| `candidacy_stage.votes_received` / `vote_percentage` | stage | Vote-share analyses. | `votes_received` is STRING — may contain literal "uncontested". Cast/clean before aggregating. |

### Domain values (per `m_civics.yaml:543-602`)

`Won`, `Lost`, `Runoff`, `Withdrew`, `Not on Ballot`, `Cannot Determine`

The `accepted_values` test on per-stage `election_result` excludes `Cannot Determine`. For binary outcomes, filter to `candidacy_result IN ('Won', 'Lost')`.

## Amplitude self-reported outcome supplement

`Candidacy - Did You Win Modal Completed` carries a self-reported outcome in `event_properties`:

```json
{"impersonation": <bool | null>, "status": "won" | "lost"}
```

- `status` is the candidate's self-reported outcome.
- `impersonation` flags staff-impersonation. Added 2026-03; events before then carry NULL (treat as not-impersonated).

Filter: `COALESCE(impersonation, false) = false` to drop staff-triggered events while keeping the legacy NULL cohort.

**Net new labels vs civics mart**: roughly +970 users have a self-reported outcome that `general_election_result` does NOT carry (most are primary-stage losses or 2025 candidacies the mart hasn't fully ingested). Layer this BENEATH civics-mart outcomes (mart authority on ties; Amplitude fills NULL).

**⚠ Response selection bias is severe.** Self-reported win rate is ~80% vs ~64% raw win rate in the broader cohort — winners are more motivated to respond. Use the labels but NOT the rate as a population estimate. (Symptom table: [gotchas.md](gotchas.md).)

See `analytics/projects/win_outcomes_scout/INVENTORY.md` Source 3.5 for the verified reconciliation table.

## Self-reported success signal (PMF / satisfaction)

A parallel "did the product work?" measure that lives next to electoral outcomes rather than inside them. The governed KR2 definition is in [canonical_metrics.md](canonical_metrics.md): **40% of ICP activated users say they would be very disappointed if they could no longer use Win**. Point-in-time readings go stale quickly (the 2026-05-28 read was 52% on n=50 ICP respondents); re-query with the recipe below before quoting a number.

**Source table:** `goodparty_data_catalog.dbt.stg_airbyte_source__hubspot_api_feedback_submissions`.

Filter by `survey_name`:
- `LIKE 'Win PMF%'` — Sean Ellis 4-option survey (started 2026-04-14).
- `LIKE 'Win User satisfaction%'` — CSAT/stars survey (n=12, sparse).

**Response columns:**
- `pmf_response` — the 4-option answer. **Option 1 = Very disappointed, Option 2 = Somewhat disappointed**; "Not disappointed" and "N/A - I no longer use" are explicit. HubSpot drops the original labels for Option 1/2 in the export; mapping verified by inspecting `pmf_additional_feedback` (Option 1 respondents express strong attachment). See [gotchas.md](gotchas.md).
- `pmf_additional_feedback` — free-text follow-up.
- `satisfaction_stars` (1–5 int) and `satisfaction_rating` (string) on the CSAT survey.

**Joining to Win users:** 2-hop via candidate. Recipe in [joins.md](joins.md).

**Selection bias:** Survey-response self-selected. Like the Did-You-Win modal, the cohort overrepresents engaged / satisfied users. Use the labels as a signal, but read rates as directional rather than population estimates until volume grows.

**Reading the KR (KR-aligned recipe):**

```sql
WITH pmf AS (
  SELECT submission_id, CAST(hs_contact_id AS BIGINT) AS hs_contact_id, pmf_response
  FROM goodparty_data_catalog.dbt.stg_airbyte_source__hubspot_api_feedback_submissions
  WHERE survey_name LIKE 'Win PMF%' AND pmf_response IS NOT NULL
),
attributed AS (
  SELECT
    p.submission_id, p.pmf_response,
    MAX(CASE WHEN u.icp_office_win THEN 1 ELSE 0 END) AS any_icp_win
  FROM pmf p
  LEFT JOIN goodparty_data_catalog.mart_civics.candidate c
    ON p.hs_contact_id = c.hubspot_contact_id
  LEFT JOIN goodparty_data_catalog.mart_analytics.users_win_candidacy u
    ON c.prod_db_user_id = u.user_id AND u.is_latest_version AND NOT u.is_demo
  GROUP BY 1, 2
)
SELECT
  COUNT(*) AS icp_respondents,
  SUM(CASE WHEN pmf_response = 'Option 1' THEN 1 ELSE 0 END) AS very_disappointed,
  ROUND(100.0 * SUM(CASE WHEN pmf_response = 'Option 1' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_very_disappointed
FROM attributed WHERE any_icp_win = 1;
```

See `analytics/projects/win_outcomes_scout/INVENTORY.md` Source 7 for the full 7-facet inventory and the cross-bucket response table.

## Cross-references

- [canonical_metrics.md](canonical_metrics.md) — governed definitions of the preferred outcome and KR2.
- [joins.md](joins.md) — the 2-hop survey join and the campaign→candidacy join.
- [gotchas.md](gotchas.md) — `latest_stage_result` reads a primary win as Won, `votes_received` STRING trap, self-report bias, Option 1/2 labels.
