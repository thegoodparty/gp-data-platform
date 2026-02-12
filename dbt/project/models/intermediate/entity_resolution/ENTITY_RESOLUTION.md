# Entity Resolution: Techspeed <> BallotReady Candidacy Matching

## Problem

Techspeed and BallotReady are two external vendor sources of candidacy data that flow into the civics mart through separate, independent pipelines. A candidate appearing in both systems gets two separate `gp_candidacy_id` values. This creates duplicates in the mart and prevents us from enriching records with data from both sources.

## Approach

We're implementing a three-pass waterfall matching pipeline that links records across the two sources with decreasing levels of certainty:

```
Pass 1: Deterministic (SQL)    — Exact/near-exact field matches
Pass 2: Probabilistic (Splink) — Statistical record linkage
Pass 3: AI Embeddings (Gemini) — Semantic similarity matching
```

Each pass only operates on records not yet matched by prior passes, so there's no double-counting. The final output is a single match table that maps source records to canonical entity UUIDs.

## Architecture

```
int__techspeed_candidates_clean ─────────────────┐
                                                  ├──> int__er_match_features_union
stg__ballotready_s3_candidacies_v3 ──────────────┘           │
                                                   ┌─────────┼─────────┐
                                                   │         │         │
                                              Pass 1    Pass 2    Pass 3
                                           deterministic  splink  ai_embed
                                                   │         │         │
                                                   └─────────┼─────────┘
                                                             │
                                                    match__candidacies
```

### Models

| Model | Type | Purpose |
|-------|------|---------|
| `int__er_match_features_union` | SQL | Unions both sources into a single table with canonicalized fields (names, state, office, dates, contact info). All normalization happens here. |
| `int__er_deterministic_match` | SQL | Six cascading deterministic rules with 1:1 enforcement. |
| `int__er_splink_match` | Python | Fellegi-Sunter probabilistic matching using Splink with Jaro-Winkler and Levenshtein comparisons. |
| `int__er_embedding_store` | Python (incremental) | Persistent Gemini `text-embedding-004` embeddings. Only generates embeddings for new records. |
| `int__er_ai_match` | Python | Cosine similarity matching on embeddings, blocked by state + election year. |
| `match__candidacies` | SQL | Final output. Consolidates all passes, enforces global 1:1, and unpivots into an extensible schema. |

### Output Schema (`match__candidacies`)

| Column | Description |
|--------|-------------|
| `candidacy_uuid` | Canonical entity ID. All rows sharing a UUID represent the same real-world candidacy. |
| `source_system` | Which vendor the record came from (`techspeed`, `ballotready`). |
| `source_identifier` | The primary key from that source system. |
| `decided_by` | Which pass made the link (`deterministic`, `splink`, `ai_embedding`). |

This schema is **N-source extensible** — a matched pair produces two rows (one per source). Adding a third vendor (e.g. DDHQ) means adding rows, not columns.

## Deterministic Match Rules (Pass 1)

Ordered by confidence, each rule excludes records already matched by higher-priority rules:

| # | Rule | Logic |
|---|------|-------|
| 1 | BR race_id cross-ref | Techspeed's `ballotready_race_id` matches BR's `race_id`, plus first + last name match |
| 2 | Candidate code | Same `generate_candidate_code` macro output (name + state + office_type + city) |
| 3 | Name + State + Date | Exact match on first name, last name, state, and election date |
| 4 | Email + State | Exact match on email + state (email not null) |
| 5 | Phone + State | Exact match on 10-digit phone + state |
| 6 | Name + State + Office + Near-date | First + last + state + office_type, election dates within 180 days |

## Current Status (Dev Testing — Ohio Only)

Tested with Ohio-only data to validate the pipeline on a manageable subset:

| Metric | Value |
|--------|-------|
| Total records in union (OH) | 19,597 (8,271 TS + 11,326 BR) |
| Deterministic matches | 5,091 (61.5% of TS records) |
| Splink matches (additional) | 81 |
| AI matches | Pending (needs Gemini API key) |
| Tests passing | 13/13 (union + deterministic) |

### Blocker: Gemini API Key

The embedding store (`int__er_embedding_store`) requires a Gemini API key in Databricks secrets:

```bash
databricks secrets put-secret dbt-secrets-dev gemini-api-key --string-value "YOUR_KEY"
databricks secrets put-secret dbt-secrets-prod gemini-api-key --string-value "YOUR_KEY"
```

Passes 1 and 2 work without it. Pass 3 (AI embeddings) is blocked until the key is added.

## Plan From Here

### Immediate Next Steps

1. **Add Gemini API key** to Databricks secrets (`dbt-secrets-dev` and `dbt-secrets-prod`).
2. **Build and validate Pass 3** (embedding store + AI match) on the Ohio subset.
3. **Build and validate `match__candidacies`** — the final output table.
4. **Remove dev filters** (Ohio-only) and run full-scale on all states.
5. **Validate full-scale results** — check match rates, spot-check edge cases in each pass.

### Civics Mart Integration

Once `match__candidacies` is validated, integrate it into the civics mart so matched candidacies from Techspeed and BallotReady share a single `gp_candidacy_id`:

- Update `int__civics_candidacy_*` models to consume the match table.
- Use the `candidacy_uuid` as the canonical ID, or use the match table as a lookup to coalesce fields from both sources into the richest possible candidacy record.

### Future Enhancements

- **LLM-based pairwise confirmation**: For AI embedding matches in the 0.85–0.90 range (below the current 0.90 threshold), use Gemini Flash to do pairwise yes/no confirmation. This would safely lower the threshold and recover more matches.
- **Additional sources**: The `match__candidacies` schema is designed for N sources. Adding a third vendor (e.g. DDHQ) means adding a new matching pipeline and appending rows — no schema changes needed.
- **Match quality monitoring**: Add dbt tests or a dashboard tracking match rates by rule, state, and office type over time to catch data drift.

## File Inventory

```
macros/variable_standardization/
    normalize_state_to_abbr.sql              # New macro: state name → 2-letter abbreviation

models/intermediate/entity_resolution/
    int__er_match_features_union.sql         # Step 1: Canonicalized union
    int__er_deterministic_match.sql          # Step 2: Deterministic rules
    int__er_splink_match.py                  # Step 3: Probabilistic (Splink)
    int__er_embedding_store.py               # Step 4: Gemini embeddings (incremental)
    int__er_ai_match.py                      # Step 5: Vector similarity
    match__candidacies.sql                   # Step 6: Final output
    int__er.yaml                             # Tests for all models
    ENTITY_RESOLUTION.md                     # This document

dbt_project.yml                              # Updated: entity_resolution model group
```

## Key Design Decisions

- **BallotReady source scope**: We use the full staging model (`stg_airbyte_source__ballotready_s3_candidacies_v3`) rather than the filtered `int__ballotready_clean_candidacies`, which excludes major-party candidates and records without phone/email. Entity resolution needs maximum coverage.
- **Normalization in one place**: All field cleaning (names, state, office, dates, phone, email) happens in `int__er_match_features_union`. Downstream models compare pre-cleaned values.
- **1:1 enforcement at every level**: Each pass and the final consolidation enforce that each source record maps to at most one entity. Priority: deterministic > splink > ai_embedding.
- **Incremental embeddings**: The embedding store only generates embeddings for new records on each run, avoiding redundant Gemini API calls.
- **Runtime pip install**: Splink and google-generativeai are installed at runtime on the Databricks cluster since they aren't pre-installed. This follows a `try/except ImportError` pattern.
